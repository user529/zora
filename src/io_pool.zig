/// io_pool.zig — blocking I/O thread pool
///
/// Executes HTTP requests and subprocesses on behalf of worker coroutines.
/// Each pool thread owns a std.http.Client; jobs are received from a bounded
/// MPSC queue; results are pushed to per-worker IoResult queues.
///
/// Ownership:
///   IoJob payload strings  — NOT owned by pool (caller guarantees lifetime)
///   IoResult outcome strings — owned by pool, freed by caller via freeIoResult
const std = @import("std");
const queue_mod = @import("queue.zig");
const metrics_mod = @import("metrics.zig");

const log = std.log.scoped(.io_pool);

// ---------------------------------------------------------------------------
// IoJob — one blocking I/O task submitted to the pool
// ---------------------------------------------------------------------------

pub const IoJob = struct {
    worker_id:   u8,
    coro_id:     u32,
    /// Absolute wall-clock deadline (std.time.milliTimestamp() units).
    /// Pass -1 to use the pool's configured IO_JOB_TIMEOUT_MS from the start
    /// of execution instead of a fixed wall-clock target.
    deadline_ms: i64,
    payload: union(enum) {
        http_request: struct {
            method:  []const u8,
            url:     []const u8,
            /// Accepted from callers but not yet forwarded to the request.
            headers: []const u8,
            body:    []const u8,
        },
        exec:  struct { argv: []const []const u8 },
        shell: struct { command: []const u8 },
    },
};

// ---------------------------------------------------------------------------
// IoResult — outcome pushed to the per-worker result queue
// ---------------------------------------------------------------------------

pub const IoOutcome = union(enum) {
    http: struct { status: u16, body: []const u8 },      // body owned
    proc: struct { exit_code: i32, stdout: []const u8 }, // stdout owned
    send: struct { message_id: i64 },                    // not produced by io_pool
    err:  []const u8,                                    // owned
};

pub const IoResult = struct {
    coro_id: u32,
    outcome: IoOutcome,
};

pub fn freeIoResult(result: IoResult, allocator: std.mem.Allocator) void {
    switch (result.outcome) {
        .http => |h| allocator.free(h.body),
        .proc => |p| allocator.free(p.stdout),
        .send => {},
        .err  => |e| allocator.free(e),
    }
}

pub fn freeIoOutcome(outcome: IoOutcome, allocator: std.mem.Allocator) void {
    freeIoResult(.{ .coro_id = 0, .outcome = outcome }, allocator);
}

// ---------------------------------------------------------------------------
// TimerCtx — kills a child process after timeout_ms unless done first
// ---------------------------------------------------------------------------

const TimerCtx = struct {
    pid:        std.posix.pid_t,
    timeout_ms: u64,
    done:       *std.atomic.Value(bool),
};

fn timerThread(ctx: TimerCtx) void {
    const poll_ms: u64 = 10;
    var waited_ms: u64 = 0;
    while (!ctx.done.load(.acquire) and waited_ms < ctx.timeout_ms) {
        std.Thread.sleep(poll_ms * std.time.ns_per_ms);
        waited_ms += poll_ms;
    }
    if (!ctx.done.load(.acquire)) {
        std.posix.kill(ctx.pid, std.posix.SIG.KILL) catch {};
    }
}

// ---------------------------------------------------------------------------
// IoPool
// ---------------------------------------------------------------------------

pub const IoPoolConfig = struct {
    thread_count:    u8,
    queue_capacity:  u16,
    timeout_ms:      u64,
    proc_max_output: usize,
    metrics:         ?*metrics_mod.Metrics = null,
};

pub const IoPool = struct {
    allocator:       std.mem.Allocator,
    job_queue:       queue_mod.Queue(IoJob),
    result_queues:   []const *queue_mod.Queue(IoResult),
    threads:         []std.Thread,
    stop:            std.atomic.Value(bool),
    timeout_ms:      u64,
    proc_max_output: usize,
    metrics:         ?*metrics_mod.Metrics,
    /// Per-thread current child PID (0 = idle). Set before child.spawn(),
    /// cleared (to 0) after child.wait(). Allows deinit() to SIGKILL in-flight children.
    current_pids:    []std.atomic.Value(std.posix.pid_t),

    /// Initialise the pool in-place. Must be called on a variable at its final
    /// address — threads are spawned here and receive `self` as a pointer.
    pub fn init(
        self:          *IoPool,
        allocator:     std.mem.Allocator,
        config:        IoPoolConfig,
        result_queues: []const *queue_mod.Queue(IoResult),
    ) !void {
        self.allocator       = allocator;
        self.result_queues   = result_queues;
        self.timeout_ms      = config.timeout_ms;
        self.proc_max_output = config.proc_max_output;
        self.metrics         = config.metrics;
        self.stop            = std.atomic.Value(bool).init(false);

        self.job_queue = try queue_mod.Queue(IoJob).init(allocator, config.queue_capacity);
        errdefer self.job_queue.deinit(allocator);
        self.job_queue.kind = .io_job;

        self.threads = try allocator.alloc(std.Thread, config.thread_count);
        errdefer allocator.free(self.threads);

        self.current_pids = try allocator.alloc(
            std.atomic.Value(std.posix.pid_t), config.thread_count);
        errdefer allocator.free(self.current_pids);
        for (self.current_pids) |*p| p.* = std.atomic.Value(std.posix.pid_t).init(0);

        for (self.threads, 0..) |*t, i| {
            t.* = std.Thread.spawn(.{}, workerThread, .{ self, @as(u8, @intCast(i)) }) catch |err| {
                self.stop.store(true, .release);
                for (self.threads[0..i]) |started| started.join();
                return err;
            };
        }
    }

    pub fn deinit(self: *IoPool) void {
        self.stop.store(true, .release);
        // SIGKILL any in-flight child processes so blocked child.wait() returns.
        for (self.current_pids) |*pv| {
            const pid = pv.load(.acquire);
            if (pid != 0) std.posix.kill(pid, std.posix.SIG.KILL) catch {};
        }
        for (self.threads) |t| t.join();
        self.job_queue.deinit(self.allocator);
        self.allocator.free(self.threads);
        self.allocator.free(self.current_pids);
    }

    /// Non-blocking submit. Returns error.QueueFull if the queue is at capacity.
    pub fn submit(self: *IoPool, job: IoJob) error{QueueFull}!void {
        try self.job_queue.push(job);
        if (self.metrics) |m| {
            _ = m.io_jobs_total.fetchAdd(1, .monotonic);
            _ = m.io_jobs_inflight.fetchAdd(1, .monotonic);
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn pushResult(self: *IoPool, job: IoJob, outcome: IoOutcome) void {
        if (self.metrics) |m| _ = m.io_jobs_inflight.fetchAdd(-1, .monotonic);
        const result = IoResult{ .coro_id = job.coro_id, .outcome = outcome };
        if (job.worker_id < self.result_queues.len) {
            self.result_queues[job.worker_id].push(result) catch {
                log.warn("result queue full for worker {d}; dropping coro={d}",
                    .{ job.worker_id, job.coro_id });
                freeIoResult(result, self.allocator);
            };
        } else {
            log.err("invalid worker_id {d} in job coro={d}", .{ job.worker_id, job.coro_id });
            freeIoResult(result, self.allocator);
        }
    }

    /// Push an error result. Drops silently on OOM (the coroutine times out via deadline).
    fn pushErr(self: *IoPool, job: IoJob, msg: []const u8) void {
        if (self.metrics) |m| {
            if (std.mem.eql(u8, msg, "timeout")) {
                _ = m.io_timeouts_total.fetchAdd(1, .monotonic);
            } else {
                _ = m.io_errors_total.fetchAdd(1, .monotonic);
            }
        }
        const owned = self.allocator.dupe(u8, msg) catch {
            // OOM: pushResult won't be called, so decrement inflight here.
            if (self.metrics) |m| _ = m.io_jobs_inflight.fetchAdd(-1, .monotonic);
            return;
        };
        self.pushResult(job, .{ .err = owned });
    }

    fn workerThread(pool: *IoPool, thread_idx: u8) void {
        var http_client = std.http.Client{ .allocator = pool.allocator };
        defer http_client.deinit();

        while (!pool.stop.load(.acquire)) {
            const job = pool.job_queue.popTimeout(10 * std.time.ns_per_ms) orelse continue;
            pool.executeJob(&http_client, thread_idx, job);
        }
        // Drain remaining queued jobs before exiting.
        while (pool.job_queue.popTimeout(0)) |job| {
            pool.executeJob(&http_client, thread_idx, job);
        }
    }

    fn executeJob(pool: *IoPool, http_client: *std.http.Client, thread_idx: u8, job: IoJob) void {
        switch (job.payload) {
            .http_request => pool.executeHttp(http_client, job),
            .exec  => |e| pool.executeProc(thread_idx, job, e.argv),
            .shell => |s| pool.executeProc(thread_idx, job,
                &[_][]const u8{ "/bin/sh", "-c", s.command }),
        }
    }

    fn executeHttp(pool: *IoPool, client: *std.http.Client, job: IoJob) void {
        const req = job.payload.http_request;
        const alloc = pool.allocator;

        var al_writer = std.Io.Writer.Allocating.initCapacity(alloc, 4096) catch {
            pool.pushErr(job, "OOM");
            return;
        };
        defer al_writer.deinit();

        const method = std.meta.stringToEnum(std.http.Method, req.method) orelse {
            pool.pushErr(job, "unsupported_http_method");
            return;
        };

        const fetch_result = client.fetch(.{
            .method          = method,
            .location        = .{ .url = req.url },
            .payload         = if (req.body.len > 0) req.body else null,
            .keep_alive      = true,
            .response_writer = &al_writer.writer,
        }) catch |err| {
            pool.pushErr(job, @errorName(err));
            return;
        };

        const body = al_writer.toOwnedSlice() catch {
            pool.pushErr(job, "OOM");
            return;
        };

        pool.pushResult(job, .{ .http = .{
            .status = @intFromEnum(fetch_result.status),
            .body   = body,
        }});
    }

    fn executeProc(pool: *IoPool, thread_idx: u8, job: IoJob, argv: []const []const u8) void {
        const alloc = pool.allocator;

        var child = std.process.Child.init(argv, alloc);
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Close;

        child.spawn() catch |err| {
            pool.pushErr(job, @errorName(err));
            return;
        };

        // Publish PID so deinit() can SIGKILL if the pool shuts down mid-run.
        pool.current_pids[thread_idx].store(child.id, .release);
        defer pool.current_pids[thread_idx].store(0, .release);

        // Start timeout timer.
        var timer_done = std.atomic.Value(bool).init(false);
        const timer = std.Thread.spawn(.{}, timerThread, .{TimerCtx{
            .pid        = child.id,
            .timeout_ms = pool.timeout_ms,
            .done       = &timer_done,
        }}) catch |err| {
            log.warn("failed to spawn timer for coro={d}: {s}", .{ job.coro_id, @errorName(err) });
            _ = child.wait() catch {};
            pool.pushErr(job, "timer_spawn_failed");
            return;
        };
        defer {
            timer_done.store(true, .release);
            timer.join();
        }

        // Collect stdout up to proc_max_output bytes.
        var stdout_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer stdout_buf.deinit(alloc);
        var truncated = false;

        if (child.stdout) |pipe| {
            var buf: [4096]u8 = undefined;
            read_loop: while (true) {
                const n = pipe.read(&buf) catch break;
                if (n == 0) break;
                const space = pool.proc_max_output -| stdout_buf.items.len;
                if (n <= space) {
                    stdout_buf.appendSlice(alloc, buf[0..n]) catch break :read_loop;
                } else {
                    stdout_buf.appendSlice(alloc, buf[0..space]) catch break :read_loop;
                    truncated = true;
                    break;
                }
            }
        }

        const term = child.wait() catch |err| {
            pool.pushErr(job, @errorName(err));
            return;
        };

        // SIGKILL means either timeout (timer) or deinit() killed it.
        switch (term) {
            .Signal => |sig| if (sig == std.posix.SIG.KILL) {
                pool.pushErr(job, "timeout");
                return;
            },
            else => {},
        }

        if (truncated) stdout_buf.appendSlice(alloc, "\n[output truncated]") catch {};

        const stdout = stdout_buf.toOwnedSlice(alloc) catch {
            pool.pushErr(job, "OOM");
            return;
        };

        const exit_code: i32 = switch (term) {
            .Exited => |code| @as(i32, code),
            .Signal => |sig|  -@as(i32, @intCast(sig)),
            else    => -1,
        };

        pool.pushResult(job, .{ .proc = .{ .exit_code = exit_code, .stdout = stdout } });
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "IoJob instantiates all payload variants" {
    const j_http = IoJob{
        .worker_id = 0, .coro_id = 1, .deadline_ms = -1,
        .payload = .{ .http_request = .{
            .method = "GET", .url = "http://x", .headers = "", .body = "",
        }},
    };
    try testing.expectEqual(@as(u32, 1), j_http.coro_id);

    const j_exec = IoJob{
        .worker_id = 0, .coro_id = 2, .deadline_ms = -1,
        .payload = .{ .exec = .{ .argv = &.{"/bin/true"} } },
    };
    try testing.expectEqual(@as(u32, 2), j_exec.coro_id);

    const j_shell = IoJob{
        .worker_id = 0, .coro_id = 3, .deadline_ms = -1,
        .payload = .{ .shell = .{ .command = "true" } },
    };
    try testing.expectEqual(@as(u32, 3), j_shell.coro_id);
}

test "freeIoResult releases all outcome variants — no leaks" {
    freeIoResult(.{ .coro_id = 0, .outcome = .{ .http = .{
        .status = 200,
        .body   = try testing.allocator.dupe(u8, "hello"),
    }}}, testing.allocator);

    freeIoResult(.{ .coro_id = 0, .outcome = .{ .proc = .{
        .exit_code = 0,
        .stdout    = try testing.allocator.dupe(u8, "world"),
    }}}, testing.allocator);

    freeIoResult(.{ .coro_id = 0, .outcome = .{
        .send = .{ .message_id = 42 },
    }}, testing.allocator);

    freeIoResult(.{ .coro_id = 0, .outcome = .{
        .err = try testing.allocator.dupe(u8, "oops"),
    }}, testing.allocator);
}

// ---------------------------------------------------------------------------
// HTTP stub helpers
// ---------------------------------------------------------------------------

/// Spawn a one-shot TCP stub: accepts one connection, reads the request (discards it),
/// writes `response`, then closes. Returns the bound port and thread handle.
const StubServer = struct { port: u16, thread: std.Thread };

fn spawnStub(response: []const u8) !StubServer {
    const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
    const srv_ptr = try testing.allocator.create(std.net.Server);
    srv_ptr.* = try addr.listen(.{ .reuse_address = true });
    const port = std.mem.bigToNative(u16, srv_ptr.listen_address.in.sa.port);
    const thread = try std.Thread.spawn(.{}, stubOnce, .{ srv_ptr, response });
    return .{ .port = port, .thread = thread };
}

fn stubOnce(srv_ptr: *std.net.Server, response: []const u8) void {
    defer {
        srv_ptr.deinit();
        testing.allocator.destroy(srv_ptr);
    }
    const conn = srv_ptr.accept() catch return;
    defer conn.stream.close();
    var buf: [8192]u8 = undefined;
    _ = conn.stream.read(&buf) catch {};
    conn.stream.writeAll(response) catch {};
}

test "http_request GET to local stub → status 200, body matches" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, 8);
    defer rq.deinit(testing.allocator);

    const stub = try spawnStub(
        "HTTP/1.1 200 OK\r\n" ++
        "Content-Length: 5\r\n" ++
        "Connection: close\r\n" ++
        "\r\nhello");
    defer stub.thread.join();

    const url = try std.fmt.allocPrint(
        testing.allocator, "http://127.0.0.2:{d}/", .{stub.port});
    defer testing.allocator.free(url);

    var pool: IoPool = undefined;
    try pool.init(testing.allocator,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 5_000, .proc_max_output = 65_536 },
        &.{&rq});
    defer pool.deinit();

    try pool.submit(.{
        .worker_id = 0, .coro_id = 42, .deadline_ms = -1,
        .payload = .{ .http_request = .{
            .method = "GET", .url = url, .headers = "", .body = "",
        }},
    });

    const result = rq.popTimeout(3 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(result, testing.allocator);

    try testing.expectEqual(@as(u32, 42), result.coro_id);
    try testing.expect(result.outcome == .http);
    try testing.expectEqual(@as(u16, 200), result.outcome.http.status);
    try testing.expectEqualStrings("hello", result.outcome.http.body);
}

test "http_request to unreachable address → IoResult.err, thread survives" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, 8);
    defer rq.deinit(testing.allocator);

    var pool: IoPool = undefined;
    try pool.init(testing.allocator,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 5_000, .proc_max_output = 65_536 },
        &.{&rq});
    defer pool.deinit();

    // Port 1 is almost certainly not listening — ECONNREFUSED expected.
    try pool.submit(.{
        .worker_id = 0, .coro_id = 1, .deadline_ms = -1,
        .payload = .{ .http_request = .{
            .method = "GET", .url = "http://127.0.0.1:1/",
            .headers = "", .body = "",
        }},
    });

    const r1 = rq.popTimeout(5 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(r1, testing.allocator);
    try testing.expect(r1.outcome == .err);

    // Confirm the thread survived by processing a second job.
    try pool.submit(.{
        .worker_id = 0, .coro_id = 2, .deadline_ms = -1,
        .payload = .{ .http_request = .{
            .method = "GET", .url = "http://127.0.0.1:1/",
            .headers = "", .body = "",
        }},
    });
    const r2 = rq.popTimeout(5 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(r2, testing.allocator);
    try testing.expect(r2.outcome == .err);
}

test "exec and shell run a command and capture output" {
    // exec: the argv form runs /bin/echo and returns its stdout.
    {
        var rq = try queue_mod.Queue(IoResult).init(testing.allocator, 8);
        defer rq.deinit(testing.allocator);
        var pool: IoPool = undefined;
        try pool.init(testing.allocator,
            .{ .thread_count = 1, .queue_capacity = 8,
               .timeout_ms = 5_000, .proc_max_output = 65_536 },
            &.{&rq});
        defer pool.deinit();

        try pool.submit(.{ .worker_id = 0, .coro_id = 10, .deadline_ms = -1,
            .payload = .{ .exec = .{ .argv = &.{ "/bin/echo", "hello" } } } });

        const result = rq.popTimeout(5 * std.time.ns_per_s) orelse return error.TestTimeout;
        defer freeIoResult(result, testing.allocator);
        try testing.expectEqual(@as(u32, 10), result.coro_id);
        try testing.expect(result.outcome == .proc);
        try testing.expectEqual(@as(i32, 0), result.outcome.proc.exit_code);
        try testing.expectEqualStrings("hello\n", result.outcome.proc.stdout);
    }
    // shell: the command runs under a shell, so $((...)) is evaluated.
    {
        var rq = try queue_mod.Queue(IoResult).init(testing.allocator, 8);
        defer rq.deinit(testing.allocator);
        var pool: IoPool = undefined;
        try pool.init(testing.allocator,
            .{ .thread_count = 1, .queue_capacity = 8,
               .timeout_ms = 5_000, .proc_max_output = 65_536 },
            &.{&rq});
        defer pool.deinit();

        try pool.submit(.{ .worker_id = 0, .coro_id = 30, .deadline_ms = -1,
            .payload = .{ .shell = .{ .command = "echo $((1+1))" } } });

        const result = rq.popTimeout(5 * std.time.ns_per_s) orelse return error.TestTimeout;
        defer freeIoResult(result, testing.allocator);
        try testing.expectEqual(@as(u32, 30), result.coro_id);
        try testing.expect(result.outcome == .proc);
        try testing.expectEqual(@as(i32, 0), result.outcome.proc.exit_code);
        try testing.expectEqualStrings("2\n", result.outcome.proc.stdout);
    }
}

test "command exceeding IO_JOB_TIMEOUT_MS → IoResult.err, child killed" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, 8);
    defer rq.deinit(testing.allocator);
    // Short timeout so the test completes in ~200 ms.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 150, .proc_max_output = 65_536 },
        &.{&rq});
    defer pool.deinit();

    try pool.submit(.{ .worker_id = 0, .coro_id = 20, .deadline_ms = -1,
        .payload = .{ .exec = .{ .argv = &.{ "/bin/sleep", "60" } } } });

    const result = rq.popTimeout(1 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(result, testing.allocator);
    // Returning without hanging confirms the child was reaped (no zombie).
    try testing.expect(result.outcome == .err);
}

test "stdout exceeding PROC_MAX_OUTPUT → truncated and marked" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, 8);
    defer rq.deinit(testing.allocator);
    // proc_max_output = 10 bytes; command produces 22 bytes.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 5_000, .proc_max_output = 10 },
        &.{&rq});
    defer pool.deinit();

    try pool.submit(.{ .worker_id = 0, .coro_id = 31, .deadline_ms = -1,
        .payload = .{ .shell = .{ .command = "printf 'AAAAAAAAAAAAAAAAAAAAAA'" } } });

    const result = rq.popTimeout(5 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(result, testing.allocator);

    try testing.expect(result.outcome == .proc);
    const out = result.outcome.proc.stdout;
    try testing.expect(out.len <= 10 + "\n[output truncated]".len);
    try testing.expect(std.mem.endsWith(u8, out, "[output truncated]"));
}

test "io_pool metrics — jobs_total, inflight, errors, timeouts" {
    var m = metrics_mod.Metrics{};

    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, 16);
    defer rq.deinit(testing.allocator);

    // Short timeout (150 ms) so the timeout job completes quickly.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator,
        .{ .thread_count = 2, .queue_capacity = 8, .timeout_ms = 150,
           .proc_max_output = 65_536, .metrics = &m },
        &.{&rq});
    defer pool.deinit();

    // Job 1: success (exec /bin/echo)
    try pool.submit(.{ .worker_id = 0, .coro_id = 1, .deadline_ms = -1,
        .payload = .{ .exec = .{ .argv = &.{ "/bin/echo", "ok" } } } });
    const rs = rq.popTimeout(3 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(rs, testing.allocator);
    try testing.expect(rs.outcome == .proc);

    // Job 2: error (HTTP to port 1 — ECONNREFUSED)
    try pool.submit(.{ .worker_id = 0, .coro_id = 2, .deadline_ms = -1,
        .payload = .{ .http_request = .{
            .method = "GET", .url = "http://127.0.0.1:1/", .headers = "", .body = "",
        }}});
    const re = rq.popTimeout(5 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(re, testing.allocator);
    try testing.expect(re.outcome == .err);

    // Job 3: timeout (exec /bin/sleep 60 with pool timeout_ms = 150)
    try pool.submit(.{ .worker_id = 0, .coro_id = 3, .deadline_ms = -1,
        .payload = .{ .exec = .{ .argv = &.{ "/bin/sleep", "60" } } } });
    const rt = rq.popTimeout(1 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(rt, testing.allocator);
    try testing.expect(rt.outcome == .err);

    try testing.expectEqual(@as(u64, 3), m.io_jobs_total.load(.monotonic));
    try testing.expectEqual(@as(i64, 0), m.io_jobs_inflight.load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.io_errors_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.io_timeouts_total.load(.monotonic));
}

test "submit at capacity returns QueueFull without inflating metrics" {
    var m = metrics_mod.Metrics{};

    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, 4);
    defer rq.deinit(testing.allocator);

    // thread_count = 0: no consumers, queue fills immediately.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator,
        .{ .thread_count = 0, .queue_capacity = 1,
           .timeout_ms = 1_000, .proc_max_output = 65_536, .metrics = &m },
        &.{&rq});
    defer pool.deinit();

    const job = IoJob{
        .worker_id = 0, .coro_id = 1, .deadline_ms = -1,
        .payload = .{ .shell = .{ .command = "true" } },
    };
    // First submit succeeds.
    try pool.submit(job);
    // Second submit fails with QueueFull.
    try testing.expectError(error.QueueFull, pool.submit(job));

    // Only 1 job was actually queued — counters must reflect that.
    try testing.expectEqual(@as(u64, 1), m.io_jobs_total.load(.monotonic));
    try testing.expectEqual(@as(i64, 1), m.io_jobs_inflight.load(.monotonic));
}

test "graceful stop — joins threads and kills in-flight children" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, 8);
    defer rq.deinit(testing.allocator);

    // Long timeout so the pool's timer doesn't fire before deinit does.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 60_000, .proc_max_output = 65_536 },
        &.{&rq});

    try pool.submit(.{ .worker_id = 0, .coro_id = 40, .deadline_ms = -1,
        .payload = .{ .exec = .{ .argv = &.{ "/bin/sleep", "60" } } } });

    // Give the pool thread time to spawn the child.
    std.Thread.sleep(80 * std.time.ns_per_ms);

    // deinit must return quickly — it SIGKILLs the child, child.wait() returns.
    const t0 = std.time.milliTimestamp();
    pool.deinit();
    const elapsed_ms = std.time.milliTimestamp() - t0;

    try testing.expect(elapsed_ms < 1_000);

    // Drain results pushed by the killed worker (e.g. "timeout" err) to prevent leak.
    while (rq.popTimeout(0)) |r| freeIoResult(r, testing.allocator);
}
