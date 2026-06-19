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
const types = @import("types.zig");
const rt = @import("rt.zig");

const log = std.log.scoped(.io_pool);

// ---------------------------------------------------------------------------
// IoJob — one blocking I/O task submitted to the pool
// ---------------------------------------------------------------------------

pub const IoJob = struct {
    worker_id:   u8,
    coro_id:     u32,
    payload: union(enum) {
        http_request: struct {
            method:  []const u8,
            url:     []const u8,
            /// Forwarded to the request as std.http extra_headers.
            /// Owned by the submitting coroutine's OwnedStrings; not freed here.
            headers: []const std.http.Header,
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
    http: struct { status: u16, body: []const u8, headers: []const std.http.Header }, // body + headers owned
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
        .http => |h| {
            for (h.headers) |hdr| {
                allocator.free(hdr.name);
                allocator.free(hdr.value);
            }
            allocator.free(h.headers);
            allocator.free(h.body);
        },
        .proc => |p| allocator.free(p.stdout),
        .send => {},
        .err  => |e| allocator.free(e),
    }
}

/// Free every duped name and value in a header list, then the list's backing
/// memory. Used on error paths in executeHttp before the list is handed off.
fn freeHeaderListItems(allocator: std.mem.Allocator, list: *std.ArrayListUnmanaged(std.http.Header)) void {
    for (list.items) |hdr| {
        allocator.free(hdr.name);
        allocator.free(hdr.value);
    }
    list.deinit(allocator);
}

// ---------------------------------------------------------------------------
// TimerCtx — kills a child process after timeout_ms unless done first
// ---------------------------------------------------------------------------

const TimerCtx = struct {
    pid:        std.posix.pid_t,
    timeout_ms: u64,
    io:         std.Io,
    done:       *std.atomic.Value(bool),
};

fn timerThread(ctx: TimerCtx) void {
    const poll_ms: u64 = 10;
    var waited_ms: u64 = 0;
    while (!ctx.done.load(.acquire) and waited_ms < ctx.timeout_ms) {
        rt.sleepNs(ctx.io, poll_ms * std.time.ns_per_ms);
        waited_ms += poll_ms;
    }
    if (!ctx.done.load(.acquire)) {
        std.posix.kill(ctx.pid, std.posix.SIG.KILL) catch {};
    }
}

// ---------------------------------------------------------------------------
// HttpWatchdogCtx — shuts a stalled HTTP connection down after timeout_ms
// ---------------------------------------------------------------------------

const HttpWatchdogCtx = struct {
    stream:     std.Io.net.Stream,
    timeout_ms: u64,
    io:         std.Io,
    done:       *std.atomic.Value(bool),
    timed_out:  *std.atomic.Value(bool),
};

/// Shut the connection's socket down once timeout_ms elapses, unless the request
/// finished first (done). 0.16's blocking Io has no socket read timeout, so a
/// peer that accepts the connection but never replies would otherwise pin the
/// pool thread in receiveHead/streamRemaining forever. The shutdown turns that
/// stall into a read error, so executeHttp returns. timed_out is set before the
/// shutdown so executeHttp reports a timeout rather than a generic read error.
/// The HTTP counterpart of timerThread; it shuts a socket down instead of
/// killing a child.
fn httpWatchdog(ctx: HttpWatchdogCtx) void {
    const poll_ms: u64 = 10;
    var waited_ms: u64 = 0;
    while (!ctx.done.load(.acquire) and waited_ms < ctx.timeout_ms) {
        rt.sleepNs(ctx.io, poll_ms * std.time.ns_per_ms);
        waited_ms += poll_ms;
    }
    if (!ctx.done.load(.acquire)) {
        ctx.timed_out.store(true, .release);
        var s = ctx.stream;
        s.shutdown(ctx.io, .both) catch {};
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
    io:              std.Io,
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
        io:            std.Io,
        config:        IoPoolConfig,
        result_queues: []const *queue_mod.Queue(IoResult),
    ) !void {
        self.allocator       = allocator;
        self.io              = io;
        self.result_queues   = result_queues;
        self.timeout_ms      = config.timeout_ms;
        self.proc_max_output = config.proc_max_output;
        self.metrics         = config.metrics;
        self.stop            = std.atomic.Value(bool).init(false);

        self.job_queue = try queue_mod.Queue(IoJob).init(allocator, io, config.queue_capacity);
        errdefer self.job_queue.deinit(allocator);
        self.job_queue.kind = .io_job;

        self.threads = try allocator.alloc(std.Thread, config.thread_count);
        errdefer allocator.free(self.threads);

        self.current_pids = try allocator.alloc(
            std.atomic.Value(std.posix.pid_t), config.thread_count);
        errdefer allocator.free(self.current_pids);
        for (self.current_pids) |*p| p.* = std.atomic.Value(std.posix.pid_t).init(0);

        for (self.threads, 0..) |*t, i| {
            t.* = std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, workerThread, .{ self, @as(u8, @intCast(i)) }) catch |err| {
                self.stop.store(true, .release);
                // Wake the already-started threads — they park in popBlocking and
                // are released only by close(), not by the stop flag.
                self.job_queue.close();
                for (self.threads[0..i]) |started| started.join();
                return err;
            };
        }
    }

    pub fn deinit(self: *IoPool) void {
        self.stop.store(true, .release);
        // Wake parked pool threads so they drain and exit (replaces the old
        // 10 ms stop-poll).
        self.job_queue.close();
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
        var http_client = std.http.Client{ .allocator = pool.allocator, .io = pool.io };
        defer http_client.deinit();

        // Park indefinitely until a job arrives or the pool is closed at
        // shutdown — no 10 ms poll, so an idle pool thread consumes no CPU.
        // popBlocking drains the queue before reporting null, so jobs queued at
        // close are still run; no separate drain loop is needed.
        while (pool.job_queue.popBlocking()) |job| {
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

    /// Drive one HTTP request through the manual std.http flow so the response
    /// headers can be captured (client.fetch discards them). Mirrors the steps
    /// in std.http.Client.fetch: request -> send -> receiveHead (follows
    /// redirects) -> iterate headers -> readerDecompressing -> streamRemaining.
    ///
    /// Headers are duped before the body reader is initialised: reader() and
    /// readerDecompressing() call head.invalidateStrings(), which clobbers
    /// head.bytes and so invalidates every name/value slice.
    fn executeHttp(pool: *IoPool, client: *std.http.Client, job: IoJob) void {
        const req_job = job.payload.http_request;
        const alloc = pool.allocator;

        const method = std.meta.stringToEnum(std.http.Method, req_job.method) orelse {
            pool.pushErr(job, "unsupported_http_method");
            return;
        };

        const uri = std.Uri.parse(req_job.url) catch |err| {
            pool.pushErr(job, @errorName(err));
            return;
        };

        // A payload makes the request non-repeatable, so std.http cannot follow
        // redirects for it; match fetch's choice of redirect behaviour.
        const has_payload = req_job.body.len > 0;
        const redirect_behavior: std.http.Client.Request.RedirectBehavior =
            if (has_payload) .unhandled else @enumFromInt(3);

        var request = client.request(method, uri, .{
            .redirect_behavior = redirect_behavior,
            .keep_alive        = true,
            .extra_headers     = req_job.headers,
        }) catch |err| {
            pool.pushErr(job, @errorName(err));
            return;
        };
        defer request.deinit();

        // Bound the response wait. A peer that accepts the connection but never
        // replies would otherwise block this pool thread in receiveHead /
        // streamRemaining forever — 0.16's blocking Io has no socket read
        // timeout. A watchdog shuts the connection's socket down at timeout_ms,
        // turning the stall into a read error; this mirrors the child-process
        // timer in executeProc. Caveat: the connect and TLS handshake run inside
        // client.request above, before the socket is reachable here, so those
        // phases are bounded by the kernel connect timeout, not this watchdog.
        var wd_done = std.atomic.Value(bool).init(false);
        var wd_timed_out = std.atomic.Value(bool).init(false);
        const watchdog: ?std.Thread = std.Thread.spawn(
            .{ .stack_size = types.THREAD_STACK_SIZE },
            httpWatchdog,
            .{HttpWatchdogCtx{
                .stream     = request.connection.?.stream_reader.stream,
                .timeout_ms = pool.timeout_ms,
                .io         = pool.io,
                .done       = &wd_done,
                .timed_out  = &wd_timed_out,
            }},
        ) catch |err| blk: {
            log.warn("http watchdog spawn failed for coro={d}: {s} — request runs unbounded", .{ job.coro_id, @errorName(err) });
            break :blk null;
        };
        // Stops before request.deinit() (LIFO): the watchdog never touches a
        // socket the connection is tearing down.
        defer if (watchdog) |t| {
            wd_done.store(true, .release);
            t.join();
        };

        if (has_payload) {
            request.transfer_encoding = .{ .content_length = req_job.body.len };
            var body_writer = request.sendBodyUnflushed(&.{}) catch |err| {
                pool.pushErr(job, @errorName(err));
                return;
            };
            body_writer.writer.writeAll(req_job.body) catch |err| {
                pool.pushErr(job, @errorName(err));
                return;
            };
            body_writer.end() catch |err| {
                pool.pushErr(job, @errorName(err));
                return;
            };
            request.connection.?.flush() catch |err| {
                pool.pushErr(job, @errorName(err));
                return;
            };
        } else {
            request.sendBodiless() catch |err| {
                pool.pushErr(job, @errorName(err));
                return;
            };
        }

        // Redirect target storage; must outlive receiveHead. Empty when not
        // following redirects (payload case), matching fetch.
        var redirect_buf: [8 * 1024]u8 = undefined;
        const redirect_slice: []u8 = if (redirect_behavior == .unhandled) &.{} else &redirect_buf;

        var response = request.receiveHead(redirect_slice) catch |err| {
            pool.pushErr(job, if (wd_timed_out.load(.acquire)) "timeout" else @errorName(err));
            return;
        };

        const status: u16 = @intFromEnum(response.head.status);
        const content_encoding = response.head.content_encoding;

        // Capture headers now, before any reader invalidates head.bytes.
        // Case-insensitive de-dup, last value wins, original casing preserved.
        var hdr_list: std.ArrayListUnmanaged(std.http.Header) = .empty;
        var capture_failed = false;
        var it = response.head.iterateHeaders();
        capture: while (it.next()) |hdr| {
            // Replace an earlier same-name (case-insensitive) entry if present.
            for (hdr_list.items) |*existing| {
                if (std.ascii.eqlIgnoreCase(existing.name, hdr.name)) {
                    const new_name = alloc.dupe(u8, hdr.name) catch {
                        capture_failed = true;
                        break :capture;
                    };
                    const new_value = alloc.dupe(u8, hdr.value) catch {
                        alloc.free(new_name);
                        capture_failed = true;
                        break :capture;
                    };
                    alloc.free(existing.name);
                    alloc.free(existing.value);
                    existing.name = new_name;
                    existing.value = new_value;
                    continue :capture;
                }
            }
            const name = alloc.dupe(u8, hdr.name) catch {
                capture_failed = true;
                break :capture;
            };
            const value = alloc.dupe(u8, hdr.value) catch {
                alloc.free(name);
                capture_failed = true;
                break :capture;
            };
            hdr_list.append(alloc, .{ .name = name, .value = value }) catch {
                alloc.free(name);
                alloc.free(value);
                capture_failed = true;
                break :capture;
            };
        }
        if (capture_failed) {
            freeHeaderListItems(alloc, &hdr_list);
            pool.pushErr(job, "OOM");
            return;
        }

        // Decompression buffer sized per negotiated content-encoding, matching
        // std.http.Client.fetch. compress (LZW) is unsupported.
        const decompress_buffer: []u8 = switch (content_encoding) {
            .identity => &.{},
            .zstd => alloc.alloc(u8, std.compress.zstd.default_window_len) catch {
                freeHeaderListItems(alloc, &hdr_list);
                pool.pushErr(job, "OOM");
                return;
            },
            .deflate, .gzip => alloc.alloc(u8, std.compress.flate.max_window_len) catch {
                freeHeaderListItems(alloc, &hdr_list);
                pool.pushErr(job, "OOM");
                return;
            },
            .compress => {
                freeHeaderListItems(alloc, &hdr_list);
                pool.pushErr(job, "unsupported_content_encoding");
                return;
            },
        };
        defer if (decompress_buffer.len > 0) alloc.free(decompress_buffer);

        var al_writer = std.Io.Writer.Allocating.initCapacity(alloc, 4096) catch {
            freeHeaderListItems(alloc, &hdr_list);
            pool.pushErr(job, "OOM");
            return;
        };
        defer al_writer.deinit();

        var transfer_buffer: [64]u8 = undefined;
        var decompress: std.http.Decompress = undefined;
        const reader = response.readerDecompressing(&transfer_buffer, &decompress, decompress_buffer);

        _ = reader.streamRemaining(&al_writer.writer) catch |err| {
            const msg = if (wd_timed_out.load(.acquire)) "timeout" else switch (err) {
                error.ReadFailed => if (response.bodyErr()) |be| @errorName(be) else "ReadFailed",
                else => @errorName(err),
            };
            freeHeaderListItems(alloc, &hdr_list);
            pool.pushErr(job, msg);
            return;
        };

        const body = al_writer.toOwnedSlice() catch {
            freeHeaderListItems(alloc, &hdr_list);
            pool.pushErr(job, "OOM");
            return;
        };

        const resp_headers = hdr_list.toOwnedSlice(alloc) catch {
            alloc.free(body);
            freeHeaderListItems(alloc, &hdr_list);
            pool.pushErr(job, "OOM");
            return;
        };

        pool.pushResult(job, .{ .http = .{
            .status  = status,
            .body    = body,
            .headers = resp_headers,
        }});
    }

    fn executeProc(pool: *IoPool, thread_idx: u8, job: IoJob, argv: []const []const u8) void {
        const alloc = pool.allocator;

        var child = std.process.spawn(pool.io, .{
            .argv   = argv,
            .stdout = .pipe,
            .stderr = .close,
        }) catch |err| {
            pool.pushErr(job, @errorName(err));
            return;
        };
        const child_pid = child.id.?;

        // Publish PID so deinit() can SIGKILL if the pool shuts down mid-run.
        pool.current_pids[thread_idx].store(child_pid, .release);
        defer pool.current_pids[thread_idx].store(0, .release);

        // Start timeout timer.
        var timer_done = std.atomic.Value(bool).init(false);
        const timer = std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, timerThread, .{TimerCtx{
            .pid        = child_pid,
            .timeout_ms = pool.timeout_ms,
            .io         = pool.io,
            .done       = &timer_done,
        }}) catch |err| {
            log.warn("failed to spawn timer for coro={d}: {s}", .{ job.coro_id, @errorName(err) });
            _ = child.wait(pool.io) catch {};
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
            var rbuf: [4096]u8 = undefined;
            var fr = pipe.reader(pool.io, &rbuf);
            var buf: [4096]u8 = undefined;
            read_loop: while (true) {
                const n = fr.interface.readSliceShort(&buf) catch break;
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

        const term = child.wait(pool.io) catch |err| {
            pool.pushErr(job, @errorName(err));
            return;
        };

        // SIGKILL means either timeout (timer) or deinit() killed it.
        switch (term) {
            .signal => |sig| if (sig == std.posix.SIG.KILL) {
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
            .exited => |code| @as(i32, code),
            .signal => |sig|  -@as(i32, @intCast(@intFromEnum(sig))),
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
        .worker_id = 0, .coro_id = 1,
        .payload = .{ .http_request = .{
            .method = "GET", .url = "http://x", .headers = &.{}, .body = "",
        }},
    };
    try testing.expectEqual(@as(u32, 1), j_http.coro_id);

    const j_exec = IoJob{
        .worker_id = 0, .coro_id = 2,
        .payload = .{ .exec = .{ .argv = &.{"/bin/true"} } },
    };
    try testing.expectEqual(@as(u32, 2), j_exec.coro_id);

    const j_shell = IoJob{
        .worker_id = 0, .coro_id = 3,
        .payload = .{ .shell = .{ .command = "true" } },
    };
    try testing.expectEqual(@as(u32, 3), j_shell.coro_id);
}

test "freeIoResult releases all outcome variants — no leaks" {
    freeIoResult(.{ .coro_id = 0, .outcome = .{ .http = .{
        .status  = 200,
        .body    = try testing.allocator.dupe(u8, "hello"),
        .headers = try testing.allocator.alloc(std.http.Header, 0),
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

// 0.16's net.Server no longer exposes the bound address, so query the socket
// directly for the port assigned to a port-0 listen.
fn boundPort(srv: *std.Io.net.Server) u16 {
    var sa: std.posix.sockaddr.in = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    _ = std.posix.system.getsockname(srv.socket.handle, @ptrCast(&sa), &len);
    return std.mem.bigToNative(u16, sa.port);
}

fn spawnStub(response: []const u8) !StubServer {
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv_ptr = try testing.allocator.create(std.Io.net.Server);
    srv_ptr.* = try addr.listen(testing.io, .{ .reuse_address = true });
    const port = boundPort(srv_ptr);
    const thread = try std.Thread.spawn(.{}, stubOnce, .{ srv_ptr, response });
    return .{ .port = port, .thread = thread };
}

/// A stalled peer: accepts one connection, drains the request head, then holds
/// the connection open without ever responding. deinit() shuts the listening
/// socket down (which unblocks the parked accept, so the stub closes its end of
/// the client connection) and joins the thread. Call deinit() after the IoPool
/// that submitted the job is torn down.
const HangStub = struct {
    server: *std.Io.net.Server,
    thread: std.Thread,
    port:   u16,
    fn deinit(self: *HangStub) void {
        const io = testing.io;
        var s = std.Io.net.Stream{ .socket = self.server.socket };
        s.shutdown(io, .recv) catch {};
        self.server.deinit(io);
        self.thread.join();
        testing.allocator.destroy(self.server);
    }
};

fn spawnHangStub() !HangStub {
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.Io.net.Server);
    srv.* = try addr.listen(testing.io, .{ .reuse_address = true });
    const port = boundPort(srv);
    const thread = try std.Thread.spawn(.{}, hangOnce, .{srv});
    return .{ .server = srv, .thread = thread, .port = port };
}

fn hangOnce(srv_ptr: *std.Io.net.Server) void {
    const io = testing.io;
    var stream = srv_ptr.accept(io) catch return;
    defer stream.close(io);
    var rbuf: [8192]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    // Drain the request head so the client's write completes; the client then
    // blocks waiting for a response that never comes.
    while (sr.interface.takeDelimiterInclusive('\n')) |line| {
        if (line.len <= 2) break; // "\r\n" terminates the header block
    } else |_| {}
    // Hold the connection open until deinit() shuts the listening socket down.
    _ = srv_ptr.accept(io) catch {};
}

fn stubOnce(srv_ptr: *std.Io.net.Server, response: []const u8) void {
    const io = testing.io;
    defer {
        srv_ptr.deinit(io);
        testing.allocator.destroy(srv_ptr);
    }
    var stream = srv_ptr.accept(io) catch return;
    defer stream.close(io);
    var rbuf: [8192]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    // Drain the request head line-by-line, stopping at the blank line that ends
    // the headers. readSliceShort would deadlock here: it returns short only on
    // EOF, and the keep-alive client never closes, so it blocks forever trying
    // to fill the buffer.
    while (sr.interface.takeDelimiterInclusive('\n')) |line| {
        if (line.len <= 2) break; // "\r\n" terminates the header block
    } else |_| {}
    var wbuf: [4096]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    sw.interface.writeAll(response) catch {};
    sw.interface.flush() catch {};
}

/// Like spawnStub but records the bytes the client sent into `req_out`.
const CaptureStub = struct { port: u16, thread: std.Thread };

fn spawnCaptureStub(req_out: *std.ArrayListUnmanaged(u8), response: []const u8) !CaptureStub {
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv_ptr = try testing.allocator.create(std.Io.net.Server);
    srv_ptr.* = try addr.listen(testing.io, .{ .reuse_address = true });
    const port = boundPort(srv_ptr);
    const thread = try std.Thread.spawn(.{}, captureOnce, .{ srv_ptr, req_out, response });
    return .{ .port = port, .thread = thread };
}

fn captureOnce(srv_ptr: *std.Io.net.Server, req_out: *std.ArrayListUnmanaged(u8), response: []const u8) void {
    const io = testing.io;
    defer {
        srv_ptr.deinit(io);
        testing.allocator.destroy(srv_ptr);
    }
    var stream = srv_ptr.accept(io) catch return;
    defer stream.close(io);
    var rbuf: [8192]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    // Capture the request head line-by-line, stopping at the blank line that
    // ends the headers. A single readSliceShort would deadlock against the
    // keep-alive client (see stubOnce).
    while (sr.interface.takeDelimiterInclusive('\n')) |line| {
        req_out.appendSlice(testing.allocator, line) catch {};
        if (line.len <= 2) break; // "\r\n" terminates the header block
    } else |_| {}
    var wbuf: [4096]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    sw.interface.writeAll(response) catch {};
    sw.interface.flush() catch {};
}

test "http_request to a stalled peer times out instead of pinning the pool thread" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
    defer rq.deinit(testing.allocator);

    var hang = try spawnHangStub();

    const url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{hang.port});
    defer testing.allocator.free(url);

    var m = metrics_mod.Metrics{};
    var pool: IoPool = undefined;
    // Short timeout so the watchdog fires well within the 3 s poll budget below.
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 200, .proc_max_output = 65_536, .metrics = &m },
        &.{&rq});

    try pool.submit(.{
        .worker_id = 0, .coro_id = 11,
        .payload = .{ .http_request = .{ .method = "GET", .url = url, .headers = &.{}, .body = "" } },
    });

    // Without the watchdog the pool thread blocks in receiveHead forever and no
    // result arrives; with it, an err lands shortly after timeout_ms.
    const result = rq.popTimeout(3 * std.time.ns_per_s);
    if (result == null) {
        // Regression: close the stub's end first to unblock the pool thread, so
        // pool.deinit() can join it, then fail loudly.
        hang.deinit();
        pool.deinit();
        return error.TestTimeout;
    }
    const r = result.?;
    defer freeIoResult(r, testing.allocator);
    defer hang.deinit();
    defer pool.deinit();

    try testing.expect(r.outcome == .err);
    try testing.expectEqualStrings("timeout", r.outcome.err);
    try testing.expectEqual(@as(u64, 1), m.io_timeouts_total.load(.monotonic));
}

test "http_request forwards extra headers to the server" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
    defer rq.deinit(testing.allocator);

    var req_bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer req_bytes.deinit(testing.allocator);

    const stub = try spawnCaptureStub(&req_bytes,
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK");
    defer stub.thread.join();

    const url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{stub.port});
    defer testing.allocator.free(url);

    var pool: IoPool = undefined;
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 },
        &.{&rq});
    defer pool.deinit();

    const hdrs = [_]std.http.Header{
        .{ .name = "X-Custom", .value = "hello-world" },
        .{ .name = "Authorization", .value = "Bearer tok123" },
    };
    try pool.submit(.{
        .worker_id = 0, .coro_id = 7,
        .payload = .{ .http_request = .{ .method = "GET", .url = url, .headers = &hdrs, .body = "" } },
    });

    const result = rq.popTimeout(3 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(result, testing.allocator);
    try testing.expectEqual(@as(u16, 200), result.outcome.http.status);

    try testing.expect(std.mem.indexOf(u8, req_bytes.items, "X-Custom: hello-world") != null);
    try testing.expect(std.mem.indexOf(u8, req_bytes.items, "Authorization: Bearer tok123") != null);
}

test "http_request captures response headers, original casing, last-wins dup" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
    defer rq.deinit(testing.allocator);

    const stub = try spawnStub(
        "HTTP/1.1 200 OK\r\n" ++
        "Content-Type: application/json\r\n" ++
        "X-Request-Id: abc\r\n" ++
        "x-request-id: def\r\n" ++ // duplicate (different case) -> last wins
        "Content-Length: 2\r\n" ++
        "Connection: close\r\n" ++
        "\r\nOK");
    defer stub.thread.join();

    const url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{stub.port});
    defer testing.allocator.free(url);

    var pool: IoPool = undefined;
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 },
        &.{&rq});
    defer pool.deinit();

    try pool.submit(.{
        .worker_id = 0, .coro_id = 9,
        .payload = .{ .http_request = .{ .method = "GET", .url = url, .headers = &.{}, .body = "" } },
    });

    const result = rq.popTimeout(3 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(result, testing.allocator);

    try testing.expectEqualStrings("OK", result.outcome.http.body);

    const H = result.outcome.http.headers;
    var have_ct = false;
    var reqid_val: []const u8 = "";
    var reqid_name: []const u8 = "";
    var reqid_count: usize = 0;
    for (H) |hdr| {
        if (std.mem.eql(u8, hdr.name, "Content-Type")) {
            have_ct = true;
            try testing.expectEqualStrings("application/json", hdr.value);
        }
        if (std.ascii.eqlIgnoreCase(hdr.name, "x-request-id")) {
            reqid_val = hdr.value;
            reqid_name = hdr.name;
            reqid_count += 1;
        }
    }
    try testing.expect(have_ct);
    // Case-insensitive de-dup collapses the two X-Request-Id headers into one.
    try testing.expectEqual(@as(usize, 1), reqid_count);
    try testing.expectEqualStrings("def", reqid_val);
    try testing.expectEqualStrings("x-request-id", reqid_name);
}

test "http_request decompresses gzip response body" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
    defer rq.deinit(testing.allocator);

    // gzip of "hello gzip world" (mtime=0 for a stable fixture).
    const gz = [_]u8{
        0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff,
        0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0x57, 0x48, 0xaf, 0xca, 0x2c,
        0x50, 0x28, 0xcf, 0x2f, 0xca, 0x49, 0x01, 0x00, 0x6b, 0x7d,
        0xe8, 0xb7, 0x10, 0x00, 0x00, 0x00,
    };

    var len_buf: [32]u8 = undefined;
    const len_line = try std.fmt.bufPrint(&len_buf, "Content-Length: {d}\r\n", .{gz.len});

    var resp: std.ArrayListUnmanaged(u8) = .empty;
    defer resp.deinit(testing.allocator);
    try resp.appendSlice(testing.allocator,
        "HTTP/1.1 200 OK\r\n" ++
        "Content-Type: text/plain\r\n" ++
        "Content-Encoding: gzip\r\n");
    try resp.appendSlice(testing.allocator, len_line);
    try resp.appendSlice(testing.allocator, "Connection: close\r\n\r\n");
    try resp.appendSlice(testing.allocator, &gz);

    // resp.items must outlive the stub thread: join() (declared below) runs
    // before resp.deinit() since defers fire last-in-first-out.
    const stub = try spawnStub(resp.items);
    defer stub.thread.join();

    const url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{stub.port});
    defer testing.allocator.free(url);

    var pool: IoPool = undefined;
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 },
        &.{&rq});
    defer pool.deinit();

    try pool.submit(.{
        .worker_id = 0, .coro_id = 11,
        .payload = .{ .http_request = .{ .method = "GET", .url = url, .headers = &.{}, .body = "" } },
    });

    const result = rq.popTimeout(3 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(result, testing.allocator);

    try testing.expectEqual(@as(u16, 200), result.outcome.http.status);
    try testing.expectEqualStrings("hello gzip world", result.outcome.http.body);
}

test "http_request GET to local stub → status 200, body matches" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
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
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 5_000, .proc_max_output = 65_536 },
        &.{&rq});
    defer pool.deinit();

    try pool.submit(.{
        .worker_id = 0, .coro_id = 42,
        .payload = .{ .http_request = .{
            .method = "GET", .url = url, .headers = &.{}, .body = "",
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
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
    defer rq.deinit(testing.allocator);

    var pool: IoPool = undefined;
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 5_000, .proc_max_output = 65_536 },
        &.{&rq});
    defer pool.deinit();

    // Port 1 is almost certainly not listening — ECONNREFUSED expected.
    try pool.submit(.{
        .worker_id = 0, .coro_id = 1,
        .payload = .{ .http_request = .{
            .method = "GET", .url = "http://127.0.0.1:1/",
            .headers = &.{}, .body = "",
        }},
    });

    const r1 = rq.popTimeout(5 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(r1, testing.allocator);
    try testing.expect(r1.outcome == .err);

    // Confirm the thread survived by processing a second job.
    try pool.submit(.{
        .worker_id = 0, .coro_id = 2,
        .payload = .{ .http_request = .{
            .method = "GET", .url = "http://127.0.0.1:1/",
            .headers = &.{}, .body = "",
        }},
    });
    const r2 = rq.popTimeout(5 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(r2, testing.allocator);
    try testing.expect(r2.outcome == .err);
}

test "exec and shell run a command and capture output" {
    // exec: the argv form runs /bin/echo and returns its stdout.
    {
        var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
        defer rq.deinit(testing.allocator);
        var pool: IoPool = undefined;
        try pool.init(testing.allocator, testing.io,
            .{ .thread_count = 1, .queue_capacity = 8,
               .timeout_ms = 5_000, .proc_max_output = 65_536 },
            &.{&rq});
        defer pool.deinit();

        try pool.submit(.{ .worker_id = 0, .coro_id = 10,
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
        var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
        defer rq.deinit(testing.allocator);
        var pool: IoPool = undefined;
        try pool.init(testing.allocator, testing.io,
            .{ .thread_count = 1, .queue_capacity = 8,
               .timeout_ms = 5_000, .proc_max_output = 65_536 },
            &.{&rq});
        defer pool.deinit();

        try pool.submit(.{ .worker_id = 0, .coro_id = 30,
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
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
    defer rq.deinit(testing.allocator);
    // Short timeout so the test completes in ~200 ms.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 150, .proc_max_output = 65_536 },
        &.{&rq});
    defer pool.deinit();

    try pool.submit(.{ .worker_id = 0, .coro_id = 20,
        .payload = .{ .exec = .{ .argv = &.{ "/bin/sleep", "60" } } } });

    const result = rq.popTimeout(1 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(result, testing.allocator);
    // Returning without hanging confirms the child was reaped (no zombie).
    try testing.expect(result.outcome == .err);
}

test "stdout exceeding PROC_MAX_OUTPUT_BYTES → truncated and marked" {
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
    defer rq.deinit(testing.allocator);
    // proc_max_output = 10 bytes; command produces 22 bytes.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 5_000, .proc_max_output = 10 },
        &.{&rq});
    defer pool.deinit();

    try pool.submit(.{ .worker_id = 0, .coro_id = 31,
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

    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 16);
    defer rq.deinit(testing.allocator);

    // Short timeout (150 ms) so the timeout job completes quickly.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 2, .queue_capacity = 8, .timeout_ms = 150,
           .proc_max_output = 65_536, .metrics = &m },
        &.{&rq});
    defer pool.deinit();

    // Job 1: success (exec /bin/echo)
    try pool.submit(.{ .worker_id = 0, .coro_id = 1,
        .payload = .{ .exec = .{ .argv = &.{ "/bin/echo", "ok" } } } });
    const rs = rq.popTimeout(3 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(rs, testing.allocator);
    try testing.expect(rs.outcome == .proc);

    // Job 2: error (HTTP to port 1 — ECONNREFUSED)
    try pool.submit(.{ .worker_id = 0, .coro_id = 2,
        .payload = .{ .http_request = .{
            .method = "GET", .url = "http://127.0.0.1:1/", .headers = &.{}, .body = "",
        }}});
    const re = rq.popTimeout(5 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(re, testing.allocator);
    try testing.expect(re.outcome == .err);

    // Job 3: timeout (exec /bin/sleep 60 with pool timeout_ms = 150)
    try pool.submit(.{ .worker_id = 0, .coro_id = 3,
        .payload = .{ .exec = .{ .argv = &.{ "/bin/sleep", "60" } } } });
    const res = rq.popTimeout(1 * std.time.ns_per_s) orelse return error.TestTimeout;
    defer freeIoResult(res, testing.allocator);
    try testing.expect(res.outcome == .err);

    try testing.expectEqual(@as(u64, 3), m.io_jobs_total.load(.monotonic));
    try testing.expectEqual(@as(i64, 0), m.io_jobs_inflight.load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.io_errors_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.io_timeouts_total.load(.monotonic));
}

test "submit at capacity returns QueueFull without inflating metrics" {
    var m = metrics_mod.Metrics{};

    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 4);
    defer rq.deinit(testing.allocator);

    // thread_count = 0: no consumers, queue fills immediately.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 0, .queue_capacity = 1,
           .timeout_ms = 1_000, .proc_max_output = 65_536, .metrics = &m },
        &.{&rq});
    defer pool.deinit();

    const job = IoJob{
        .worker_id = 0, .coro_id = 1,
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
    var rq = try queue_mod.Queue(IoResult).init(testing.allocator, testing.io, 8);
    defer rq.deinit(testing.allocator);

    // Long timeout so the pool's timer doesn't fire before deinit does.
    var pool: IoPool = undefined;
    try pool.init(testing.allocator, testing.io,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 60_000, .proc_max_output = 65_536 },
        &.{&rq});

    try pool.submit(.{ .worker_id = 0, .coro_id = 40,
        .payload = .{ .exec = .{ .argv = &.{ "/bin/sleep", "60" } } } });

    // Give the pool thread time to spawn the child.
    rt.sleepNs(rt.io(), 80 * std.time.ns_per_ms);

    // deinit must return quickly — it SIGKILLs the child, child.wait() returns.
    const t0 = rt.nowMs(rt.io());
    pool.deinit();
    const elapsed_ms = rt.nowMs(rt.io()) - t0;

    try testing.expect(elapsed_ms < 1_000);

    // Drain results pushed by the killed worker (e.g. "timeout" err) to prevent leak.
    while (rq.popTimeout(0)) |r| freeIoResult(r, testing.allocator);
}
