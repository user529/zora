/// worker.zig — worker thread: owns lua_State + SQLite connection
///
/// Each worker runs in a dedicated thread and owns:
///   - a LuaEngine (Lua state + on_message function)
///   - a StateStore connection (WAL allows concurrent readers)
///
/// Main loop (pseudo-code):
///   loop:
///     item = queue.tryPop()  -- WorkItem{ body, user_id }; check stop flag between polls
///     if global_reload_version > local_reload_version:
///         lua_engine.loadFile(rules_path)
///         local_reload_version = global_reload_version
///     actions = lua_engine.callOnMessage(item.body) -> []ApiCall
///     for action in actions: dispatcher_queue.push(action)
///     free actions slice (string payloads now owned by dispatcher)
///     free item.body
///
/// Routing helper:
///   hashUserId(user_id, worker_count) → worker index
///   Deterministic, uniform distribution via 64-bit multiplicative hash.

const std = @import("std");
const ziglua = @import("ziglua");
const types = @import("types.zig");
const queue_mod = @import("queue.zig");
const lua_engine = @import("lua_engine.zig");
const lua_api = @import("lua_api.zig");
const state_store = @import("state_store.zig");
const tg_schema = @import("tg_schema.zig");
const io_pool = @import("io_pool.zig");
const metrics_mod = @import("metrics.zig");

const log = std.log.scoped(.worker);

// ---------------------------------------------------------------------------
// WorkerArgs — all parameters for a single worker thread
// ---------------------------------------------------------------------------

pub const WorkerArgs = struct {
    id: u8,
    /// Null-terminated path to the Lua rules file (for initial load + reload).
    rules_path: [:0]const u8,
    /// Used for Update→JSON conversion, action string allocation, and Lua
    /// engine internals.  Must be thread-safe (e.g., a GPA).
    allocator: std.mem.Allocator,
    /// Input queue: server pushes WorkItem values (raw body + routing user_id).
    /// The worker frees WorkItem.body after callOnMessage returns.
    queue: *queue_mod.Queue(types.WorkItem),
    /// Output queue: worker pushes ApiCall values, dispatcher pops them.
    /// The `method` and `body` strings inside each ApiCall are allocated from
    /// `allocator`; the dispatcher is responsible for freeing them after sending.
    dispatcher_queue: *queue_mod.Queue(types.ApiCall),
    /// Per-worker SQLite connection (WAL mode allows concurrent readers).
    db: *state_store.StateStore,
    /// Set to true to request graceful shutdown.  Worker exits the loop
    /// after the current update (if any) finishes.
    stop: *std.atomic.Value(bool),
    /// Monotonically-increasing reload counter.  When the value advances past
    /// the worker's local copy the rules file is reloaded before the next
    /// on_message call.  Production code passes &watcher.reload_version;
    /// tests inject a local counter for isolation.
    reload_ver: *std.atomic.Value(u64),
    /// Hot-reloadable API schema for outgoing-call validation.
    /// Defaults to null — no schema, no validation (used by tests).
    schema: ?*tg_schema.SchemaSlot = null,
    /// Validation policy.  `.off` (default) disables validation.
    validation: types.ValidationMode = .off,
    /// Maximum file size in bytes for multipart upload (from config.multipart_max_file).
    multipart_max_file: usize = 52428800,
    /// Maximum bytes for json.decode input / json.encode output (from config.json_max_bytes).
    json_max_bytes: usize = 1048576,
    /// The io_pool that executes blocking I/O jobs for parked coroutines.
    /// May be null in tests that exercise only the sync (non-yielding) path.
    io_pool_ptr: ?*io_pool.IoPool = null,
    /// Per-worker queue where the io_pool pushes I/O results.
    /// Must be non-null when io_pool_ptr is non-null.
    io_result_queue: ?*queue_mod.Queue(io_pool.IoResult) = null,
    /// Max coroutines parked simultaneously on this worker.
    max_inflight: u16 = 64,
    /// Wall-clock deadline (ms) for a single coroutine workflow.
    workflow_deadline_ms: u64 = 60_000,
    /// Optional metrics sink.  Null means "don't count" (most unit tests pass null).
    metrics: ?*metrics_mod.Metrics = null,
};

// ---------------------------------------------------------------------------
// In-flight coroutine tracking
// ---------------------------------------------------------------------------

const InFlightEntry = struct {
    handle:           lua_engine.CoroHandle,
    deadline_ms:      i64,
    owned_strings:    lua_api.OwnedStrings,
    is_tracked_send:  bool = false,
};

const InFlightMap = std.AutoHashMap(u32, InFlightEntry);

fn reapEntry(entry: InFlightEntry, main: *ziglua.Lua, allocator: std.mem.Allocator) void {
    lua_engine.teardownCoro(main, entry.handle);
    lua_api.freeOwnedStrings(entry.owned_strings, allocator);
}

/// Decrement the in-flight gauge once, in lock-step with removing a coroutine
/// from `inflight`, so the gauge can never drift.  Used on its own only when
/// resumeHandler has already torn the coroutine down (the .done and .err
/// outcomes), where calling reapEntry again would double-free.
fn decInflight(metrics: ?*metrics_mod.Metrics) void {
    if (metrics) |m| _ = m.coroutines_inflight.fetchAdd(-1, .monotonic);
}

/// Remove `cid` from `inflight`, decrement the gauge, and tear down its
/// coroutine — the three steps that must always happen together when a
/// still-live coroutine is abandoned.  Returns true if an entry was dropped.
fn dropInflight(
    inflight:  *InFlightMap,
    cid:       u32,
    main:      *ziglua.Lua,
    allocator: std.mem.Allocator,
    metrics:   ?*metrics_mod.Metrics,
) bool {
    if (inflight.fetchRemove(cid)) |kv| {
        decInflight(metrics);
        reapEntry(kv.value, main, allocator);
        return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Worker thread entry point
// ---------------------------------------------------------------------------

pub fn workerThread(args: WorkerArgs) void {
    defer {
        if (!args.stop.load(.acquire)) {
            log.err("worker {d}: unexpected exit — aborting", .{args.id});
            std.process.abort();
        }
    }

    std.debug.assert((args.io_pool_ptr == null) == (args.io_result_queue == null));

    var api_ctx = lua_api.ApiCtx{
        .db             = args.db,
        .allocator      = args.allocator,
        .max_file_bytes = args.multipart_max_file,
        .json_max_bytes = args.json_max_bytes,
    };

    var engine = lua_engine.LuaEngine.init(args.allocator, &api_ctx) catch |err| {
        log.err("worker {d}: LuaEngine.init failed: {s}", .{ args.id, @errorName(err) });
        return;
    };
    defer engine.deinit();

    engine.loadFile(args.rules_path) catch {
        log.warn("worker {d}: initial rules load failed", .{args.id});
    };
    engine.setValidation(args.schema, args.validation);

    var local_ver: u64 = args.reload_ver.load(.acquire);
    var coro_id_counter: u32 = 1;

    var inflight = InFlightMap.init(args.allocator);
    defer {
        var it = inflight.iterator();
        while (it.next()) |kv| {
            // Shutdown abandonment — not a deadline reap, so reaped_total is not incremented.
            decInflight(args.metrics);
            reapEntry(kv.value_ptr.*, engine.lua, args.allocator);
        }
        inflight.deinit();
    }

    var drain_logged = false;
    while (true) {
        const stopping = args.stop.load(.acquire);
        if (stopping and inflight.count() == 0) break;

        // ── 1. Resume coroutines whose I/O completed ──────────────────────
        if (args.io_result_queue) |rq| {
            // When draining on stop, block up to 5ms on the first pop so the
            // thread doesn't busy-spin while waiting for in-flight results.
            var first_timeout: u64 = if (stopping and inflight.count() > 0)
                5 * std.time.ns_per_ms
            else
                0;
            while (rq.popTimeout(first_timeout)) |result| {
                first_timeout = 0;
                // Save coro_id before freeIoResult invalidates the struct.
                const cid = result.coro_id;
                if (inflight.getPtr(cid)) |entry| {
                    const owned = entry.owned_strings;
                    // resumeHandler always frees `owned` and calls lua.unref
                    // for .done and .err outcomes; for .yielded it does neither.
                    const outcome = engine.resumeHandler(
                        entry.handle, result, owned, entry.is_tracked_send, args.allocator,
                    ) catch |err| blk: {
                        log.err("worker {d}: resumeHandler OOM: {s}", .{ args.id, @errorName(err) });
                        // resumeHandler freed owned + called unref before returning error.
                        break :blk lua_engine.CoroOutcome.err;
                    };
                    io_pool.freeIoResult(result, args.allocator);

                    switch (outcome) {
                        .done => |actions| {
                            // resumeHandler already called lua.unref + freeOwnedStrings.
                            _ = inflight.remove(cid);
                            decInflight(args.metrics);
                            for (actions) |action| {
                                args.dispatcher_queue.push(action) catch {
                                    log.warn("worker {d}: dispatcher queue full", .{args.id});
                                    types.freeApiCall(action, args.allocator);
                                };
                            }
                            args.allocator.free(actions);
                        },
                        .yielded => |y| {
                            // resumeHandler freed old owned_strings; new ones are in y.
                            // Update the inflight entry for the next yield type.
                            if (inflight.getPtr(cid)) |e| {
                                switch (y.pending_job) {
                                    .io => |io| {
                                        e.owned_strings   = io.owned_strings;
                                        e.is_tracked_send = false;
                                        if (args.io_pool_ptr) |pool| {
                                            pool.submit(io.io_job) catch {
                                                log.warn("worker {d}: io_pool full; dropping coro {d}", .{ args.id, cid });
                                                // entry.owned_strings == io.owned_strings — dropInflight frees it.
                                                _ = dropInflight(&inflight, cid, engine.lua, args.allocator, args.metrics);
                                            };
                                        }
                                    },
                                    .tracked_send => |api_call| {
                                        e.owned_strings   = .none;
                                        e.is_tracked_send = true;
                                        args.dispatcher_queue.push(api_call) catch {
                                            log.warn("worker {d}: dispatcher queue full; dropping re-yield coro {d}", .{ args.id, cid });
                                            _ = dropInflight(&inflight, cid, engine.lua, args.allocator, args.metrics);
                                            types.freeApiCall(api_call, args.allocator);
                                        };
                                    },
                                }
                            }
                        },
                        .err => {
                            // resumeHandler already called lua.unref + freeOwnedStrings.
                            _ = inflight.remove(cid);
                            decInflight(args.metrics);
                        },
                    }
                } else {
                    io_pool.freeIoResult(result, args.allocator);
                }
            }
        }

        // ── 2. Reap coroutines past their wall-clock deadline ─────────────
        {
            const now_ms = std.time.milliTimestamp();
            var to_reap: [64]u32 = undefined;
            var n_reap: usize = 0;
            var it = inflight.iterator();
            while (it.next()) |kv| {
                if (now_ms > kv.value_ptr.deadline_ms) {
                    to_reap[n_reap] = kv.key_ptr.*;
                    n_reap += 1;
                    if (n_reap >= to_reap.len) break;
                }
            }
            for (to_reap[0..n_reap]) |cid| {
                log.warn("worker {d}: reaping coro {d} (deadline exceeded)", .{ args.id, cid });
                if (dropInflight(&inflight, cid, engine.lua, args.allocator, args.metrics)) {
                    if (args.metrics) |m| _ = m.coroutines_reaped_total.fetchAdd(1, .monotonic);
                }
            }
        }

        // ── drain guard: when stopping, skip Step 3 until inflight is empty ──
        // Step 1 already performed a 5ms timed wait when draining, so no extra
        // sleep is needed here — deadline reap (Step 2) fires on every iteration.
        if (stopping) {
            if (inflight.count() > 0 and !drain_logged) {
                log.info("worker {d}: stopping — draining {d} in-flight coroutine(s)",
                         .{ args.id, inflight.count() });
                drain_logged = true;
            }
            continue;
        }

        // ── 3. Accept new update if below inflight ceiling ────────────────
        if (inflight.count() >= args.max_inflight) {
            // At ceiling: sleep briefly (avoids busy-spin) and check hot-reload
            // so rules changes are not blocked indefinitely by a full inflight map.
            const global_ver = args.reload_ver.load(.acquire);
            if (global_ver > local_ver) {
                engine.loadFile(args.rules_path) catch {
                    log.warn("worker {d}: reload failed", .{args.id});
                };
                local_ver = global_ver;
                log.info("worker {d}: reloaded rules (v{d})", .{ args.id, local_ver });
            }
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        }
        {
            const maybe_item = args.queue.popTimeout(10 * std.time.ns_per_ms);
            if (maybe_item == null) continue;
            const item = maybe_item.?;
            defer args.allocator.free(item.body);

            {
                const global_ver = args.reload_ver.load(.acquire);
                if (global_ver > local_ver) {
                    engine.loadFile(args.rules_path) catch {
                        log.warn("worker {d}: reload failed", .{args.id});
                    };
                    local_ver = global_ver;
                    log.info("worker {d}: reloaded rules (v{d})", .{ args.id, local_ver });
                }
            }

            const cid = coro_id_counter;
            coro_id_counter +%= 1;
            if (coro_id_counter == 0) coro_id_counter = 1; // skip 0 (used as sentinel)

            args.db.beginImmediate() catch |err| {
                log.err("worker {d}: BEGIN IMMEDIATE failed: {s}", .{ args.id, @errorName(err) });
                continue;
            };

            const outcome = engine.startHandler(item.body, cid, args.id, args.allocator) catch |err| {
                log.err("worker {d}: startHandler OOM: {s}", .{ args.id, @errorName(err) });
                args.db.rollback();
                continue;
            };

            switch (outcome) {
                .done => |actions| {
                    args.db.commit() catch |err| {
                        log.err("worker {d}: COMMIT failed: {s}", .{ args.id, @errorName(err) });
                        args.db.rollback();
                        for (actions) |action| types.freeApiCall(action, args.allocator);
                        args.allocator.free(actions);
                        continue;
                    };
                    for (actions) |action| {
                        args.dispatcher_queue.push(action) catch {
                            log.warn("worker {d}: dispatcher queue full", .{args.id});
                            types.freeApiCall(action, args.allocator);
                        };
                    }
                    args.allocator.free(actions);
                },
                .yielded => |y| {
                    args.db.commit() catch |err| {
                        log.err("worker {d}: COMMIT before yield failed: {s}", .{ args.id, @errorName(err) });
                    };
                    const deadline = std.time.milliTimestamp() +
                        @as(i64, @intCast(args.workflow_deadline_ms));
                    switch (y.pending_job) {
                        .io => |io| {
                            inflight.put(cid, InFlightEntry{
                                .handle          = y.handle,
                                .deadline_ms     = deadline,
                                .owned_strings   = io.owned_strings,
                                .is_tracked_send = false,
                            }) catch {
                                log.err("worker {d}: inflight map OOM", .{args.id});
                                lua_api.freeOwnedStrings(io.owned_strings, args.allocator);
                                lua_engine.teardownCoro(engine.lua, y.handle);
                                continue;
                            };
                            if (args.metrics) |m| _ = m.coroutines_inflight.fetchAdd(1, .monotonic);
                            if (args.io_pool_ptr) |pool| {
                                pool.submit(io.io_job) catch {
                                    log.warn("worker {d}: io_pool full on start", .{args.id});
                                    _ = dropInflight(&inflight, cid, engine.lua, args.allocator, args.metrics);
                                };
                            }
                        },
                        .tracked_send => |api_call| {
                            inflight.put(cid, InFlightEntry{
                                .handle          = y.handle,
                                .deadline_ms     = deadline,
                                .owned_strings   = .none,
                                .is_tracked_send = true,
                            }) catch {
                                log.err("worker {d}: inflight map OOM (tracked send)", .{args.id});
                                types.freeApiCall(api_call, args.allocator);
                                lua_engine.teardownCoro(engine.lua, y.handle);
                                continue;
                            };
                            if (args.metrics) |m| _ = m.coroutines_inflight.fetchAdd(1, .monotonic);
                            args.dispatcher_queue.push(api_call) catch {
                                log.warn("worker {d}: dispatcher queue full; dropping tracked send coro {d}", .{ args.id, cid });
                                _ = dropInflight(&inflight, cid, engine.lua, args.allocator, args.metrics);
                                types.freeApiCall(api_call, args.allocator);
                            };
                        },
                    }
                },
                .err => {
                    args.db.rollback();
                },
            }
        }
    }
    log.info("worker {d}: stopped", .{args.id});
}

// ---------------------------------------------------------------------------
// Routing helper
// ---------------------------------------------------------------------------

/// Map a Telegram user_id to a worker index in [0, worker_count).
/// Uses a 64-bit multiplicative hash — deterministic and well-distributed.
pub fn hashUserId(user_id: i64, worker_count: u32) u32 {
    const u: u64 = @bitCast(user_id);
    // Knuth multiplicative hash (64-bit variant).
    const h = u *% 0x9e3779b97f4a7c15;
    return @intCast(h % @as(u64, worker_count));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;
const Queue = queue_mod.Queue;

/// Test helper: spin-wait until `queue.len() >= n` or `timeout_ms` elapses.
fn waitQueue(q: anytype, n: usize, timeout_ms: u64) bool {
    const start = std.time.milliTimestamp();
    while (q.len() < n) {
        if (@as(u64, @intCast(std.time.milliTimestamp() - start)) >= timeout_ms) return false;
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }
    return true;
}

/// Test helper: pop an ApiCall from the queue and free its string payloads
/// using the provided allocator after the caller is done with it.
/// Returns the ApiCall for inspection.
fn popAction(q: *Queue(types.ApiCall)) types.ApiCall {
    return q.pop();
}

// ── Shared test setup ────────────────────────────────────────────────────────

/// Build a WorkItem from a JSON body string for tests.
/// `body` is heap-duplicated; the worker frees it after callOnMessage.
fn testWorkItem(allocator: std.mem.Allocator, body: []const u8) !types.WorkItem {
    return .{ .body = try allocator.dupe(u8, body), .user_id = null };
}

// ---------------------------------------------------------------------------
// Test helpers — HTTP stubs and async test context
// ---------------------------------------------------------------------------

const StubServer = struct { port: u16, thread: std.Thread };

/// One-shot stub: accepts one connection, responds immediately.
fn spawnStub(response: []const u8) !StubServer {
    const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.net.Server);
    srv.* = try addr.listen(.{ .reuse_address = true });
    const port = srv.listen_address.in.sa.port;
    const t = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.net.Server, resp: []const u8) void {
            defer { s.deinit(); testing.allocator.destroy(s); }
            const conn = s.accept() catch return;
            defer conn.stream.close();
            var buf: [4096]u8 = undefined;
            _ = conn.stream.read(&buf) catch {};
            conn.stream.writeAll(resp) catch {};
        }
    }.run, .{ srv, response });
    return .{ .port = std.mem.bigToNative(u16, port), .thread = t };
}

/// Silent stub: accepts one connection, never responds.
const SilentStub = struct {
    server: *std.net.Server,
    thread: std.Thread,
    fn deinit(self: *SilentStub) void {
        // shutdown(SHUT_RD) reliably unblocks any thread blocked in accept()
        // on this socket before closing the fd.
        std.posix.shutdown(self.server.stream.handle, .recv) catch {};
        self.server.deinit();   // close socket
        self.thread.join();     // wait for thread to exit before freeing memory
        testing.allocator.destroy(self.server);
    }
};

fn spawnSilentStub() !SilentStub {
    const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.net.Server);
    srv.* = try addr.listen(.{ .reuse_address = true });
    const t = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.net.Server) void {
            const conn = s.accept() catch return;
            var buf: [1]u8 = undefined;
            _ = conn.stream.read(&buf) catch {};
            conn.stream.close();
        }
    }.run, .{srv});
    return .{ .server = srv, .thread = t };
}

/// Hang stub: accepts one connection, drains the request, then holds the
/// connection open without responding. The caller MUST call deinit() before
/// joining any io_pool that submitted a job to this server — otherwise the
/// pool thread blocks forever in fetch().
///
/// deinit() closes the listening socket via server.deinit(), which unblocks
/// the second accept() in the stub thread. That causes the stub to exit and
/// close the accepted conn, releasing the HTTP client.
fn spawnHangStub() !SilentStub {
    const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.net.Server);
    srv.* = try addr.listen(.{ .reuse_address = true });
    const t = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.net.Server) void {
            const conn = s.accept() catch return;
            defer conn.stream.close();
            // Drain the HTTP request so the client's write completes;
            // the client then blocks waiting for a response.
            var buf: [4096]u8 = undefined;
            _ = conn.stream.read(&buf) catch {};
            // Block until deinit() closes the server socket.
            _ = s.accept() catch {};
        }
    }.run, .{srv});
    return .{ .server = srv, .thread = t };
}

/// Async-capable test context: extends TestCtx with an IoPool + result queue.
///
/// IMPORTANT: `init` takes `self: *AsyncTestCtx` (pointer receiver) so that
/// `pool.init` receives a stable address for `self.result_q`.  Returning by
/// value would invalidate the pointer that the pool stores internally.
/// Callers must declare the variable first and then call init:
///   var ctx: AsyncTestCtx = undefined;
///   try ctx.init(allocator, lua_src, pool_cfg);
const AsyncTestCtx = struct {
    tmp:        testing.TmpDir,
    db:         state_store.StateStore,
    input_q:    Queue(types.WorkItem),
    output_q:   Queue(types.ApiCall),
    result_q:   Queue(io_pool.IoResult),
    /// Stable single-element slice of result queue pointers passed to pool.init.
    /// Must be a field (not a local) so its address remains valid after init returns.
    rq_ptrs:    [1]*Queue(io_pool.IoResult),
    pool:       io_pool.IoPool,
    stop:       std.atomic.Value(bool),
    reload_ver: std.atomic.Value(u64),
    path_buf:   [std.fs.max_path_bytes + 1]u8,
    rules_path: [:0]const u8,
    allocator:  std.mem.Allocator,

    /// Initialise in-place.  `self` must already be at its final stack address.
    fn init(
        self:      *AsyncTestCtx,
        allocator: std.mem.Allocator,
        lua_src:   []const u8,
        pool_cfg:  io_pool.IoPoolConfig,
    ) !void {
        self.allocator  = allocator;
        self.tmp        = testing.tmpDir(.{});
        errdefer self.tmp.cleanup();

        var f = try self.tmp.dir.createFile("rules.lua", .{});
        try f.writeAll(lua_src);
        f.close();

        const path_slice = try self.tmp.dir.realpath("rules.lua", self.path_buf[0..std.fs.max_path_bytes]);
        self.path_buf[path_slice.len] = 0;
        self.rules_path = self.path_buf[0..path_slice.len :0];

        self.db = try state_store.StateStore.open(allocator, ":memory:");
        errdefer self.db.close();

        self.input_q  = try Queue(types.WorkItem).init(allocator, 64);
        errdefer self.input_q.deinit(allocator);

        self.output_q = try Queue(types.ApiCall).init(allocator, 256);
        errdefer self.output_q.deinit(allocator);

        self.result_q = try Queue(io_pool.IoResult).init(allocator, 256);
        errdefer self.result_q.deinit(allocator);
        self.result_q.kind = .io_result;
        self.result_q.id   = 0; // single-worker test context (spawnWorker hardcodes id=0)

        // rq_ptrs[0] points into self.result_q — stable because self is at its
        // final address (pointer receiver, declared before init is called).
        self.rq_ptrs[0] = &self.result_q;
        try self.pool.init(allocator, pool_cfg, &self.rq_ptrs);
        errdefer self.pool.deinit();

        self.stop       = std.atomic.Value(bool).init(false);
        self.reload_ver = std.atomic.Value(u64).init(0);
    }

    fn spawnWorker(self: *AsyncTestCtx, max_inflight: u16, workflow_deadline_ms: u64) !std.Thread {
        return std.Thread.spawn(.{}, workerThread, .{WorkerArgs{
            .id                   = 0,
            .rules_path           = self.rules_path,
            .allocator            = self.allocator,
            .queue                = &self.input_q,
            .dispatcher_queue     = &self.output_q,
            .db                   = &self.db,
            .stop                 = &self.stop,
            .reload_ver           = &self.reload_ver,
            .io_pool_ptr          = &self.pool,
            .io_result_queue      = &self.result_q,
            .max_inflight         = max_inflight,
            .workflow_deadline_ms = workflow_deadline_ms,
        }});
    }

    fn deinit(self: *AsyncTestCtx, t: std.Thread) void {
        self.stop.store(true, .release);
        t.join();
        while (self.input_q.popTimeout(0))  |item| self.allocator.free(item.body);
        while (self.output_q.popTimeout(0)) |call| types.freeApiCall(call, self.allocator);
        while (self.result_q.popTimeout(0)) |res|  io_pool.freeIoResult(res, self.allocator);
        self.pool.deinit();
        self.result_q.deinit(self.allocator);
        self.output_q.deinit(self.allocator);
        self.input_q.deinit(self.allocator);
        self.db.close();
        self.tmp.cleanup();
    }
};

fn asyncWorkItem(allocator: std.mem.Allocator, body: []const u8) !types.WorkItem {
    return .{ .body = try allocator.dupe(u8, body), .user_id = null };
}

const TestCtx = struct {
    tmp: testing.TmpDir,
    db: state_store.StateStore,
    input_q: Queue(types.WorkItem),
    output_q: Queue(types.ApiCall),
    stop: std.atomic.Value(bool),
    reload_ver: std.atomic.Value(u64),
    path_buf: [std.fs.max_path_bytes + 1]u8,
    rules_path: [:0]const u8,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, lua_src: []const u8) !TestCtx {
        var self: TestCtx = undefined;
        self.allocator = allocator;
        self.tmp = testing.tmpDir(.{});
        errdefer self.tmp.cleanup();

        // Write initial rules file.
        var f = try self.tmp.dir.createFile("rules.lua", .{});
        try f.writeAll(lua_src);
        f.close();

        // Resolve absolute path (null-terminated).
        const path_slice = try self.tmp.dir.realpath("rules.lua", self.path_buf[0..std.fs.max_path_bytes]);
        self.path_buf[path_slice.len] = 0;
        self.rules_path = self.path_buf[0..path_slice.len :0];

        self.db = try state_store.StateStore.open(allocator, ":memory:");
        errdefer self.db.close();

        self.input_q  = try Queue(types.WorkItem).init(allocator, 64);
        errdefer self.input_q.deinit(allocator);

        self.output_q = try Queue(types.ApiCall).init(allocator, 256);
        errdefer self.output_q.deinit(allocator);

        self.stop       = std.atomic.Value(bool).init(false);
        self.reload_ver = std.atomic.Value(u64).init(0);
        return self;
    }

    fn spawnWorker(self: *TestCtx) !std.Thread {
        const args = WorkerArgs{
            .id               = 0,
            .rules_path       = self.rules_path,
            .allocator        = self.allocator,
            .queue            = &self.input_q,
            .dispatcher_queue = &self.output_q,
            .db               = &self.db,
            .stop             = &self.stop,
            .reload_ver       = &self.reload_ver,
        };
        return std.Thread.spawn(.{}, workerThread, .{args});
    }

    fn deinit(self: *TestCtx, t: std.Thread) void {
        self.stop.store(true, .release);
        t.join();
        // Drain anything the worker didn't consume before stop, freeing
        // owned payloads so the test runs leak-clean under testing.allocator.
        while (self.input_q.popTimeout(0)) |item| self.allocator.free(item.body);
        while (self.output_q.popTimeout(0)) |call| types.freeApiCall(call, self.allocator);
        self.output_q.deinit(self.allocator);
        self.input_q.deinit(self.allocator);
        self.db.close();
        self.tmp.cleanup();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

// luaTableToJson key order is unspecified — assert ApiCall bodies by substring.
fn bodyHas(body: []const u8, needle: []const u8) bool {
    return std.mem.indexOf(u8, body, needle) != null;
}

test "single update → dispatcher receives expected action" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u)
        \\  return { { method="sendMessage", params={ chat_id=42, text="hello" } } }
        \\end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    // Give worker time to start and load rules.
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":1}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    const action = popAction(&ctx.output_q);
    defer types.freeApiCall(action, testing.allocator);

    try testing.expectEqualStrings("sendMessage", action.method);
    try testing.expect(bodyHas(action.payload.json, "\"chat_id\":42"));
    try testing.expect(bodyHas(action.payload.json, "\"text\":\"hello\""));
}

test "reload_version incremented → worker reloads before on_message" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u)
        \\  return { { method="sendMessage", params={ chat_id=1, text="v1" } } }
        \\end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    // Let the worker load initial rules and record local_ver.
    std.Thread.sleep(30 * std.time.ns_per_ms);

    // Overwrite rules file with v2.
    {
        var f = try ctx.tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(
            \\function on_message(u)
            \\  return { { method="sendMessage", params={ chat_id=1, text="v2" } } }
            \\end
        );
    }

    // Signal reload via the isolated per-test counter.
    _ = ctx.reload_ver.fetchAdd(1, .release);

    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":2}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    const action = popAction(&ctx.output_q);
    defer types.freeApiCall(action, testing.allocator);

    try testing.expect(bodyHas(action.payload.json, "\"text\":\"v2\""));
}

test "update that triggers reload is still processed (not dropped)" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u)
        \\  return { { method="sendMessage", params={ chat_id=99, text="ok" } } }
        \\end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    std.Thread.sleep(30 * std.time.ns_per_ms);

    // Signal reload via the isolated per-test counter.
    _ = ctx.reload_ver.fetchAdd(1, .release);

    // Push the update — it should be processed after the reload, not dropped.
    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":3}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    const action = popAction(&ctx.output_q);
    defer types.freeApiCall(action, testing.allocator);

    try testing.expect(bodyHas(action.payload.json, "\"text\":\"ok\""));
}

test "Lua error on first update → worker continues; second update succeeds" {
    var ctx = try TestCtx.init(testing.allocator,
        \\local call_count = 0
        \\function on_message(u)
        \\  call_count = call_count + 1
        \\  if call_count == 1 then
        \\    error("intentional error")
        \\  end
        \\  return { { method="sendMessage", params={ chat_id=1, text="second" } } }
        \\end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    std.Thread.sleep(30 * std.time.ns_per_ms);

    // First update → Lua error, empty slice returned, nothing pushed.
    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":4}"));
    // Second update → succeeds.
    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":5}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    const action = popAction(&ctx.output_q);
    defer types.freeApiCall(action, testing.allocator);
    try testing.expect(bodyHas(action.payload.json, "\"text\":\"second\""));

    // Confirm no second action appeared (first update produced nothing).
    try testing.expectEqual(@as(usize, 0), ctx.output_q.len());
}

test "worker thread alive (not exited) after 200ms with empty queue" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u) return {} end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    std.Thread.sleep(200 * std.time.ns_per_ms);

    // If the worker exited, pushing to the queue would still work (queue is
    // independent), so the test verifies the thread is still alive via a liveness probe:
    // push an update and check the dispatcher gets a response (which requires
    // the worker to still be running).
    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":6}"));

    // on_message returns {}, so no actions expected — but the worker must
    // still be alive to pop and process the update.
    // The test verifies liveness by observing that the update is consumed.
    var waited: u64 = 0;
    while (ctx.input_q.len() > 0 and waited < 1000) : (waited += 5) {
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expectEqual(@as(usize, 0), ctx.input_q.len());
}

test "worker in strict mode drops calls that fail schema validation" {
    const SCHEMA =
        \\{"methods":{"sendMessage":{"fields":[
        \\  {"name":"chat_id","types":["Integer","String"],"required":true},
        \\  {"name":"text","types":["String"],"required":true}
        \\]}},"types":{}}
    ;
    var slot = tg_schema.SchemaSlot.init(testing.allocator);
    slot.install(try tg_schema.SchemaStore.fromSlice(testing.allocator, SCHEMA));

    var ctx = try TestCtx.init(testing.allocator,
        // returns sendMessage with only chat_id — missing required `text`
        \\function on_message(u)
        \\  return { { method="sendMessage", params={ chat_id=1 } } }
        \\end
    );
    const args = WorkerArgs{
        .id               = 0,
        .rules_path       = ctx.rules_path,
        .allocator        = testing.allocator,
        .queue            = &ctx.input_q,
        .dispatcher_queue = &ctx.output_q,
        .db               = &ctx.db,
        .stop             = &ctx.stop,
        .reload_ver       = &ctx.reload_ver,
        .schema           = &slot,
        .validation       = .strict,
    };
    const t = try std.Thread.spawn(.{}, workerThread, .{args});
    // Cleanup order: join worker first, then free slot.
    defer { ctx.deinit(t); slot.deinit(); }

    std.Thread.sleep(30 * std.time.ns_per_ms);
    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":1}"));
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // strict mode: missing required `text` → call dropped, dispatcher queue stays empty
    try testing.expectEqual(@as(usize, 0), ctx.output_q.len());
}

test "worker in warn mode keeps calls that fail schema validation" {
    const SCHEMA =
        \\{"methods":{"sendMessage":{"fields":[
        \\  {"name":"chat_id","types":["Integer","String"],"required":true},
        \\  {"name":"text","types":["String"],"required":true}
        \\]}},"types":{}}
    ;
    var slot = tg_schema.SchemaSlot.init(testing.allocator);
    slot.install(try tg_schema.SchemaStore.fromSlice(testing.allocator, SCHEMA));

    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u)
        \\  return { { method="sendMessage", params={ chat_id=1 } } }
        \\end
    );
    const args = WorkerArgs{
        .id               = 0,
        .rules_path       = ctx.rules_path,
        .allocator        = testing.allocator,
        .queue            = &ctx.input_q,
        .dispatcher_queue = &ctx.output_q,
        .db               = &ctx.db,
        .stop             = &ctx.stop,
        .reload_ver       = &ctx.reload_ver,
        .schema           = &slot,
        .validation       = .warn,
    };
    const t = try std.Thread.spawn(.{}, workerThread, .{args});
    defer { ctx.deinit(t); slot.deinit(); }

    std.Thread.sleep(30 * std.time.ns_per_ms);
    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":1}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    // warn mode: invalid call is kept (logged but forwarded)
    const action = popAction(&ctx.output_q);
    defer types.freeApiCall(action, testing.allocator);
    try testing.expectEqualStrings("sendMessage", action.method);
}

test "hashUserId is deterministic — 10k ids × 8 workers always same index" {
    const WORKERS: u32 = 8;
    const N: u32 = 10_000;

    // First pass: record each id's assigned worker.
    var assignments = [_]u32{0} ** N;
    for (0..N) |i| {
        const id: i64 = @as(i64, @intCast(i)) - 5000; // range: -5000 … 4999
        assignments[i] = hashUserId(id, WORKERS);
        try testing.expect(assignments[i] < WORKERS);
    }

    // Second pass: must produce identical results.
    for (0..N) |i| {
        const id: i64 = @as(i64, @intCast(i)) - 5000;
        try testing.expectEqual(assignments[i], hashUserId(id, WORKERS));
    }

    // Also verify negative ids are handled without panic.
    try testing.expect(hashUserId(std.math.minInt(i64), WORKERS) < WORKERS);
    try testing.expect(hashUserId(-1, WORKERS) < WORKERS);
}

test "bot.http_request → coroutine yields, io_pool runs, result resumes, action delivered" {
    const HTTP_OK =
        "HTTP/1.1 200 OK\r\n" ++
        "Content-Length: 2\r\n" ++
        "Connection: close\r\n" ++
        "\r\nOK";

    const stub = try spawnStub(HTTP_OK);
    defer stub.thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  local resp = bot.http_request{{ method="GET", url="http://127.0.0.2:{d}/" }}
        \\  return {{ {{ method="reply", params={{ status=resp.status }} }} }}
        \\end
    , .{stub.port});
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 2, .queue_capacity = 16,
           .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);

    std.Thread.sleep(30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":1}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 5_000));
    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);

    try testing.expectEqualStrings("reply", action.method);
    try testing.expect(std.mem.indexOf(u8, action.payload.json, "\"status\":200") != null);
}

test "slow coroutine parked; same worker processes fast update first" {
    const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
    const slow_srv = try testing.allocator.create(std.net.Server);
    slow_srv.* = try addr.listen(.{ .reuse_address = true });
    const slow_port = std.mem.bigToNative(u16, slow_srv.listen_address.in.sa.port);
    const slow_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.net.Server) void {
            defer { s.deinit(); testing.allocator.destroy(s); }
            const conn = s.accept() catch return;
            defer conn.stream.close();
            var buf: [4096]u8 = undefined;
            _ = conn.stream.read(&buf) catch {};
            std.Thread.sleep(300 * std.time.ns_per_ms);
            conn.stream.writeAll(
                "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nslow"
            ) catch {};
        }
    }.run, .{slow_srv});
    defer slow_thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  if u.slow then
        \\    local r = bot.http_request{{ method="GET", url=u.url }}
        \\    return {{ {{ method="slow_done", params={{}} }} }}
        \\  else
        \\    return {{ {{ method="fast_done", params={{}} }} }}
        \\  end
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 2, .queue_capacity = 16, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    const slow_url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{slow_port});
    defer testing.allocator.free(slow_url);
    const slow_body = try std.fmt.allocPrint(testing.allocator,
        "{{\"slow\":true,\"url\":\"{s}\"}}", .{slow_url});
    defer testing.allocator.free(slow_body);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, slow_body));
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"slow\":false}"));

    std.Thread.sleep(100 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 1), ctx.output_q.len());
    const fast_action = ctx.output_q.pop();
    defer types.freeApiCall(fast_action, testing.allocator);
    try testing.expectEqualStrings("fast_done", fast_action.method);

    try testing.expect(waitQueue(&ctx.output_q, 1, 1_000));
    const slow_action = ctx.output_q.pop();
    defer types.freeApiCall(slow_action, testing.allocator);
    try testing.expectEqualStrings("slow_done", slow_action.method);
}

test "two sequential bot.http_request calls → two yields, correct order" {
    const HTTP_A = "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\nA";
    const HTTP_B = "HTTP/1.1 201 Created\r\nContent-Length: 1\r\nConnection: close\r\n\r\nB";

    const stub_a = try spawnStub(HTTP_A);
    const stub_b = try spawnStub(HTTP_B);
    defer stub_a.thread.join();
    defer stub_b.thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  local r1 = bot.http_request{{ method="GET", url="http://127.0.0.2:{d}/" }}
        \\  local r2 = bot.http_request{{ method="GET", url="http://127.0.0.2:{d}/" }}
        \\  return {{ {{ method="done", params={{ s1=r1.status, s2=r2.status }} }} }}
        \\end
    , .{ stub_a.port, stub_b.port });
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 2, .queue_capacity = 16, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":1}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 5_000));
    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);

    try testing.expectEqualStrings("done", action.method);
    try testing.expect(std.mem.indexOf(u8, action.payload.json, "\"s1\":200") != null);
    try testing.expect(std.mem.indexOf(u8, action.payload.json, "\"s2\":201") != null);
}

test "coroutine past WORKFLOW_DEADLINE_MS is reaped; worker continues" {
    var silent = try spawnSilentStub();
    defer silent.deinit();
    const port = std.mem.bigToNative(u16, silent.server.listen_address.in.sa.port);

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  if u.hang then
        \\    bot.http_request{{ method="GET", url=u.url }}
        \\    return {{}}
        \\  end
        \\  return {{ {{ method="ok", params={{}} }} }}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    const hang_url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(hang_url);
    const hang_body = try std.fmt.allocPrint(testing.allocator,
        "{{\"hang\":true,\"url\":\"{s}\"}}", .{hang_url});
    defer testing.allocator.free(hang_body);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 60_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 200); // 200ms deadline
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, hang_body));
    std.Thread.sleep(400 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"hang\":false}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));

    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);
    try testing.expectEqualStrings("ok", action.method);
}

test "at inflight ceiling new updates are not dequeued until a slot frees" {
    const HTTP_OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";

    const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.net.Server);
    srv.* = try addr.listen(.{ .reuse_address = true });
    const port = std.mem.bigToNative(u16, srv.listen_address.in.sa.port);
    const slow_t = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.net.Server, resp: []const u8) void {
            defer { s.deinit(); testing.allocator.destroy(s); }
            const conn = s.accept() catch return;
            defer conn.stream.close();
            var buf: [4096]u8 = undefined;
            _ = conn.stream.read(&buf) catch {};
            std.Thread.sleep(300 * std.time.ns_per_ms);
            conn.stream.writeAll(resp) catch {};
        }
    }.run, .{ srv, HTTP_OK });
    defer slow_t.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  if u.block then
        \\    bot.http_request{{ method="GET", url=u.url }}
        \\    return {{ {{ method="blocked", params={{}} }} }}
        \\  end
        \\  return {{ {{ method="queued", params={{}} }} }}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 2, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(1, 60_000); // max_inflight = 1
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    const block_url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(block_url);
    const block_body = try std.fmt.allocPrint(testing.allocator,
        "{{\"block\":true,\"url\":\"{s}\"}}", .{block_url});
    defer testing.allocator.free(block_body);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, block_body));
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"block\":false}"));

    std.Thread.sleep(100 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 0), ctx.output_q.len());

    try testing.expect(waitQueue(&ctx.output_q, 2, 2_000));
    try testing.expectEqual(@as(usize, 2), ctx.output_q.len());
}

test "bot.exec and bot.shell deliver results to coroutine" {
    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator,
        \\function on_message(u)
        \\  if u.kind == "exec" then
        \\    local r = bot.exec{ argv = { "/bin/echo", "execok" } }
        \\    return { { method="exec_result", params={ code=r.exit_code, out=r.stdout } } }
        \\  elseif u.kind == "shell" then
        \\    local r = bot.shell{ command = "echo shellok" }
        \\    return { { method="shell_result", params={ code=r.exit_code, out=r.stdout } } }
        \\  end
        \\  return {}
        \\end
    , .{ .thread_count = 2, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"kind\":\"exec\"}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 3_000));
    const exec_action = ctx.output_q.pop();
    defer types.freeApiCall(exec_action, testing.allocator);
    try testing.expectEqualStrings("exec_result", exec_action.method);
    try testing.expect(std.mem.indexOf(u8, exec_action.payload.json, "\"code\":0") != null);
    try testing.expect(std.mem.indexOf(u8, exec_action.payload.json, "execok") != null);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"kind\":\"shell\"}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 3_000));
    const shell_action = ctx.output_q.pop();
    defer types.freeApiCall(shell_action, testing.allocator);
    try testing.expectEqualStrings("shell_result", shell_action.method);
    try testing.expect(std.mem.indexOf(u8, shell_action.payload.json, "\"code\":0") != null);
    try testing.expect(std.mem.indexOf(u8, shell_action.payload.json, "shellok") != null);
}

test "hot-reload while coroutine is parked; parked completes on old rules; next update uses new rules" {
    const HTTP_OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
    const stub = try spawnStub(HTTP_OK);
    defer stub.thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  if u.fetch then
        \\    local r = bot.http_request{{ method="GET", url=u.url }}
        \\    return {{ {{ method="old_rules", params={{}} }} }}
        \\  end
        \\  return {{ {{ method="fast", params={{}} }} }}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    const url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{stub.port});
    defer testing.allocator.free(url);
    const fetch_body = try std.fmt.allocPrint(testing.allocator,
        "{{\"fetch\":true,\"url\":\"{s}\"}}", .{url});
    defer testing.allocator.free(fetch_body);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, fetch_body));
    std.Thread.sleep(30 * std.time.ns_per_ms);

    {
        var f = try ctx.tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(
            \\function on_message(u)
            \\  return { { method="new_rules", params={} } }
            \\end
        );
    }
    _ = ctx.reload_ver.fetchAdd(1, .release);

    try testing.expect(waitQueue(&ctx.output_q, 1, 5_000));
    const old_action = ctx.output_q.pop();
    defer types.freeApiCall(old_action, testing.allocator);
    try testing.expectEqualStrings("old_rules", old_action.method);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":99}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const new_action = ctx.output_q.pop();
    defer types.freeApiCall(new_action, testing.allocator);
    try testing.expectEqualStrings("new_rules", new_action.method);
}

test "Lua error in resumed step → workflow aborted, slot freed, worker continues" {
    const HTTP_OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
    const stub = try spawnStub(HTTP_OK);
    defer stub.thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  if u.crash then
        \\    local _ = bot.http_request{{ method="GET", url=u.url }}
        \\    error("intentional crash after yield")
        \\  end
        \\  return {{ {{ method="survived", params={{}} }} }}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 2, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    const url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{stub.port});
    defer testing.allocator.free(url);
    const crash_body = try std.fmt.allocPrint(testing.allocator,
        "{{\"crash\":true,\"url\":\"{s}\"}}", .{url});
    defer testing.allocator.free(crash_body);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, crash_body));
    std.Thread.sleep(500 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"crash\":false}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));

    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);
    try testing.expectEqualStrings("survived", action.method);

    try testing.expectEqual(@as(usize, 0), ctx.output_q.len());
}

test "bot.send_message → worker parks, dispatcher result → coroutine gets message_id" {
    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator,
        \\function on_message(u)
        \\  local mid = bot.send_message{ chat_id = 1, text = "placeholder" }
        \\  bot.emit{ method = "confirm", params = { mid = mid } }
        \\  return {}
        \\end
    , .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":1}"));

    // Worker parks: it pushes the tracked ApiCall to output_q (dispatcher queue).
    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const tracked_call = ctx.output_q.pop();
    defer types.freeApiCall(tracked_call, testing.allocator);

    try testing.expectEqualStrings("sendMessage", tracked_call.method);
    try testing.expect(tracked_call.tracking != null);
    const tracking = tracked_call.tracking.?;
    try testing.expect(std.mem.indexOf(u8, tracked_call.payload.json, "\"chat_id\":1") != null);

    // Simulate dispatcher success: push IoResult{ .send = .{ message_id = 42 } }.
    try ctx.result_q.push(.{
        .coro_id = tracking.coro_id,
        .outcome = .{ .send = .{ .message_id = 42 } },
    });

    // Worker resumes; Lua emits "confirm" with mid=42.
    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const confirm = ctx.output_q.pop();
    defer types.freeApiCall(confirm, testing.allocator);

    try testing.expectEqualStrings("confirm", confirm.method);
    try testing.expect(std.mem.indexOf(u8, confirm.payload.json, "\"mid\":42") != null);
}

test "bot.send_message mid used in editMessageText — end-to-end chain" {
    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator,
        \\function on_message(u)
        \\  local mid = bot.send_message{ chat_id = 7, text = "searching..." }
        \\  bot.emit{ method = "editMessageText", params = { chat_id = 7, message_id = mid, text = "done" } }
        \\  return {}
        \\end
    , .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":2}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const send_call = ctx.output_q.pop();
    defer types.freeApiCall(send_call, testing.allocator);
    try testing.expectEqualStrings("sendMessage", send_call.method);
    const tracking = send_call.tracking.?;

    try ctx.result_q.push(.{
        .coro_id = tracking.coro_id,
        .outcome = .{ .send = .{ .message_id = 99 } },
    });

    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const edit_call = ctx.output_q.pop();
    defer types.freeApiCall(edit_call, testing.allocator);
    try testing.expectEqualStrings("editMessageText", edit_call.method);
    try testing.expect(std.mem.indexOf(u8, edit_call.payload.json, "\"message_id\":99") != null);
    try testing.expect(std.mem.indexOf(u8, edit_call.payload.json, "\"text\":\"done\"") != null);
}

test "tracked send failure → Lua error raised; pcall catches; worker continues" {
    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator,
        \\function on_message(u)
        \\  local ok, err = pcall(bot.send_message, { chat_id = 1, text = "hi" })
        \\  if not ok then
        \\    bot.emit{ method = "error_handled", params = { msg = err } }
        \\  end
        \\  return {}
        \\end
    , .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":3}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const send_call = ctx.output_q.pop();
    const tracking = send_call.tracking.?;
    types.freeApiCall(send_call, testing.allocator);

    // Simulate dispatcher failure — err string is owned (freeIoResult frees it).
    const err_msg = try testing.allocator.dupe(u8, "HTTP 500");
    try ctx.result_q.push(.{
        .coro_id = tracking.coro_id,
        .outcome = .{ .err = err_msg },
    });

    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const handled = ctx.output_q.pop();
    defer types.freeApiCall(handled, testing.allocator);
    try testing.expectEqualStrings("error_handled", handled.method);
    try testing.expect(std.mem.indexOf(u8, handled.payload.json, "HTTP 500") != null);

    // Worker continues: process another update.
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":4}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const next = ctx.output_q.pop();
    defer types.freeApiCall(next, testing.allocator);
    // The error path resumes and returns {} (empty actions), then the next update
    // goes through the same rule — should emit error_handled again (since pcall
    // always gets called). Just verify the worker didn't crash.
    try testing.expect(next.method.len > 0);
}

test "bot.emit send_message (fire-and-forget) does not park coroutine" {
    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator,
        \\function on_message(u)
        \\  bot.emit{ method = "sendMessage", params = { chat_id = 1, text = "ff" } }
        \\  return {}
        \\end
    , .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":5}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);

    // Fire-and-forget: no tracking field; result_q stays empty.
    try testing.expect(action.tracking == null);
    try testing.expectEqual(@as(usize, 0), ctx.result_q.len());
}

test "sync rule (no bot.send_message) completes in one resume — no regression" {
    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator,
        \\function on_message(u)
        \\  bot.emit{ method = "echo", params = {} }
        \\  return {}
        \\end
    , .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":6}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1_000));
    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);
    try testing.expectEqualStrings("echo", action.method);
    try testing.expectEqual(@as(usize, 0), ctx.result_q.len());
}

test "coroutines_inflight / coroutines_reaped_total metrics" {
    var m = metrics_mod.Metrics{};

    // Use spawnHangStub (not spawnSilentStub): it holds the connection open
    // so the coroutine stays parked long enough to verify the metrics, and it
    // is safe to deinit() before ctx.deinit() to unblock the io_pool thread.
    var silent = try spawnHangStub();
    const port = std.mem.bigToNative(u16, silent.server.listen_address.in.sa.port);

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  bot.http_request{{ method="GET", url=u.url }}
        \\  return {{}}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    const hang_url = try std.fmt.allocPrint(testing.allocator,
        "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(hang_url);
    const body = try std.fmt.allocPrint(testing.allocator,
        "{{\"url\":\"{s}\"}}", .{hang_url});
    defer testing.allocator.free(body);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 60_000, .proc_max_output = 65_536 });

    const t = try std.Thread.spawn(.{}, workerThread, .{WorkerArgs{
        .id                   = 0,
        .rules_path           = ctx.rules_path,
        .allocator            = ctx.allocator,
        .queue                = &ctx.input_q,
        .dispatcher_queue     = &ctx.output_q,
        .db                   = &ctx.db,
        .stop                 = &ctx.stop,
        .reload_ver           = &ctx.reload_ver,
        .io_pool_ptr          = &ctx.pool,
        .io_result_queue      = &ctx.result_q,
        .max_inflight         = 64,
        .workflow_deadline_ms = 200, // short deadline for test speed
        .metrics              = &m,
    }});

    std.Thread.sleep(30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, body));

    // Give the worker time to park the coroutine.
    std.Thread.sleep(100 * std.time.ns_per_ms);
    const inflight_while_parked = m.coroutines_inflight.load(.monotonic);

    // Wait for deadline reap (200ms deadline + 100ms margin).
    std.Thread.sleep(400 * std.time.ns_per_ms);
    const reaped = m.coroutines_reaped_total.load(.monotonic);
    const inflight_after_reap = m.coroutines_inflight.load(.monotonic);

    // Close the stub server first so the io_pool HTTP thread unblocks before
    // ctx.deinit() tries to join the pool threads.
    silent.deinit();
    ctx.deinit(t);

    try testing.expectEqual(@as(i64, 1), inflight_while_parked);
    try testing.expectEqual(@as(u64, 1), reaped);
    try testing.expectEqual(@as(i64, 0), inflight_after_reap);
}

// ── graceful drain on stop ───────────────────────────────────────────────────

test "worker drains in-flight coroutines on stop; exits cleanly" {
    var silent = try spawnHangStub();
    const port = std.mem.bigToNative(u16, silent.server.listen_address.in.sa.port);

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  bot.http_request{{ method="GET", url=u.url }}
        \\  return {{}}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    const hang_url = try std.fmt.allocPrint(testing.allocator,
        "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(hang_url);
    const body = try std.fmt.allocPrint(testing.allocator,
        "{{\"url\":\"{s}\"}}", .{hang_url});
    defer testing.allocator.free(body);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 60_000, .proc_max_output = 65_536 });

    const t = try std.Thread.spawn(.{}, workerThread, .{WorkerArgs{
        .id                   = 0,
        .rules_path           = ctx.rules_path,
        .allocator            = ctx.allocator,
        .queue                = &ctx.input_q,
        .dispatcher_queue     = &ctx.output_q,
        .db                   = &ctx.db,
        .stop                 = &ctx.stop,
        .reload_ver           = &ctx.reload_ver,
        .io_pool_ptr          = &ctx.pool,
        .io_result_queue      = &ctx.result_q,
        .max_inflight         = 64,
        .workflow_deadline_ms = 5_000,
    }});

    std.Thread.sleep(30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, body));

    // Let the coroutine park on the hang stub.
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Signal stop while coroutine is in-flight — the worker emits the drain log.
    ctx.stop.store(true, .release);

    // Close stub so io_pool thread unblocks and pushes an error IoResult;
    // the worker drains the coroutine via the error resume path.
    silent.deinit();

    // ctx.deinit joins the worker (calls stop.store again, idempotent).
    ctx.deinit(t);
}

// ── exit bounded by WORKFLOW_DEADLINE_MS ─────────────────────────────────────

test "state written in sync handler is committed after .done" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u)
        \\    bot.set_user_state(1, {x=42})
        \\    return { { method="sendMessage", params={chat_id=1, text="ok"} } }
        \\end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    std.Thread.sleep(30 * std.time.ns_per_ms);
    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":1}"));

    // Wait for the dispatched action — proves .done path completed.
    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));
    const action = popAction(&ctx.output_q);
    types.freeApiCall(action, testing.allocator);

    const data = try ctx.db.getUserState(1);
    defer testing.allocator.free(data);
    try testing.expect(std.mem.indexOf(u8, data, "\"x\":42") != null);
}

test "state written before Lua error is rolled back" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u)
        \\    bot.set_user_state(1, {x=99})
        \\    error("deliberate error to test rollback")
        \\end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    // Set initial committed state before pushing the message.
    try ctx.db.setUserState(1, "{\"x\":0}");

    std.Thread.sleep(30 * std.time.ns_per_ms);
    try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":1}"));

    // No output is dispatched on Lua error — wait long enough for processing.
    std.Thread.sleep(150 * std.time.ns_per_ms);

    const data = try ctx.db.getUserState(1);
    defer testing.allocator.free(data);
    // Rollback must have reverted the {x:99} write.
    try testing.expect(std.mem.indexOf(u8, data, "\"x\":99") == null);
    try testing.expect(std.mem.indexOf(u8, data, "\"x\":0") != null);
}

test "worker exits within WORKFLOW_DEADLINE_MS + 200ms with non-responding stub" {
    var silent = try spawnHangStub();
    const port = std.mem.bigToNative(u16, silent.server.listen_address.in.sa.port);

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  bot.http_request{{ method="GET", url=u.url }}
        \\  return {{}}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    const hang_url = try std.fmt.allocPrint(testing.allocator,
        "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(hang_url);
    const body = try std.fmt.allocPrint(testing.allocator,
        "{{\"url\":\"{s}\"}}", .{hang_url});
    defer testing.allocator.free(body);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src,
        .{ .thread_count = 1, .queue_capacity = 8,
           .timeout_ms = 60_000, .proc_max_output = 65_536 });

    const t = try std.Thread.spawn(.{}, workerThread, .{WorkerArgs{
        .id                   = 0,
        .rules_path           = ctx.rules_path,
        .allocator            = ctx.allocator,
        .queue                = &ctx.input_q,
        .dispatcher_queue     = &ctx.output_q,
        .db                   = &ctx.db,
        .stop                 = &ctx.stop,
        .reload_ver           = &ctx.reload_ver,
        .io_pool_ptr          = &ctx.pool,
        .io_result_queue      = &ctx.result_q,
        .max_inflight         = 64,
        .workflow_deadline_ms = 200, // short deadline for this test
    }});

    std.Thread.sleep(30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, body));

    // Let the coroutine park on the hang stub.
    std.Thread.sleep(100 * std.time.ns_per_ms);

    // Signal stop WITHOUT closing the stub. The worker must drain via deadline reap.
    ctx.stop.store(true, .release);
    const t0 = std.time.milliTimestamp();
    t.join();
    const elapsed_ms = std.time.milliTimestamp() - t0;

    // Close stub after worker exits to unblock the io_pool thread.
    silent.deinit();

    // Manual cleanup (ctx.deinit can't be used: t was already joined above).
    // pool.deinit() joins the pool thread first so all pushErr calls complete
    // before result_q is drained — avoids a race with popTimeout(0).
    ctx.pool.deinit();
    while (ctx.input_q.popTimeout(0))  |item| ctx.allocator.free(item.body);
    while (ctx.output_q.popTimeout(0)) |call| types.freeApiCall(call, ctx.allocator);
    while (ctx.result_q.popTimeout(0)) |res|  io_pool.freeIoResult(res, ctx.allocator);
    ctx.result_q.deinit(ctx.allocator);
    ctx.output_q.deinit(ctx.allocator);
    ctx.input_q.deinit(ctx.allocator);
    ctx.db.close();
    ctx.tmp.cleanup();

    // worker must exit within WORKFLOW_DEADLINE_MS + 200ms epsilon.
    try testing.expect(elapsed_ms < 400);
}
