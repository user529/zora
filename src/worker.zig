/// worker.zig — worker thread: owns lua_State + SQLite connection
///
/// Each worker runs in a dedicated thread and owns:
///   - a LuaEngine (Lua state + on_message function)
///   - a StateStore connection (WAL allows concurrent readers)
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
const rt = @import("rt.zig");
const scheduler = @import("scheduler.zig");

const log = std.log.scoped(.worker);

/// Maximum file size for multipart uploads — the Telegram bot upload limit.
const MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;

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
    /// Runtime for blocking I/O (Lua file reads), sleeps, and wall-clock reads.
    io: std.Io,
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
    /// Scheduler handle so bot.schedule_* can wake the timer after inserting a job.
    scheduler: ?*scheduler.Scheduler = null,
    /// Test-only deterministic clock for the coroutine reaper. When non-null the
    /// worker reads the workflow deadline and the current time from this counter
    /// instead of the wall clock, so a test fires the deadline by advancing the
    /// counter rather than sleeping past a real-time margin. Null in production:
    /// the worker then uses the monotonic wall clock via rt.nowMs.
    clock_ms: ?*std.atomic.Value(i64) = null,
};

/// Current time in milliseconds for deadline math. Returns the injected test
/// clock when present, otherwise the process wall clock. The reaper and the
/// at-yield deadline computation share this one source so an injected clock
/// governs both consistently.
fn workerNowMs(args: *const WorkerArgs) i64 {
    if (args.clock_ms) |clk| return clk.load(.acquire);
    return rt.nowMs(args.io);
}

// ---------------------------------------------------------------------------
// In-flight coroutine tracking
// ---------------------------------------------------------------------------

const InFlightEntry = struct {
    handle: lua_engine.CoroHandle,
    deadline_ms: i64,
    owned_strings: lua_api.OwnedStrings,
    is_tracked_send: bool = false,
    schedule_id: ?i64 = null,
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
    inflight: *InFlightMap,
    cid: u32,
    main: *ziglua.Lua,
    allocator: std.mem.Allocator,
    metrics: ?*metrics_mod.Metrics,
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
        .db = args.db,
        .allocator = args.allocator,
        .io = args.io,
        .max_file_bytes = MAX_UPLOAD_BYTES,
        .json_max_bytes = args.json_max_bytes,
        .scheduler = args.scheduler,
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
                    // Wrap the resumed segment in its own transaction so its state
                    // writes are atomic rather than silent per-statement
                    // autocommits — the resume counterpart of the start path's
                    // begin/commit. DEFERRED takes the write lock only if the
                    // segment writes, so a read-only or send-only resume never
                    // blocks another worker's writer. The transaction is held only
                    // during synchronous execution — committed before the next
                    // yield (below) or on .done — so it is never open across I/O.
                    // A begin failure is non-fatal: the segment then runs in
                    // autocommit (the prior behavior) rather than abandoning a
                    // parked coroutine. Segments do not span a yield, so this does
                    // not make a cross-yield read-modify-write atomic.
                    var in_txn = true;
                    args.db.beginDeferred() catch |err| {
                        log.err("worker {d}: BEGIN before resume failed: {s} — resuming in autocommit", .{ args.id, @errorName(err) });
                        in_txn = false;
                    };
                    // resumeHandler always frees `owned` and calls lua.unref
                    // for .done and .err outcomes; for .yielded it does neither.
                    const outcome = engine.resumeHandler(
                        entry.handle,
                        result,
                        owned,
                        entry.is_tracked_send,
                        args.allocator,
                    ) catch |err| blk: {
                        log.err("worker {d}: resumeHandler OOM: {s}", .{ args.id, @errorName(err) });
                        // resumeHandler freed owned + called unref before returning error.
                        break :blk lua_engine.CoroOutcome.err;
                    };
                    io_pool.freeIoResult(result, args.allocator);

                    switch (outcome) {
                        .done => |actions| {
                            // resumeHandler already called lua.unref + freeOwnedStrings.
                            const sid = entry.schedule_id;
                            _ = inflight.remove(cid);
                            decInflight(args.metrics);
                            // Delete the schedule row before commit so it is included
                            // in the open transaction when in_txn == true (a commit
                            // failure rolls it back, preserving at-least-once). When
                            // in_txn == false (autocommit fallback) the delete still
                            // runs so a completed job is not re-fired unnecessarily.
                            if (sid) |s| _ = args.db.scheduleDelete(s) catch |err| {
                                log.warn("worker {d}: schedule delete failed (will re-fire): {s}", .{ args.id, @errorName(err) });
                            };
                            // Commit the segment's state writes before emitting its
                            // actions. If the commit fails the state is gone, so the
                            // actions (which assume it) are dropped, not sent.
                            const commit_ok = if (in_txn) blk: {
                                args.db.commit() catch |err| {
                                    // Recoverable: roll back, drop this segment's
                                    // actions, and continue. Logged at warn (not
                                    // err) so deterministic fault-injection tests
                                    // can assert this path. warn is the level for an
                                    // expected, handled failure.
                                    log.warn("worker {d}: COMMIT after resume failed: {s}", .{ args.id, @errorName(err) });
                                    args.db.rollback();
                                    break :blk false;
                                };
                                break :blk true;
                            } else true;
                            if (commit_ok) {
                                for (actions) |action| {
                                    args.dispatcher_queue.push(action) catch {
                                        log.warn("worker {d}: dispatcher queue full", .{args.id});
                                        types.freeApiCall(action, args.allocator);
                                    };
                                }
                            } else {
                                for (actions) |action| types.freeApiCall(action, args.allocator);
                            }
                            args.allocator.free(actions);
                        },
                        .yielded => |y| {
                            // Commit the just-finished segment before re-parking:
                            // the next I/O wait must not hold a transaction. On
                            // failure, log and proceed — the next segment opens a
                            // fresh transaction on resume.
                            if (in_txn) args.db.commit() catch |err| {
                                log.err("worker {d}: COMMIT before re-yield failed: {s}", .{ args.id, @errorName(err) });
                            };
                            // resumeHandler freed old owned_strings; new ones are in y.
                            // Update the inflight entry for the next yield type.
                            // `entry` is still valid here: nothing mutates the map
                            // between the lookup above and this update.
                            switch (y.pending_job) {
                                .io => |io| {
                                    entry.owned_strings = io.owned_strings;
                                    entry.is_tracked_send = false;
                                    if (args.io_pool_ptr) |pool| {
                                        pool.submit(io.io_job) catch {
                                            log.warn("worker {d}: io_pool full; dropping coro {d}", .{ args.id, cid });
                                            // entry.owned_strings == io.owned_strings — dropInflight frees it.
                                            _ = dropInflight(&inflight, cid, engine.lua, args.allocator, args.metrics);
                                        };
                                    }
                                },
                                .tracked_send => |api_call| {
                                    entry.owned_strings = .none;
                                    entry.is_tracked_send = true;
                                    args.dispatcher_queue.push(api_call) catch {
                                        log.warn("worker {d}: dispatcher queue full; dropping re-yield coro {d}", .{ args.id, cid });
                                        _ = dropInflight(&inflight, cid, engine.lua, args.allocator, args.metrics);
                                        types.freeApiCall(api_call, args.allocator);
                                    };
                                },
                            }
                        },
                        .err => {
                            // resumeHandler already called lua.unref + freeOwnedStrings.
                            if (in_txn) args.db.rollback();
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
            const now_ms = workerNowMs(&args);
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
                log.info("worker {d}: stopping — draining {d} in-flight coroutine(s)", .{ args.id, inflight.count() });
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
            rt.sleepNs(args.io, 1 * std.time.ns_per_ms);
            continue;
        }
        {
            // With nothing in flight, no io_result or deadline can occur — both
            // belong to parked coroutines (Steps 1–2) — so park indefinitely on
            // the update queue rather than polling: an idle worker then consumes
            // no CPU and is released only by a push or by close() at shutdown.
            // With work in flight, keep the 10 ms wait so the next iteration
            // services resumes and deadline reaps promptly.
            const maybe_item = if (inflight.count() == 0)
                args.queue.popBlocking()
            else
                args.queue.popTimeout(10 * std.time.ns_per_ms);
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

            const handler: [:0]const u8 = switch (item.kind) {
                .message  => "on_message",
                .schedule => "on_schedule",
            };
            const outcome = engine.startHandler(item.body, cid, args.id, args.allocator, handler, item.schedule_id) catch |err| {
                log.err("worker {d}: startHandler OOM: {s}", .{ args.id, @errorName(err) });
                args.db.rollback();
                continue;
            };

            switch (outcome) {
                .done => |actions| {
                    if (item.schedule_id) |sid| _ = args.db.scheduleDelete(sid) catch |err| {
                        log.warn("worker {d}: schedule delete failed (will re-fire): {s}", .{ args.id, @errorName(err) });
                    };
                    args.db.commit() catch |err| {
                        // Recoverable: roll back (which also un-deletes any
                        // schedule row deleted above, preserving at-least-once),
                        // drop this handler's actions, and move on. Logged at warn
                        // (not err) so deterministic fault-injection tests can
                        // assert this path.
                        log.warn("worker {d}: COMMIT failed: {s}", .{ args.id, @errorName(err) });
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
                    const deadline = workerNowMs(&args) +
                        @as(i64, @intCast(args.workflow_deadline_ms));
                    switch (y.pending_job) {
                        .io => |io| {
                            inflight.put(cid, InFlightEntry{
                                .handle = y.handle,
                                .deadline_ms = deadline,
                                .owned_strings = io.owned_strings,
                                .is_tracked_send = false,
                                .schedule_id = item.schedule_id,
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
                                .handle = y.handle,
                                .deadline_ms = deadline,
                                .owned_strings = .none,
                                .is_tracked_send = true,
                                .schedule_id = item.schedule_id,
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
    const start = rt.nowMs(testing.io);
    while (q.len() < n) {
        if (@as(u64, @intCast(rt.nowMs(testing.io) - start)) >= timeout_ms) return false;
        rt.sleepNs(testing.io, 2 * std.time.ns_per_ms);
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

// 0.16's net.Server no longer exposes the bound address; query the socket for
// the port assigned to a port-0 listen. Returns native byte order.
fn boundPort(server: *const std.Io.net.Server) u16 {
    var sa: std.posix.sockaddr.in = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    _ = std.posix.system.getsockname(server.socket.handle, @ptrCast(&sa), &len);
    return std.mem.bigToNative(u16, sa.port);
}

/// Drain an HTTP request head line-by-line (keep-alive clients never EOF, so a
/// single readSliceShort would block forever trying to fill its buffer).
fn drainRequestHead(reader: *std.Io.Reader) void {
    while (reader.takeDelimiterInclusive('\n')) |line| {
        if (line.len <= 2) break; // "\r\n" ends the headers
    } else |_| {}
}

/// One-shot stub: accepts one connection, responds immediately.
fn spawnStub(response: []const u8) !StubServer {
    const io = testing.io;
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.Io.net.Server);
    srv.* = try addr.listen(io, .{ .reuse_address = true });
    const port = boundPort(srv);
    const t = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.Io.net.Server, resp: []const u8) void {
            const cio = testing.io;
            defer {
                s.deinit(cio);
                testing.allocator.destroy(s);
            }
            var stream = s.accept(cio) catch return;
            defer stream.close(cio);
            var rbuf: [4096]u8 = undefined;
            var sr = stream.reader(cio, &rbuf);
            drainRequestHead(&sr.interface);
            var wbuf: [4096]u8 = undefined;
            var sw = stream.writer(cio, &wbuf);
            sw.interface.writeAll(resp) catch {};
            sw.interface.flush() catch {};
        }
    }.run, .{ srv, response });
    return .{ .port = port, .thread = t };
}

/// Multi-shot stub: serves `n` connections one at a time, sleeping `delay_ms`
/// before each response. The serial accept keeps later coroutines parked while
/// earlier ones resume, so a worker's inflight map stays populated.
fn spawnMultiStub(n: usize, delay_ms: u64, response: []const u8) !StubServer {
    const io = testing.io;
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.Io.net.Server);
    srv.* = try addr.listen(io, .{ .reuse_address = true, .kernel_backlog = 64 });
    const port = boundPort(srv);
    const t = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.Io.net.Server, count: usize, dms: u64, resp: []const u8) void {
            const cio = testing.io;
            defer {
                s.deinit(cio);
                testing.allocator.destroy(s);
            }
            for (0..count) |_| {
                var stream = s.accept(cio) catch return;
                defer stream.close(cio);
                var rbuf: [4096]u8 = undefined;
                var sr = stream.reader(cio, &rbuf);
                drainRequestHead(&sr.interface);
                if (dms > 0) rt.sleepNs(cio, dms * std.time.ns_per_ms);
                var wbuf: [4096]u8 = undefined;
                var sw = stream.writer(cio, &wbuf);
                sw.interface.writeAll(resp) catch {};
                sw.interface.flush() catch {};
            }
        }
    }.run, .{ srv, n, delay_ms, response });
    return .{ .port = port, .thread = t };
}

/// Silent stub: accepts one connection, never responds.
const SilentStub = struct {
    server: *std.Io.net.Server,
    thread: std.Thread,
    fn deinit(self: *SilentStub) void {
        const io = testing.io;
        // shutdown(SHUT_RD) reliably unblocks any thread blocked in accept()
        // on this socket before closing the fd. 0.16 exposes shutdown on Stream,
        // so wrap the listening socket to reach it.
        var s = std.Io.net.Stream{ .socket = self.server.socket };
        s.shutdown(io, .recv) catch {};
        self.server.deinit(io); // close socket
        self.thread.join(); // wait for thread to exit before freeing memory
        testing.allocator.destroy(self.server);
    }
};

fn spawnSilentStub() !SilentStub {
    const io = testing.io;
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.Io.net.Server);
    srv.* = try addr.listen(io, .{ .reuse_address = true });
    const t = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.Io.net.Server) void {
            const cio = testing.io;
            var stream = s.accept(cio) catch return;
            var rbuf: [1]u8 = undefined;
            var sr = stream.reader(cio, &rbuf);
            var b: [1]u8 = undefined;
            _ = sr.interface.readSliceShort(&b) catch {};
            stream.close(cio);
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
    const io = testing.io;
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.Io.net.Server);
    srv.* = try addr.listen(io, .{ .reuse_address = true });
    const t = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.Io.net.Server) void {
            const cio = testing.io;
            var stream = s.accept(cio) catch return;
            defer stream.close(cio);
            // Drain the HTTP request so the client's write completes;
            // the client then blocks waiting for a response.
            var rbuf: [4096]u8 = undefined;
            var sr = stream.reader(cio, &rbuf);
            drainRequestHead(&sr.interface);
            // Block until deinit() closes the server socket.
            _ = s.accept(cio) catch {};
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
    tmp: testing.TmpDir,
    db: state_store.StateStore,
    input_q: Queue(types.WorkItem),
    output_q: Queue(types.ApiCall),
    result_q: Queue(io_pool.IoResult),
    /// Stable single-element slice of result queue pointers passed to pool.init.
    /// Must be a field (not a local) so its address remains valid after init returns.
    rq_ptrs: [1]*Queue(io_pool.IoResult),
    pool: io_pool.IoPool,
    stop: std.atomic.Value(bool),
    reload_ver: std.atomic.Value(u64),
    /// Deterministic clock for tests that fire the coroutine reaper structurally.
    /// Default-initialised here; a test passes its address to spawnWorkerClock
    /// and advances it instead of sleeping past the wall-clock deadline.
    clock: std.atomic.Value(i64),
    path_buf: [std.fs.max_path_bytes + 1]u8,
    rules_path: [:0]const u8,
    allocator: std.mem.Allocator,

    /// Initialise in-place.  `self` must already be at its final stack address.
    fn init(
        self: *AsyncTestCtx,
        allocator: std.mem.Allocator,
        lua_src: []const u8,
        pool_cfg: io_pool.IoPoolConfig,
    ) !void {
        self.allocator = allocator;
        self.tmp = testing.tmpDir(.{});
        errdefer self.tmp.cleanup();

        try self.tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = lua_src });

        const path_len = try self.tmp.dir.realPathFile(testing.io, "rules.lua", self.path_buf[0..std.fs.max_path_bytes]);
        self.path_buf[path_len] = 0;
        self.rules_path = self.path_buf[0..path_len :0];

        self.db = try state_store.StateStore.open(allocator, ":memory:");
        errdefer self.db.close();

        self.input_q = try Queue(types.WorkItem).init(allocator, testing.io, 64);
        errdefer self.input_q.deinit(allocator);

        self.output_q = try Queue(types.ApiCall).init(allocator, testing.io, 256);
        errdefer self.output_q.deinit(allocator);

        self.result_q = try Queue(io_pool.IoResult).init(allocator, testing.io, 256);
        errdefer self.result_q.deinit(allocator);
        self.result_q.kind = .io_result;
        self.result_q.id = 0; // single-worker test context (spawnWorker hardcodes id=0)

        // rq_ptrs[0] points into self.result_q — stable because self is at its
        // final address (pointer receiver, declared before init is called).
        self.rq_ptrs[0] = &self.result_q;
        try self.pool.init(allocator, testing.io, pool_cfg, &self.rq_ptrs);
        errdefer self.pool.deinit();

        self.stop = std.atomic.Value(bool).init(false);
        self.reload_ver = std.atomic.Value(u64).init(0);
        self.clock = std.atomic.Value(i64).init(0);
    }

    fn spawnWorker(self: *AsyncTestCtx, max_inflight: u16, workflow_deadline_ms: u64) !std.Thread {
        return self.spawnWorkerClock(max_inflight, workflow_deadline_ms, null);
    }

    /// Spawn the worker, optionally driving its reaper from `clock` (the test's
    /// deterministic clock). Pass null to use the wall clock (most tests).
    fn spawnWorkerClock(
        self: *AsyncTestCtx,
        max_inflight: u16,
        workflow_deadline_ms: u64,
        clock: ?*std.atomic.Value(i64),
    ) !std.Thread {
        return std.Thread.spawn(.{}, workerThread, .{WorkerArgs{
            .id = 0,
            .rules_path = self.rules_path,
            .allocator = self.allocator,
            .io = testing.io,
            .queue = &self.input_q,
            .dispatcher_queue = &self.output_q,
            .db = &self.db,
            .stop = &self.stop,
            .reload_ver = &self.reload_ver,
            .io_pool_ptr = &self.pool,
            .io_result_queue = &self.result_q,
            .max_inflight = max_inflight,
            .workflow_deadline_ms = workflow_deadline_ms,
            .clock_ms = clock,
        }});
    }

    fn deinit(self: *AsyncTestCtx, t: std.Thread) void {
        self.stop.store(true, .release);
        self.input_q.close(); // wake the worker if it parked idle in popBlocking
        t.join();
        while (self.input_q.popTimeout(0)) |item| self.allocator.free(item.body);
        while (self.output_q.popTimeout(0)) |call| types.freeApiCall(call, self.allocator);
        // Quiesce the io_pool before draining its result queue, mirroring the
        // production shutdown order (main.zig: pool.deinit() then drain result_qs).
        // deinit() joins the io threads and SIGKILLs in-flight children, so any
        // final result — e.g. a "timeout"/error pushErr for a coroutine that was
        // already reaped — is in the queue by the time we drain. Draining before
        // the join races that late push and leaks the result's owned string.
        self.pool.deinit();
        while (self.result_q.popTimeout(0)) |res| io_pool.freeIoResult(res, self.allocator);
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
        return initWithDb(allocator, lua_src, ":memory:");
    }

    fn initWithDb(allocator: std.mem.Allocator, lua_src: []const u8, db_path: [:0]const u8) !TestCtx {
        var self: TestCtx = undefined;
        self.allocator = allocator;
        self.tmp = testing.tmpDir(.{});
        errdefer self.tmp.cleanup();

        // Write initial rules file.
        try self.tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = lua_src });

        // Resolve absolute path (null-terminated).
        const path_len = try self.tmp.dir.realPathFile(testing.io, "rules.lua", self.path_buf[0..std.fs.max_path_bytes]);
        self.path_buf[path_len] = 0;
        self.rules_path = self.path_buf[0..path_len :0];

        self.db = try state_store.StateStore.open(allocator, db_path);
        errdefer self.db.close();

        self.input_q = try Queue(types.WorkItem).init(allocator, testing.io, 64);
        errdefer self.input_q.deinit(allocator);

        self.output_q = try Queue(types.ApiCall).init(allocator, testing.io, 256);
        errdefer self.output_q.deinit(allocator);

        self.stop = std.atomic.Value(bool).init(false);
        self.reload_ver = std.atomic.Value(u64).init(0);
        return self;
    }

    fn spawnWorker(self: *TestCtx) !std.Thread {
        const args = WorkerArgs{
            .id = 0,
            .rules_path = self.rules_path,
            .allocator = self.allocator,
            .io = testing.io,
            .queue = &self.input_q,
            .dispatcher_queue = &self.output_q,
            .db = &self.db,
            .stop = &self.stop,
            .reload_ver = &self.reload_ver,
        };
        return std.Thread.spawn(.{}, workerThread, .{args});
    }

    fn deinit(self: *TestCtx, t: std.Thread) void {
        self.stop.store(true, .release);
        self.input_q.close(); // wake the worker if it parked idle in popBlocking
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
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

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
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    // Overwrite rules file with v2.
    {
        try ctx.tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data =
            \\function on_message(u)
            \\  return { { method="sendMessage", params={ chat_id=1, text="v2" } } }
            \\end
        });
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

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

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

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

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

    rt.sleepNs(testing.io, 200 * std.time.ns_per_ms);

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
        rt.sleepNs(testing.io, 5 * std.time.ns_per_ms);
    }
    try testing.expectEqual(@as(usize, 0), ctx.input_q.len());
}

test "schema validation mode drops or keeps a failing call" {
    const SCHEMA =
        \\{"methods":{"sendMessage":{"fields":[
        \\  {"name":"chat_id","types":["Integer","String"],"required":true},
        \\  {"name":"text","types":["String"],"required":true}
        \\]}},"types":{}}
    ;
    // The rule returns sendMessage with only chat_id — the required `text` is missing.
    const RULE =
        \\function on_message(u)
        \\  return { { method="sendMessage", params={ chat_id=1 } } }
        \\end
    ;

    // strict mode: the invalid call is dropped, the dispatcher queue stays empty.
    {
        var slot = tg_schema.SchemaSlot.init(testing.allocator, testing.io);
        slot.install(try tg_schema.SchemaStore.fromSlice(testing.allocator, SCHEMA));
        var ctx = try TestCtx.init(testing.allocator, RULE);
        const args = WorkerArgs{
            .id = 0,
            .rules_path = ctx.rules_path,
            .allocator = testing.allocator,
            .io = testing.io,
            .queue = &ctx.input_q,
            .dispatcher_queue = &ctx.output_q,
            .db = &ctx.db,
            .stop = &ctx.stop,
            .reload_ver = &ctx.reload_ver,
            .schema = &slot,
            .validation = .strict,
        };
        const t = try std.Thread.spawn(.{}, workerThread, .{args});
        // Cleanup order: join worker first, then free slot.
        defer {
            ctx.deinit(t);
            slot.deinit();
        }

        rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
        try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":1}"));
        rt.sleepNs(testing.io, 100 * std.time.ns_per_ms);
        try testing.expectEqual(@as(usize, 0), ctx.output_q.len());
    }
    // warn mode: the same invalid call is kept (logged but forwarded).
    {
        var slot = tg_schema.SchemaSlot.init(testing.allocator, testing.io);
        slot.install(try tg_schema.SchemaStore.fromSlice(testing.allocator, SCHEMA));
        var ctx = try TestCtx.init(testing.allocator, RULE);
        const args = WorkerArgs{
            .id = 0,
            .rules_path = ctx.rules_path,
            .allocator = testing.allocator,
            .io = testing.io,
            .queue = &ctx.input_q,
            .dispatcher_queue = &ctx.output_q,
            .db = &ctx.db,
            .stop = &ctx.stop,
            .reload_ver = &ctx.reload_ver,
            .schema = &slot,
            .validation = .warn,
        };
        const t = try std.Thread.spawn(.{}, workerThread, .{args});
        defer {
            ctx.deinit(t);
            slot.deinit();
        }

        rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
        try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":1}"));
        try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

        const action = popAction(&ctx.output_q);
        defer types.freeApiCall(action, testing.allocator);
        try testing.expectEqualStrings("sendMessage", action.method);
    }
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
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 2, .queue_capacity = 16, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":1}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 5_000));
    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);

    try testing.expectEqualStrings("reply", action.method);
    try testing.expect(std.mem.indexOf(u8, action.payload.json, "\"status\":200") != null);
}

test "bot.http_request exposes response headers with case-insensitive lookup" {
    const HTTP_OK =
        "HTTP/1.1 200 OK\r\n" ++
        "Content-Type: application/json\r\n" ++
        "X-Trace: xyz\r\n" ++
        "Content-Length: 2\r\n" ++
        "Connection: close\r\n" ++
        "\r\nOK";

    const stub = try spawnStub(HTTP_OK);
    defer stub.thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  local resp = bot.http_request{{ method="GET", url="http://127.0.0.2:{d}/" }}
        \\  local lower = resp.headers["content-type"]
        \\  local exact = resp.headers["Content-Type"]
        \\  local upper = resp.headers["CONTENT-TYPE"]
        \\  local verbatim = ""
        \\  for k,_ in pairs(resp.headers) do
        \\    if k == "Content-Type" then verbatim = "yes" end
        \\  end
        \\  return {{ {{ method="reply", params={{ lower=lower, exact=exact, upper=upper, verbatim=verbatim }} }} }}
        \\end
    , .{stub.port});
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 2, .queue_capacity = 16, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":1}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 5_000));
    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);

    const j = action.payload.json;
    try testing.expect(std.mem.indexOf(u8, j, "\"lower\":\"application/json\"") != null);
    try testing.expect(std.mem.indexOf(u8, j, "\"exact\":\"application/json\"") != null);
    try testing.expect(std.mem.indexOf(u8, j, "\"upper\":\"application/json\"") != null);
    try testing.expect(std.mem.indexOf(u8, j, "\"verbatim\":\"yes\"") != null);
}

test "slow coroutine parked; same worker processes fast update first" {
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const slow_srv = try testing.allocator.create(std.Io.net.Server);
    slow_srv.* = try addr.listen(testing.io, .{ .reuse_address = true });
    const slow_port = boundPort(slow_srv);
    const slow_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.Io.net.Server) void {
            const cio = testing.io;
            defer {
                s.deinit(cio);
                testing.allocator.destroy(s);
            }
            var stream = s.accept(cio) catch return;
            defer stream.close(cio);
            var rbuf: [4096]u8 = undefined;
            var sr = stream.reader(cio, &rbuf);
            drainRequestHead(&sr.interface);
            rt.sleepNs(cio, 300 * std.time.ns_per_ms);
            var wbuf: [256]u8 = undefined;
            var sw = stream.writer(cio, &wbuf);
            sw.interface.writeAll(
                "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nslow",
            ) catch {};
            sw.interface.flush() catch {};
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
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 2, .queue_capacity = 16, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    const slow_url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{slow_port});
    defer testing.allocator.free(slow_url);
    const slow_body = try std.fmt.allocPrint(testing.allocator, "{{\"slow\":true,\"url\":\"{s}\"}}", .{slow_url});
    defer testing.allocator.free(slow_body);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, slow_body));
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"slow\":false}"));

    rt.sleepNs(testing.io, 100 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 1), ctx.output_q.len());
    const fast_action = ctx.output_q.pop();
    defer types.freeApiCall(fast_action, testing.allocator);
    try testing.expectEqualStrings("fast_done", fast_action.method);

    try testing.expect(waitQueue(&ctx.output_q, 1, 1_000));
    const slow_action = ctx.output_q.pop();
    defer types.freeApiCall(slow_action, testing.allocator);
    try testing.expectEqualStrings("slow_done", slow_action.method);
}

test "state written in a resumed (post-I/O) segment commits and is read by a later update" {
    // Guards the resume-path transaction: a write made after a bot.http_request
    // yield must commit (not be lost, and not leave a transaction open that would
    // break the next update). Update 1 yields, then writes state in its resumed
    // segment; update 2 (pushed only after update 1 completes, so they cannot
    // interleave) reads that state back.
    const HTTP_OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
    const stub = try spawnStub(HTTP_OK);
    defer stub.thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  if u.write then
        \\    local resp = bot.http_request{{ method="GET", url="http://127.0.0.2:{d}/" }}
        \\    bot.set_user_state(7, {{ status = resp.status }})
        \\    return {{ {{ method="wrote", params={{}} }} }}
        \\  else
        \\    local s = bot.get_user_state(7)
        \\    return {{ {{ method="read", params={{ status = s.status or 0 }} }} }}
        \\  end
        \\end
    , .{stub.port});
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 2, .queue_capacity = 16, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    // Update 1: yields on http_request, writes user_state in the resumed segment.
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"write\":true}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 5_000));
    {
        const a = ctx.output_q.pop();
        defer types.freeApiCall(a, testing.allocator);
        try testing.expectEqualStrings("wrote", a.method);
    }

    // Update 2 (sync): reads the state the resumed segment committed.
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"write\":false}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 5_000));
    {
        const a = ctx.output_q.pop();
        defer types.freeApiCall(a, testing.allocator);
        try testing.expectEqualStrings("read", a.method);
        try testing.expect(std.mem.indexOf(u8, a.payload.json, "\"status\":200") != null);
    }
}

test "write before a yield survives a later-segment failure (per-segment durability)" {
    // Per-segment durability contract: state written in the
    // pre-yield segment is committed when the coroutine parks, and is NOT rolled
    // back if a later segment fails. The handler writes user_state, yields on
    // bot.http_request, then errors in the resumed segment. The failed segment's
    // own (empty) transaction rolls back; the pre-yield write must remain.
    //
    // Contrast with the resume-path COMMIT-failure test below: there the FAILED
    // segment's own write is dropped. Here the SURVIVING write is the one made
    // before the yield. The two together pin both halves of the contract.
    const HTTP_OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
    const stub = try spawnStub(HTTP_OK);
    defer stub.thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  bot.set_user_state(11, {{ pre = "kept" }})
        \\  bot.http_request{{ method="GET", url="http://127.0.0.2:{d}/" }}
        \\  error("fail after the yield")
        \\end
    , .{stub.port});
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 2, .queue_capacity = 16, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":1}"));

    // The resumed segment errors, so no action is dispatched. Poll the state
    // store until the pre-yield write lands (it commits when the coroutine
    // parks), bounded so a regression that drops it fails rather than hangs.
    var found = false;
    var waited: u64 = 0;
    while (waited < 5_000) : (waited += 10) {
        const data = try ctx.db.getUserState(11);
        defer testing.allocator.free(data);
        if (std.mem.indexOf(u8, data, "\"pre\":\"kept\"") != null) {
            found = true;
            break;
        }
        rt.sleepNs(testing.io, 10 * std.time.ns_per_ms);
    }
    try testing.expect(found);

    // No action was dispatched by the failed workflow.
    try testing.expectEqual(@as(usize, 0), ctx.output_q.len());
}

test "resume-path COMMIT failure drops the segment's actions and its state write" {
    // P4-42 gap. The resume path commits the just-finished post-yield segment
    // (worker.zig:258) before emitting its actions; on a COMMIT failure it rolls
    // back and drops the actions, because they assume state that is now gone.
    // This pins both halves: (a) the resumed segment's action is NOT dispatched,
    // and (b) its state write is ABSENT (rolled back).
    //
    // Direct contrast with "write before a yield survives a later-segment
    // failure" above: there the PRE-yield write survives because it committed
    // when the coroutine parked; here the POST-yield write is in the SAME
    // transaction as the failing commit, so it must NOT survive. The handler is
    // kept deliberately parallel so the two fixtures read together.
    //
    // The stub delays its response, opening a window in which the coroutine is
    // parked: the pre-yield (empty) segment has already committed, so arming
    // fail_next_commit now trips ONLY the resume commit, not the pre-yield one.
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.Io.net.Server);
    srv.* = try addr.listen(testing.io, .{ .reuse_address = true });
    const port = boundPort(srv);
    const stub_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.Io.net.Server) void {
            const cio = testing.io;
            defer {
                s.deinit(cio);
                testing.allocator.destroy(s);
            }
            var stream = s.accept(cio) catch return;
            defer stream.close(cio);
            var rbuf: [4096]u8 = undefined;
            var sr = stream.reader(cio, &rbuf);
            drainRequestHead(&sr.interface);
            // Hold the response so the test can arm fail_next_commit while the
            // coroutine is parked (after the pre-yield commit, before resume).
            rt.sleepNs(cio, 400 * std.time.ns_per_ms);
            var wbuf: [256]u8 = undefined;
            var sw = stream.writer(cio, &wbuf);
            sw.interface.writeAll(
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK",
            ) catch {};
            sw.interface.flush() catch {};
        }
    }.run, .{srv});
    defer stub_thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  bot.http_request{{ method="GET", url="http://127.0.0.2:{d}/" }}
        \\  bot.set_user_state(33, {{ post = "dropped" }})
        \\  return {{ {{ method="resumed_done", params={{}} }} }}
        \\end
    , .{port});
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 2, .queue_capacity = 16, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":1}"));

    // Wait long enough for the coroutine to park on the http_request yield (the
    // pre-yield segment has committed by now), then arm the one-shot failure so
    // the resume commit is the one that trips it. 150 ms is well inside the
    // stub's 400 ms hold.
    rt.sleepNs(testing.io, 150 * std.time.ns_per_ms);
    ctx.db.fail_next_commit = true;

    // The resume commit fails → actions dropped, state rolled back. There is no
    // dispatched action to wait on, so poll the seam reset as the completion
    // marker (it clears when the resume commit fires).
    var fired = false;
    var waited: u64 = 0;
    while (waited < 5_000) : (waited += 10) {
        if (!ctx.db.fail_next_commit) {
            fired = true;
            break;
        }
        rt.sleepNs(testing.io, 10 * std.time.ns_per_ms);
    }
    try testing.expect(fired);

    // (a) the resumed segment's action was NOT dispatched.
    try testing.expectEqual(@as(usize, 0), ctx.output_q.len());

    // (b) the resumed segment's state write is ABSENT — rolled back with the
    // failed commit. Give a brief settle margin, then read it back.
    rt.sleepNs(testing.io, 50 * std.time.ns_per_ms);
    const data = try ctx.db.getUserState(33);
    defer testing.allocator.free(data);
    try testing.expect(std.mem.indexOf(u8, data, "\"post\":\"dropped\"") == null);
}

test "no transaction is held across a yield — a second writer is not blocked" {
    // A transaction is never held across a yield: that would block
    // every other writer for the duration of the network call. Update 1 parks
    // on a hang stub (a yield that never resolves within the test). Update 2,
    // pushed while update 1 is parked, writes state and commits. If the parked
    // coroutine still held a write transaction, update 2's BEGIN IMMEDIATE would
    // block for the full busy_timeout (5 s) and the write would not land
    // promptly. Asserting update 2 completes is the structural signal.
    var hang = try spawnHangStub();
    const port = boundPort(hang.server);

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  if u.park then
        \\    bot.http_request{{ method="GET", url=u.url }}
        \\    return {{}}
        \\  end
        \\  bot.set_user_state(22, {{ w = "done" }})
        \\  return {{ {{ method="committed", params={{}} }} }}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    const url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(url);
    const park_body = try std.fmt.allocPrint(testing.allocator, "{{\"park\":true,\"url\":\"{s}\"}}", .{url});
    defer testing.allocator.free(park_body);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 60_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    // Update 1 parks on the hang stub.
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, park_body));
    rt.sleepNs(testing.io, 100 * std.time.ns_per_ms);

    // Update 2 must write and commit while update 1 is parked. A 2 s wait far
    // exceeds the prompt path yet stays well under the 5 s busy_timeout a held
    // transaction would impose — so a regression (transaction held across the
    // yield) fails this assertion instead of passing slowly.
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"park\":false}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    {
        const a = ctx.output_q.pop();
        defer types.freeApiCall(a, testing.allocator);
        try testing.expectEqualStrings("committed", a.method);
    }
    const data = try ctx.db.getUserState(22);
    defer testing.allocator.free(data);
    try testing.expect(std.mem.indexOf(u8, data, "\"w\":\"done\"") != null);

    // Unblock the io_pool HTTP thread before deinit joins the pool.
    hang.deinit();
    ctx.deinit(t);
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
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 2, .queue_capacity = 16, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":1}"));

    try testing.expect(waitQueue(&ctx.output_q, 1, 5_000));
    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);

    try testing.expectEqualStrings("done", action.method);
    try testing.expect(std.mem.indexOf(u8, action.payload.json, "\"s1\":200") != null);
    try testing.expect(std.mem.indexOf(u8, action.payload.json, "\"s2\":201") != null);
}

test "re-yield against a populated inflight map — 8 coroutines, two yields each" {
    // Exercises the entry update on a resume that yields again while the
    // inflight map holds several other parked coroutines. A wrong-entry update
    // would corrupt another coroutine's owned_strings (a double-free or leak
    // that testing.allocator reports) or mix up the returned statuses.
    const N = 8;
    const HTTP_A = "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\nA";
    const HTTP_B = "HTTP/1.1 201 Created\r\nContent-Length: 1\r\nConnection: close\r\n\r\nB";

    // Stub A answers the first yield of every coroutine, serially with a
    // delay: all N coroutines park before the first resume arrives, and each
    // re-yield happens with the other entries still in the map.
    const stub_a = try spawnMultiStub(N, 30, HTTP_A);
    const stub_b = try spawnMultiStub(N, 0, HTTP_B);
    defer stub_a.thread.join();
    defer stub_b.thread.join();

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  local a = bot.http_request{{ method="GET", url="http://127.0.0.2:{d}/" }}
        \\  local b = bot.http_request{{ method="GET", url="http://127.0.0.2:{d}/" }}
        \\  return {{ {{ method="done", params={{ s1=a.status, s2=b.status }} }} }}
        \\end
    , .{ stub_a.port, stub_b.port });
    defer testing.allocator.free(lua_src);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 4, .queue_capacity = 32, .timeout_ms = 10_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    for (0..N) |_| {
        try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"update_id\":1}"));
    }

    try testing.expect(waitQueue(&ctx.output_q, N, 10_000));
    for (0..N) |_| {
        const action = ctx.output_q.pop();
        defer types.freeApiCall(action, testing.allocator);
        try testing.expectEqualStrings("done", action.method);
        try testing.expect(std.mem.indexOf(u8, action.payload.json, "\"s1\":200") != null);
        try testing.expect(std.mem.indexOf(u8, action.payload.json, "\"s2\":201") != null);
    }
}

test "coroutine past WORKFLOW_DEADLINE_MS is reaped; worker continues" {
    // The reaper runs on an injected deterministic clock, not the wall clock:
    // the test parks a coroutine on a hang stub, advances the clock past the
    // workflow deadline, then polls the reaped-count metric. Firing the deadline
    // structurally — rather than sleeping past a real-time margin — removes the
    // race against parallel-suite OS scheduling that made this test flaky.
    var m = metrics_mod.Metrics{};
    var hang = try spawnHangStub();
    const port = boundPort(hang.server);

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
    const hang_body = try std.fmt.allocPrint(testing.allocator, "{{\"hang\":true,\"url\":\"{s}\"}}", .{hang_url});
    defer testing.allocator.free(hang_body);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 60_000, .proc_max_output = 65_536 });
    // Seed the clock so the deadline (clock + 200) is a fixed, known value.
    ctx.clock.store(1_000, .release);
    const t = try std.Thread.spawn(.{}, workerThread, .{WorkerArgs{
        .id = 0,
        .rules_path = ctx.rules_path,
        .allocator = ctx.allocator,
        .io = testing.io,
        .queue = &ctx.input_q,
        .dispatcher_queue = &ctx.output_q,
        .db = &ctx.db,
        .stop = &ctx.stop,
        .reload_ver = &ctx.reload_ver,
        .io_pool_ptr = &ctx.pool,
        .io_result_queue = &ctx.result_q,
        .max_inflight = 64,
        .workflow_deadline_ms = 200,
        .metrics = &m,
        .clock_ms = &ctx.clock,
    }});

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, hang_body));

    // Wait for the coroutine to park (the reaper only acts on parked entries).
    {
        var waited: u64 = 0;
        while (m.coroutines_inflight.load(.monotonic) == 0 and waited < 2_000) : (waited += 5) {
            rt.sleepNs(testing.io, 5 * std.time.ns_per_ms);
        }
        try testing.expectEqual(@as(i64, 1), m.coroutines_inflight.load(.monotonic));
    }

    // Advance the clock past the deadline (1_000 + 200). The next reaper pass
    // fires; poll the reaped count rather than sleeping a wall-clock margin.
    ctx.clock.store(1_500, .release);
    {
        var waited: u64 = 0;
        while (m.coroutines_reaped_total.load(.monotonic) == 0 and waited < 2_000) : (waited += 5) {
            rt.sleepNs(testing.io, 5 * std.time.ns_per_ms);
        }
    }
    try testing.expectEqual(@as(u64, 1), m.coroutines_reaped_total.load(.monotonic));
    try testing.expectEqual(@as(i64, 0), m.coroutines_inflight.load(.monotonic));

    // The worker keeps running after the reap: a fresh update is answered.
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"hang\":false}"));
    try testing.expect(waitQueue(&ctx.output_q, 1, 2_000));
    const action = ctx.output_q.pop();
    defer types.freeApiCall(action, testing.allocator);
    try testing.expectEqualStrings("ok", action.method);

    // Unblock the io_pool HTTP thread before deinit joins the pool.
    hang.deinit();
    ctx.deinit(t);
}

test "at inflight ceiling new updates are not dequeued until a slot frees" {
    const HTTP_OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";

    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv = try testing.allocator.create(std.Io.net.Server);
    srv.* = try addr.listen(testing.io, .{ .reuse_address = true });
    const port = boundPort(srv);
    const slow_t = try std.Thread.spawn(.{}, struct {
        fn run(s: *std.Io.net.Server, resp: []const u8) void {
            const cio = testing.io;
            defer {
                s.deinit(cio);
                testing.allocator.destroy(s);
            }
            var stream = s.accept(cio) catch return;
            defer stream.close(cio);
            var rbuf: [4096]u8 = undefined;
            var sr = stream.reader(cio, &rbuf);
            drainRequestHead(&sr.interface);
            rt.sleepNs(cio, 300 * std.time.ns_per_ms);
            var wbuf: [256]u8 = undefined;
            var sw = stream.writer(cio, &wbuf);
            sw.interface.writeAll(resp) catch {};
            sw.interface.flush() catch {};
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
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 2, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(1, 60_000); // max_inflight = 1
    defer ctx.deinit(t);
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    const block_url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(block_url);
    const block_body = try std.fmt.allocPrint(testing.allocator, "{{\"block\":true,\"url\":\"{s}\"}}", .{block_url});
    defer testing.allocator.free(block_body);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, block_body));
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, "{\"block\":false}"));

    rt.sleepNs(testing.io, 100 * std.time.ns_per_ms);
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
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

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
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    const url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{stub.port});
    defer testing.allocator.free(url);
    const fetch_body = try std.fmt.allocPrint(testing.allocator, "{{\"fetch\":true,\"url\":\"{s}\"}}", .{url});
    defer testing.allocator.free(fetch_body);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, fetch_body));
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    {
        try ctx.tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data =
            \\function on_message(u)
            \\  return { { method="new_rules", params={} } }
            \\end
        });
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
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 2, .queue_capacity = 8, .timeout_ms = 5_000, .proc_max_output = 65_536 });
    const t = try ctx.spawnWorker(64, 60_000);
    defer ctx.deinit(t);
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    const url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{stub.port});
    defer testing.allocator.free(url);
    const crash_body = try std.fmt.allocPrint(testing.allocator, "{{\"crash\":true,\"url\":\"{s}\"}}", .{url});
    defer testing.allocator.free(crash_body);

    try ctx.input_q.push(try asyncWorkItem(testing.allocator, crash_body));
    rt.sleepNs(testing.io, 500 * std.time.ns_per_ms);

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
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

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
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

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
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

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
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

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
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

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
    // so the coroutine stays parked, and it is safe to deinit() before
    // ctx.deinit() to unblock the io_pool thread.
    var silent = try spawnHangStub();
    const port = boundPort(silent.server);

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  bot.http_request{{ method="GET", url=u.url }}
        \\  return {{}}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    const hang_url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(hang_url);
    const body = try std.fmt.allocPrint(testing.allocator, "{{\"url\":\"{s}\"}}", .{hang_url});
    defer testing.allocator.free(body);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 60_000, .proc_max_output = 65_536 });
    // Inject a deterministic clock so the reap fires on a clock advance, not a
    // wall-clock sleep — the de-flake shared with the P2-1 reaper test.
    ctx.clock.store(1_000, .release);

    const t = try std.Thread.spawn(.{}, workerThread, .{WorkerArgs{
        .id = 0,
        .rules_path = ctx.rules_path,
        .allocator = ctx.allocator,
        .io = testing.io,
        .queue = &ctx.input_q,
        .dispatcher_queue = &ctx.output_q,
        .db = &ctx.db,
        .stop = &ctx.stop,
        .reload_ver = &ctx.reload_ver,
        .io_pool_ptr = &ctx.pool,
        .io_result_queue = &ctx.result_q,
        .max_inflight = 64,
        .workflow_deadline_ms = 200,
        .metrics = &m,
        .clock_ms = &ctx.clock,
    }});

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, body));

    // Poll for the coroutine to park: the inflight gauge reaches 1.
    {
        var waited: u64 = 0;
        while (m.coroutines_inflight.load(.monotonic) == 0 and waited < 2_000) : (waited += 5) {
            rt.sleepNs(testing.io, 5 * std.time.ns_per_ms);
        }
    }
    try testing.expectEqual(@as(i64, 1), m.coroutines_inflight.load(.monotonic));

    // Advance the clock past the deadline (1_000 + 200) and poll for the reap.
    ctx.clock.store(1_500, .release);
    {
        var waited: u64 = 0;
        while (m.coroutines_reaped_total.load(.monotonic) == 0 and waited < 2_000) : (waited += 5) {
            rt.sleepNs(testing.io, 5 * std.time.ns_per_ms);
        }
    }
    const reaped = m.coroutines_reaped_total.load(.monotonic);
    const inflight_after_reap = m.coroutines_inflight.load(.monotonic);

    // Close the stub server first so the io_pool HTTP thread unblocks before
    // ctx.deinit() tries to join the pool threads.
    silent.deinit();
    ctx.deinit(t);

    try testing.expectEqual(@as(u64, 1), reaped);
    try testing.expectEqual(@as(i64, 0), inflight_after_reap);
}

// ── graceful drain on stop ───────────────────────────────────────────────────

test "worker drains in-flight coroutines on stop; exits cleanly" {
    var silent = try spawnHangStub();
    const port = boundPort(silent.server);

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  bot.http_request{{ method="GET", url=u.url }}
        \\  return {{}}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    const hang_url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(hang_url);
    const body = try std.fmt.allocPrint(testing.allocator, "{{\"url\":\"{s}\"}}", .{hang_url});
    defer testing.allocator.free(body);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 60_000, .proc_max_output = 65_536 });

    const t = try std.Thread.spawn(.{}, workerThread, .{WorkerArgs{
        .id = 0,
        .rules_path = ctx.rules_path,
        .allocator = ctx.allocator,
        .io = testing.io,
        .queue = &ctx.input_q,
        .dispatcher_queue = &ctx.output_q,
        .db = &ctx.db,
        .stop = &ctx.stop,
        .reload_ver = &ctx.reload_ver,
        .io_pool_ptr = &ctx.pool,
        .io_result_queue = &ctx.result_q,
        .max_inflight = 64,
        .workflow_deadline_ms = 5_000,
    }});

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, body));

    // Let the coroutine park on the hang stub.
    rt.sleepNs(testing.io, 100 * std.time.ns_per_ms);

    // Signal stop while coroutine is in-flight — the worker emits the drain log.
    ctx.stop.store(true, .release);

    // Close stub so io_pool thread unblocks and pushes an error IoResult;
    // the worker drains the coroutine via the error resume path.
    silent.deinit();

    // ctx.deinit joins the worker (calls stop.store again, idempotent).
    ctx.deinit(t);
}

// ── exit bounded by WORKFLOW_DEADLINE_MS ─────────────────────────────────────

test "state commits after .done and rolls back on Lua error" {
    // A handler that writes state then returns: the write is committed.
    {
        var ctx = try TestCtx.init(testing.allocator,
            \\function on_message(u)
            \\    bot.set_user_state(1, {x=42})
            \\    return { { method="sendMessage", params={chat_id=1, text="ok"} } }
            \\end
        );
        const t = try ctx.spawnWorker();
        defer ctx.deinit(t);

        rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
        try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":1}"));

        // Wait for the dispatched action — proves the .done path completed.
        try testing.expect(waitQueue(&ctx.output_q, 1, 1000));
        const action = popAction(&ctx.output_q);
        types.freeApiCall(action, testing.allocator);

        const data = try ctx.db.getUserState(1);
        defer testing.allocator.free(data);
        try testing.expect(std.mem.indexOf(u8, data, "\"x\":42") != null);
    }
    // A handler that writes state then errors: the write is rolled back.
    {
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

        rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
        try ctx.input_q.push(try testWorkItem(testing.allocator, "{\"update_id\":1}"));

        // No output is dispatched on Lua error — wait long enough for processing.
        rt.sleepNs(testing.io, 150 * std.time.ns_per_ms);

        const data = try ctx.db.getUserState(1);
        defer testing.allocator.free(data);
        // Rollback must have reverted the {x:99} write.
        try testing.expect(std.mem.indexOf(u8, data, "\"x\":99") == null);
        try testing.expect(std.mem.indexOf(u8, data, "\"x\":0") != null);
    }
}

test "worker exits within WORKFLOW_DEADLINE_MS + 200ms with non-responding stub" {
    var silent = try spawnHangStub();
    const port = boundPort(silent.server);

    const lua_src = try std.fmt.allocPrint(testing.allocator,
        \\function on_message(u)
        \\  bot.http_request{{ method="GET", url=u.url }}
        \\  return {{}}
        \\end
    , .{});
    defer testing.allocator.free(lua_src);

    const hang_url = try std.fmt.allocPrint(testing.allocator, "http://127.0.0.2:{d}/", .{port});
    defer testing.allocator.free(hang_url);
    const body = try std.fmt.allocPrint(testing.allocator, "{{\"url\":\"{s}\"}}", .{hang_url});
    defer testing.allocator.free(body);

    var ctx: AsyncTestCtx = undefined;
    try ctx.init(testing.allocator, lua_src, .{ .thread_count = 1, .queue_capacity = 8, .timeout_ms = 60_000, .proc_max_output = 65_536 });

    const t = try std.Thread.spawn(.{}, workerThread, .{WorkerArgs{
        .id = 0,
        .rules_path = ctx.rules_path,
        .allocator = ctx.allocator,
        .io = testing.io,
        .queue = &ctx.input_q,
        .dispatcher_queue = &ctx.output_q,
        .db = &ctx.db,
        .stop = &ctx.stop,
        .reload_ver = &ctx.reload_ver,
        .io_pool_ptr = &ctx.pool,
        .io_result_queue = &ctx.result_q,
        .max_inflight = 64,
        .workflow_deadline_ms = 200, // short deadline for this test
    }});

    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);
    try ctx.input_q.push(try asyncWorkItem(testing.allocator, body));

    // Let the coroutine park on the hang stub.
    rt.sleepNs(testing.io, 100 * std.time.ns_per_ms);

    // Signal stop WITHOUT closing the stub. The worker must drain via deadline reap.
    ctx.stop.store(true, .release);
    const t0 = rt.nowMs(testing.io);
    t.join();
    const elapsed_ms = rt.nowMs(testing.io) - t0;

    // Close stub after worker exits to unblock the io_pool thread.
    silent.deinit();

    // Manual cleanup (ctx.deinit can't be used: t was already joined above).
    // pool.deinit() joins the pool thread first so all pushErr calls complete
    // before result_q is drained — avoids a race with popTimeout(0).
    ctx.pool.deinit();
    while (ctx.input_q.popTimeout(0)) |item| ctx.allocator.free(item.body);
    while (ctx.output_q.popTimeout(0)) |call| types.freeApiCall(call, ctx.allocator);
    while (ctx.result_q.popTimeout(0)) |res| io_pool.freeIoResult(res, ctx.allocator);
    ctx.result_q.deinit(ctx.allocator);
    ctx.output_q.deinit(ctx.allocator);
    ctx.input_q.deinit(ctx.allocator);
    ctx.db.close();
    ctx.tmp.cleanup();

    // worker must exit within WORKFLOW_DEADLINE_MS + 200ms epsilon.
    try testing.expect(elapsed_ms < 400);
}

test "scheduled job fires on_schedule and deletes its row (at-least-once happy path)" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes + 1]u8 = undefined;
    const plen = try tmp.dir.realPathFile(testing.io, ".", pbuf[0..std.fs.max_path_bytes]);
    var dbbuf: [std.fs.max_path_bytes + 8]u8 = undefined;
    const db_path = try std.fmt.bufPrintZ(&dbbuf, "{s}/s.db", .{pbuf[0..plen]});

    // Seed a due job through one connection.
    {
        var seed = try state_store.StateStore.open(testing.allocator, db_path);
        defer seed.close();
        _ = try seed.scheduleInsert(rt.nowMs(testing.io) - 1, "{\"chat\":7}");
    }

    var ctx = try TestCtx.initWithDb(testing.allocator,
        \\function on_schedule(payload, id)
        \\  return { { method = "sendMessage", params = { chat_id = payload.chat, text = "fired" } } }
        \\end
    , db_path);
    // Scheduler teardown defer registered AFTER ctx.deinit defer so it runs FIRST (LIFO).
    const wt = try ctx.spawnWorker();
    defer ctx.deinit(wt);

    var sched = scheduler.Scheduler{ .io = testing.io, .wait_cap_ns = 50 * std.time.ns_per_ms };
    var sched_db = try state_store.StateStore.open(testing.allocator, db_path);
    errdefer sched_db.close();
    var qs = [_]*Queue(types.WorkItem){&ctx.input_q};
    const st = try std.Thread.spawn(.{}, scheduler.schedulerThread, .{scheduler.SchedulerArgs{
        .db = &sched_db, .worker_qs = &qs, .sched = &sched,
        .stop = &ctx.stop, .io = testing.io, .allocator = testing.allocator,
    }});
    // This defer runs BEFORE ctx.deinit(wt) above (LIFO), stopping the scheduler
    // thread and closing its DB before the worker's DB and tmp dir are cleaned up.
    defer {
        ctx.stop.store(true, .release);
        sched.wakeUp();
        st.join();
        sched_db.close();
    }

    // The worker emits the sendMessage from on_schedule.
    try testing.expect(waitQueue(&ctx.output_q, 1, 3000));
    const action = popAction(&ctx.output_q);
    defer types.freeApiCall(action, testing.allocator);
    try testing.expect(bodyHas(action.payload.json, "\"text\":\"fired\""));

    // The schedule row must be gone — deleted inside the handler transaction.
    var check = try state_store.StateStore.open(testing.allocator, db_path);
    defer check.close();
    try testing.expectEqual(@as(?i64, null), try check.scheduleMinFire(rt.nowMs(testing.io) + 1_000_000));
}

test "scheduled job whose handler COMMIT fails keeps its row (delete atomic with commit)" {
    // P3-9 strengthening sub-case. The happy-path test above checks only the
    // success end state. This pins the atomicity contract: the row delete runs
    // INSIDE the handler transaction (scheduleDelete then commit), so a COMMIT
    // failure must roll the delete back too — a failed handler commit must never
    // orphan-delete the row. The job stays present and re-claimable, preserving
    // at-least-once (a crashed/failed firing is retried, never silently lost).
    //
    // The schedule WorkItem is pushed directly (kind = .schedule, no scheduler
    // thread) so the firing is deterministic, and fail_next_commit is armed
    // before the push so the one commit that finalizes on_schedule is the one
    // that fails.
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var pbuf: [std.fs.max_path_bytes + 1]u8 = undefined;
    const plen = try tmp.dir.realPathFile(testing.io, ".", pbuf[0..std.fs.max_path_bytes]);
    var dbbuf: [std.fs.max_path_bytes + 8]u8 = undefined;
    const db_path = try std.fmt.bufPrintZ(&dbbuf, "{s}/s.db", .{pbuf[0..plen]});

    // Seed one due job through a throwaway connection so the row is durable in
    // the file DB before the worker fires it.
    var sid: i64 = undefined;
    {
        var seed = try state_store.StateStore.open(testing.allocator, db_path);
        defer seed.close();
        sid = try seed.scheduleInsert(rt.nowMs(testing.io) - 1, "{\"chat\":7}");
    }

    var ctx = try TestCtx.initWithDb(testing.allocator,
        \\function on_schedule(payload, id)
        \\  return { { method = "sendMessage", params = { chat_id = payload.chat, text = "fired" } } }
        \\end
    , db_path);
    const wt = try ctx.spawnWorker();
    defer ctx.deinit(wt);

    // Arm the one-shot commit failure, then fire the job. The on_schedule
    // handler does not yield, so the start-path commit that finalizes it is the
    // next (and only) commit — exactly the one this gate trips.
    ctx.db.fail_next_commit = true;
    try ctx.input_q.push(.{
        .body = try testing.allocator.dupe(u8, "{\"chat\":7}"),
        .user_id = null,
        .kind = .schedule,
        .schedule_id = sid,
    });

    // Give the worker time to fire the handler and hit the failing commit. No
    // action is dispatched (commit failed → actions dropped), so there is no
    // output-queue signal to wait on; poll the seam's reset as the completion
    // marker, then assert the row survived.
    var fired = false;
    var waited: u64 = 0;
    while (waited < 3_000) : (waited += 10) {
        if (!ctx.db.fail_next_commit) {
            fired = true;
            break;
        }
        rt.sleepNs(testing.io, 10 * std.time.ns_per_ms);
    }
    try testing.expect(fired);

    // The row must still be present and re-claimable through a fresh connection:
    // the delete was rolled back with the failed commit, not orphaned.
    var check = try state_store.StateStore.open(testing.allocator, db_path);
    defer check.close();
    try testing.expect((try check.scheduleMinFire(rt.nowMs(testing.io) + 1_000_000)) != null);
    // And no action escaped the failed firing.
    try testing.expectEqual(@as(usize, 0), ctx.output_q.len());
}
