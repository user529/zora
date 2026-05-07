/// worker.zig — worker thread: owns lua_State + SQLite connection
///
/// Each worker runs in a dedicated thread and owns:
///   - a LuaEngine (Lua state + on_message function)
///   - a StateStore connection (WAL allows concurrent readers)
///
/// Main loop (pseudo-code from CLAUDE.md):
///   loop:
///     update = queue.tryPop()  -- check stop flag between polls
///     if global_reload_version > local_reload_version:
///         lua_engine.loadFile(rules_path)
///         local_reload_version = global_reload_version
///     actions = lua_engine.callOnMessage(update) -> []Action
///     for action in actions: dispatcher_queue.push(action)
///     free actions slice (string payloads now owned by dispatcher)
///
/// Routing helper:
///   hashUserId(user_id, worker_count) → worker index
///   Deterministic, uniform distribution via 64-bit multiplicative hash.

const std = @import("std");
const types = @import("types.zig");
const queue_mod = @import("queue.zig");
const lua_engine = @import("lua_engine.zig");
const lua_api = @import("lua_api.zig");
const state_store = @import("state_store.zig");

const log = std.log.scoped(.worker);

// ---------------------------------------------------------------------------
// WorkerArgs — all parameters for a single worker thread
// ---------------------------------------------------------------------------

pub const WorkerArgs = struct {
    id: u32,
    /// Null-terminated path to the Lua rules file (for initial load + reload).
    rules_path: [:0]const u8,
    /// Used for Update→JSON conversion, action string allocation, and Lua
    /// engine internals.  Must be thread-safe (e.g., a GPA).
    allocator: std.mem.Allocator,
    /// Input queue: server pushes Parsed(Update) values, worker pops and deinits them.
    queue: *queue_mod.Queue(std.json.Parsed(types.Update)),
    /// Output queue: worker pushes Action values, dispatcher pops them.
    /// String payloads inside each Action are allocated from `allocator`;
    /// the dispatcher is responsible for freeing them after sending.
    dispatcher_queue: *queue_mod.Queue(types.Action),
    /// Per-worker SQLite connection (WAL mode allows concurrent readers).
    db: *state_store.StateStore,
    /// Set to true to request graceful shutdown.  Worker exits the loop
    /// after the current update (if any) finishes.
    stop: *std.atomic.Value(bool),
    /// Monotonically-increasing reload counter.  When the value advances past
    /// the worker's local copy the rules file is reloaded before the next
    /// on_message call.  Production code passes &reload.reload_version;
    /// tests inject a local counter for isolation.
    reload_ver: *std.atomic.Value(u64),
};

// ---------------------------------------------------------------------------
// Worker thread entry point
// ---------------------------------------------------------------------------

pub fn workerThread(args: WorkerArgs) void {
    // Guard: returning while stop=false means the worker died unexpectedly.
    // Aborting is intentional — silent capacity loss is worse than a visible crash.
    defer {
        if (!args.stop.load(.acquire)) {
            log.err("worker {d}: unexpected exit — aborting process to prevent silent degradation", .{args.id});
            std.process.abort();
        }
    }

    // Build the ApiCtx that bot.* Lua functions use.
    var api_ctx = lua_api.ApiCtx{
        .db        = args.db,
        .allocator = args.allocator,
    };

    // Initialise Lua state.
    var engine = lua_engine.LuaEngine.init(args.allocator, &api_ctx) catch |err| {
        log.err("worker {d}: LuaEngine.init failed: {s}", .{ args.id, @errorName(err) });
        return;
    };
    defer engine.deinit();

    // Load the initial rules file, falling back to the backup if the primary
    // fails.  A load failure is non-fatal: the worker continues with no rules
    // and will retry on the next reload signal.
    engine.loadFile(args.rules_path) catch {
        log.warn("worker {d}: initial load failed, trying backup", .{args.id});
        var bk_buf: [std.fs.max_path_bytes + 5]u8 = undefined;
        if (std.fmt.bufPrintZ(&bk_buf, "{s}.bak", .{args.rules_path})) |bk| {
            engine.loadFile(bk) catch {
                log.warn("worker {d}: backup also failed, no rules loaded", .{args.id});
            };
        } else |_| {
            log.warn("worker {d}: backup path too long, no rules loaded", .{args.id});
        }
    };

    // Track which reload generation this worker has applied.
    var local_ver: u64 = args.reload_ver.load(.acquire);

    // Metrics counters (reset each reporting period).
    var processed:  u64 = 0;
    var lua_errors: u64 = 0;
    var dropped:    u64 = 0;
    var last_log_ns: i128 = std.time.nanoTimestamp();
    const log_interval_ns: i128 = 5 * std.time.ns_per_s;

    // Main loop.
    while (!args.stop.load(.acquire)) {
        // Periodic metrics log every 5 seconds.
        const now_ns = std.time.nanoTimestamp();
        if (now_ns - last_log_ns >= log_interval_ns) {
            log.info("worker {d}: processed={d} lua_errors={d} dropped={d} queue_depth={d}", .{
                args.id, processed, lua_errors, dropped, args.queue.len(),
            });
            processed  = 0;
            lua_errors = 0;
            dropped    = 0;
            last_log_ns = now_ns;
        }

        // Non-blocking pop so we can honour the stop flag promptly.
        const maybe_parsed = args.queue.tryPop();
        if (maybe_parsed == null) {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        }
        const parsed = maybe_parsed.?;
        defer parsed.deinit();
        const update = parsed.value;

        // ── Hot-reload check ───────────────────────────────────────────────
        const global_ver = args.reload_ver.load(.acquire);
        if (global_ver > local_ver) {
            engine.loadFile(args.rules_path) catch {
                log.warn("worker {d}: reload loadFile failed", .{args.id});
            };
            local_ver = global_ver;
            log.info("worker {d}: reloaded rules (version {d})", .{ args.id, local_ver });
        }

        // ── Call on_message ────────────────────────────────────────────────
        const actions = engine.callOnMessage(args.allocator, update) catch |err| {
            log.err("worker {d}: callOnMessage OOM: {s}", .{ args.id, @errorName(err) });
            lua_errors += 1;
            continue;
        };
        if (actions.len == 0) lua_errors += 1; // callOnMessage returns empty on Lua error

        // ── Forward to dispatcher ──────────────────────────────────────────
        // String payloads inside each Action are transferred to the dispatcher.
        // Only the wrapper slice is freed here.
        for (actions) |action| {
            args.dispatcher_queue.push(action) catch {
                log.warn("worker {d}: dispatcher queue full, dropping action", .{args.id});
                freeActionPayload(action, args.allocator);
                dropped += 1;
            };
        }
        args.allocator.free(actions);
        processed += 1;
    }
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
// Private helper: free the string payloads owned by a single Action
// ---------------------------------------------------------------------------

/// Free all heap-allocated strings inside `action`.
/// Does NOT free the Action value itself (it is stack/queue allocated).
fn freeActionPayload(action: types.Action, allocator: std.mem.Allocator) void {
    switch (action) {
        .send_message    => |a| allocator.free(a.text),
        .send_message_ex => |a| { allocator.free(a.text); allocator.free(a.opts); },
        .answer_callback => |a| {
            allocator.free(a.callback_query_id);
            if (a.text) |t| allocator.free(t);
        },
        .delete_message => {},
    }
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

/// Test helper: pop an Action from the queue and free its string payloads
/// using the provided allocator after the caller is done with it.
/// Returns the Action for inspection.
fn popAction(q: *Queue(types.Action)) types.Action {
    return q.pop();
}

/// Free an Action's string payloads (test-side cleanup).
fn freeAction(action: types.Action, allocator: std.mem.Allocator) void {
    freeActionPayload(action, allocator);
}

// ── Shared test setup ────────────────────────────────────────────────────────

/// Wrap a bare Update in a Parsed(Update) for tests (no string allocations).
fn testUpdate(allocator: std.mem.Allocator, update: types.Update) !std.json.Parsed(types.Update) {
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    return .{ .arena = arena, .value = update };
}

const TestCtx = struct {
    tmp: testing.TmpDir,
    db: state_store.StateStore,
    input_q: Queue(std.json.Parsed(types.Update)),
    output_q: Queue(types.Action),
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

        self.input_q  = try Queue(std.json.Parsed(types.Update)).init(allocator, 64);
        errdefer self.input_q.deinit(allocator);

        self.output_q = try Queue(types.Action).init(allocator, 256);
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
        self.output_q.deinit(self.allocator);
        self.input_q.deinit(self.allocator);
        self.db.close();
        self.tmp.cleanup();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

test "AC-8.1: single update → dispatcher receives expected action" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u)
        \\  return { {action="send_message", chat_id=42, text="hello"} }
        \\end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    // Give worker time to start and load rules.
    std.Thread.sleep(30 * std.time.ns_per_ms);

    try ctx.input_q.push(try testUpdate(testing.allocator, types.Update{ .update_id = 1 }));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    const action = popAction(&ctx.output_q);
    defer freeAction(action, testing.allocator);

    try testing.expectEqual(types.ActionTag.send_message, std.meta.activeTag(action));
    try testing.expectEqual(@as(i64, 42), action.send_message.chat_id);
    try testing.expectEqualStrings("hello", action.send_message.text);
}

test "AC-8.2: reload_version incremented → worker reloads before on_message" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u)
        \\  return { {action="send_message", chat_id=1, text="v1"} }
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
            \\  return { {action="send_message", chat_id=1, text="v2"} }
            \\end
        );
    }

    // Signal reload via the isolated per-test counter (TD-2 fix).
    _ = ctx.reload_ver.fetchAdd(1, .release);

    try ctx.input_q.push(try testUpdate(testing.allocator, types.Update{ .update_id = 2 }));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    const action = popAction(&ctx.output_q);
    defer freeAction(action, testing.allocator);

    try testing.expectEqualStrings("v2", action.send_message.text);
}

test "AC-8.3: update that triggers reload is still processed (not dropped)" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u)
        \\  return { {action="send_message", chat_id=99, text="ok"} }
        \\end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    std.Thread.sleep(30 * std.time.ns_per_ms);

    // Signal reload via the isolated per-test counter (TD-2 fix).
    _ = ctx.reload_ver.fetchAdd(1, .release);

    // Push the update — it should be processed after the reload, not dropped.
    try ctx.input_q.push(try testUpdate(testing.allocator, types.Update{ .update_id = 3 }));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    const action = popAction(&ctx.output_q);
    defer freeAction(action, testing.allocator);

    try testing.expectEqualStrings("ok", action.send_message.text);
}

test "AC-8.4: Lua error on first update → worker continues; second update succeeds" {
    var ctx = try TestCtx.init(testing.allocator,
        \\local call_count = 0
        \\function on_message(u)
        \\  call_count = call_count + 1
        \\  if call_count == 1 then
        \\    error("intentional error")
        \\  end
        \\  return { {action="send_message", chat_id=1, text="second"} }
        \\end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    std.Thread.sleep(30 * std.time.ns_per_ms);

    // First update → Lua error, empty slice returned, nothing pushed.
    try ctx.input_q.push(try testUpdate(testing.allocator, types.Update{ .update_id = 4 }));
    // Second update → succeeds.
    try ctx.input_q.push(try testUpdate(testing.allocator, types.Update{ .update_id = 5 }));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    const action = popAction(&ctx.output_q);
    defer freeAction(action, testing.allocator);
    try testing.expectEqualStrings("second", action.send_message.text);

    // Confirm no second action appeared (first update produced nothing).
    try testing.expectEqual(@as(usize, 0), ctx.output_q.len());
}

test "AC-8.5: worker thread alive (not exited) after 200ms with empty queue" {
    var ctx = try TestCtx.init(testing.allocator,
        \\function on_message(u) return {} end
    );
    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    std.Thread.sleep(200 * std.time.ns_per_ms);

    // If the worker exited, pushing to the queue would still work (queue is
    // independent), but we verify the thread is still alive via a liveness probe:
    // push an update and check the dispatcher gets a response (which requires
    // the worker to still be running).
    try ctx.input_q.push(try testUpdate(testing.allocator, types.Update{ .update_id = 6 }));

    // on_message returns {}, so no actions expected — but the worker must
    // still be alive to pop and process the update.
    // We verify liveness by observing that the update is consumed.
    var waited: u64 = 0;
    while (ctx.input_q.len() > 0 and waited < 1000) : (waited += 5) {
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expectEqual(@as(usize, 0), ctx.input_q.len());
}

test "fallback: broken primary rules.lua → worker loads from rules.lua.bak" {
    // Write an invalid primary rules file.
    var ctx = try TestCtx.init(testing.allocator, "function ( -- INVALID SYNTAX");

    // Write a valid backup BEFORE spawning the worker so it is available on
    // the initial load attempt.
    {
        var f = try ctx.tmp.dir.createFile("rules.lua.bak", .{});
        defer f.close();
        try f.writeAll(
            \\function on_message(u)
            \\  return { {action="send_message", chat_id=99, text="from-backup"} }
            \\end
        );
    }

    const t = try ctx.spawnWorker();
    defer ctx.deinit(t);

    // Give worker time to start and load rules via backup.
    std.Thread.sleep(50 * std.time.ns_per_ms);

    try ctx.input_q.push(try testUpdate(testing.allocator, types.Update{ .update_id = 20 }));

    try testing.expect(waitQueue(&ctx.output_q, 1, 1000));

    const action = popAction(&ctx.output_q);
    defer freeAction(action, testing.allocator);
    try testing.expectEqualStrings("from-backup", action.send_message.text);
}

test "AC-8.6: hashUserId is deterministic — 10k ids × 8 workers always same index" {
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
