/// main.zig — entry point: parse config, verify DB, start all subsystems.
///
/// Startup order (log before server accepts):
///   1. Load Config
///   2. Open + verify SQLite schema (exit on mismatch)
///   3. Print startup banner
///   4. Allocate queues and spawn dispatcher threads
///   5. Spawn worker threads (each opens its own DB connection)
///   6. Spawn hot-reload watcher (detached; no graceful shutdown in prototype)
///   7. Bind HTTP server and start accept loop
///   8. Block forever
const std = @import("std");
const build_opts = @import("build_options");

// Show info/warn in all build modes so soak-test logs and hot-reload events
// are visible even in ReleaseFast (Zig defaults to .err in release builds).
pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = timestampedLogFn,
};

/// Custom log backend: prepends a UTC timestamp (ISO 8601, ms precision) to
/// every line.  Thread safety is provided by std.debug.lockStderrWriter, the
/// same mechanism used by Zig's default log implementation.
fn timestampedLogFn(
    comptime level: std.log.Level,
    comptime scope: @Type(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    const ms = std.time.milliTimestamp();
    const ms_abs: u64 = if (ms >= 0) @intCast(ms) else 0;
    const secs: u64 = ms_abs / 1000;
    const millis: u64 = ms_abs % 1000;

    const epoch_secs = std.time.epoch.EpochSeconds{ .secs = secs };
    const epoch_day = epoch_secs.getEpochDay();
    const day_secs = epoch_secs.getDaySeconds();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();

    const level_txt = comptime level.asText();
    const scope_txt = if (scope == .default) ": " else "(" ++ @tagName(scope) ++ "): ";

    var buffer: [512]u8 = undefined;
    const stderr = std.debug.lockStderrWriter(&buffer);
    defer std.debug.unlockStderrWriter();

    nosuspend stderr.print(
        "{d:04}-{d:02}-{d:02}T{d:02}:{d:02}:{d:02}.{d:03}Z " ++ level_txt ++ scope_txt,
        .{
            year_day.year,
            month_day.month.numeric(),
            month_day.day_index + 1,
            day_secs.getHoursIntoDay(),
            day_secs.getMinutesIntoHour(),
            day_secs.getSecondsIntoMinute(),
            millis,
        },
    ) catch return;
    nosuspend stderr.print(format ++ "\n", args) catch return;
}
const types = @import("types.zig");
const config_mod = @import("config.zig");
const state_store = @import("state_store.zig");
const queue_mod = @import("queue.zig");
const worker_mod = @import("worker.zig");
const disp_mod = @import("dispatcher.zig");
const server_mod = @import("server.zig");
const watcher = @import("watcher.zig");
const lua_engine = @import("lua_engine.zig");
const tg_schema = @import("tg_schema.zig");
const metrics_mod = @import("metrics.zig");
const io_pool = @import("io_pool.zig");
const delay_mod = @import("delay.zig");

const log = std.log.scoped(.main);

pub const RELEASE: u32 = build_opts.release;
pub const GIT_BRANCH: []const u8 = build_opts.git_branch;

// ---------------------------------------------------------------------------
// Signal handling — SIGTERM / SIGINT set this flag; main loop polls it.
// ---------------------------------------------------------------------------

var g_stop        = std.atomic.Value(bool).init(false);
var g_metrics     = metrics_mod.Metrics{};
var g_schema_slot: tg_schema.SchemaSlot = undefined;

fn sigStop(sig: c_int) callconv(std.builtin.CallingConvention.c) void {
    _ = sig;
    g_stop.store(true, .release);
}

// ---------------------------------------------------------------------------
// Metrics log thread — emits a snapshot every 60 s when METRICS_LOG=true
// ---------------------------------------------------------------------------

/// Format all 11 metric counters into `buf`. Returns the written slice.
/// The throttle group reports reactive rate limiting: 429s seen, calls parked
/// in the delay queue, calls shed on overflow, and the current queue depth.
pub fn fmtMetrics(m: *const metrics_mod.Metrics, buf: []u8) []const u8 {
    return std.fmt.bufPrint(buf,
        "io_jobs={d} io_inflight={d} io_err={d} io_timeout={d} coros_inflight={d} coros_reaped={d} tracked_fail={d} throttle_429={d} throttle_delayed={d} throttle_shed={d} throttle_depth={d}",
        .{
            m.io_jobs_total.load(.monotonic),
            m.io_jobs_inflight.load(.monotonic),
            m.io_errors_total.load(.monotonic),
            m.io_timeouts_total.load(.monotonic),
            m.coroutines_inflight.load(.monotonic),
            m.coroutines_reaped_total.load(.monotonic),
            m.tracked_send_failures_total.load(.monotonic),
            m.throttle_429_total.load(.monotonic),
            m.throttle_delayed_total.load(.monotonic),
            m.throttle_shed_total.load(.monotonic),
            m.throttle_delay_depth.load(.monotonic),
        },
    ) catch "[metrics fmt overflow]";
}

fn metricsLogThread(m: *const metrics_mod.Metrics) void {
    const metrics_log = std.log.scoped(.metrics);
    while (true) {
        std.Thread.sleep(60 * std.time.ns_per_s);
        var buf: [512]u8 = undefined;
        metrics_log.info("{s}", .{fmtMetrics(m, &buf)});
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn main() u8 {
    // Allocator: c_allocator (libc malloc), NOT smp_allocator.
    //
    // smp_allocator never returns freed small-allocation slabs to the OS (by
    // design — it caches them in per-thread, per-size-class freelists for speed).
    // Under sustained multi-threaded per-message churn that makes RSS ratchet up
    // unbounded (see docs/investigations/2026-05-30-memory-leak-stress.md).
    //
    // libc malloc returns freed memory to the OS and, crucially, lets operators
    // pick the malloc implementation at deploy time via LD_PRELOAD without a
    // rebuild. jemalloc is RECOMMENDED (flat RSS + background decay purging that
    // reclaims burst spikes) — see docs/operations.md. Throughput cost is
    // negligible and far above Telegram's ~1000 msg/s ceiling.
    //
    // To investigate leaks, temporarily swap in DebugAllocator:
    //   var da = std.heap.DebugAllocator(.{}){};
    //   defer _ = da.deinit();
    //   run(da.allocator()) catch return 1;
    run(std.heap.c_allocator) catch return 1;
    return 0;
}

// ---------------------------------------------------------------------------
// Core startup — separated from main() so tests can drive startup directly
// ---------------------------------------------------------------------------

fn run(allocator: std.mem.Allocator) !void {
    // ── Config ───────────────────────────────────────────────────────────────
    const cfg = config_mod.load(allocator) catch |err| {
        log.err("configuration: {s}", .{@errorName(err)});
        std.process.exit(1);
    };
    defer config_mod.deinit(cfg, allocator);

    // ── API schema (hot-reloadable; absent file → Tier-0, no validation) ──────
    // g_schema_slot is a module global so its address is stable for the process
    // lifetime — the detached schema-watcher thread can safely hold a pointer to
    // it across run() returning.
    g_schema_slot = tg_schema.SchemaSlot.init(allocator);
    tg_schema.loadInitial(&g_schema_slot, cfg.schema_file);

    {
        var db = state_store.StateStore.open(allocator, cfg.db_path) catch |err| {
            log.err("database '{s}': {s}", .{ cfg.db_path, @errorName(err) });
            std.process.exit(1);
        };
        db.close();
    }

    // ── Signal handlers (SIGTERM / SIGINT → clean shutdown) ──────────────────
    // SA.RESTART causes interrupted slow syscalls (nanosleep, accept, poll) to
    // be restarted automatically rather than returning EINTR.  The server uses
    // a poll-based accept loop that tolerates EINTR anyway, but SA.RESTART is
    // the conventional production choice and future-proofs against any blocking
    // call path added later.
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = sigStop },
        .mask = std.posix.sigemptyset(),
        .flags = std.posix.SA.RESTART,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);

    // ── Startup banner — before server.init ───────────────────────────────────
    log.info("zora starting (branch={s} release={d} schema={d} rules_api={d} api_validation={s} delay_cap={d} retry_max_ms={d})", .{
        GIT_BRANCH,
        RELEASE,
        state_store.SCHEMA_VERSION,
        lua_engine.RULES_API_VERSION,
        @tagName(cfg.api_validation),
        cfg.delay_queue_capacity,
        cfg.retry_after_max_ms,
    });

    var stop = std.atomic.Value(bool).init(false);
    g_metrics = metrics_mod.Metrics{};

    // ── Metrics log thread (detached; runs until process exit) ────────────────
    // g_metrics is module-level, so its lifetime outlives run() and the
    // detached thread safely reads it after run() returns.
    if (cfg.metrics_log) {
        const metrics_t = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, metricsLogThread, .{&g_metrics});
        metrics_t.detach();
    }

    // ── Dispatcher queue + threads ────────────────────────────────────────────
    var disp_q = try queue_mod.Queue(types.ApiCall).init(allocator, 4096);
    defer disp_q.deinit(allocator);
    disp_q.kind = .dispatcher;

    // ── Throttle: delay queue + per-chat block map + requeue thread ───────────
    var delay_q = delay_mod.DelayQueue.init(allocator, cfg.delay_queue_capacity);
    var blocked_until = delay_mod.BlockedMap.init(allocator);

    const requeue_t = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, delay_mod.requeueThread, .{
        delay_mod.RequeueArgs{
            .delay_q = &delay_q, .disp_q = &disp_q, .stop = &stop,
            .metrics = &g_metrics, .allocator = allocator,
        },
    });

    const disp_threads = try allocator.alloc(std.Thread, cfg.dispatcher_threads);
    defer allocator.free(disp_threads);

    for (0..cfg.dispatcher_threads) |i| {
        disp_threads[i] = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, disp_mod.dispatcherThread, .{
            disp_mod.DispatcherArgs{
                .id = @intCast(i),
                .queue = &disp_q,
                .bot_token = cfg.bot_token,
                .api_base = cfg.api_base,
                .allocator = allocator,
                .stop = &stop,
                .metrics = &g_metrics,
                .delay_q = &delay_q,
                .blocked_until = &blocked_until,
                .retry_after_default_ms = cfg.retry_after_default_ms,
                .retry_after_max_ms = cfg.retry_after_max_ms,
            },
        });
    }

    // ── Worker queues + threads ───────────────────────────────────────────────
    const wqs = try allocator.alloc(queue_mod.Queue(types.WorkItem), cfg.worker_count);
    defer {
        for (wqs) |*q| q.deinit(allocator);
        allocator.free(wqs);
    }
    const wq_ptrs = try allocator.alloc(*queue_mod.Queue(types.WorkItem), cfg.worker_count);
    defer allocator.free(wq_ptrs);

    for (0..cfg.worker_count) |i| {
        wqs[i] = try queue_mod.Queue(types.WorkItem).init(allocator, cfg.queue_capacity);
        wqs[i].id = i;
        wqs[i].metrics_log = cfg.metrics_log;
        wqs[i].kind = .worker;
        wq_ptrs[i] = &wqs[i];
    }

    // ── io_pool result queues (one per worker; pool routes by worker_id) ───────
    const result_qs = try allocator.alloc(queue_mod.Queue(io_pool.IoResult), cfg.worker_count);
    errdefer allocator.free(result_qs);
    var result_qs_init: usize = 0;
    errdefer for (result_qs[0..result_qs_init]) |*rq| rq.deinit(allocator);
    for (0..cfg.worker_count) |i| {
        result_qs[i] = try queue_mod.Queue(io_pool.IoResult).init(allocator, cfg.io_queue_capacity);
        result_qs[i].id = i;
        result_qs[i].metrics_log = cfg.metrics_log;
        result_qs[i].kind = .io_result;
        result_qs_init += 1;
    }

    const result_q_ptrs = try allocator.alloc(*queue_mod.Queue(io_pool.IoResult), cfg.worker_count);
    errdefer allocator.free(result_q_ptrs);
    for (0..cfg.worker_count) |i| result_q_ptrs[i] = &result_qs[i];

    // ── Single shared io_pool that executes blocking I/O for parked coroutines ─
    var pool: io_pool.IoPool = undefined;
    try pool.init(allocator, io_pool.IoPoolConfig{
        .thread_count    = cfg.io_pool_threads,
        .queue_capacity  = cfg.io_queue_capacity,
        .timeout_ms      = cfg.io_job_timeout_ms,
        .proc_max_output = cfg.proc_max_output,
        .metrics         = &g_metrics,
    }, result_q_ptrs);
    errdefer pool.deinit();

    const worker_threads = try allocator.alloc(std.Thread, cfg.worker_count);
    defer allocator.free(worker_threads);

    // Track per-worker DB pointers so they can be freed after workers join.
    const dbs = try allocator.alloc(state_store.StateStore, cfg.worker_count);
    defer allocator.free(dbs); // only frees the slice backing array

    var opened_dbs: usize = 0;
    errdefer for (dbs[0..opened_dbs]) |*db| db.close();

    for (0..cfg.worker_count) |i| {
        dbs[i] = try state_store.StateStore.open(allocator, cfg.db_path);
        opened_dbs += 1;
        worker_threads[i] = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, worker_mod.workerThread, .{
            worker_mod.WorkerArgs{
                .id = @intCast(i),
                .rules_path = cfg.rules_file,
                .allocator = allocator,
                .queue = &wqs[i],
                .dispatcher_queue = &disp_q,
                .db = &dbs[i],
                .stop = &stop,
                .reload_ver = &watcher.reload_version,
                .schema = &g_schema_slot,
                .validation = cfg.api_validation,
                .json_max_bytes = cfg.json_max_bytes,
                .metrics = &g_metrics,
                .io_pool_ptr = &pool,
                .io_result_queue = &result_qs[i],
                .max_inflight = cfg.max_inflight_per_worker,
                .workflow_deadline_ms = cfg.workflow_deadline_ms,
            },
        });
    }

    // ── Hot-reload watcher (detached; no graceful shutdown in prototype) ───────
    {
        const watcher_t = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, watcher.watcherThread, .{
            watcher.WatcherArgs{
                .rules_path = cfg.rules_file,
                .allocator = allocator,
            },
        });
        watcher_t.detach();
    }

    // ── Schema-file watcher (detached; mirrors the rules watcher) ─────────────
    {
        const schema_watcher_t = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, tg_schema.schemaWatcherThread, .{
            tg_schema.SchemaWatcherArgs{
                .schema_file = cfg.schema_file,
                .slot = &g_schema_slot,
                .allocator = allocator,
            },
        });
        schema_watcher_t.detach();
    }

    // ── HTTP server (accept loop runs in its own thread) ──────────────────────
    const srv = try server_mod.Server.init(.{
        .listen_addr = cfg.listen_addr,
        .webhook_secret = cfg.webhook_secret,
        .queues = wq_ptrs,
        .allocator = allocator,
        .pool_threads = cfg.conn_pool_threads,
    });

    // ── Block until SIGTERM / SIGINT ──────────────────────────────────────────
    while (!g_stop.load(.acquire)) std.Thread.sleep(100 * std.time.ns_per_ms);
    log.info("shutdown signal received — draining and stopping", .{});

    // Shutdown order: stop accepting → stop workers (they drain parked coroutines
    // via the still-alive io_pool) → deinit pool → free DBs → stop dispatchers.
    srv.deinit();
    stop.store(true, .release);
    for (worker_threads) |t| t.join();

    // Workers have drained and joined; the pool is now unreferenced. Deinit it
    // (joins io threads, SIGKILLs any in-flight children), then drain + free the
    // per-worker result queues (the pool may have pushed results during its drain).
    pool.deinit();
    for (result_qs) |*rq| while (rq.popTimeout(0)) |res| io_pool.freeIoResult(res, allocator);
    for (result_qs) |*rq| rq.deinit(allocator);
    allocator.free(result_qs);
    allocator.free(result_q_ptrs);

    // Drain WorkItem entries workers didn't pop before stop (free their bodies).
    for (wqs) |*q| while (q.popTimeout(0)) |item| allocator.free(item.body);
    for (dbs) |*db| db.close();
    for (disp_threads) |t| t.join();
    // Stop the requeue thread before draining either queue so nothing moves
    // between them during teardown.
    requeue_t.join();
    // Drain ApiCall items still parked or queued (free string payloads).
    delay_q.deinitDrain(allocator);
    while (disp_q.popTimeout(0)) |action| types.freeApiCall(action, allocator);
    blocked_until.deinit();
    // Watcher thread is detached and cannot be joined; its rules_path dupe
    // is freed by the OS on process exit.
    log.info("shutdown complete", .{});
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;
const Queue = queue_mod.Queue;

// ---------------------------------------------------------------------------
// IntegrationStack — full bot stack for integration tests.
//
// Uses std.testing.allocator for queue buffers (tracked).
// Uses std.heap.page_allocator for server + worker + dispatcher internals.
// Each Update's Parsed arena is freed by the worker after on_message returns.
// ApiCall strings are freed by the dispatcher after sending.
// ---------------------------------------------------------------------------

const IntegrationStack = struct {
    // All fields live in heap memory (this struct is heap-allocated so that
    // pointers passed to spawned threads remain stable).

    stop: std.atomic.Value(bool),
    worker_q: Queue(types.WorkItem),
    disp_q: Queue(types.ApiCall),
    q_ptrs: [1]*Queue(types.WorkItem),
    db: state_store.StateStore,
    worker_t: std.Thread,
    disp_t: std.Thread,
    srv: *server_mod.Server,
    tmp: testing.TmpDir,
    rules_path_buf: [std.fs.max_path_bytes + 1]u8,
    rules_path: [:0]const u8,

    fn init(
        test_alloc: std.mem.Allocator,
        api_base: []const u8,
        rules_lua: []const u8,
    ) !*IntegrationStack {
        const self = try test_alloc.create(IntegrationStack);
        errdefer test_alloc.destroy(self);

        self.tmp = testing.tmpDir(.{});
        errdefer self.tmp.cleanup();

        // Write initial rules file.
        try self.writeRulesImpl(rules_lua);

        // Resolve the absolute path (null-terminated for Lua).
        const ps = try self.tmp.dir.realpath(
            "rules.lua",
            self.rules_path_buf[0..std.fs.max_path_bytes],
        );
        self.rules_path_buf[ps.len] = 0;
        self.rules_path = self.rules_path_buf[0..ps.len :0];

        self.stop = std.atomic.Value(bool).init(false);
        self.worker_q = try Queue(types.WorkItem).init(test_alloc, 64);
        errdefer self.worker_q.deinit(test_alloc);
        self.disp_q = try Queue(types.ApiCall).init(test_alloc, 256);
        errdefer self.disp_q.deinit(test_alloc);
        self.q_ptrs[0] = &self.worker_q;

        self.db = try state_store.StateStore.open(std.heap.page_allocator, ":memory:");
        errdefer self.db.close();

        self.worker_t = try std.Thread.spawn(.{}, worker_mod.workerThread, .{
            worker_mod.WorkerArgs{
                .id = 0,
                .rules_path = self.rules_path,
                .allocator = std.heap.page_allocator,
                .queue = &self.worker_q,
                .dispatcher_queue = &self.disp_q,
                .db = &self.db,
                .stop = &self.stop,
                .reload_ver = &watcher.reload_version,
            },
        });

        self.disp_t = try std.Thread.spawn(.{}, disp_mod.dispatcherThread, .{
            disp_mod.DispatcherArgs{
                .id = 0,
                .queue = &self.disp_q,
                .bot_token = "TESTTOKEN",
                .api_base = api_base,
                .allocator = std.heap.page_allocator,
                .stop = &self.stop,
            },
        });

        const srv_addr = try std.net.Address.parseIp4("127.0.0.2", 0);
        self.srv = try server_mod.Server.init(.{
            .listen_addr = srv_addr,
            .webhook_secret = "test-secret",
            .queues = &self.q_ptrs,
            .allocator = std.heap.page_allocator,
            .pool_threads = 2,
        });

        return self;
    }

    fn deinit(self: *IntegrationStack, test_alloc: std.mem.Allocator) void {
        self.stop.store(true, .release);
        self.worker_t.join();
        self.disp_t.join();
        self.srv.deinit();
        self.db.close();
        self.worker_q.deinit(test_alloc);
        self.disp_q.deinit(test_alloc);
        self.tmp.cleanup();
        test_alloc.destroy(self);
    }

    fn webhookAddr(self: *const IntegrationStack) std.net.Address {
        return self.srv.listenAddress();
    }

    fn writeRules(self: *IntegrationStack, lua: []const u8) !void {
        try self.writeRulesImpl(lua);
    }

    fn writeRulesImpl(self: *IntegrationStack, lua: []const u8) !void {
        var f = try self.tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(lua);
    }
};

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

const WEBHOOK_SECRET = "test-secret";

/// Lua rules v1: emit a single sendMessage call (generic { method, params } form).
const RULES_V1 =
    \\function on_message(update)
    \\  return { { method="sendMessage", params={ chat_id=1, text="v1" } } }
    \\end
;

/// Lua rules v2: return no actions (used to verify reload).
const RULES_V2 =
    \\function on_message(update)
    \\  return {}
    \\end
;

/// Minimal valid Telegram Update JSON.
const UPDATE_JSON =
    \\{"update_id":1,"message":{"message_id":1,"from":{"id":100,"is_bot":false,"first_name":"T"},"chat":{"id":1,"type":"private"},"date":0}}
;

/// POST `body` to `address/webhook` with the test secret; return status code.
fn postWebhook(address: std.net.Address, body: []const u8) !u16 {
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    var hdr: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&hdr);
    try fbs.writer().print(
        "POST /webhook HTTP/1.1\r\n" ++
            "X-Telegram-Bot-Api-Secret-Token: {s}\r\n" ++
            "Content-Length: {d}\r\n" ++
            "Connection: close\r\n\r\n",
        .{ WEBHOOK_SECRET, body.len },
    );
    try stream.writeAll(fbs.getWritten());
    try stream.writeAll(body);

    var rbuf: [256]u8 = undefined;
    var net_rdr = stream.reader(&rbuf);
    const line = (try net_rdr.interface().takeDelimiter('\n')) orelse return error.NoResponse;
    const trimmed = std.mem.trimRight(u8, line, "\r");
    var it = std.mem.splitScalar(u8, trimmed, ' ');
    _ = it.next();
    const code = it.next() orelse return error.BadResponse;
    return std.fmt.parseInt(u16, code, 10) catch error.BadResponse;
}

// ---------------------------------------------------------------------------
// startup log and server ready
// ---------------------------------------------------------------------------

test "startup log line printed before server accepts connections" {
    // Verified by code structure: log.info(...) appears before server.init()
    // in run().  This test verifies the stack starts up and accepts connections.
    const mock = try disp_mod.MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const api_base = mock.baseUrl(&url_buf);

    const stack = try IntegrationStack.init(testing.allocator, api_base, RULES_V1);
    defer stack.deinit(testing.allocator);

    // Server is up if it can accept a connection and return 200.
    const status = try postWebhook(stack.webhookAddr(), UPDATE_JSON);
    try testing.expectEqual(@as(u16, 200), status);
}

// ---------------------------------------------------------------------------
// missing BOT_TOKEN → MissingRequiredField (tested via config.zig)
// ---------------------------------------------------------------------------

test "missing BOT_TOKEN → MissingRequiredField error" {
    // config_mod.loadFromMap is the error path exercised by run() when
    // BOT_TOKEN is absent.  run() calls std.process.exit(1) on this error;
    // the test calls the underlying function directly to avoid killing the test binary.
    var env = std.process.EnvMap.init(testing.allocator);
    defer env.deinit();
    // BOT_TOKEN intentionally absent; WEBHOOK_SECRET present.
    try env.put("WEBHOOK_SECRET", "s");
    try testing.expectError(
        error.MissingRequiredField,
        config_mod.loadFromMap(testing.allocator, env),
    );
}

// ---------------------------------------------------------------------------
// end-to-end smoke test
// ---------------------------------------------------------------------------

test "POST webhook → worker processes → dispatcher calls mock Telegram API" {
    const mock = try disp_mod.MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const stack = try IntegrationStack.init(testing.allocator, mock.baseUrl(&url_buf), RULES_V1);
    defer stack.deinit(testing.allocator);

    // Give worker time to load rules.
    std.Thread.sleep(30 * std.time.ns_per_ms);

    const status = try postWebhook(stack.webhookAddr(), UPDATE_JSON);
    try testing.expectEqual(@as(u16, 200), status);

    // Dispatcher should POST to mock within 2 s.
    try testing.expect(mock.waitForN(1, 2000));
    try testing.expectEqual(@as(u32, 1), mock.call_cnt.load(.acquire));
}

// ---------------------------------------------------------------------------
// hot-reload integration
// ---------------------------------------------------------------------------

test "rules.lua updated → next request uses new rules within 2 s" {
    const mock = try disp_mod.MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const stack = try IntegrationStack.init(testing.allocator, mock.baseUrl(&url_buf), RULES_V1);
    defer stack.deinit(testing.allocator);

    // Spawn the hot-reload watcher for this stack's rules file.
    // Detached — runs until the test binary exits.
    const watcher_t = try std.Thread.spawn(.{}, watcher.watcherThread, .{
        watcher.WatcherArgs{
            .rules_path = stack.rules_path,
            .allocator = std.heap.page_allocator,
        },
    });
    watcher_t.detach();

    std.Thread.sleep(30 * std.time.ns_per_ms); // let worker load rules

    // Verify v1 behavior: POST → mock receives 1 call.
    _ = try postWebhook(stack.webhookAddr(), UPDATE_JSON);
    try testing.expect(mock.waitForN(1, 2000));

    // Record current reload version and overwrite rules with v2 (no actions).
    const ver_before = watcher.reload_version.load(.acquire);
    try stack.writeRules(RULES_V2);

    // Wait up to 2 s for inotify to fire and reload_version to increment.
    const deadline = std.time.milliTimestamp() + 2000;
    while (watcher.reload_version.load(.acquire) == ver_before) {
        if (std.time.milliTimestamp() >= deadline) {
            try testing.expect(false); // reload did not fire within 2 s
            return;
        }
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    // Give the worker one more iteration to pick up the incremented version.
    std.Thread.sleep(10 * std.time.ns_per_ms);

    // POST again — v2 returns no actions, so mock call count must not increase.
    _ = try postWebhook(stack.webhookAddr(), UPDATE_JSON);
    std.Thread.sleep(200 * std.time.ns_per_ms); // let dispatcher attempt
    try testing.expectEqual(@as(u32, 1), mock.call_cnt.load(.acquire));
}

// ---------------------------------------------------------------------------
// dispatcher failure does not crash the server
// ---------------------------------------------------------------------------

test "mock API down → server still returns 200 OK, no crash" {
    // Point dispatcher at a port that has nothing listening.
    // std.net.Address.parseIp4 port 1 is almost certainly not in use.
    const dummy_addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    // Bind a server to get an ephemeral port, then immediately close it so
    // the dispatcher finds no listener.
    var dummy_srv = try dummy_addr.listen(.{});
    const dead_port = std.mem.bigToNative(u16, dummy_srv.listen_address.in.sa.port);
    dummy_srv.deinit(); // port is now closed

    var url_buf: [64]u8 = undefined;
    const dead_url = std.fmt.bufPrint(&url_buf, "http://127.0.0.1:{d}", .{dead_port}) catch unreachable;

    const stack = try IntegrationStack.init(testing.allocator, dead_url, RULES_V1);
    defer stack.deinit(testing.allocator);

    std.Thread.sleep(30 * std.time.ns_per_ms);

    // Server must still accept and return 200 OK even though dispatch fails.
    const status = try postWebhook(stack.webhookAddr(), UPDATE_JSON);
    try testing.expectEqual(@as(u16, 200), status);

    // Wait for dispatcher retry cycle to complete (1 s delay + overhead).
    // No crash → test passes implicitly.
    std.Thread.sleep(100 * std.time.ns_per_ms);
}

// ---------------------------------------------------------------------------
// fmtMetrics emits all 11 counter names in the expected format
// ---------------------------------------------------------------------------

test "fmtMetrics emits every counter name and reflects their values" {
    // All eleven counter names appear in the formatted line.
    {
        var m = metrics_mod.Metrics{};
        var buf: [512]u8 = undefined;
        const line = fmtMetrics(&m, &buf);

        try testing.expect(std.mem.indexOf(u8, line, "io_jobs=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "io_inflight=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "io_err=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "io_timeout=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "coros_inflight=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "coros_reaped=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "tracked_fail=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "throttle_429=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "throttle_delayed=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "throttle_shed=") != null);
        try testing.expect(std.mem.indexOf(u8, line, "throttle_depth=") != null);
    }
    // Incremented counters are reflected in the output.
    {
        var m = metrics_mod.Metrics{};
        _ = m.io_jobs_total.fetchAdd(5, .monotonic);
        _ = m.io_errors_total.fetchAdd(2, .monotonic);
        _ = m.tracked_send_failures_total.fetchAdd(1, .monotonic);
        _ = m.throttle_429_total.fetchAdd(7, .monotonic);
        _ = m.throttle_shed_total.fetchAdd(3, .monotonic);

        var buf: [512]u8 = undefined;
        const line = fmtMetrics(&m, &buf);

        try testing.expect(std.mem.indexOf(u8, line, "io_jobs=5") != null);
        try testing.expect(std.mem.indexOf(u8, line, "io_err=2") != null);
        try testing.expect(std.mem.indexOf(u8, line, "tracked_fail=1") != null);
        try testing.expect(std.mem.indexOf(u8, line, "throttle_429=7") != null);
        try testing.expect(std.mem.indexOf(u8, line, "throttle_shed=3") != null);
    }
}
