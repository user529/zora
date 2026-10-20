//! main.zig — entry point: parse config, verify DB, start all subsystems.
//!
//! Startup order (everything logs before the server accepts):
//!   1. Load Config and the API schema
//!   2. Open + verify the SQLite schema and encryption key (exit on mismatch)
//!   3. Install the SIGTERM/SIGINT handlers; print the banner and the config
//!   4. Allocate queues; spawn the dispatcher and requeue threads
//!   5. Start the io_pool and the scheduler; spawn the worker threads (each
//!      opens its own DB connection)
//!   6. Spawn the rules and schema watchers (detached; never joined)
//!   7. Bind the HTTP server and, when METRICS_ADDR is set, the scrape endpoint
//!   8. Block until SIGTERM/SIGINT, then shut down in order (see run())
const std = @import("std");
const build_opts = @import("build_options");

// Pin the log level to .info in every build mode, so a debug build logs the
// same lines as a release build (Zig's default for debug builds is .debug).
pub const std_options: std.Options = .{
    .log_level = .info,
    .logFn = timestampedLogFn,
};

/// Custom log backend: prepends a UTC timestamp (ISO 8601, ms precision) to
/// every line. std.debug.lockStderr serializes the writes across threads.
fn timestampedLogFn(
    comptime level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: []const u8,
    args: anytype,
) void {
    const ms = rt.nowMs(rt.io());
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
    const locked = std.debug.lockStderr(&buffer);
    defer std.debug.unlockStderr();
    const stderr = &locked.file_writer.interface;

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
const metrics_server_mod = @import("metrics_server.zig");
const io_pool = @import("io_pool.zig");
const delay_mod = @import("delay.zig");
const scheduler_mod = @import("scheduler.zig");
const rt = @import("rt.zig");
const Latch = @import("latch.zig").Latch;

const log = std.log.scoped(.main);

pub const RELEASE: u32 = build_opts.release;
pub const GIT_BRANCH: []const u8 = build_opts.git_branch;

// ---------------------------------------------------------------------------
// Signal handling — SIGTERM / SIGINT set this latch; run() blocks on it.
// ---------------------------------------------------------------------------

/// run() opens the latch before it installs the handlers and never closes it:
/// a late signal must not write into a closed fd number that has since been
/// reused.
var g_stop: Latch = undefined;
var g_metrics = metrics_mod.Metrics{};
var g_schema_slot: tg_schema.SchemaSlot = undefined;

fn sigStop(sig: std.posix.SIG) callconv(std.lang.CallingConvention.c) void {
    _ = sig;
    g_stop.set();
}

// ---------------------------------------------------------------------------
// Startup banner
// ---------------------------------------------------------------------------

/// Write the startup banner to `writer`: the six identity fields (branch,
/// release, schema, rules_api, api_validation, and enc — on/off, never the
/// passphrase itself). Used by the production startup path (which formats
/// then `log.info`s the result) and by tests (which inspect the output
/// directly, since test_runner.zig owns the std.log backend).
fn writeBanner(writer: *std.Io.Writer, validation: types.ValidationMode, enc: bool) std.Io.Writer.Error!void {
    try writer.print(
        "zora starting (branch={s} release={d} schema={d} rules_api={d} api_validation={s} enc={s})",
        .{
            GIT_BRANCH,
            RELEASE,
            state_store.SCHEMA_VERSION,
            lua_engine.RULES_API_VERSION,
            @tagName(validation),
            if (enc) "on" else "off",
        },
    );
}

// ---------------------------------------------------------------------------
// Metrics log thread — emits a snapshot every 60 s when METRICS_LOG=true
// ---------------------------------------------------------------------------

/// Format all 14 metric counters into `buf`. Returns the written slice.
/// Deprecated: frozen at the pre-Prometheus counter set — it gains no new
/// counters; metrics.renderPrometheus is the authoritative exposition.
/// The throttle group reports reactive rate limiting: 429s seen, calls parked
/// in the delay queue, calls shed on overflow, and the current queue depth.
/// The route group reports per-user-affinity relaxations: updates overflowed to
/// a non-primary worker, and updates dropped with every queue full.
pub fn fmtMetrics(m: *const metrics_mod.Metrics, buf: []u8) []const u8 {
    return std.fmt.bufPrint(
        buf,
        "io_jobs={d} io_inflight={d} io_err={d} io_timeout={d} coros_inflight={d} coros_reaped={d} tracked_fail={d} resp_oversize={d} throttle_429={d} throttle_delayed={d} throttle_shed={d} throttle_depth={d} route_overflow={d} route_drop={d}",
        .{
            m.io_jobs_total.load(.monotonic),
            m.io_jobs_inflight.load(.monotonic),
            m.io_errors_total.load(.monotonic),
            m.io_timeouts_total.load(.monotonic),
            m.coroutines_inflight.load(.monotonic),
            m.coroutines_reaped_total.load(.monotonic),
            m.tracked_send_failures_total.load(.monotonic),
            m.response_oversize_total.load(.monotonic),
            m.throttle_429_total.load(.monotonic),
            m.throttle_delayed_total.load(.monotonic),
            m.throttle_shed_total.load(.monotonic),
            m.throttle_delay_depth.load(.monotonic),
            m.route_overflow_total.load(.monotonic),
            m.route_drop_total.load(.monotonic),
        },
    ) catch "[metrics fmt overflow]";
}

fn metricsLogThread(m: *const metrics_mod.Metrics) void {
    const metrics_log = std.log.scoped(.metrics);
    while (true) {
        rt.sleepNs(rt.io(), 60 * std.time.ns_per_s);
        var buf: [512]u8 = undefined;
        metrics_log.info("{s}", .{fmtMetrics(m, &buf)});
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn main(init: std.process.Init) u8 {
    // Allocator: c_allocator (libc malloc), NOT smp_allocator.
    //
    // smp_allocator never returns freed small-allocation slabs to the OS (by
    // design — it caches them in per-thread, per-size-class freelists for speed).
    // Under sustained multi-threaded per-message churn that makes RSS ratchet up
    // unbounded.
    //
    // libc malloc returns freed memory to the OS and, crucially, lets operators
    // pick the malloc implementation at deploy time via LD_PRELOAD without a
    // rebuild. jemalloc is RECOMMENDED (flat RSS + background decay purging that
    // reclaims burst spikes). The throughput cost is negligible; throughput
    // stays far above Telegram's ~1000 msg/s ceiling.
    //
    // To investigate leaks, temporarily swap in SafeAllocator:
    //   var da: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{});
    //   defer _ = da.deinit();
    //   run(da.allocator(), rt.io(), init.environ_map.*) catch return 1;
    //
    // The process Io comes from rt (one shared OS-backed runtime — see
    // rt.zig); `std.process.Init` supplies the environment map.
    //
    // run() returns an error only from startup (a bind, spawn, allocation, or
    // fd failure); main() names it for the operator before exiting with 1.
    run(std.heap.c_allocator, rt.io(), init.environ_map.*) catch |err| {
        log.err("startup failed: {s}", .{@errorName(err)});
        return 1;
    };
    return 0;
}

// ---------------------------------------------------------------------------
// Core startup — separated from main() so every startup error returns to
// the one place that logs it
// ---------------------------------------------------------------------------

fn run(allocator: std.mem.Allocator, io: std.Io, env: std.process.Environ.Map) !void {
    // ── Config ───────────────────────────────────────────────────────────────
    const cfg = config_mod.loadFromMap(allocator, env) catch |err| {
        log.err("configuration: {s}", .{@errorName(err)});
        std.process.exit(1);
    };
    defer config_mod.deinit(cfg, allocator);

    // ── Encryption open-options (derived once; passed to every StateStore open) ─
    // The passphrase is never printed — only on/off appears in the banner.
    const open_opts: state_store.OpenOptions = if (cfg.state_encryption_key) |k|
        .{ .encryption = .{ .passphrase = k, .io = io } }
    else
        .{};

    // ── Sanitized child environment (secrets stripped; passed to io_pool) ───────
    // BOT_TOKEN, WEBHOOK_SECRET, and STATE_ENCRYPTION_KEY are excluded so child
    // processes (bot.shell / bot.exec) cannot read them.
    var child_env = try config_mod.sanitizeChildEnv(allocator, env);
    defer child_env.deinit();

    // ── API schema (hot-reloadable; absent file → no validation) ──────────────
    // g_schema_slot is a module global so its address is stable for the process
    // lifetime — the detached schema-watcher thread can safely hold a pointer to
    // it across run() returning.
    g_schema_slot = tg_schema.SchemaSlot.init(allocator, io);
    tg_schema.loadInitial(&g_schema_slot, cfg.schema_file);

    // The first connection verifies the schema and the encryption key before
    // anything starts (exit on failure), then serves the scheduler.
    var sched_db = state_store.StateStore.openWithOptions(allocator, cfg.db_path, open_opts) catch |err| {
        switch (err) {
            error.EncryptionModeMismatch => log.err(
                "database '{s}': encryption mode does not match STATE_ENCRYPTION_KEY (set/unset) — run the migration tool",
                .{cfg.db_path},
            ),
            error.WrongEncryptionKey => log.err(
                "database '{s}': wrong STATE_ENCRYPTION_KEY",
                .{cfg.db_path},
            ),
            else => log.err("database '{s}': {s}", .{ cfg.db_path, @errorName(err) }),
        }
        std.process.exit(1);
    };
    defer sched_db.close();

    // ── Signal handlers (SIGTERM / SIGINT → clean shutdown) ──────────────────
    // `SA.RESTART` restarts most slow syscalls a handler interrupts (read,
    // recv, accept) instead of failing them with `EINTR`. poll() is never
    // restarted on Linux; `std.posix.poll` retries on `EINTR` itself, so the
    // poll-based waits do not depend on the flag.
    g_stop = try Latch.init();
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = sigStop },
        .mask = std.posix.sigemptyset(),
        .flags = std.posix.SA.RESTART,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);

    // ── Startup banner — before server.init ───────────────────────────────────
    var banner_buf: [256]u8 = undefined;
    var banner_fw = std.Io.Writer.fixed(&banner_buf);
    writeBanner(&banner_fw, cfg.api_validation, cfg.state_encryption_key != null) catch {}; // 256 B covers all fields
    log.info("{s}", .{banner_fw.buffered()});

    // Effective configuration (secrets partially masked) — one line per param.
    config_mod.logEffective(cfg);

    var stop = std.atomic.Value(bool).init(false);

    // ── Metrics log thread (detached; runs until process exit) ────────────────
    // g_metrics is module-level, so its lifetime outlives run() and the
    // detached thread safely reads it after run() returns.
    if (cfg.metrics_log) {
        log.warn("METRICS_LOG snapshot line is deprecated and frozen; scrape GET /metrics (METRICS_ADDR) instead", .{});
        const metrics_t = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, metricsLogThread, .{&g_metrics});
        metrics_t.detach();
    }

    // ── Per-worker result queues ──────────────────────────────────────────────
    // The io_pool pushes I/O results here, and the dispatchers push the outcome
    // of tracked sends (bot.send_message), both routed by worker_id. They are
    // created before either producer starts and freed after both have stopped.
    const result_qs = try allocator.alloc(queue_mod.Queue(io_pool.IoResult), cfg.worker_threads);
    errdefer allocator.free(result_qs);
    var result_qs_init: usize = 0;
    errdefer for (result_qs[0..result_qs_init]) |*rq| rq.deinit(allocator);
    for (0..cfg.worker_threads) |i| {
        result_qs[i] = try queue_mod.Queue(io_pool.IoResult).init(allocator, io, cfg.io_queue_capacity);
        result_qs[i].id = i;
        result_qs[i].metrics_log = cfg.metrics_log;
        result_qs[i].kind = .io_result;
        result_qs_init += 1;
    }

    const result_q_ptrs = try allocator.alloc(*queue_mod.Queue(io_pool.IoResult), cfg.worker_threads);
    errdefer allocator.free(result_q_ptrs);
    for (0..cfg.worker_threads) |i| result_q_ptrs[i] = &result_qs[i];

    // ── Dispatcher queue + threads ────────────────────────────────────────────
    var disp_q = try queue_mod.Queue(types.ApiCall).init(allocator, io, 4096);
    defer disp_q.deinit(allocator);
    disp_q.kind = .dispatcher;

    // ── Throttle: delay queue + per-chat block map + requeue thread ───────────
    var delay_q = delay_mod.DelayQueue.init(allocator, io, cfg.delay_queue_capacity);
    var blocked_until = delay_mod.BlockedMap.init(allocator, io);

    const requeue_t = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, delay_mod.requeueThread, .{
        delay_mod.RequeueArgs{
            .delay_q = &delay_q,
            .disp_q = &disp_q,
            .metrics = &g_metrics,
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
                .api_base = cfg.bot_api_base,
                .allocator = allocator,
                .io = io,
                .stop = &stop,
                .io_result_queues = result_q_ptrs,
                .metrics = &g_metrics,
                .delay_q = &delay_q,
                .blocked_until = &blocked_until,
                .retry_after_default_ms = cfg.retry_after_default_ms,
                .retry_after_max_ms = cfg.retry_after_max_ms,
            },
        });
    }

    // ── Worker queues + threads ───────────────────────────────────────────────
    const wqs = try allocator.alloc(queue_mod.Queue(types.WorkItem), cfg.worker_threads);
    defer {
        for (wqs) |*q| q.deinit(allocator);
        allocator.free(wqs);
    }
    const wq_ptrs = try allocator.alloc(*queue_mod.Queue(types.WorkItem), cfg.worker_threads);
    defer allocator.free(wq_ptrs);

    for (0..cfg.worker_threads) |i| {
        wqs[i] = try queue_mod.Queue(types.WorkItem).init(allocator, io, cfg.worker_queue_capacity);
        wqs[i].id = i;
        wqs[i].metrics_log = cfg.metrics_log;
        wqs[i].kind = .worker;
        wq_ptrs[i] = &wqs[i];
    }

    // ── Single shared io_pool that executes blocking I/O for parked coroutines ─
    var pool: io_pool.IoPool = undefined;
    try pool.init(allocator, io, io_pool.IoPoolConfig{
        .thread_count = cfg.io_pool_threads,
        .queue_capacity = cfg.io_queue_capacity,
        .timeout_ms = cfg.io_job_timeout_ms,
        .proc_max_output = cfg.proc_max_output_bytes,
        .metrics = &g_metrics,
        .child_env = &child_env,
    }, result_q_ptrs);
    errdefer pool.deinit();

    // ── Scheduler: own DB connection + timer thread ───────────────────────────
    // lease_ms must exceed workflow_deadline_ms so a legitimately-slow async job
    // is never reclaimed and double-fired.
    std.debug.assert(scheduler_mod.DEFAULT_LEASE_MS > @as(i64, @intCast(cfg.workflow_deadline_ms)));
    var g_scheduler = scheduler_mod.Scheduler{ .io = io };
    const scheduler_t = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, scheduler_mod.schedulerThread, .{
        scheduler_mod.SchedulerArgs{
            .db = &sched_db,
            .worker_qs = wq_ptrs,
            .sched = &g_scheduler,
            .stop = &stop,
            .io = io,
            .allocator = allocator,
            .metrics = &g_metrics,
        },
    });

    const worker_threads = try allocator.alloc(std.Thread, cfg.worker_threads);
    defer allocator.free(worker_threads);

    // Track per-worker DB pointers so they can be freed after workers join.
    const dbs = try allocator.alloc(state_store.StateStore, cfg.worker_threads);
    defer allocator.free(dbs); // only frees the slice backing array

    var opened_dbs: usize = 0;
    errdefer for (dbs[0..opened_dbs]) |*db| db.close();

    for (0..cfg.worker_threads) |i| {
        dbs[i] = try state_store.StateStore.openWithOptions(allocator, cfg.db_path, open_opts);
        opened_dbs += 1;
        worker_threads[i] = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, worker_mod.workerThread, .{
            worker_mod.WorkerArgs{
                .id = @intCast(i),
                .rules_path = cfg.rules_file,
                .allocator = allocator,
                .io = io,
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
                .max_inflight = cfg.worker_max_inflight,
                .workflow_deadline_ms = cfg.workflow_deadline_ms,
                .scheduler = &g_scheduler,
            },
        });
    }

    // ── Hot-reload watcher (detached; never joined) ───────────────────────────
    // Each detached watcher outlives `cfg`, which run() frees on return, so
    // it gets its own copy of its path, made here before the spawn: no
    // watcher thread ever reads `cfg`.
    {
        const rules_path = try allocator.dupe(u8, cfg.rules_file);
        errdefer allocator.free(rules_path);
        const watcher_t = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, watcher.watcherThread, .{
            watcher.WatcherArgs{
                .rules_path = rules_path,
                .allocator = allocator,
                .io = io,
            },
        });
        watcher_t.detach();
    }

    // ── Schema-file watcher (detached; mirrors the rules watcher) ─────────────
    {
        const schema_file = try allocator.dupe(u8, cfg.schema_file);
        errdefer allocator.free(schema_file);
        const schema_watcher_t = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, tg_schema.schemaWatcherThread, .{
            tg_schema.SchemaWatcherArgs{
                .schema_file = schema_file,
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
        .io = io,
        .pool_threads = cfg.webhook_pool_threads,
        .metrics = &g_metrics,
    });

    // ── Prometheus scrape endpoint (only when METRICS_ADDR is set) ────────────
    const metrics_srv: ?*metrics_server_mod.MetricsServer = if (cfg.metrics_addr) |maddr|
        try metrics_server_mod.MetricsServer.init(.{
            .listen_addr = maddr,
            .metrics = &g_metrics,
            .sources = .{
                .worker_queues = wq_ptrs,
                .dispatcher_queue = &disp_q,
                .release = RELEASE,
                .branch = GIT_BRANCH,
            },
            .io = io,
            .allocator = allocator,
        })
    else
        null;

    // ── Block until SIGTERM / SIGINT ──────────────────────────────────────────
    g_stop.wait() catch |err| log.err("waiting for a stop signal: {s} — shutting down", .{@errorName(err)});
    log.info("shutdown signal received — draining and stopping", .{});

    // Shutdown order: stop accepting → stop the scrape endpoint (join before the
    // queues it samples are freed) → stop workers (they drain parked coroutines
    // via the still-alive io_pool) → stop scheduler → deinit pool → free DBs →
    // stop dispatchers.
    srv.deinit();
    if (metrics_srv) |ms| ms.deinit();
    stop.store(true, .release);
    // Wake any workers parked idle in popBlocking so they observe stop and exit.
    for (wqs) |*q| q.close();
    for (worker_threads) |t| t.join();

    // Unpark the scheduler timer so it observes stop and exits promptly.
    // Placed after workers join: workers may still call bot.schedule_* while
    // draining, so g_scheduler must outlive them. sched_db is closed by its
    // defer after this join, which is the correct order.
    g_scheduler.wakeUp();
    scheduler_t.join();

    // Workers have drained and joined; the pool is now unreferenced. Deinit it
    // (joins io threads, SIGKILLs any in-flight children).
    pool.deinit();

    // Drain WorkItem entries workers didn't pop before stop (free their bodies).
    for (wqs) |*q| while (q.popTimeout(0)) |item| allocator.free(item.body);
    for (dbs) |*db| db.close();
    // Wake the parked dispatchers so they drain and exit. close() is the
    // shutdown signal for the queue consumers.
    disp_q.close();
    for (disp_threads) |t| t.join();
    // Stop the requeue thread before draining either queue so nothing moves
    // between them during teardown.
    delay_q.close();
    requeue_t.join();
    // Drain ApiCall items still parked or queued (free string payloads).
    delay_q.deinitDrain();
    while (disp_q.popTimeout(0)) |action| types.freeApiCall(action, allocator);
    // Both producers of the result queues (io_pool, dispatchers) have stopped:
    // drain what they pushed during shutdown and free the queues.
    for (result_qs) |*rq| while (rq.popTimeout(0)) |res| io_pool.freeIoResult(res, allocator);
    for (result_qs) |*rq| rq.deinit(allocator);
    allocator.free(result_qs);
    allocator.free(result_q_ptrs);
    blocked_until.deinit();
    // The watcher threads are detached and cannot be joined; their path
    // copies are freed by the OS on process exit.
    log.info("shutdown complete", .{});
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;
const test_util = @import("test_util.zig");
const Queue = queue_mod.Queue;

// ---------------------------------------------------------------------------
// IntegrationStack — full bot stack for the end-to-end smoke test.
//
// Uses std.testing.allocator for queue buffers (tracked).
// Uses std.heap.page_allocator for server + worker + dispatcher internals.
// The worker frees each update body once its handler has started; the
// dispatcher frees each ApiCall after sending.
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

    // Shared by server, worker and dispatcher so counting runs the real path.
    metrics: metrics_mod.Metrics,

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
        try self.tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = rules_lua });

        // Resolve the absolute path (null-terminated for Lua).
        const ps_len = try self.tmp.dir.realPathFile(
            testing.io,
            "rules.lua",
            self.rules_path_buf[0..std.fs.max_path_bytes],
        );
        self.rules_path_buf[ps_len] = 0;
        self.rules_path = self.rules_path_buf[0..ps_len :0];

        self.stop = std.atomic.Value(bool).init(false);
        self.worker_q = try Queue(types.WorkItem).init(test_alloc, testing.io, 64);
        errdefer self.worker_q.deinit(test_alloc);
        self.disp_q = try Queue(types.ApiCall).init(test_alloc, testing.io, 256);
        errdefer self.disp_q.deinit(test_alloc);
        self.q_ptrs[0] = &self.worker_q;

        self.db = try state_store.StateStore.open(std.heap.page_allocator, ":memory:");
        errdefer self.db.close();

        // Constructed before the threads spawn so its address is stable for
        // the args pointers.
        self.metrics = .{};

        self.worker_t = try std.Thread.spawn(.{}, worker_mod.workerThread, .{
            worker_mod.WorkerArgs{
                .id = 0,
                .rules_path = self.rules_path,
                .allocator = std.heap.page_allocator,
                .io = testing.io,
                .queue = &self.worker_q,
                .dispatcher_queue = &self.disp_q,
                .db = &self.db,
                .stop = &self.stop,
                .reload_ver = &watcher.reload_version,
                .metrics = &self.metrics,
            },
        });

        self.disp_t = try std.Thread.spawn(.{}, disp_mod.dispatcherThread, .{
            disp_mod.DispatcherArgs{
                .id = 0,
                .queue = &self.disp_q,
                .bot_token = "TESTTOKEN",
                .api_base = api_base,
                .allocator = std.heap.page_allocator,
                .io = testing.io,
                .stop = &self.stop,
                .metrics = &self.metrics,
            },
        });

        const srv_addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
        self.srv = try server_mod.Server.init(.{
            .listen_addr = srv_addr,
            .webhook_secret = "test-secret",
            .queues = &self.q_ptrs,
            .allocator = std.heap.page_allocator,
            .io = testing.io,
            .pool_threads = 2,
            .metrics = &self.metrics,
        });

        return self;
    }

    fn deinit(self: *IntegrationStack, test_alloc: std.mem.Allocator) void {
        self.stop.store(true, .release);
        self.worker_q.close(); // wake the parked worker so it can observe stop
        self.worker_t.join();
        self.disp_q.close(); // wake the parked dispatcher so it can observe stop
        self.disp_t.join();
        self.srv.deinit();
        self.db.close();
        self.worker_q.deinit(test_alloc);
        self.disp_q.deinit(test_alloc);
        self.tmp.cleanup();
        test_alloc.destroy(self);
    }

    fn webhookAddr(self: *const IntegrationStack) std.Io.net.IpAddress {
        return test_util.boundAddress(&self.srv.listener);
    }
};

const WEBHOOK_SECRET = "test-secret";

/// Lua rules: emit a single sendMessage call (generic { method, params } form).
const RULES_V1 =
    \\function on_message(update)
    \\  return { { method="sendMessage", params={ chat_id=1, text="v1" } } }
    \\end
;

/// Minimal valid Telegram Update JSON.
const UPDATE_JSON =
    \\{"update_id":1,"message":{"message_id":1,"from":{"id":100,"is_bot":false,"first_name":"T"},"chat":{"id":1,"type":"private"},"date":0}}
;

/// POST `body` to `address/webhook` with the test secret; return status code.
fn postWebhook(address: std.Io.net.IpAddress, body: []const u8) !u16 {
    const io = testing.io;
    var stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var wbuf: [1024]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    const w = &sw.interface;
    try w.print(
        "POST /webhook HTTP/1.1\r\n" ++
            "X-Telegram-Bot-Api-Secret-Token: {s}\r\n" ++
            "Content-Length: {d}\r\n" ++
            "Connection: close\r\n\r\n",
        .{ WEBHOOK_SECRET, body.len },
    );
    try w.writeAll(body);
    try w.flush();

    var rbuf: [256]u8 = undefined;
    var net_rdr = stream.reader(io, &rbuf);
    const line = (try net_rdr.interface.takeDelimiter('\n')) orelse return error.NoResponse;
    const trimmed = std.mem.trimEnd(u8, line, "\r");
    var it = std.mem.splitScalar(u8, trimmed, ' ');
    _ = it.next();
    const code = it.next() orelse return error.BadResponse;
    return std.fmt.parseInt(u16, code, 10) catch error.BadResponse;
}

test "startup banner contains all six identity fields" {
    // The production startup path formats the banner via writeBanner then
    // log.info()s it; test_runner.zig owns the std.log backend, so the test
    // inspects writeBanner's output directly (the same string the banner logs).
    var buf: [256]u8 = undefined;
    var fw = std.Io.Writer.fixed(&buf);
    try writeBanner(&fw, .warn, false);
    const banner = fw.buffered();

    // The banner prefix and every field name=value must be present.
    try testing.expect(std.mem.find(u8, banner, "zora starting") != null);

    var rel_buf: [16]u8 = undefined;
    const rel = std.fmt.bufPrint(&rel_buf, "release={d}", .{RELEASE}) catch unreachable;
    var sch_buf: [16]u8 = undefined;
    const sch = std.fmt.bufPrint(&sch_buf, "schema={d}", .{state_store.SCHEMA_VERSION}) catch unreachable;
    var api_buf: [20]u8 = undefined;
    const api = std.fmt.bufPrint(&api_buf, "rules_api={d}", .{lua_engine.RULES_API_VERSION}) catch unreachable;

    var branch_buf: [64]u8 = undefined;
    const branch = std.fmt.bufPrint(&branch_buf, "branch={s}", .{GIT_BRANCH}) catch unreachable;

    try testing.expect(std.mem.find(u8, banner, branch) != null);
    try testing.expect(std.mem.find(u8, banner, rel) != null);
    try testing.expect(std.mem.find(u8, banner, sch) != null);
    try testing.expect(std.mem.find(u8, banner, api) != null);
    // api_validation reflects the passed mode (proves the field is not hard-coded).
    try testing.expect(std.mem.find(u8, banner, "api_validation=warn") != null);
    // enc reflects the passed flag (false -> off).
    try testing.expect(std.mem.find(u8, banner, "enc=off") != null);
}

// End-to-end smoke test: the module tests cover each component; this one
// checks they are wired together.
test "POST webhook → worker processes → dispatcher calls mock Telegram API" {
    const mock = try disp_mod.MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const stack = try IntegrationStack.init(testing.allocator, mock.baseUrl(&url_buf), RULES_V1);
    defer stack.deinit(testing.allocator);

    // Give worker time to load rules.
    rt.sleepNs(testing.io, 30 * std.time.ns_per_ms);

    const status = try postWebhook(stack.webhookAddr(), UPDATE_JSON);
    try testing.expectEqual(@as(u16, 200), status);

    // Dispatcher should POST to mock within 2 s.
    try testing.expect(mock.waitForN(1, 2000));
    try testing.expectEqual(@as(u32, 1), mock.call_cnt.load(.acquire));
}
