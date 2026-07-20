/// config.zig — load configuration from environment variables.
///
/// Entry point:
///   loadFromMap(allocator, env)  — reads from a caller-supplied environment map.
///
/// The caller owns the environment map. The process entry point obtains it from
/// the runtime (`std.process.Init.environ_map`); tests build one by hand. Zig
/// 0.16 removed the self-fetching `std.process.getEnvMap`, so the environment is
/// always passed in.
///
/// String fields in the returned Config are allocated from `allocator`.
/// Call deinit(config, allocator) when the config is no longer needed.

const std = @import("std");
const types = @import("types.zig");

const log_config = std.log.scoped(.config);

pub const Config = types.Config;

pub const ConfigError = error{
    MissingRequiredField,
    InvalidConfig,
    OutOfMemory,
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Load config from an explicit environment map. It duplicates string fields
/// from `env` values; `allocator` owns them.
pub fn loadFromMap(allocator: std.mem.Allocator, env: std.process.Environ.Map) ConfigError!Config {
    // Required fields
    const bot_token = try getRequired(allocator, env, "BOT_TOKEN");
    errdefer allocator.free(bot_token);

    const webhook_secret = try getRequired(allocator, env, "WEBHOOK_SECRET");
    errdefer allocator.free(webhook_secret);
    if (webhook_secret.len == 0) return error.InvalidConfig;

    // Optional: LISTEN_ADDR
    const listen_addr_str = env.get("LISTEN_ADDR") orelse "0.0.0.0:8443";
    const listen_addr = std.Io.net.IpAddress.parseLiteral(listen_addr_str) catch
        return error.InvalidConfig;

    // Optional: RULES_FILE
    const rules_file = allocator.dupeSentinel(u8, env.get("RULES_FILE") orelse "rules/rules.lua", 0) catch
        return error.OutOfMemory;
    errdefer allocator.free(rules_file);

    // Optional: DB_PATH
    const db_path = allocator.dupeSentinel(u8, env.get("DB_PATH") orelse "state.db", 0) catch
        return error.OutOfMemory;
    errdefer allocator.free(db_path);

    // Optional: STATE_ENCRYPTION_KEY (passphrase). Presence enables encrypted
    // state-at-rest; absence keeps the database plaintext.
    const state_encryption_key: ?[]const u8 = if (env.get("STATE_ENCRYPTION_KEY")) |v|
        (allocator.dupe(u8, v) catch return error.OutOfMemory)
    else
        null;
    errdefer if (state_encryption_key) |k| allocator.free(k);

    // Optional: SCHEMA_FILE
    const schema_file = allocator.dupeSentinel(u8, env.get("SCHEMA_FILE") orelse "schema/botapi.json", 0) catch
        return error.OutOfMemory;
    errdefer allocator.free(schema_file);

    // Optional: API_VALIDATION
    const api_validation = try parseValidationMode(env);

    // Optional: BOT_API_BASE
    const bot_api_base = allocator.dupe(u8, env.get("BOT_API_BASE") orelse "https://api.telegram.org") catch
        return error.OutOfMemory;
    errdefer allocator.free(bot_api_base);

    // Optional: WORKER_THREADS (default: cpu_count, minimum 2 — a single
    // worker degrades affinity routing and removes parallelism; two is the
    // smallest safe value).
    const worker_threads = try parseUintMin(u8, env, "WORKER_THREADS", cpuScaledThreads(1), 2);

    // Optional: WORKER_QUEUE_CAPACITY (per-worker inbound burst buffer; full → update
    // dropped and Telegram retries, so size for bursts — RAM is abundant here)
    const worker_queue_capacity = try parseUintMin(u16, env, "WORKER_QUEUE_CAPACITY", 1024, 1);

    // Optional: DISPATCHER_THREADS (default: 2 * cpu_count, minimum 2 —
    // network-I/O-bound outbound senders; thread count multiplies throughput).
    const dispatcher_threads = try parseUintMin(u8, env, "DISPATCHER_THREADS", cpuScaledThreads(2), 1);

    // Optional: WEBHOOK_POOL_THREADS (default: cpu_count, minimum 2 — webhook
    // connection-handler pool size). The pool reuses threads, so this bounds
    // concurrency, not per-message allocation.
    const webhook_pool_threads = try parseUint(u8, env, "WEBHOOK_POOL_THREADS", cpuScaledThreads(1));

    // Optional: JSON_MAX_BYTES (1 MB default — max size for json.decode/encode in Lua)
    const json_max_bytes = try parseUint(usize, env, "JSON_MAX_BYTES", 1048576);

    // Optional: IO_POOL_THREADS / IO_QUEUE_CAPACITY / IO_JOB_TIMEOUT_MS / PROC_MAX_OUTPUT_BYTES
    // IO_POOL_THREADS kept flat (not cpu-scaled): this pool also spawns
    // subprocesses (exec/shell), which are heavy and shouldn't scale with box size.
    const io_pool_threads   = try parseUintMin(u8,  env, "IO_POOL_THREADS",   8,   1);
    const io_queue_capacity = try parseUintMin(u16, env, "IO_QUEUE_CAPACITY", 256, 1);
    const io_job_timeout_ms = try parseUint(u64,   env, "IO_JOB_TIMEOUT_MS", 30_000);
    const proc_max_output_bytes   = try parseUint(usize, env, "PROC_MAX_OUTPUT_BYTES",   65_536);

    const worker_max_inflight = try parseUintMin(u16, env, "WORKER_MAX_INFLIGHT", 64, 1);
    const workflow_deadline_ms    = try parseUint(u64, env, "WORKFLOW_DEADLINE_MS", 60_000);
    // Deprecated (frozen counter set); off by default — scrape METRICS_ADDR instead.
    const metrics_log             = try parseBool(env, "METRICS_LOG", false);

    const delay_queue_capacity   = try parseUint(u16, env, "DELAY_QUEUE_CAPACITY", 4096);
    const retry_after_max_ms     = try parseUint(u64, env, "RETRY_AFTER_MAX_MS", 60_000);
    const retry_after_default_ms = try parseUint(u64, env, "RETRY_AFTER_DEFAULT_MS", 1_000);

    // Optional: METRICS_ADDR — Prometheus scrape endpoint; unset disables it.
    const metrics_addr: ?std.Io.net.IpAddress = if (env.get("METRICS_ADDR")) |s|
        std.Io.net.IpAddress.parseLiteral(s) catch return error.InvalidConfig
    else
        null;

    return Config{
        .bot_token          = bot_token,
        .webhook_secret     = webhook_secret,
        .listen_addr        = listen_addr,
        .rules_file         = rules_file,
        .db_path            = db_path,
        .state_encryption_key = state_encryption_key,
        .worker_threads       = worker_threads,
        .worker_queue_capacity     = worker_queue_capacity,
        .dispatcher_threads = dispatcher_threads,
        .webhook_pool_threads  = webhook_pool_threads,
        .schema_file        = schema_file,
        .api_validation     = api_validation,
        .bot_api_base           = bot_api_base,
        .json_max_bytes     = json_max_bytes,
        .io_pool_threads    = io_pool_threads,
        .io_queue_capacity  = io_queue_capacity,
        .io_job_timeout_ms  = io_job_timeout_ms,
        .proc_max_output_bytes         = proc_max_output_bytes,
        .worker_max_inflight = worker_max_inflight,
        .workflow_deadline_ms    = workflow_deadline_ms,
        .metrics_log             = metrics_log,
        .delay_queue_capacity    = delay_queue_capacity,
        .retry_after_max_ms      = retry_after_max_ms,
        .retry_after_default_ms  = retry_after_default_ms,
        .metrics_addr            = metrics_addr,
    };
}

/// Render `value` as a partial mask for logging a secret: reveal a short prefix
/// and suffix scaled to the length, with a fixed `****` middle so the true
/// length is not disclosed either. At least half of every value stays hidden;
/// values of three characters or fewer collapse to `****`. `out` must hold at
/// least 11 bytes (front 3 + "****" + back 4). Returns a slice into `out`, or
/// the static "****" when nothing is revealed.
pub fn maskSecret(value: []const u8, out: []u8) []const u8 {
    const n = value.len;
    const front = @min(@as(usize, 3), n / 4);
    const back = @min(@as(usize, 4), n / 4);
    if (front == 0 and back == 0) return "****";

    var i: usize = 0;
    @memcpy(out[i..][0..front], value[0..front]);
    i += front;
    @memcpy(out[i..][0..4], "****");
    i += 4;
    @memcpy(out[i..][0..back], value[n - back ..]);
    i += back;
    return out[0..i];
}

/// Log every effective configuration parameter at startup, grouped by subsystem
/// in pipeline order (inbound → processing → outbound). Secrets are partially
/// masked (see maskSecret); all other values print verbatim. Call once after the
/// startup banner.
///
/// This is an explicit ordered list, not struct reflection: env-var names are
/// not the field names, and the secret/non-secret split must be deliberate. When
/// adding a field to Config, add a line here and classify it secret-or-not.
pub fn logEffective(cfg: Config) void {
    // Render the full effective-config block into a stack buffer, then emit
    // each line via the scoped logger. A 2 KB buffer covers all current fields
    // with room to spare (the full output runs to ~600 bytes in practice).
    var out_buf: [2048]u8 = undefined;
    var fw = std.Io.Writer.fixed(&out_buf);
    writeEffective(cfg, &fw) catch return; // only fails if out_buf is too small

    var lines = std.mem.splitScalar(u8, fw.buffered(), '\n');
    while (lines.next()) |line| {
        if (line.len > 0) log_config.info("{s}", .{line});
    }
}

/// Write every effective configuration parameter to `writer`, one `KEY=value`
/// line per field, grouped by subsystem. Secrets are masked. Used by
/// logEffective (for production logging) and by tests (to inspect the output
/// without relying on std.log interception).
fn writeEffective(cfg: Config, writer: *std.Io.Writer) std.Io.Writer.Error!void {
    var sbuf: [16]u8 = undefined; // secret-mask scratch
    var abuf: [64]u8 = undefined; // listen-address scratch

    // [bot]
    try writer.print("[bot] BOT_TOKEN={s}\n",       .{maskSecret(cfg.bot_token, &sbuf)});
    try writer.print("[bot] WEBHOOK_SECRET={s}\n",  .{maskSecret(cfg.webhook_secret, &sbuf)});
    try writer.print("[bot] BOT_API_BASE={s}\n",    .{cfg.bot_api_base});
    try writer.print("[bot] RULES_FILE={s}\n",      .{cfg.rules_file});
    try writer.print("[bot] DB_PATH={s}\n",         .{cfg.db_path});
    if (cfg.state_encryption_key) |k|
        try writer.print("[bot] STATE_ENCRYPTION_KEY={s}\n", .{maskSecret(k, &sbuf)})
    else
        try writer.print("[bot] STATE_ENCRYPTION_KEY=(unset)\n", .{});
    try writer.print("[bot] SCHEMA_FILE={s}\n",     .{cfg.schema_file});
    try writer.print("[bot] API_VALIDATION={s}\n",  .{@tagName(cfg.api_validation)});
    try writer.print("[bot] METRICS_LOG={}\n",      .{cfg.metrics_log});

    // [server]
    const addr = std.fmt.bufPrint(&abuf, "{f}", .{cfg.listen_addr}) catch "?";
    try writer.print("[server] LISTEN_ADDR={s}\n",           .{addr});
    try writer.print("[server] WEBHOOK_POOL_THREADS={d}\n",  .{cfg.webhook_pool_threads});

    // [worker]
    try writer.print("[worker] WORKER_THREADS={d}\n",        .{cfg.worker_threads});
    try writer.print("[worker] WORKER_QUEUE_CAPACITY={d}\n", .{cfg.worker_queue_capacity});
    try writer.print("[worker] WORKER_MAX_INFLIGHT={d}\n",   .{cfg.worker_max_inflight});
    try writer.print("[worker] WORKFLOW_DEADLINE_MS={d}\n",  .{cfg.workflow_deadline_ms});
    try writer.print("[worker] JSON_MAX_BYTES={d}\n",        .{cfg.json_max_bytes});

    // [io]
    try writer.print("[io] IO_POOL_THREADS={d}\n",         .{cfg.io_pool_threads});
    try writer.print("[io] IO_QUEUE_CAPACITY={d}\n",       .{cfg.io_queue_capacity});
    try writer.print("[io] IO_JOB_TIMEOUT_MS={d}\n",       .{cfg.io_job_timeout_ms});
    try writer.print("[io] PROC_MAX_OUTPUT_BYTES={d}\n",   .{cfg.proc_max_output_bytes});

    // [dispatcher]
    try writer.print("[dispatcher] DISPATCHER_THREADS={d}\n",     .{cfg.dispatcher_threads});
    try writer.print("[dispatcher] DELAY_QUEUE_CAPACITY={d}\n",   .{cfg.delay_queue_capacity});
    try writer.print("[dispatcher] RETRY_AFTER_MAX_MS={d}\n",     .{cfg.retry_after_max_ms});
    try writer.print("[dispatcher] RETRY_AFTER_DEFAULT_MS={d}\n", .{cfg.retry_after_default_ms});

    // [metrics]
    if (cfg.metrics_addr) |ma| {
        const maddr = std.fmt.bufPrint(&abuf, "{f}", .{ma}) catch "?";
        try writer.print("[metrics] METRICS_ADDR={s}\n", .{maddr});
    } else {
        try writer.print("[metrics] METRICS_ADDR=disabled\n", .{});
    }
}

/// Free all string fields previously allocated by loadFromMap().
pub fn deinit(config: Config, allocator: std.mem.Allocator) void {
    allocator.free(config.bot_token);
    allocator.free(config.webhook_secret);
    allocator.free(config.rules_file);
    allocator.free(config.db_path);
    if (config.state_encryption_key) |k| allocator.free(k);
    allocator.free(config.schema_file);
    allocator.free(config.bot_api_base);
}

/// Build a child-process environment with the bot's secrets removed. Children
/// spawned by the io_pool (bot.shell/exec) must not inherit BOT_TOKEN,
/// WEBHOOK_SECRET, or STATE_ENCRYPTION_KEY. Returns a new map owned by the
/// caller (deinit it); the source `env` is unchanged.
pub fn sanitizeChildEnv(
    allocator: std.mem.Allocator,
    env: std.process.Environ.Map,
) !std.process.Environ.Map {
    const deny = [_][]const u8{ "BOT_TOKEN", "WEBHOOK_SECRET", "STATE_ENCRYPTION_KEY" };
    var out = std.process.Environ.Map.init(allocator);
    errdefer out.deinit();
    var it = env.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        var secret = false;
        for (deny) |d| {
            if (std.mem.eql(u8, key, d)) {
                secret = true;
                break;
            }
        }
        if (!secret) try out.put(key, entry.value_ptr.*);
    }
    return out;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn getRequired(
    allocator: std.mem.Allocator,
    env: std.process.Environ.Map,
    key: []const u8,
) ConfigError![]const u8 {
    const val = env.get(key) orelse return error.MissingRequiredField;
    return allocator.dupe(u8, val) catch return error.OutOfMemory;
}

/// Parse `key` from `env` as T. Returns `default` if the key is absent.
/// Returns error.InvalidConfig if the value is present but not a valid T.
fn parseUint(comptime T: type, env: std.process.Environ.Map, key: []const u8, default: T) ConfigError!T {
    const raw = env.get(key) orelse return default;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    return std.fmt.parseInt(T, trimmed, 10) catch return error.InvalidConfig;
}

/// Like parseUint, but rejects a value below `min`. Counts and ring-buffer
/// capacities must be positive: a zero worker count divides by zero in the
/// hash routing, a zero queue capacity divides by zero in the ring buffer (the
/// Queue.init assert is compiled out in ReleaseFast), and a zero dispatcher,
/// io-pool, or inflight bound silently processes nothing. Defaults are already
/// positive; this guards an explicit `KEY=0`.
fn parseUintMin(comptime T: type, env: std.process.Environ.Map, key: []const u8, default: T, min: T) ConfigError!T {
    const v = try parseUint(T, env, key, default);
    if (v < min) return error.InvalidConfig;
    return v;
}

/// Parse `key` from `env` as bool. Returns `default` if the key is absent.
/// Accepts "true"/"1" → true, "false"/"0" → false (case-insensitive).
/// Returns error.InvalidConfig for any other value.
fn parseBool(env: std.process.Environ.Map, key: []const u8, default: bool) ConfigError!bool {
    const raw = env.get(key) orelse return default;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    if (std.mem.eql(u8, trimmed, "true")  or std.mem.eql(u8, trimmed, "1")) return true;
    if (std.mem.eql(u8, trimmed, "false") or std.mem.eql(u8, trimmed, "0")) return false;
    return error.InvalidConfig;
}

/// Parse `API_VALIDATION` into a ValidationMode. Absent → `.warn`.
/// An unrecognised value → error.InvalidConfig.
fn parseValidationMode(env: std.process.Environ.Map) ConfigError!types.ValidationMode {
    const raw = env.get("API_VALIDATION") orelse return .warn;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    if (std.mem.eql(u8, trimmed, "off")) return .off;
    if (std.mem.eql(u8, trimmed, "warn")) return .warn;
    if (std.mem.eql(u8, trimmed, "strict")) return .strict;
    return error.InvalidConfig;
}

/// Returns cpu_count * multiplier, clamped to [2, maxInt(u8)].
///
/// Worker and connection-pool defaults use multiplier 1: that work is
/// CPU-bound, so one thread per core is ample. The dispatcher uses
/// multiplier 2: its threads are network-I/O-bound (parked in fetch almost
/// always), and outbound throughput ≈ threads × (1 / per_send_latency), so a
/// flat low default would silently cap sends at free-tier rates on small
/// boxes regardless of the account's actual limit. Minimum 2 keeps a second
/// thread available under contention.
fn cpuScaledThreads(multiplier: usize) u8 {
    const cpu_count = std.Thread.getCpuCount() catch 1;
    const n = @min(cpu_count * multiplier, @as(usize, std.math.maxInt(u8)));
    return @max(2, @as(u8, @intCast(n)));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

// --- Output capture for logEffective tests (P3-1, P4-3, P4-43) ---
//
// logEffective routes its output through std.log, whose backend in the test
// runner is owned by the test_runner.zig root and cannot be redirected from a
// test file. Instead, tests call writeEffective directly with a fixed-buffer
// writer — the function that logEffective itself delegates to — and inspect
// the returned slice.

/// Write the full effective-config output for `cfg` into `buf` and return the
/// populated slice. Used by tests to inspect line content and group prefixes
/// without intercepting std.log. The buffer need only hold the rendered output;
/// 2 KB is sufficient for all current fields (the output runs to ~600 bytes).
fn captureEffective(cfg: Config, buf: []u8) []const u8 {
    var fw = std.Io.Writer.fixed(buf);
    writeEffective(cfg, &fw) catch {};
    return fw.buffered();
}

/// Build a minimal environment map from a slice of key-value pairs for testing.
fn makeEnv(allocator: std.mem.Allocator, pairs: []const [2][]const u8) !std.process.Environ.Map {
    var env = std.process.Environ.Map.init(allocator);
    errdefer env.deinit();
    for (pairs) |pair| try env.put(pair[0], pair[1]);
    return env;
}

test "loads every field from a fully populated environment" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",              "tok123" },
        .{ "WEBHOOK_SECRET",         "sec456" },
        .{ "LISTEN_ADDR",            "127.0.0.1:9443" },
        .{ "RULES_FILE",             "custom/rules.lua" },
        .{ "DB_PATH",                "custom.db" },
        .{ "SCHEMA_FILE",            "/etc/zora/api.json" },
        .{ "API_VALIDATION",         "strict" },
        .{ "BOT_API_BASE",           "http://127.0.0.1:9000" },
        .{ "WORKER_THREADS",         "4" },
        .{ "WORKER_QUEUE_CAPACITY",  "512" },
        .{ "DISPATCHER_THREADS",     "3" },
        .{ "WEBHOOK_POOL_THREADS",   "5" },
        .{ "JSON_MAX_BYTES",         "524288" },
        .{ "IO_POOL_THREADS",        "6" },
        .{ "IO_QUEUE_CAPACITY",      "128" },
        .{ "IO_JOB_TIMEOUT_MS",      "15000" },
        .{ "PROC_MAX_OUTPUT_BYTES",  "32768" },
        .{ "WORKER_MAX_INFLIGHT",    "32" },
        .{ "WORKFLOW_DEADLINE_MS",   "30000" },
        .{ "METRICS_LOG",            "true" },
        .{ "DELAY_QUEUE_CAPACITY",   "2048" },
        .{ "RETRY_AFTER_MAX_MS",     "30000" },
        .{ "RETRY_AFTER_DEFAULT_MS", "500" },
        .{ "METRICS_ADDR",           "127.0.0.1:9100" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    // [bot]
    try testing.expectEqualStrings("tok123", cfg.bot_token);
    try testing.expectEqualStrings("sec456", cfg.webhook_secret);
    try testing.expectEqualStrings("custom/rules.lua", cfg.rules_file);
    try testing.expectEqualStrings("custom.db", cfg.db_path);
    try testing.expectEqualStrings("/etc/zora/api.json", cfg.schema_file);
    try testing.expectEqual(types.ValidationMode.strict, cfg.api_validation);
    try testing.expectEqualStrings("http://127.0.0.1:9000", cfg.bot_api_base);
    try testing.expectEqual(true, cfg.metrics_log);

    // [server]
    try testing.expectEqual(@as(u16, 9443), cfg.listen_addr.ip4.port);
    try testing.expectEqual(@as(u8, 5), cfg.webhook_pool_threads);

    // [worker]
    try testing.expectEqual(@as(u8, 4), cfg.worker_threads);
    try testing.expectEqual(@as(u16, 512), cfg.worker_queue_capacity);
    try testing.expectEqual(@as(u16, 32), cfg.worker_max_inflight);
    try testing.expectEqual(@as(u64, 30_000), cfg.workflow_deadline_ms);
    try testing.expectEqual(@as(usize, 524_288), cfg.json_max_bytes);

    // [io]
    try testing.expectEqual(@as(u8,    6),      cfg.io_pool_threads);
    try testing.expectEqual(@as(u16,   128),    cfg.io_queue_capacity);
    try testing.expectEqual(@as(u64,   15_000), cfg.io_job_timeout_ms);
    try testing.expectEqual(@as(usize, 32_768), cfg.proc_max_output_bytes);

    // [dispatcher]
    try testing.expectEqual(@as(u8,  3),      cfg.dispatcher_threads);
    try testing.expectEqual(@as(u16, 2_048),  cfg.delay_queue_capacity);
    try testing.expectEqual(@as(u64, 30_000), cfg.retry_after_max_ms);
    try testing.expectEqual(@as(u64, 500),    cfg.retry_after_default_ms);

    // [metrics]
    try testing.expectEqual(@as(u16, 9100), cfg.metrics_addr.?.ip4.port);
}

test "applies defaults when optional fields are absent" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    // [bot]
    try testing.expectEqualStrings("rules/rules.lua", cfg.rules_file);
    try testing.expectEqualStrings("state.db", cfg.db_path);
    try testing.expectEqualStrings("schema/botapi.json", cfg.schema_file);
    try testing.expectEqualStrings("https://api.telegram.org", cfg.bot_api_base);
    try testing.expectEqual(types.ValidationMode.warn, cfg.api_validation);
    try testing.expectEqual(false, cfg.metrics_log);

    // [server]: LISTEN_ADDR default is 0.0.0.0:8443. In 0.16 IpAddress the
    // port is stored in native byte order.
    try testing.expectEqual(@as(u16, 8443), cfg.listen_addr.ip4.port);

    // [worker]: minimum 2 workers; queue capacity 1024.
    try testing.expect(cfg.worker_threads >= 2);
    try testing.expectEqual(@as(u16, 1024), cfg.worker_queue_capacity);

    // [io]: IO_POOL_THREADS = 8, IO_QUEUE_CAPACITY = 256, IO_JOB_TIMEOUT_MS =
    // 30 000, PROC_MAX_OUTPUT_BYTES = 65 536.
    try testing.expectEqual(@as(u8,    8),      cfg.io_pool_threads);
    try testing.expectEqual(@as(u16,   256),    cfg.io_queue_capacity);
    try testing.expectEqual(@as(u64,   30_000), cfg.io_job_timeout_ms);
    try testing.expectEqual(@as(usize, 65_536), cfg.proc_max_output_bytes);

    // [dispatcher]: at least 2 threads; throttle defaults.
    try testing.expect(cfg.dispatcher_threads >= 2);
    try testing.expectEqual(@as(u16, 4_096),  cfg.delay_queue_capacity);
    try testing.expectEqual(@as(u64, 60_000), cfg.retry_after_max_ms);
    try testing.expectEqual(@as(u64, 1_000),  cfg.retry_after_default_ms);
}

test "thread counts scale with CPU and accept explicit overrides" {
    const cpu_count = std.Thread.getCpuCount() catch 1;

    // Absent: worker/dispatcher/conn-pool default to CPU-scaled values (min 2).
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);

        // WORKER_THREADS → cpu_count, minimum 2.
        const exp_worker: u32 = @max(2, @as(u32, @intCast(@min(cpu_count, @as(usize, std.math.maxInt(u32))))));
        try testing.expectEqual(exp_worker, cfg.worker_threads);
        try testing.expect(cfg.worker_threads >= 2);

        // DISPATCHER_THREADS → 2 * cpu_count, minimum 2.
        const exp_disp: u8 = @max(2, @as(u8, @intCast(@min(cpu_count * 2, @as(usize, std.math.maxInt(u8))))));
        try testing.expectEqual(exp_disp, cfg.dispatcher_threads);
        try testing.expect(cfg.dispatcher_threads >= 2);

        // WEBHOOK_POOL_THREADS → cpu_count, minimum 2.
        try testing.expect(cfg.webhook_pool_threads >= 2);
    }
    // An explicit WEBHOOK_POOL_THREADS is used verbatim.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "WEBHOOK_POOL_THREADS", "5" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u8, 5), cfg.webhook_pool_threads);
    }
}

test "WORKER_THREADS trims whitespace; minimum is 2, not 1" {
    // Whitespace around the value is trimmed.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "WORKER_THREADS", "  4  " },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u32, 4), cfg.worker_threads);
    }
    // 1 is below the mandated minimum of 2 — must be rejected.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "WORKER_THREADS", "1" },
        });
        defer env.deinit();
        try testing.expectError(error.InvalidConfig, loadFromMap(testing.allocator, env));
    }
    // 2 is the minimum — must be accepted.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "WORKER_THREADS", "2" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u32, 2), cfg.worker_threads);
    }
}

test "missing a required field returns MissingRequiredField" {
    // BOT_TOKEN absent.
    {
        var env = try makeEnv(testing.allocator, &.{.{ "WEBHOOK_SECRET", "sec" }});
        defer env.deinit();
        try testing.expectError(error.MissingRequiredField, loadFromMap(testing.allocator, env));
    }
    // WEBHOOK_SECRET absent.
    {
        var env = try makeEnv(testing.allocator, &.{.{ "BOT_TOKEN", "tok" }});
        defer env.deinit();
        try testing.expectError(error.MissingRequiredField, loadFromMap(testing.allocator, env));
    }
    // Both absent → the first missing field errors.
    {
        var env = try makeEnv(testing.allocator, &.{});
        defer env.deinit();
        try testing.expectError(error.MissingRequiredField, loadFromMap(testing.allocator, env));
    }
}

test "numeric fields reject non-numeric input" {
    // Every integer field rejects a value that is not a base-10 integer. The
    // float case guards against accidental parseFloat tolerance.
    const cases = [_]struct { key: []const u8, val: []const u8 }{
        .{ .key = "WORKER_THREADS",            .val = "not_a_number" },
        .{ .key = "WORKER_THREADS",            .val = "3.5" },
        .{ .key = "WORKER_QUEUE_CAPACITY",          .val = "lots" },
        .{ .key = "DISPATCHER_THREADS",      .val = "four" },
        .{ .key = "IO_POOL_THREADS",         .val = "many" },
        .{ .key = "WORKER_MAX_INFLIGHT", .val = "lots" },
        .{ .key = "WORKFLOW_DEADLINE_MS",    .val = "never" },
        .{ .key = "JSON_MAX_BYTES",          .val = "abc" },
    };
    for (cases) |c| {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ c.key, c.val },
        });
        defer env.deinit();
        testing.expectError(error.InvalidConfig, loadFromMap(testing.allocator, env)) catch |e| {
            std.debug.print("field {s}={s} did not return InvalidConfig\n", .{ c.key, c.val });
            return e;
        };
    }
}

test "positive-count fields reject an explicit zero" {
    // Counts and ring-buffer capacities that must be ≥1. An explicit 0 would
    // divide by zero (hash routing, ring buffer) or silently process nothing
    // (dispatcher, io-pool, inflight bound), so loadFromMap rejects it. The
    // CPU-scaled defaults are already positive; this guards the explicit case.
    const keys = [_][]const u8{
        "WORKER_THREADS",
        "WORKER_QUEUE_CAPACITY",
        "DISPATCHER_THREADS",
        "IO_POOL_THREADS",
        "IO_QUEUE_CAPACITY",
        "WORKER_MAX_INFLIGHT",
    };
    for (keys) |key| {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ key, "0" },
        });
        defer env.deinit();
        testing.expectError(error.InvalidConfig, loadFromMap(testing.allocator, env)) catch |e| {
            std.debug.print("field {s}=0 did not return InvalidConfig\n", .{key});
            return e;
        };
    }
}

test "BOT_API_BASE absent uses the default; present is used" {
    var env_default = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env_default.deinit();
    const cfg_default = try loadFromMap(testing.allocator, env_default);
    defer deinit(cfg_default, testing.allocator);
    try testing.expectEqualStrings("https://api.telegram.org", cfg_default.bot_api_base);

    var env_set = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        .{ "BOT_API_BASE", "http://127.0.0.1:9000" },
    });
    defer env_set.deinit();
    const cfg_set = try loadFromMap(testing.allocator, env_set);
    defer deinit(cfg_set, testing.allocator);
    try testing.expectEqualStrings("http://127.0.0.1:9000", cfg_set.bot_api_base);
}

test "API_VALIDATION parsed; default warn; invalid → InvalidConfig" {
    var env_default = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env_default.deinit();
    const cfg_default = try loadFromMap(testing.allocator, env_default);
    defer deinit(cfg_default, testing.allocator);
    try testing.expectEqual(types.ValidationMode.warn, cfg_default.api_validation);

    inline for (.{ "off", "warn", "strict" }) |mode_str| {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "API_VALIDATION", mode_str },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(
            @field(types.ValidationMode, mode_str),
            cfg.api_validation,
        );
    }

    var env_bad = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        .{ "API_VALIDATION", "loud" },
    });
    defer env_bad.deinit();
    try testing.expectError(error.InvalidConfig, loadFromMap(testing.allocator, env_bad));
}

test "SCHEMA_FILE absent uses the default; present is used" {
    var env_default = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env_default.deinit();
    const cfg_default = try loadFromMap(testing.allocator, env_default);
    defer deinit(cfg_default, testing.allocator);
    try testing.expectEqualStrings("schema/botapi.json", cfg_default.schema_file);

    var env_set = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        .{ "SCHEMA_FILE", "/etc/zora/api.json" },
    });
    defer env_set.deinit();
    const cfg_set = try loadFromMap(testing.allocator, env_set);
    defer deinit(cfg_set, testing.allocator);
    try testing.expectEqualStrings("/etc/zora/api.json", cfg_set.schema_file);
}

test "JSON_MAX_BYTES parses a value and defaults to 1 MB" {
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(usize, 1048576), cfg.json_max_bytes);
    }
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "JSON_MAX_BYTES", "524288" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(usize, 524288), cfg.json_max_bytes);
    }
}

test "io_pool fields parse explicit values and apply defaults" {
    // Absent → defaults.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u8,    8),      cfg.io_pool_threads);
        try testing.expectEqual(@as(u16,   256),    cfg.io_queue_capacity);
        try testing.expectEqual(@as(u64,   30_000), cfg.io_job_timeout_ms);
        try testing.expectEqual(@as(usize, 65_536), cfg.proc_max_output_bytes);
    }
    // Present → parsed.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN",         "tok" }, .{ "WEBHOOK_SECRET",   "sec" },
            .{ "IO_POOL_THREADS",   "8" },   .{ "IO_QUEUE_CAPACITY", "512" },
            .{ "IO_JOB_TIMEOUT_MS", "10000" }, .{ "PROC_MAX_OUTPUT_BYTES", "131072" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u8,    8),       cfg.io_pool_threads);
        try testing.expectEqual(@as(u16,   512),     cfg.io_queue_capacity);
        try testing.expectEqual(@as(u64,   10_000),  cfg.io_job_timeout_ms);
        try testing.expectEqual(@as(usize, 131_072), cfg.proc_max_output_bytes);
    }
}

test "WORKER_MAX_INFLIGHT and WORKFLOW_DEADLINE_MS parse and default" {
    // Absent → defaults.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u16, 64),     cfg.worker_max_inflight);
        try testing.expectEqual(@as(u64, 60_000), cfg.workflow_deadline_ms);
    }
    // Present → parsed.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "WORKER_MAX_INFLIGHT", "32" }, .{ "WORKFLOW_DEADLINE_MS", "30000" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u16, 32),     cfg.worker_max_inflight);
        try testing.expectEqual(@as(u64, 30_000), cfg.workflow_deadline_ms);
    }
}

test "METRICS_LOG parses booleans and rejects invalid values" {
    const ok = [_]struct { val: ?[]const u8, want: bool }{
        .{ .val = null,    .want = false }, // absent → default false (deprecated)
        .{ .val = "false", .want = false },
        .{ .val = "0",     .want = false },
        .{ .val = "true",  .want = true },
    };
    for (ok) |c| {
        var env = if (c.val) |v|
            try makeEnv(testing.allocator, &.{
                .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" }, .{ "METRICS_LOG", v },
            })
        else
            try makeEnv(testing.allocator, &.{
                .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(c.want, cfg.metrics_log);
    }
    // An unrecognised value is rejected.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" }, .{ "METRICS_LOG", "yes" },
        });
        defer env.deinit();
        try testing.expectError(error.InvalidConfig, loadFromMap(testing.allocator, env));
    }
}

test "throttle config defaults and overrides" {
    // Defaults (only required keys provided).
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "t" }, .{ "WEBHOOK_SECRET", "s" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u16, 4096),  cfg.delay_queue_capacity);
        try testing.expectEqual(@as(u64, 60000), cfg.retry_after_max_ms);
        try testing.expectEqual(@as(u64, 1000),  cfg.retry_after_default_ms);
    }
    // Overrides.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "t" }, .{ "WEBHOOK_SECRET", "s" },
            .{ "DELAY_QUEUE_CAPACITY", "256" },
            .{ "RETRY_AFTER_MAX_MS", "30000" },
            .{ "RETRY_AFTER_DEFAULT_MS", "500" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u16, 256),   cfg.delay_queue_capacity);
        try testing.expectEqual(@as(u64, 30000), cfg.retry_after_max_ms);
        try testing.expectEqual(@as(u64, 500),   cfg.retry_after_default_ms);
    }
}

test "logEffective output: subsystem groups, field lines, and secret masking" {
    // Verifies the formatted output of writeEffective (the function logEffective
    // delegates to) against the three audit items:
    //   P3-1: each [subsystem] KEY=value line appears for all five groups.
    //   P4-3: BOT_TOKEN and WEBHOOK_SECRET lines contain "****" and do NOT
    //         contain the full secret values (end-to-end masking).
    //   P4-43: all five group prefixes ([bot], [server], [worker], [io],
    //          [dispatcher]) appear.
    //
    // The token and secret are long enough for maskSecret to produce a partial
    // reveal (both exceed 12 chars → front 3 + "****" + back 4 visible).
    //
    // The production logEffective function calls writeEffective and routes each
    // line through std.log.  Tests inspect writeEffective directly because the
    // test_runner.zig root owns std_options and the log backend cannot be
    // redirected from a test file.
    const token  = "1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ";
    const secret = "supersecretvalue";

    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      token  },
        .{ "WEBHOOK_SECRET", secret },
    });
    defer env.deinit();
    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    var buf: [2048]u8 = undefined;
    const captured = captureEffective(cfg, &buf);

    // --- P4-43: all five subsystem group prefixes present ---
    try testing.expect(std.mem.indexOf(u8, captured, "[bot]")        != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[server]")     != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[worker]")     != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[io]")         != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[dispatcher]") != null);

    // --- P3-1: representative field per group ---
    try testing.expect(std.mem.indexOf(u8, captured, "[bot] BOT_TOKEN=")                != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[bot] WEBHOOK_SECRET=")           != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[bot] BOT_API_BASE=")             != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[bot] RULES_FILE=")               != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[server] LISTEN_ADDR=")           != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[worker] WORKER_THREADS=")        != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[io] IO_POOL_THREADS=")           != null);
    try testing.expect(std.mem.indexOf(u8, captured, "[dispatcher] DISPATCHER_THREADS=") != null);

    // --- P4-3: masking end-to-end ---
    // The BOT_TOKEN line must contain "****" and must NOT contain the full token.
    const tok_start = std.mem.indexOf(u8, captured, "[bot] BOT_TOKEN=").?;
    const tok_end   = std.mem.indexOfPos(u8, captured, tok_start, "\n") orelse captured.len;
    const tok_line  = captured[tok_start..tok_end];
    try testing.expect(std.mem.indexOf(u8, tok_line, "****")  != null);
    try testing.expect(std.mem.indexOf(u8, tok_line, token)   == null);

    // The WEBHOOK_SECRET line must contain "****" and must NOT contain the
    // full secret.
    const sec_start = std.mem.indexOf(u8, captured, "[bot] WEBHOOK_SECRET=").?;
    const sec_end   = std.mem.indexOfPos(u8, captured, sec_start, "\n") orelse captured.len;
    const sec_line  = captured[sec_start..sec_end];
    try testing.expect(std.mem.indexOf(u8, sec_line, "****")   != null);
    try testing.expect(std.mem.indexOf(u8, sec_line, secret)   == null);
}


test "maskSecret reveals a scaled prefix and suffix and hides the middle" {
    var buf: [16]u8 = undefined;
    // Long value (46 chars): first 3 + "****" + last 4.
    try testing.expectEqualStrings(
        "abc****GHIJ",
        maskSecret("abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ", &buf),
    );
    // 8 chars (boundary): n/4 = 2 → 2 + "****" + 2.
    try testing.expectEqualStrings("ab****gh", maskSecret("abcdefgh", &buf));
    // 6 chars: n/4 = 1 → 1 + "****" + 1.
    try testing.expectEqualStrings("a****f", maskSecret("abcdef", &buf));
    // 3 chars and empty: fully masked.
    try testing.expectEqualStrings("****", maskSecret("abc", &buf));
    try testing.expectEqualStrings("****", maskSecret("", &buf));
}

test "empty WEBHOOK_SECRET is rejected with InvalidConfig" {
    // An empty WEBHOOK_SECRET is syntactically present but semantically invalid:
    // the webhook auth check in server.zig compares against this string, and an
    // empty secret would match any request that omits the header (or sends an
    // empty one), breaking authentication. loadFromMap rejects it at line 40.
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "" },
    });
    defer env.deinit();
    try testing.expectError(error.InvalidConfig, loadFromMap(testing.allocator, env));
}

test "STATE_ENCRYPTION_KEY is parsed when present and null when absent" {
    var env = std.process.Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try env.put("BOT_TOKEN", "t");
    try env.put("WEBHOOK_SECRET", "s");
    {
        const cfg = try loadFromMap(std.testing.allocator, env);
        defer deinit(cfg, std.testing.allocator);
        try std.testing.expectEqual(@as(?[]const u8, null), cfg.state_encryption_key);
    }
    try env.put("STATE_ENCRYPTION_KEY", "passphrase");
    {
        const cfg = try loadFromMap(std.testing.allocator, env);
        defer deinit(cfg, std.testing.allocator);
        try std.testing.expect(cfg.state_encryption_key != null);
        try std.testing.expectEqualStrings("passphrase", cfg.state_encryption_key.?);
    }
}

test "sanitizeChildEnv strips secrets and keeps the rest" {
    var env = std.process.Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try env.put("BOT_TOKEN", "t");
    try env.put("WEBHOOK_SECRET", "s");
    try env.put("STATE_ENCRYPTION_KEY", "k");
    try env.put("PATH", "/usr/bin");

    var clean = try sanitizeChildEnv(std.testing.allocator, env);
    defer clean.deinit();
    try std.testing.expectEqual(@as(?[]const u8, null), clean.get("BOT_TOKEN"));
    try std.testing.expectEqual(@as(?[]const u8, null), clean.get("WEBHOOK_SECRET"));
    try std.testing.expectEqual(@as(?[]const u8, null), clean.get("STATE_ENCRYPTION_KEY"));
    try std.testing.expectEqualStrings("/usr/bin", clean.get("PATH").?);
}

test "METRICS_ADDR parses when set, disables when unset, rejects garbage" {
    // Unset → disabled (null).
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(?std.Io.net.IpAddress, null), cfg.metrics_addr);
    }
    // Set → parsed address.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "METRICS_ADDR", "127.0.0.1:9100" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u16, 9100), cfg.metrics_addr.?.ip4.port);
    }
    // Garbage → InvalidConfig.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "METRICS_ADDR", "not-an-address" },
        });
        defer env.deinit();
        try testing.expectError(error.InvalidConfig, loadFromMap(testing.allocator, env));
    }
}
