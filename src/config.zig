/// config.zig — load configuration from environment variables.
///
/// Two public entry points:
///   load(allocator)              — reads from the real process environment.
///   loadFromMap(allocator, env)  — reads from a caller-supplied EnvMap (testable).
///
/// String fields in the returned Config are allocated from `allocator`.
/// Call deinit(config, allocator) when the config is no longer needed.

const std = @import("std");
const types = @import("types.zig");

pub const Config = types.Config;

pub const ConfigError = error{
    MissingRequiredField,
    InvalidConfig,
    OutOfMemory,
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Load config from the real process environment variables.
/// String fields are owned by the caller; free with deinit().
pub fn load(allocator: std.mem.Allocator) ConfigError!Config {
    var env = std.process.getEnvMap(allocator) catch return error.OutOfMemory;
    defer env.deinit();
    return loadFromMap(allocator, env);
}

/// Load config from an explicit EnvMap. Used directly by tests.
/// String fields are duplicated from `env` values and owned by `allocator`.
pub fn loadFromMap(allocator: std.mem.Allocator, env: std.process.EnvMap) ConfigError!Config {
    // Required fields
    const bot_token = try getRequired(allocator, env, "BOT_TOKEN");
    errdefer allocator.free(bot_token);

    const webhook_secret = try getRequired(allocator, env, "WEBHOOK_SECRET");
    errdefer allocator.free(webhook_secret);
    if (webhook_secret.len == 0) return error.InvalidConfig;

    // Optional: LISTEN_ADDR
    const listen_addr_str = env.get("LISTEN_ADDR") orelse "0.0.0.0:8443";
    const listen_addr = std.net.Address.parseIpAndPort(listen_addr_str) catch
        return error.InvalidConfig;

    // Optional: RULES_FILE
    const rules_file = allocator.dupeZ(u8, env.get("RULES_FILE") orelse "rules/rules.lua") catch
        return error.OutOfMemory;
    errdefer allocator.free(rules_file);

    // Optional: DB_PATH
    const db_path = allocator.dupeZ(u8, env.get("DB_PATH") orelse "state.db") catch
        return error.OutOfMemory;
    errdefer allocator.free(db_path);

    // Optional: SCHEMA_FILE
    const schema_file = allocator.dupeZ(u8, env.get("SCHEMA_FILE") orelse "schema/botapi.json") catch
        return error.OutOfMemory;
    errdefer allocator.free(schema_file);

    // Optional: API_VALIDATION
    const api_validation = try parseValidationMode(env);

    // Optional: BOT_API_BASE
    const api_base = allocator.dupe(u8, env.get("BOT_API_BASE") orelse "https://api.telegram.org") catch
        return error.OutOfMemory;
    errdefer allocator.free(api_base);

    // Optional: WORKER_COUNT (default: cpu_count, minimum 2)
    const worker_count = try parseUint(u8, env, "WORKER_COUNT", cpuScaledThreads(1));

    // Optional: QUEUE_CAPACITY (per-worker inbound burst buffer; full → update
    // dropped and Telegram retries, so size for bursts — RAM is abundant here)
    const queue_capacity = try parseUint(u16, env, "QUEUE_CAPACITY", 1024);

    // Optional: DISPATCHER_THREADS (default: 2 * cpu_count, minimum 2 —
    // network-I/O-bound outbound senders; thread count multiplies throughput).
    const dispatcher_threads = try parseUint(u8, env, "DISPATCHER_THREADS", cpuScaledThreads(2));

    // Optional: CONN_POOL_THREADS (default: cpu_count, minimum 2 — webhook
    // connection-handler pool size). Threads are reused, so this bounds
    // concurrency, not per-message allocation.
    const conn_pool_threads = try parseUint(u8, env, "CONN_POOL_THREADS", cpuScaledThreads(1));

    // Optional: MULTIPART_MAX_FILE (50 MB — Telegram bot upload limit)
    const multipart_max_file = try parseUint(usize, env, "MULTIPART_MAX_FILE", 52428800);

    // Optional: JSON_MAX_BYTES (1 MB default — max size for json.decode/encode in Lua)
    const json_max_bytes = try parseUint(usize, env, "JSON_MAX_BYTES", 1048576);

    // Optional: IO_POOL_THREADS / IO_QUEUE_CAPACITY / IO_JOB_TIMEOUT_MS / PROC_MAX_OUTPUT
    // IO_POOL_THREADS kept flat (not cpu-scaled): this pool also spawns
    // subprocesses (exec/shell), which are heavy and shouldn't scale with box size.
    const io_pool_threads   = try parseUint(u8,    env, "IO_POOL_THREADS",   8);
    const io_queue_capacity = try parseUint(u16,   env, "IO_QUEUE_CAPACITY", 256);
    const io_job_timeout_ms = try parseUint(u64,   env, "IO_JOB_TIMEOUT_MS", 30_000);
    const proc_max_output   = try parseUint(usize, env, "PROC_MAX_OUTPUT",   65_536);

    const max_inflight_per_worker = try parseUint(u16, env, "MAX_INFLIGHT_PER_WORKER", 64);
    const workflow_deadline_ms    = try parseUint(u64, env, "WORKFLOW_DEADLINE_MS", 60_000);
    const metrics_log             = try parseBool(env, "METRICS_LOG", true);

    const delay_queue_capacity   = try parseUint(u16, env, "DELAY_QUEUE_CAPACITY", 4096);
    const retry_after_max_ms     = try parseUint(u64, env, "RETRY_AFTER_MAX_MS", 60_000);
    const retry_after_default_ms = try parseUint(u64, env, "RETRY_AFTER_DEFAULT_MS", 1_000);

    return Config{
        .bot_token          = bot_token,
        .webhook_secret     = webhook_secret,
        .listen_addr        = listen_addr,
        .rules_file         = rules_file,
        .db_path            = db_path,
        .worker_count       = worker_count,
        .queue_capacity     = queue_capacity,
        .dispatcher_threads = dispatcher_threads,
        .conn_pool_threads  = conn_pool_threads,
        .schema_file        = schema_file,
        .api_validation     = api_validation,
        .api_base           = api_base,
        .multipart_max_file = multipart_max_file,
        .json_max_bytes     = json_max_bytes,
        .io_pool_threads    = io_pool_threads,
        .io_queue_capacity  = io_queue_capacity,
        .io_job_timeout_ms  = io_job_timeout_ms,
        .proc_max_output         = proc_max_output,
        .max_inflight_per_worker = max_inflight_per_worker,
        .workflow_deadline_ms    = workflow_deadline_ms,
        .metrics_log             = metrics_log,
        .delay_queue_capacity    = delay_queue_capacity,
        .retry_after_max_ms      = retry_after_max_ms,
        .retry_after_default_ms  = retry_after_default_ms,
    };
}

/// Free all string fields previously allocated by load() or loadFromMap().
pub fn deinit(config: Config, allocator: std.mem.Allocator) void {
    allocator.free(config.bot_token);
    allocator.free(config.webhook_secret);
    allocator.free(config.rules_file);
    allocator.free(config.db_path);
    allocator.free(config.schema_file);
    allocator.free(config.api_base);
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn getRequired(
    allocator: std.mem.Allocator,
    env: std.process.EnvMap,
    key: []const u8,
) ConfigError![]const u8 {
    const val = env.get(key) orelse return error.MissingRequiredField;
    return allocator.dupe(u8, val) catch return error.OutOfMemory;
}

/// Parse `key` from `env` as T. Returns `default` if the key is absent.
/// Returns error.InvalidConfig if the value is present but not a valid T.
fn parseUint(comptime T: type, env: std.process.EnvMap, key: []const u8, default: T) ConfigError!T {
    const raw = env.get(key) orelse return default;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    return std.fmt.parseInt(T, trimmed, 10) catch return error.InvalidConfig;
}

/// Parse `key` from `env` as bool. Returns `default` if the key is absent.
/// Accepts "true"/"1" → true, "false"/"0" → false (case-insensitive).
/// Returns error.InvalidConfig for any other value.
fn parseBool(env: std.process.EnvMap, key: []const u8, default: bool) ConfigError!bool {
    const raw = env.get(key) orelse return default;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    if (std.mem.eql(u8, trimmed, "true")  or std.mem.eql(u8, trimmed, "1")) return true;
    if (std.mem.eql(u8, trimmed, "false") or std.mem.eql(u8, trimmed, "0")) return false;
    return error.InvalidConfig;
}

/// Parse `API_VALIDATION` into a ValidationMode. Absent → `.warn`.
/// An unrecognised value → error.InvalidConfig.
fn parseValidationMode(env: std.process.EnvMap) ConfigError!types.ValidationMode {
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

/// Build a minimal EnvMap from a slice of key-value pairs for testing.
fn makeEnv(allocator: std.mem.Allocator, pairs: []const [2][]const u8) !std.process.EnvMap {
    var env = std.process.EnvMap.init(allocator);
    errdefer env.deinit();
    for (pairs) |pair| try env.put(pair[0], pair[1]);
    return env;
}

test "loads every field from a fully populated environment" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",          "tok123" },
        .{ "WEBHOOK_SECRET",     "sec456" },
        .{ "LISTEN_ADDR",        "127.0.0.1:9443" },
        .{ "RULES_FILE",         "custom/rules.lua" },
        .{ "DB_PATH",            "custom.db" },
        .{ "WORKER_COUNT",       "8" },
        .{ "QUEUE_CAPACITY",     "512" },
        .{ "DISPATCHER_THREADS", "2" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    try testing.expectEqualStrings("tok123", cfg.bot_token);
    try testing.expectEqualStrings("sec456", cfg.webhook_secret);
    try testing.expectEqualStrings("custom/rules.lua", cfg.rules_file);
    try testing.expectEqualStrings("custom.db", cfg.db_path);
    try testing.expectEqual(@as(u32, 8), cfg.worker_count);
    try testing.expectEqual(@as(u32, 512), cfg.queue_capacity);
    try testing.expectEqual(@as(u32, 2), cfg.dispatcher_threads);

    // Field types: the numeric fields are unsigned ints and the parsed listen
    // address carries a non-zero port.
    const wc: u32 = cfg.worker_count;
    try testing.expectEqual(@as(u32, 8), wc);
    const qc: u32 = cfg.queue_capacity;
    try testing.expectEqual(@as(u32, 512), qc);
    try testing.expect(cfg.listen_addr.in.sa.port != 0);
}

test "applies defaults when optional fields are absent" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    try testing.expectEqualStrings("rules/rules.lua", cfg.rules_file);
    try testing.expectEqualStrings("state.db", cfg.db_path);
    try testing.expectEqual(@as(u32, 1024), cfg.queue_capacity);
    try testing.expect(cfg.dispatcher_threads >= 2);
    // LISTEN_ADDR default is 0.0.0.0:8443 (port in network byte order).
    try testing.expectEqual(std.mem.nativeToBig(u16, 8443), cfg.listen_addr.in.sa.port);
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

        // WORKER_COUNT → cpu_count, minimum 2.
        const exp_worker: u32 = @max(2, @as(u32, @intCast(@min(cpu_count, @as(usize, std.math.maxInt(u32))))));
        try testing.expectEqual(exp_worker, cfg.worker_count);
        try testing.expect(cfg.worker_count >= 2);

        // DISPATCHER_THREADS → 2 * cpu_count, minimum 2.
        const exp_disp: u8 = @max(2, @as(u8, @intCast(@min(cpu_count * 2, @as(usize, std.math.maxInt(u8))))));
        try testing.expectEqual(exp_disp, cfg.dispatcher_threads);
        try testing.expect(cfg.dispatcher_threads >= 2);

        // CONN_POOL_THREADS → cpu_count, minimum 2.
        try testing.expect(cfg.conn_pool_threads >= 2);
    }
    // An explicit CONN_POOL_THREADS is used verbatim.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "CONN_POOL_THREADS", "5" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u8, 5), cfg.conn_pool_threads);
    }
}

test "WORKER_COUNT trims whitespace and accepts the minimum of 1" {
    // Whitespace around the value is trimmed.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "WORKER_COUNT", "  4  " },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u32, 4), cfg.worker_count);
    }
    // 1 is accepted (the minimum boundary).
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "WORKER_COUNT", "1" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u32, 1), cfg.worker_count);
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
        .{ .key = "WORKER_COUNT",            .val = "not_a_number" },
        .{ .key = "WORKER_COUNT",            .val = "3.5" },
        .{ .key = "QUEUE_CAPACITY",          .val = "lots" },
        .{ .key = "DISPATCHER_THREADS",      .val = "four" },
        .{ .key = "IO_POOL_THREADS",         .val = "many" },
        .{ .key = "MAX_INFLIGHT_PER_WORKER", .val = "lots" },
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

test "BOT_API_BASE absent uses the default; present is used" {
    var env_default = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env_default.deinit();
    const cfg_default = try loadFromMap(testing.allocator, env_default);
    defer deinit(cfg_default, testing.allocator);
    try testing.expectEqualStrings("https://api.telegram.org", cfg_default.api_base);

    var env_set = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        .{ "BOT_API_BASE", "http://127.0.0.1:9000" },
    });
    defer env_set.deinit();
    const cfg_set = try loadFromMap(testing.allocator, env_set);
    defer deinit(cfg_set, testing.allocator);
    try testing.expectEqualStrings("http://127.0.0.1:9000", cfg_set.api_base);
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

test "MULTIPART_MAX_FILE parses a value and defaults to 50 MB" {
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "MULTIPART_MAX_FILE", "10485760" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(usize, 10485760), cfg.multipart_max_file);
    }
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(usize, 52428800), cfg.multipart_max_file);
    }
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
        try testing.expectEqual(@as(usize, 65_536), cfg.proc_max_output);
    }
    // Present → parsed.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN",         "tok" }, .{ "WEBHOOK_SECRET",   "sec" },
            .{ "IO_POOL_THREADS",   "8" },   .{ "IO_QUEUE_CAPACITY", "512" },
            .{ "IO_JOB_TIMEOUT_MS", "10000" }, .{ "PROC_MAX_OUTPUT", "131072" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u8,    8),       cfg.io_pool_threads);
        try testing.expectEqual(@as(u16,   512),     cfg.io_queue_capacity);
        try testing.expectEqual(@as(u64,   10_000),  cfg.io_job_timeout_ms);
        try testing.expectEqual(@as(usize, 131_072), cfg.proc_max_output);
    }
}

test "MAX_INFLIGHT_PER_WORKER and WORKFLOW_DEADLINE_MS parse and default" {
    // Absent → defaults.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u16, 64),     cfg.max_inflight_per_worker);
        try testing.expectEqual(@as(u64, 60_000), cfg.workflow_deadline_ms);
    }
    // Present → parsed.
    {
        var env = try makeEnv(testing.allocator, &.{
            .{ "BOT_TOKEN", "tok" }, .{ "WEBHOOK_SECRET", "sec" },
            .{ "MAX_INFLIGHT_PER_WORKER", "32" }, .{ "WORKFLOW_DEADLINE_MS", "30000" },
        });
        defer env.deinit();
        const cfg = try loadFromMap(testing.allocator, env);
        defer deinit(cfg, testing.allocator);
        try testing.expectEqual(@as(u16, 32),     cfg.max_inflight_per_worker);
        try testing.expectEqual(@as(u64, 30_000), cfg.workflow_deadline_ms);
    }
}

test "METRICS_LOG parses booleans and rejects invalid values" {
    const ok = [_]struct { val: ?[]const u8, want: bool }{
        .{ .val = null,    .want = true },  // absent → default true
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
