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

    // Optional: BOT_API_BASE (AD-8 — first-class setting)
    const api_base = allocator.dupe(u8, env.get("BOT_API_BASE") orelse "https://api.telegram.org") catch
        return error.OutOfMemory;
    errdefer allocator.free(api_base);

    // Optional: WORKER_COUNT (default: cpu_count, minimum 2)
    const worker_count = try parseUint(u8, env, "WORKER_COUNT", defaultWorkerCount());

    // Optional: QUEUE_CAPACITY
    const queue_capacity = try parseUint(u16, env, "QUEUE_CAPACITY", 255);

    // Optional: DISPATCHER_THREADS
    const dispatcher_threads = try parseUint(u8, env, "DISPATCHER_THREADS", 2);

    // Optional: MULTIPART_MAX_FILE (50 MB — Telegram bot upload limit)
    const multipart_max_file = try parseUint(usize, env, "MULTIPART_MAX_FILE", 52428800);

    return Config{
        .bot_token          = bot_token,
        .webhook_secret     = webhook_secret,
        .listen_addr        = listen_addr,
        .rules_file         = rules_file,
        .db_path            = db_path,
        .worker_count       = worker_count,
        .queue_capacity     = queue_capacity,
        .dispatcher_threads = dispatcher_threads,
        .schema_file        = schema_file,
        .api_validation     = api_validation,
        .api_base           = api_base,
        .multipart_max_file = multipart_max_file,
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

/// Returns cpu_count, clamped to [2, maxInt(u32)].
/// One worker per CPU core is ample for Telegram's ~30 msg/s outbound limit.
/// Minimum 2 ensures a second worker is available during SQLite write contention.
fn defaultWorkerCount() u8 {
    const cpu_count = std.Thread.getCpuCount() catch 1;
    return @max(2, @as(u8, @intCast(@min(cpu_count, @as(usize, std.math.maxInt(u8))))));
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

test "AC-3.1: all required and optional vars set — returns correct Config" {
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
}

test "AC-3.2: BOT_TOKEN absent returns error.MissingRequiredField" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env.deinit();

    const result = loadFromMap(testing.allocator, env);
    try testing.expectError(error.MissingRequiredField, result);
}

test "AC-3.3: WEBHOOK_SECRET absent returns error.MissingRequiredField" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN", "tok" },
    });
    defer env.deinit();

    const result = loadFromMap(testing.allocator, env);
    try testing.expectError(error.MissingRequiredField, result);
}

test "AC-3.4: all optional fields absent — defaults applied" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    try testing.expectEqualStrings("rules/rules.lua", cfg.rules_file);
    try testing.expectEqualStrings("state.db", cfg.db_path);
    try testing.expectEqual(@as(u32, 255), cfg.queue_capacity);
    try testing.expectEqual(@as(u32, 2), cfg.dispatcher_threads);
    // listen_addr default: 0.0.0.0:8443
    const expected_addr = try std.net.Address.parseIpAndPort("0.0.0.0:8443");
    try testing.expectEqual(expected_addr.in.sa.port, cfg.listen_addr.in.sa.port);
}

test "AC-3.5: WORKER_COUNT absent — defaults to cpu_count, minimum 2" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    const cpu_count = std.Thread.getCpuCount() catch 1;
    const expected: u32 = @max(2, @as(u32, @intCast(@min(cpu_count, @as(usize, std.math.maxInt(u32))))));
    try testing.expectEqual(expected, cfg.worker_count);
    try testing.expect(cfg.worker_count >= 2);
}

test "AC-3.6: WORKER_COUNT non-numeric returns error.InvalidConfig" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
        .{ "WORKER_COUNT",   "not_a_number" },
    });
    defer env.deinit();

    const result = loadFromMap(testing.allocator, env);
    try testing.expectError(error.InvalidConfig, result);
}

test "AC-3.6: WORKER_COUNT float string returns error.InvalidConfig" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
        .{ "WORKER_COUNT",   "3.5" },
    });
    defer env.deinit();

    const result = loadFromMap(testing.allocator, env);
    try testing.expectError(error.InvalidConfig, result);
}

test "AC-3.7: QUEUE_CAPACITY non-numeric returns error.InvalidConfig" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",       "tok" },
        .{ "WEBHOOK_SECRET",  "sec" },
        .{ "QUEUE_CAPACITY",  "lots" },
    });
    defer env.deinit();

    const result = loadFromMap(testing.allocator, env);
    try testing.expectError(error.InvalidConfig, result);
}

test "AC-3.7: DISPATCHER_THREADS non-numeric returns error.InvalidConfig" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",           "tok" },
        .{ "WEBHOOK_SECRET",      "sec" },
        .{ "DISPATCHER_THREADS",  "four" },
    });
    defer env.deinit();

    const result = loadFromMap(testing.allocator, env);
    try testing.expectError(error.InvalidConfig, result);
}

test "AC-3.8: Config fields have correct types" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
        .{ "WORKER_COUNT",   "4" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    // worker_count is u32
    const wc: u32 = cfg.worker_count;
    try testing.expectEqual(@as(u32, 4), wc);

    // queue_capacity is u32
    const qc: u32 = cfg.queue_capacity;
    try testing.expectEqual(@as(u32, 255), qc);

    // listen_addr is std.net.Address — verify it holds a port
    const port = cfg.listen_addr.in.sa.port;
    try testing.expect(port != 0);
}

test "AC-3.4: LISTEN_ADDR default parses to 0.0.0.0:8443" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    // Port 8443 in network byte order
    const expected_port = std.mem.nativeToBig(u16, 8443);
    try testing.expectEqual(expected_port, cfg.listen_addr.in.sa.port);
}

test "AC-3.6: WORKER_COUNT whitespace-trimmed value parses correctly" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
        .{ "WORKER_COUNT",   "  4  " },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    try testing.expectEqual(@as(u32, 4), cfg.worker_count);
}

test "AC-3.2+AC-3.3: both required fields absent — first missing field errors" {
    var env = try makeEnv(testing.allocator, &.{});
    defer env.deinit();

    const result = loadFromMap(testing.allocator, env);
    try testing.expectError(error.MissingRequiredField, result);
}

test "AC-3.4: WORKER_COUNT=1 is accepted (minimum boundary)" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
        .{ "WORKER_COUNT",   "1" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);

    try testing.expectEqual(@as(u32, 1), cfg.worker_count);
}

test "AC-14.10: BOT_API_BASE absent → default; present → used" {
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

test "AC-14.11: API_VALIDATION parsed; default warn; invalid → InvalidConfig" {
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

test "AC-14: SCHEMA_FILE absent → default; present → used" {
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

test "AC-15.x: MULTIPART_MAX_FILE parses correctly" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",           "tok" },
        .{ "WEBHOOK_SECRET",      "sec" },
        .{ "MULTIPART_MAX_FILE",  "10485760" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);
    try testing.expectEqual(@as(usize, 10485760), cfg.multipart_max_file);
}

test "AC-15.x: MULTIPART_MAX_FILE defaults to 52428800" {
    var env = try makeEnv(testing.allocator, &.{
        .{ "BOT_TOKEN",      "tok" },
        .{ "WEBHOOK_SECRET", "sec" },
    });
    defer env.deinit();

    const cfg = try loadFromMap(testing.allocator, env);
    defer deinit(cfg, testing.allocator);
    try testing.expectEqual(@as(usize, 52428800), cfg.multipart_max_file);
}
