/// lua_api.zig — Lua-callable functions registered as bot.*
///
/// Registers the following Lua globals when `register()` is called:
///
///   bot.get_user_state(user_id: integer) -> table
///   bot.set_user_state(user_id: integer, data: table)
///   bot.get_chat_state(chat_id: integer) -> table
///   bot.set_chat_state(chat_id: integer, data: table)
///   bot.get_global(key: string) -> string|nil
///   bot.set_global(key: string, value: string)
///   bot.log(level: string, message: string)
///   bot.rules_api_version (integer, read-only by convention)
///
/// The ApiCtx pointer is stored in the Lua registry under "_zora_ctx" and
/// retrieved by each C function.  It must remain live for the entire lifetime
/// of the Lua state.

const std = @import("std");
const ziglua = @import("ziglua");
const state_store = @import("state_store.zig");
const serializer = @import("serializer.zig");

const Lua = ziglua.Lua;

// ---------------------------------------------------------------------------
// ApiCtx — context shared by all bot.* functions via the Lua registry
// ---------------------------------------------------------------------------

/// Per-engine context stored in the Lua registry.
/// The db allocator is used for state_store operations;
/// the api allocator is used for temporary serializer buffers.
pub const ApiCtx = struct {
    db: *state_store.StateStore,
    /// Allocator for temporary allocations inside bot.* functions
    /// (e.g., JSON intermediate buffers).  Must be an arena or GPA —
    /// every allocation is freed before the C function returns.
    allocator: std.mem.Allocator,
};

const REGISTRY_KEY: [:0]const u8 = "_zora_ctx";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Register all bot.* functions and bot.rules_api_version into `lua`.
/// `ctx` must remain live for the entire lifetime of `lua`.
/// `rules_api_version` is exposed as the read-only Lua integer
/// `bot.rules_api_version`.
pub fn register(lua: *Lua, ctx: *ApiCtx, rules_api_version: u32) void {
    // Store ApiCtx pointer in the registry so CFns can retrieve it.
    lua.pushLightUserdata(@ptrCast(ctx));
    lua.setField(ziglua.registry_index, REGISTRY_KEY);

    // Create the `bot` table.
    lua.newTable();

    // Register each function as bot.<name>.
    const fns = [_]struct { name: [:0]const u8, func: ziglua.CFn }{
        .{ .name = "get_user_state", .func = botGetUserState },
        .{ .name = "set_user_state", .func = botSetUserState },
        .{ .name = "get_chat_state", .func = botGetChatState },
        .{ .name = "set_chat_state", .func = botSetChatState },
        .{ .name = "get_global",     .func = botGetGlobal },
        .{ .name = "set_global",     .func = botSetGlobal },
        .{ .name = "log",            .func = botLog },
    };
    for (fns) |f| {
        lua.pushFunction(f.func);
        lua.setField(-2, f.name);
    }

    // bot.rules_api_version = rules_api_version
    lua.pushInteger(@intCast(rules_api_version));
    lua.setField(-2, "rules_api_version");

    // _G.bot = bot_table
    lua.setGlobal("bot");
}

// ---------------------------------------------------------------------------
// Private: retrieve ApiCtx from the Lua registry
// ---------------------------------------------------------------------------

fn getCtx(lua: *Lua) *ApiCtx {
    _ = lua.getField(ziglua.registry_index, REGISTRY_KEY);
    const ptr = lua.toPointer(-1) catch unreachable; // always a light userdata
    lua.pop(1);
    return @constCast(@alignCast(@ptrCast(ptr)));
}

// ---------------------------------------------------------------------------
// bot.get_user_state(user_id: integer) -> table
// ---------------------------------------------------------------------------

fn botGetUserState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const user_id = lua.checkInteger(1);

    const json = ctx.db.getUserState(user_id) catch |err| {
        lua.raiseErrorStr("get_user_state db error: %s", .{@errorName(err).ptr});
    };
    defer ctx.db.allocator.free(json);

    serializer.jsonToLuaTable(lua, json, ctx.allocator) catch |err| {
        lua.raiseErrorStr("get_user_state deserialize error: %s", .{@errorName(err).ptr});
    };
    return 1;
}

// ---------------------------------------------------------------------------
// bot.set_user_state(user_id: integer, data: table)
// ---------------------------------------------------------------------------

fn botSetUserState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const user_id = lua.checkInteger(1);
    lua.checkType(2, .table);

    const json = serializer.luaTableToJson(lua, 2, ctx.allocator) catch |err| {
        lua.raiseErrorStr("set_user_state serialize error: %s", .{@errorName(err).ptr});
    };
    defer ctx.allocator.free(json);

    ctx.db.setUserState(user_id, json) catch |err| {
        lua.raiseErrorStr("set_user_state db error: %s", .{@errorName(err).ptr});
    };
    return 0;
}

// ---------------------------------------------------------------------------
// bot.get_chat_state(chat_id: integer) -> table
// ---------------------------------------------------------------------------

fn botGetChatState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const chat_id = lua.checkInteger(1);

    const json = ctx.db.getChatState(chat_id) catch |err| {
        lua.raiseErrorStr("get_chat_state db error: %s", .{@errorName(err).ptr});
    };
    defer ctx.db.allocator.free(json);

    serializer.jsonToLuaTable(lua, json, ctx.allocator) catch |err| {
        lua.raiseErrorStr("get_chat_state deserialize error: %s", .{@errorName(err).ptr});
    };
    return 1;
}

// ---------------------------------------------------------------------------
// bot.set_chat_state(chat_id: integer, data: table)
// ---------------------------------------------------------------------------

fn botSetChatState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const chat_id = lua.checkInteger(1);
    lua.checkType(2, .table);

    const json = serializer.luaTableToJson(lua, 2, ctx.allocator) catch |err| {
        lua.raiseErrorStr("set_chat_state serialize error: %s", .{@errorName(err).ptr});
    };
    defer ctx.allocator.free(json);

    ctx.db.setChatState(chat_id, json) catch |err| {
        lua.raiseErrorStr("set_chat_state db error: %s", .{@errorName(err).ptr});
    };
    return 0;
}

// ---------------------------------------------------------------------------
// bot.get_global(key: string) -> string|nil
// ---------------------------------------------------------------------------

fn botGetGlobal(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const key = lua.checkString(1);

    const maybe_val = ctx.db.getGlobal(key) catch |err| {
        lua.raiseErrorStr("get_global db error: %s", .{@errorName(err).ptr});
    };

    if (maybe_val) |val| {
        defer ctx.db.allocator.free(val);
        _ = lua.pushString(val);
    } else {
        lua.pushNil();
    }
    return 1;
}

// ---------------------------------------------------------------------------
// bot.set_global(key: string, value: string)
// ---------------------------------------------------------------------------

fn botSetGlobal(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const key = lua.checkString(1);
    const val = lua.checkString(2);

    ctx.db.setGlobal(key, val) catch |err| {
        lua.raiseErrorStr("set_global db error: %s", .{@errorName(err).ptr});
    };
    return 0;
}

// ---------------------------------------------------------------------------
// bot.log(level: string, message: string)
// ---------------------------------------------------------------------------

fn botLog(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const level = lua.checkString(1);
    const message = lua.checkString(2);

    const log = std.log.scoped(.lua);
    if (std.mem.eql(u8, level, "info")) {
        log.info("{s}", .{message});
    } else if (std.mem.eql(u8, level, "warn")) {
        log.warn("{s}", .{message});
    } else if (std.mem.eql(u8, level, "error")) {
        log.err("{s}", .{message});
    } else {
        lua.raiseErrorStr(
            "bot.log: invalid level '%s'; expected info, warn, or error",
            .{level.ptr},
        );
    }
    return 0;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "AC-6.7: bot.set_user_state + bot.get_user_state round-trip" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator };

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // set_user_state(1, {count = 3})
    try lua.doString(
        \\bot.set_user_state(1, {count = 3})
    );

    // get_user_state(1) → push table onto stack
    try lua.doString(
        \\local s = bot.get_user_state(1)
        \\assert(s.count == 3, "expected count=3 got " .. tostring(s.count))
    );
}

test "AC-6.8: bot.set_chat_state + bot.get_chat_state round-trip" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator };

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    try lua.doString(
        \\bot.set_chat_state(42, {step = "started"})
        \\local s = bot.get_chat_state(42)
        \\assert(s.step == "started", "expected step=started got " .. tostring(s.step))
    );
}

test "AC-6.9: bot.get_global / bot.set_global" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator };

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    try lua.doString(
        \\bot.set_global("counter", "42")
        \\local v = bot.get_global("counter")
        \\assert(v == "42", "expected 42 got " .. tostring(v))
        \\local missing = bot.get_global("__no_such_key__")
        \\assert(missing == nil, "expected nil for missing key")
    );
}

test "AC-6.10: bot.log valid levels succeed; invalid level errors" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator };

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // Valid levels info and warn should not error.
    try lua.doString(
        \\bot.log("info",  "info message")
        \\bot.log("warn",  "warn message")
    );

    // Invalid level should raise a Lua error (any error variant is fine).
    const result = lua.doString(
        \\bot.log("bad_level", "oops")
    );
    if (result) |_| return error.TestExpectedLuaError else |_| {}
}

test "AC-6.11: bot.rules_api_version matches Zig constant" {
    const lua_engine = @import("lua_engine.zig");

    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator };

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, lua_engine.RULES_API_VERSION);

    // Read the version from Lua and compare.
    try lua.doString(
        \\assert(type(bot.rules_api_version) == "number",
        \\       "rules_api_version should be a number")
    );

    _ = try lua.getGlobal("bot");
    _ = lua.getField(-1, "rules_api_version");
    const ver = try lua.toInteger(-1);
    lua.pop(2);

    try testing.expectEqual(@as(ziglua.Integer, lua_engine.RULES_API_VERSION), ver);
}

test "AC-6.12: bot.get_user_state with non-integer arg → Lua error" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator };

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    const result = lua.doString(
        \\bot.get_user_state("not_an_int")
    );
    if (result) |_| return error.TestExpectedLuaError else |_| {}
}

test "AC-6.13: two engines have independent ApiCtx (no registry aliasing)" {
    const lua_engine = @import("lua_engine.zig");

    var db1 = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db1.close();
    var db2 = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db2.close();

    var ctx1 = ApiCtx{ .db = &db1, .allocator = testing.allocator };
    var ctx2 = ApiCtx{ .db = &db2, .allocator = testing.allocator };

    var engine1 = try lua_engine.LuaEngine.init(testing.allocator, &ctx1);
    defer engine1.deinit();
    var engine2 = try lua_engine.LuaEngine.init(testing.allocator, &ctx2);
    defer engine2.deinit();

    // Write to engine1's db via engine1.
    try engine1.lua.doString(
        \\bot.set_global("x", "from_engine1")
    );

    // engine2 should NOT see the value written by engine1 (separate db).
    try engine2.lua.doString(
        \\local v = bot.get_global("x")
        \\assert(v == nil, "engine2 should not see engine1 state: " .. tostring(v))
    );

    // Confirm engine1 still has its value.
    try engine1.lua.doString(
        \\local v = bot.get_global("x")
        \\assert(v == "from_engine1", "engine1 lost its state")
    );
}
