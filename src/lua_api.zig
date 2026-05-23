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
const types = @import("types.zig");

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
    allocator:      std.mem.Allocator,
    /// Maximum file size in bytes for multipart upload descriptors.
    max_file_bytes: usize,
};

const REGISTRY_KEY: [:0]const u8 = "_zora_ctx";

/// Registry key for the per-invocation `bot.emit` accumulator table.
const EMIT_KEY: [:0]const u8 = "_zora_emit";

/// The tg.* ergonomic facade (ADR-0001 §AD-1).  `tg.<method>{params}` is
/// exactly `bot.emit{ method = "<method>", params = {params} }` — pure sugar;
/// the call is schema-validated downstream regardless of which form is used.
const TG_FACADE_LUA: [:0]const u8 =
    \\tg = setmetatable({}, { __index = function(_, method)
    \\  return function(params)
    \\    return bot.emit{ method = method, params = params }
    \\  end
    \\end })
;

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
        .{ .name = "emit",           .func = botEmit },
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

    // Install an initial (empty) emit accumulator so the registry slot is
    // always a table; callOnMessage replaces it per invocation.
    beginEmitBatch(lua);

    // Install the tg.* facade.  The snippet is a fixed constant — a failure
    // here would be a build-time bug, not a runtime condition.
    lua.doString(TG_FACADE_LUA) catch unreachable;
}

// ---------------------------------------------------------------------------
// Private: retrieve ApiCtx from the Lua registry
// ---------------------------------------------------------------------------

pub fn getCtx(lua: *Lua) *ApiCtx {
    _ = lua.getField(ziglua.registry_index, REGISTRY_KEY);
    const ptr = lua.toPointer(-1) catch unreachable; // always a light userdata
    lua.pop(1);
    return @constCast(@alignCast(@ptrCast(ptr)));
}

// ---------------------------------------------------------------------------
// Public: buildApiCall — build an ApiCall from a Lua params table (Phase 15)
// ---------------------------------------------------------------------------

/// Build an ApiCall from a Lua params table at stack index `params_idx`.
/// Single scan: if any value is a file-descriptor table → multipart payload;
/// otherwise → JSON payload. `method` is duped into the returned ApiCall.
///
/// File-descriptor tables:
///   { __file = "/abs/path" }                      — read from disk
///   { __file_bytes = "...", filename = "name" }   — inline bytes
///
/// Errors: error.FileTooLarge, error.MissingFilename, file-read errors, OOM.
pub fn buildApiCall(
    lua:        *Lua,
    params_idx: i32,
    method:     []const u8,
    ctx:        *ApiCtx,
) !types.ApiCall {
    const alloc = ctx.allocator;
    const method_owned = try alloc.dupe(u8, method);
    errdefer alloc.free(method_owned);

    // No params → JSON "{}"
    if (!lua.isTable(params_idx)) {
        return .{
            .method  = method_owned,
            .payload = .{ .json = try alloc.dupe(u8, "{}") },
        };
    }

    // Single-pass scan over the params table.
    var parts: std.ArrayListUnmanaged(types.MultipartPart) = .empty;
    errdefer {
        for (parts.items) |p| {
            alloc.free(p.name);
            alloc.free(p.content);
            if (p.filename) |f| alloc.free(f);
        }
        parts.deinit(alloc);
    }
    var has_files = false;

    lua.pushNil(); // initial key for lua.next()
    while (lua.next(params_idx)) {
        // Stack: [..., params_table, key, value]
        // defer pops value at end of every iteration (including continue/return).
        defer lua.pop(1);

        const key_type = lua.typeOf(-2);
        if (key_type != .string and key_type != .number) continue;
        const key_z = lua.toString(-2) catch continue;
        const key = try alloc.dupe(u8, key_z);
        // key_in_parts: true once key is transferred to parts (so defer must not free it).
        var key_in_parts = false;
        defer if (!key_in_parts) alloc.free(key);

        if (lua.isTable(-1)) {
            // Check for __file (file-path descriptor)
            const ft = lua.getField(-1, "__file");
            if (ft == .string) {
                const path_z = lua.toString(-1) catch {
                    lua.pop(1); // pop __file value
                    return error.InvalidDescriptor; // defer frees key
                };
                lua.pop(1); // pop __file value

                const bytes = std.fs.cwd().readFileAlloc(
                    alloc, path_z, ctx.max_file_bytes +| 1,
                ) catch |err| {
                    return if (err == error.FileTooBig) error.FileTooLarge else err; // defer frees key
                };
                if (bytes.len > ctx.max_file_bytes) {
                    alloc.free(bytes);
                    return error.FileTooLarge; // defer frees key
                }

                const fname = alloc.dupe(u8, std.fs.path.basename(path_z)) catch |err| {
                    alloc.free(bytes);
                    return err; // defer frees key
                };
                parts.append(alloc, .{ .name = key, .content = bytes, .filename = fname }) catch |err| {
                    alloc.free(fname);
                    alloc.free(bytes);
                    return err; // defer frees key
                };
                key_in_parts = true; // parts owns key; defer must not free it
                has_files = true;
                continue;
            }
            lua.pop(1); // pop nil __file result

            // Check for __file_bytes (inline-bytes descriptor)
            const fbt = lua.getField(-1, "__file_bytes");
            if (fbt == .string) {
                const src = lua.toString(-1) catch {
                    lua.pop(1); // pop __file_bytes value
                    return error.InvalidDescriptor; // defer frees key
                };
                if (src.len > ctx.max_file_bytes) {
                    lua.pop(1); // pop __file_bytes value
                    return error.FileTooLarge; // defer frees key
                }
                const bytes = alloc.dupe(u8, src) catch |err| {
                    lua.pop(1); // pop __file_bytes value
                    return err; // defer frees key
                };
                lua.pop(1); // pop __file_bytes value

                // filename sub-key is required
                const fn_type = lua.getField(-1, "filename");
                if (fn_type != .string) {
                    lua.pop(1); // pop nil filename result
                    alloc.free(bytes);
                    return error.MissingFilename; // defer frees key
                }
                const fname_z = lua.toString(-1) catch unreachable;
                const fname   = alloc.dupe(u8, fname_z) catch |err| {
                    lua.pop(1); // pop filename value
                    alloc.free(bytes);
                    return err; // defer frees key
                };
                lua.pop(1); // pop filename value

                parts.append(alloc, .{ .name = key, .content = bytes, .filename = fname }) catch |err| {
                    alloc.free(fname);
                    alloc.free(bytes);
                    return err; // defer frees key
                };
                key_in_parts = true; // parts owns key; defer must not free it
                has_files = true;
                continue;
            }
            lua.pop(1); // pop nil __file_bytes result
            // Fall through: table value that is not a descriptor → skip as scalar
        }

        // Scalar value — stringify into a text part, or skip unsupported types.
        const val_type = lua.typeOf(-1);
        const scalar: []const u8 = switch (val_type) {
            .string  => alloc.dupe(u8, lua.toString(-1) catch "") catch |err| return err,
            .number  => blk: {
                if (lua.isInteger(-1)) {
                    const n = lua.toInteger(-1) catch 0;
                    break :blk std.fmt.allocPrint(alloc, "{d}", .{n}) catch |err| return err;
                } else {
                    const f = lua.toNumber(-1) catch 0;
                    break :blk std.fmt.allocPrint(alloc, "{d}", .{f}) catch |err| return err;
                }
            },
            .boolean => blk: {
                const b = lua.toBoolean(-1);
                break :blk alloc.dupe(u8, if (b) "true" else "false") catch |err| return err;
            },
            else => continue, // skip nil, tables, etc. — defer frees key
        };

        parts.append(alloc, .{ .name = key, .content = scalar, .filename = null }) catch |err| {
            alloc.free(scalar);
            return err; // defer frees key
        };
        key_in_parts = true; // parts owns key; defer must not free it
    }

    if (has_files) {
        return .{
            .method  = method_owned,
            .payload = .{ .multipart = try parts.toOwnedSlice(alloc) },
        };
    }

    // No file parts — free accumulated scalar parts; serialize table as JSON.
    for (parts.items) |p| {
        alloc.free(p.name);
        alloc.free(p.content);
        // filename is always null for scalar parts
    }
    parts.deinit(alloc);

    const body = try serializer.luaTableToJson(lua, params_idx, alloc);
    return .{
        .method  = method_owned,
        .payload = .{ .json = body },
    };
}

// ---------------------------------------------------------------------------
// Private: getStateImpl
// ---------------------------------------------------------------------------
//
fn getStateImpl(
    lua: *Lua,
    ctx: *ApiCtx,
    id: ziglua.Integer,
    comptime dbFn: fn (*state_store.StateStore, i64) anyerror![]u8,
    comptime errPrefix: []const u8,
) c_int {
    const json = dbFn(ctx.db, id) catch |err| {
        lua.raiseErrorStr(errPrefix ++ " db error: %s", .{@errorName(err).ptr});
    };
    defer ctx.db.allocator.free(json);
    serializer.jsonToLuaTable(lua, json, ctx.allocator) catch |err| {
        lua.raiseErrorStr(errPrefix ++ " deserialize error: %s", .{@errorName(err).ptr});
    };
    return 1;
}

fn setStateImpl(
    lua: *Lua,
    ctx: *ApiCtx,
    id: ziglua.Integer,
    comptime dbFn: fn (*state_store.StateStore, i64, []const u8) anyerror!void,
    comptime errPrefix: []const u8,
) c_int {
    lua.checkType(2, .table);

    const json = serializer.luaTableToJson(lua, 2, ctx.allocator) catch |err| {
        lua.raiseErrorStr(errPrefix ++ " serialize error: %s", .{@errorName(err).ptr});
    };
    defer ctx.allocator.free(json);

    dbFn(ctx.db, id, json) catch |err| {
        lua.raiseErrorStr(errPrefix ++ " db error: %s", .{@errorName(err).ptr});
    };
    return 0;
}

// ---------------------------------------------------------------------------
// bot.get_user_state(user_id: integer) -> table
// ---------------------------------------------------------------------------

fn botGetUserState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const user_id = lua.checkInteger(1);

    return getStateImpl(lua, ctx, user_id, state_store.StateStore.getUserState, "getUserState");
}

// ---------------------------------------------------------------------------
// bot.set_user_state(user_id: integer, data: table)
// ---------------------------------------------------------------------------

fn botSetUserState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const user_id = lua.checkInteger(1);
    return setStateImpl(lua, ctx, user_id, state_store.StateStore.setUserState, "setUserState");
}

// ---------------------------------------------------------------------------
// bot.get_chat_state(chat_id: integer) -> table
// ---------------------------------------------------------------------------

fn botGetChatState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const chat_id = lua.checkInteger(1);

    return getStateImpl(lua, ctx, chat_id, state_store.StateStore.getChatState, "getChatState");
}

// ---------------------------------------------------------------------------
// bot.set_chat_state(chat_id: integer, data: table)
// ---------------------------------------------------------------------------

fn botSetChatState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const chat_id = lua.checkInteger(1);
    lua.checkType(2, .table);

    return setStateImpl(lua, ctx, chat_id, state_store.StateStore.setChatState, "setChatState");
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
// bot.emit — fire-and-forget API-call accumulator (ADR-0001 §AD-1)
//
// `bot.emit{ method = ..., params = {...} }` appends an API-call table to a
// per-invocation accumulator held in the Lua registry.  lua_engine drains the
// accumulator after on_message returns: emitted calls are dispatched in call
// order, before the on_message return-list.
// ---------------------------------------------------------------------------

/// Install a fresh, empty emit accumulator.  Call once before each on_message.
pub fn beginEmitBatch(lua: *Lua) void {
    lua.newTable();
    lua.setField(ziglua.registry_index, EMIT_KEY);
}

/// Push the current emit accumulator table onto the Lua stack.
pub fn pushEmitBatch(lua: *Lua) void {
    _ = lua.getField(ziglua.registry_index, EMIT_KEY);
}

fn botEmit(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table); // bot.emit{ method = ..., params = {...} }

    _ = lua.getField(ziglua.registry_index, EMIT_KEY); // [arg1, batch]
    const next: ziglua.Integer = @intCast(lua.rawLen(-1) + 1);
    lua.pushValue(1);          // [arg1, batch, arg1]
    lua.rawSetIndex(-2, next); // batch[next] = arg1 (pops value) → [arg1, batch]
    lua.pop(1);                // [arg1]
    return 0;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "AC-6.7: bot.set_user_state + bot.get_user_state round-trip" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

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

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

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

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

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

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

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

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

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

    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    const result = lua.doString(
        \\bot.get_user_state("not_an_int")
    );
    if (result) |_| return error.TestExpectedLuaError else |_| {}
}

test "AC-14.4: tg.<method>{...} == bot.emit{method=...,params=...}" {
    const lua_engine = @import("lua_engine.zig");
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };
    var engine = try lua_engine.LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  tg.sendMessage{ chat_id = 7, text = "via facade" }
        \\  return {}
        \\end
    );
    const actions = try engine.callOnMessage(testing.allocator, "{}");
    defer types.freeApiCalls(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 1), actions.len);
    try testing.expectEqualStrings("sendMessage", actions[0].method);
    const json_body = actions[0].payload.json;
    try testing.expect(std.mem.indexOf(u8, json_body, "\"chat_id\":7") != null);
    try testing.expect(std.mem.indexOf(u8, json_body, "\"text\":\"via facade\"") != null);
}

test "AC-6.13: two engines have independent ApiCtx (no registry aliasing)" {
    const lua_engine = @import("lua_engine.zig");

    var db1 = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db1.close();
    var db2 = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db2.close();

    var ctx1 = ApiCtx{ .db = &db1, .allocator = testing.allocator, .max_file_bytes = 52428800 };
    var ctx2 = ApiCtx{ .db = &db2, .allocator = testing.allocator, .max_file_bytes = 52428800 };

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

// ---------------------------------------------------------------------------
// AC-15.1 (unit) — buildApiCall detects __file and returns multipart
// AC-15.3 — file larger than max_file_bytes → error.FileTooLarge
// ---------------------------------------------------------------------------

test "AC-15.1 (unit): buildApiCall with __file descriptor returns multipart" {
    const alloc = testing.allocator;
    var db = try state_store.StateStore.open(alloc, ":memory:");
    defer db.close();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "img.jpg", .data = "\xff\xd8\xff" });
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath("img.jpg", &path_buf);

    var ctx = ApiCtx{ .db = &db, .allocator = alloc, .max_file_bytes = 1024 };
    var lua = try Lua.init(alloc);
    defer lua.deinit();
    lua.openBase();
    register(lua, &ctx, 1);

    // Build params table: { photo = { __file = path } }
    lua.newTable();           // params at index 1
    lua.newTable();           // descriptor at index 2
    _ = lua.pushString(path);
    lua.setField(-2, "__file"); // descriptor.__file = path
    lua.setField(-2, "photo");  // params.photo = descriptor
    const params_idx: i32 = lua.getTop();

    const call = try buildApiCall(lua, params_idx, "sendPhoto", &ctx);
    defer types.freeApiCall(call, alloc);

    try testing.expectEqualStrings("sendPhoto", call.method);
    switch (call.payload) {
        .multipart => |parts| {
            try testing.expectEqual(@as(usize, 1), parts.len);
            try testing.expectEqualStrings("photo", parts[0].name);
            try testing.expectEqualStrings("\xff\xd8\xff", parts[0].content);
            try testing.expectEqualStrings("img.jpg", parts[0].filename.?);
        },
        .json => return error.ExpectedMultipart,
    }
}

test "AC-15.3: buildApiCall returns FileTooLarge when file exceeds limit" {
    const alloc = testing.allocator;
    var db = try state_store.StateStore.open(alloc, ":memory:");
    defer db.close();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    // 11 bytes, limit is 10
    try tmp.dir.writeFile(.{ .sub_path = "big.bin", .data = "hello world" });
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath("big.bin", &path_buf);

    var ctx = ApiCtx{ .db = &db, .allocator = alloc, .max_file_bytes = 10 };
    var lua = try Lua.init(alloc);
    defer lua.deinit();
    lua.openBase();
    register(lua, &ctx, 1);

    lua.newTable();
    lua.newTable();
    _ = lua.pushString(path);
    lua.setField(-2, "__file");
    lua.setField(-2, "photo");
    const params_idx: i32 = lua.getTop();

    const result = buildApiCall(lua, params_idx, "sendPhoto", &ctx);
    try testing.expectError(error.FileTooLarge, result);
}
