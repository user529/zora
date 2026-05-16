/// lua_engine.zig — load/reload rules.lua, call on_message()
///
/// One LuaEngine per worker thread; never shared between threads.
///
/// Lifecycle:
///   init(allocator, ctx)    → allocates Lua state, opens safe stdlib, registers bot.*
///   loadFile(path)          → doFile (replaces current chunk)
///   loadString(src)         → doString (for tests / hot-reload from memory)
///   callOnMessage(update)   → calls on_message(update_table) → []Action
///   freeActions(allocator, actions) → free slice + owned strings
///   deinit()                → closes Lua state

const std = @import("std");
const ziglua = @import("ziglua");
const types = @import("types.zig");
const serializer = @import("serializer.zig");
const lua_api = @import("lua_api.zig");

const Lua = ziglua.Lua;
const log = std.log.scoped(.lua_engine);

// ---------------------------------------------------------------------------
// Version constant — increment on breaking bot.* API changes
// ---------------------------------------------------------------------------

pub const RULES_API_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// LuaEngine
// ---------------------------------------------------------------------------

pub const LuaEngine = struct {
    lua: *Lua,
    allocator: std.mem.Allocator,

    /// Create a new Lua state with safe stdlib and the bot.* API registered.
    /// `ctx` must outlive this engine.
    pub fn init(allocator: std.mem.Allocator, ctx: *lua_api.ApiCtx) !LuaEngine {
        const lua = try Lua.init(allocator);
        errdefer lua.deinit();

        // Open only the safe subset of the standard library.
        lua.openBase();
        lua.openMath();
        lua.openString();
        lua.openTable();
        lua.openUtf8();
        // io, os, package, debug are intentionally NOT opened.

        // dofile and loadfile are registered by openBase but read from the
        // filesystem — remove them so rules cannot escape the sandbox.
        lua.pushNil();
        lua.setGlobal("dofile");
        lua.pushNil();
        lua.setGlobal("loadfile");

        // Register bot.* API and bot.rules_api_version.
        lua_api.register(lua, ctx, RULES_API_VERSION);

        return LuaEngine{ .lua = lua, .allocator = allocator };
    }

    pub fn deinit(self: *LuaEngine) void {
        self.lua.deinit();
    }

    /// Load and execute a Lua file.  The file must define `on_message`.
    /// Returns error.LuaError on syntax or runtime error (caller should log).
    pub fn loadFile(self: *LuaEngine, path: [:0]const u8) !void {
        self.lua.doFile(path) catch {
            return error.LuaError;
        };
    }

    /// Load and execute a Lua string (used in tests and for hot-reload).
    /// Returns error.LuaError on syntax or runtime error (caller should log).
    pub fn loadString(self: *LuaEngine, src: [:0]const u8) !void {
        self.lua.doString(src) catch {
            return error.LuaError;
        };
    }

    /// Call the Lua `on_message(update_table)` function.
    ///
    /// - Serializes `update` to JSON, then pushes it as a Lua table.
    /// - Calls on_message with a protected call (no Lua error escapes).
    /// - On Lua error: logs the error, returns an empty slice (no Zig error).
    /// - On Zig-level allocation error: propagates the error.
    /// - The returned slice and all string payloads are allocated from
    ///   `allocator`; free them with `freeActions`.
    ///
    /// OWNERSHIP INVARIANT: the caller must always call `freeActions` on the
    /// returned slice, even when `len == 0`.  Every code path — including Lua
    /// error and failed serialization — returns a heap-allocated slice.
    pub fn callOnMessage(
        self: *LuaEngine,
        allocator: std.mem.Allocator,
        update: types.Update,
    ) ![]types.Action {
        const lua = self.lua;
        const stack_base = lua.getTop();

        // ── 1. Serialize Update → JSON ────────────────────────────────────
        const json = try std.json.Stringify.valueAlloc(
            allocator,
            update,
            .{ .emit_null_optional_fields = false },
        );
        defer allocator.free(json);

        // ── 2. Push on_message function ───────────────────────────────────
        const fn_type = lua.getGlobal("on_message") catch {
            log.warn("on_message lookup failed", .{});
            return try allocator.alloc(types.Action, 0);
        };
        if (fn_type != .function) {
            lua.setTop(stack_base);
            log.warn("on_message is not a function (type={s})", .{lua.typeName(fn_type)});
            return try allocator.alloc(types.Action, 0);
        }

        // ── 3. Push update as Lua table ───────────────────────────────────
        serializer.jsonToLuaTable(lua, json, allocator) catch |err| {
            lua.setTop(stack_base);
            log.err("failed to push update table: {s}", .{@errorName(err)});
            return try allocator.alloc(types.Action, 0);
        };

        // ── 4. Protected call: on_message(update_table) ───────────────────
        lua.protectedCall(.{ .args = 1, .results = 1, .msg_handler = 0 }) catch {
            const err_msg = lua.toString(-1) catch "(no message)";
            log.warn("on_message error: {s}", .{err_msg});
            lua.setTop(stack_base);
            return try allocator.alloc(types.Action, 0);
        };

        // Stack: [..., result_table]   (base + 1 element)
        defer lua.setTop(stack_base);

        // ── 5. Parse result table → []Action ─────────────────────────────
        return parseActions(lua, lua.getTop(), allocator);
    }
};

// ---------------------------------------------------------------------------
// Action parsing helpers
// ---------------------------------------------------------------------------

/// Parse a Lua table (at `table_idx` on the stack) as an array of Actions.
/// All string payloads are duplicated into `allocator`.
fn parseActions(lua: *Lua, table_idx: i32, allocator: std.mem.Allocator) ![]types.Action {
    if (!lua.isTable(table_idx)) {
        log.warn("on_message did not return a table", .{});
        return allocator.alloc(types.Action, 0);
    }

    const n = lua.rawLen(table_idx);
    var list = std.ArrayListUnmanaged(types.Action).empty;
    errdefer {
        // Free all string payloads accumulated so far, then the list itself.
        for (list.items) |action| {
            types.freeActionPayload(action, allocator);
        }
        list.deinit(allocator);
    }

    var i: ziglua.Integer = 1;
    while (i <= @as(ziglua.Integer, @intCast(n))) : (i += 1) {
        _ = lua.rawGetIndex(table_idx, i);
        // action element is now at top (-1)
        if (lua.isTable(-1)) {
            const abs = lua.getTop(); // absolute index of action element
            if (parseOneAction(lua, abs, allocator)) |action| {
                try list.append(allocator, action);
            } else |err| {
                log.warn("skipping action {d}: {s}", .{ i, @errorName(err) });
            }
        }
        lua.pop(1);
    }

    return list.toOwnedSlice(allocator);
}

/// Parse a single action table (at absolute `table_idx`) into an Action.
/// All returned string fields are newly allocated from `allocator`.
fn parseOneAction(lua: *Lua, table_idx: i32, allocator: std.mem.Allocator) !types.Action {
    // ── Read "action" field ───────────────────────────────────────────────
    _ = lua.getField(table_idx, "action");
    const action_z = lua.toString(-1) catch {
        lua.pop(1);
        return error.InvalidAction;
    };
    // Copy before popping so we can pop without invalidating the pointer.
    var action_buf: [64]u8 = undefined;
    const action_len = @min(action_z.len, action_buf.len - 1);
    @memcpy(action_buf[0..action_len], action_z[0..action_len]);
    const action_type = action_buf[0..action_len];
    lua.pop(1);

    // ── Dispatch ─────────────────────────────────────────────────────────
    if (std.mem.eql(u8, action_type, "send_message")) {
        const chat_id = try getIntField(lua, table_idx, "chat_id");
        const text    = try getStringField(lua, table_idx, "text", allocator);
        return .{ .send_message = .{ .chat_id = chat_id, .text = text } };

    } else if (std.mem.eql(u8, action_type, "send_message_ex")) {
        const chat_id = try getIntField(lua, table_idx, "chat_id");
        const text    = try getStringField(lua, table_idx, "text", allocator);
        errdefer allocator.free(text);

        // opts can be a table (serialized) or a string (passed as-is).
        _ = lua.getField(table_idx, "opts");
        const opts: []const u8 = if (lua.isTable(-1)) blk: {
            const j = try serializer.luaTableToJson(lua, -1, allocator);
            lua.pop(1);
            break :blk j;
        } else if (lua.isString(-1)) blk: {
            const s = lua.toString(-1) catch {
                lua.pop(1);
                const copy = try allocator.dupe(u8, "{}");
                break :blk copy;
            };
            const copy = try allocator.dupe(u8, s);
            lua.pop(1);
            break :blk copy;
        } else blk: {
            lua.pop(1);
            break :blk try allocator.dupe(u8, "{}");
        };
        errdefer allocator.free(opts);

        return .{ .send_message_ex = .{ .chat_id = chat_id, .text = text, .opts = opts } };

    } else if (std.mem.eql(u8, action_type, "answer_callback")) {
        const id = try getStringField(lua, table_idx, "callback_query_id", allocator);
        errdefer allocator.free(id);

        // text field is optional.
        _ = lua.getField(table_idx, "text");
        const text: ?[]const u8 = if (lua.isNil(-1)) blk: {
            lua.pop(1);
            break :blk null;
        } else blk: {
            const s = lua.toString(-1) catch {
                lua.pop(1);
                break :blk null;
            };
            const copy = try allocator.dupe(u8, s);
            lua.pop(1);
            break :blk copy;
        };

        return .{ .answer_callback = .{ .callback_query_id = id, .text = text } };

    } else if (std.mem.eql(u8, action_type, "delete_message")) {
        const chat_id    = try getIntField(lua, table_idx, "chat_id");
        const message_id = try getIntField(lua, table_idx, "message_id");
        return .{ .delete_message = .{ .chat_id = chat_id, .message_id = message_id } };
    }

    return error.InvalidAction;
}

/// Read an integer field `key` from the table at `table_idx`.
fn getIntField(lua: *Lua, table_idx: i32, key: [:0]const u8) !i64 {
    _ = lua.getField(table_idx, key);
    const n = lua.toInteger(-1) catch {
        lua.pop(1);
        return error.InvalidAction;
    };
    lua.pop(1);
    return n;
}

/// Read a string field `key` from the table at `table_idx`.
/// Returns a newly allocated copy owned by `allocator`.
fn getStringField(lua: *Lua, table_idx: i32, key: [:0]const u8, allocator: std.mem.Allocator) ![]const u8 {
    _ = lua.getField(table_idx, key);
    const s = lua.toString(-1) catch {
        lua.pop(1);
        return error.InvalidAction;
    };
    const copy = allocator.dupe(u8, s) catch {
        lua.pop(1);
        return error.OutOfMemory;
    };
    lua.pop(1);
    return copy;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;
const state_store = @import("state_store.zig");

/// Helper: create a LuaEngine backed by an in-memory SQLite db.
fn makeEngine(allocator: std.mem.Allocator) !struct {
    engine: LuaEngine,
    db: state_store.StateStore,
    ctx: lua_api.ApiCtx,
} {
    var db = try state_store.StateStore.open(allocator, ":memory:");
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = allocator };
    const engine = try LuaEngine.init(allocator, &ctx);
    return .{ .engine = engine, .db = db, .ctx = ctx };
}

// Needed because db/ctx are in the returned struct by value;
// we'll manage them manually in each test.

test "AC-6.1: io.open / os.execute / require raise errors; math/string/table/utf8 work" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    // Dangerous libs must NOT be available.
    try testing.expectError(error.LuaError, engine.loadString("io.open('x','r')"));
    try testing.expectError(error.LuaError, engine.loadString("os.execute('echo hi')"));
    try testing.expectError(error.LuaError, engine.loadString("require('io')"));

    // Safe libs must work.
    try engine.loadString("return math.floor(1.5)");
    try engine.loadString("return string.upper('hello')");
    try engine.loadString("local t = {}; table.insert(t,1); return t[1]");
    try engine.loadString("return utf8.len('hello')");
}

test "AC-6.2: on_message returning one send_message → one correct Action" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(update)
        \\  return { {action="send_message", chat_id=7, text="hi"} }
        \\end
    );

    const update = types.Update{ .update_id = 1 };
    const actions = try engine.callOnMessage(testing.allocator, update);
    defer types.freeActions(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 1), actions.len);
    try testing.expectEqual(types.ActionTag.send_message, std.meta.activeTag(actions[0]));
    try testing.expectEqual(@as(i64, 7), actions[0].send_message.chat_id);
    try testing.expectEqualStrings("hi", actions[0].send_message.text);
}

test "AC-6.3: on_message returning {} → empty slice" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(update) return {} end
    );

    const update = types.Update{ .update_id = 2 };
    const actions = try engine.callOnMessage(testing.allocator, update);
    defer types.freeActions(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 0), actions.len);
}

test "AC-6.4: on_message returning 3 mixed actions → all 3 parsed" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  return {
        \\    {action="send_message",    chat_id=1, text="msg1"},
        \\    {action="delete_message",  chat_id=2, message_id=99},
        \\    {action="answer_callback", callback_query_id="cq1", text="done"},
        \\  }
        \\end
    );

    const update = types.Update{ .update_id = 3 };
    const actions = try engine.callOnMessage(testing.allocator, update);
    defer types.freeActions(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 3), actions.len);
    try testing.expectEqual(types.ActionTag.send_message,    std.meta.activeTag(actions[0]));
    try testing.expectEqual(types.ActionTag.delete_message,  std.meta.activeTag(actions[1]));
    try testing.expectEqual(types.ActionTag.answer_callback, std.meta.activeTag(actions[2]));
    try testing.expectEqual(@as(i64, 99),     actions[1].delete_message.message_id);
    try testing.expectEqualStrings("cq1",     actions[2].answer_callback.callback_query_id);
    try testing.expectEqualStrings("done",    actions[2].answer_callback.text.?);
}

test "AC-6.5: on_message error → empty slice logged, next call succeeds" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  error("boom")
        \\end
    );

    const update = types.Update{ .update_id = 4 };
    const actions = try engine.callOnMessage(testing.allocator, update);
    defer types.freeActions(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 0), actions.len);

    // Replace the function with a good one; next call must succeed.
    try engine.loadString(
        \\function on_message(u)
        \\  return { {action="send_message", chat_id=5, text="ok"} }
        \\end
    );
    const actions2 = try engine.callOnMessage(testing.allocator, update);
    defer types.freeActions(actions2, testing.allocator);

    try testing.expectEqual(@as(usize, 1), actions2.len);
}

test "AC-6.6: invalid Lua syntax → loadString returns error; engine still usable" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try testing.expectError(error.LuaError, engine.loadString("function ("));

    // Engine must still be usable after the syntax error.
    try engine.loadString("function on_message(u) return {} end");
    const update = types.Update{ .update_id = 5 };
    const actions = try engine.callOnMessage(testing.allocator, update);
    defer types.freeActions(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 0), actions.len);
}
