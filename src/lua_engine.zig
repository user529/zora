/// lua_engine.zig — load/reload rules.lua, call on_message()
///
/// One LuaEngine per worker thread; never shared between threads.
///
/// Lifecycle:
///   init(allocator, ctx)    → allocates Lua state, opens safe stdlib, registers bot.*
///   loadFile(path)          → doFile (replaces current chunk)
///   loadString(src)         → doString (for tests / hot-reload from memory)
///   callOnMessage(body)     → decodes JSON body, calls on_message(table) → []ApiCall
///   types.freeApiCalls(actions, allocator) → free slice + owned strings
///   deinit()                → closes Lua state
///
/// on_message returns a list of generic API-call tables — each shaped
/// `{ method = "...", params = {...} }` (ADR-0001 §AD-1).  Rules may also push
/// fire-and-forget calls mid-handler with `bot.emit{...}`; those are collected
/// ahead of the return-list.

const std = @import("std");
const ziglua = @import("ziglua");
const types = @import("types.zig");
const serializer = @import("serializer.zig");
const lua_api = @import("lua_api.zig");
const schema_store = @import("schema_store.zig");

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
    /// Hot-reloadable schema for outgoing-call validation. null → no schema.
    schema_slot: ?*schema_store.SchemaSlot = null,
    /// Validation policy. `.off` (the default) disables validation.
    validation: types.ValidationMode = .off,

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

    /// Enable schema validation for subsequent callOnMessage invocations.
    /// `slot` may be null; mode `.off` disables validation regardless.
    pub fn setValidation(
        self: *LuaEngine,
        slot: ?*schema_store.SchemaSlot,
        mode: types.ValidationMode,
    ) void {
        self.schema_slot = slot;
        self.validation = mode;
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
    /// - Decodes the raw webhook JSON `body` directly into a Lua table
    ///   (ADR-0001 §AD-2 — no typed Update tree, no re-serialization).
    /// - Calls on_message with a protected call (no Lua error escapes).
    /// - On Lua error: logs the error, returns an empty slice (no Zig error).
    /// - On a malformed `body`: logs, returns an empty slice (no Zig error).
    /// - On a Zig-level allocation error: propagates the error.
    /// - The returned slice and all string payloads are allocated from
    ///   `allocator`; free them with `types.freeApiCalls`.
    ///
    /// OWNERSHIP INVARIANT: the caller must always call `types.freeApiCalls`
    /// on the returned slice, even when `len == 0`.  Every code path —
    /// including Lua error and malformed body — returns a heap-allocated slice.
    pub fn callOnMessage(
        self: *LuaEngine,
        allocator: std.mem.Allocator,
        body: []const u8,
    ) ![]types.ApiCall {
        const lua = self.lua;
        const stack_base = lua.getTop();

        // ── 1. Push on_message function ───────────────────────────────────
        const fn_type = lua.getGlobal("on_message") catch {
            log.warn("on_message lookup failed", .{});
            return try allocator.alloc(types.ApiCall, 0);
        };
        if (fn_type != .function) {
            lua.setTop(stack_base);
            log.warn("on_message is not a function (type={s})", .{lua.typeName(fn_type)});
            return try allocator.alloc(types.ApiCall, 0);
        }

        // ── 2. Decode raw webhook JSON → Lua table ────────────────────────
        serializer.jsonToLuaTable(lua, body, allocator) catch |err| {
            lua.setTop(stack_base);
            log.err("failed to decode update body: {s}", .{@errorName(err)});
            return try allocator.alloc(types.ApiCall, 0);
        };

        // ── 3. Protected call: on_message(update_table) ───────────────────
        // Fresh emit accumulator so bot.emit{...} lands in this invocation.
        lua_api.beginEmitBatch(lua);
        lua.protectedCall(.{ .args = 1, .results = 1, .msg_handler = 0 }) catch {
            const err_msg = lua.toString(-1) catch "(no message)";
            log.warn("on_message error: {s}", .{err_msg});
            lua.setTop(stack_base);
            return try allocator.alloc(types.ApiCall, 0);
        };

        // Stack: [..., result_table]   (base + 1 element)
        defer lua.setTop(stack_base);
        const result_idx = lua.getTop();

        // ── 4. Collect ApiCalls: bot.emit accumulator, then return-list ──
        var list: std.ArrayListUnmanaged(types.ApiCall) = .empty;
        errdefer {
            for (list.items) |c| types.freeApiCall(c, allocator);
            list.deinit(allocator);
        }

        // Live schema for this update (null when none is loaded → Tier-0).
        const store: ?*const schema_store.SchemaStore =
            if (self.schema_slot) |slot| slot.get() else null;

        // Fire-and-forget bot.emit calls first, in call order.
        lua_api.pushEmitBatch(lua); // [..., result_table, emit_batch]
        try appendApiCalls(lua, lua.getTop(), allocator, &list, store, self.validation);
        lua.pop(1); // [..., result_table]

        // Then the on_message return-list.
        try appendApiCalls(lua, result_idx, allocator, &list, store, self.validation);

        return try list.toOwnedSlice(allocator);
    }
};

// ---------------------------------------------------------------------------
// ApiCall parsing — { method, params } Lua tables → types.ApiCall
// ---------------------------------------------------------------------------

/// Parse one `{ method = "...", params = {...} }` Lua table (at absolute
/// `table_idx`) into an ApiCall.  `method` is required; `params` is optional.
/// When `mode != .off` and `schema` is non-null, params are schema-validated.
/// Payload building (JSON vs multipart) is delegated to lua_api.buildApiCall.
fn parseOneApiCall(
    lua:       *Lua,
    table_idx: i32,
    allocator: std.mem.Allocator,
    schema:    ?*const schema_store.SchemaStore,
    mode:      types.ValidationMode,
) !types.ApiCall {
    _ = allocator; // buildApiCall uses ctx.allocator internally

    // ── method (required string) ──────────────────────────────────────────
    _ = lua.getField(table_idx, "method");
    const method_z = lua.toString(-1) catch {
        lua.pop(1);
        return error.InvalidApiCall;
    };
    // Borrow the string temporarily; buildApiCall dupes it.
    const method_tmp = method_z;
    lua.pop(1);

    // ── params value on the stack ─────────────────────────────────────────
    _ = lua.getField(table_idx, "params");
    const params_idx = lua.getTop();
    defer lua.pop(1); // always pop params

    // ── schema validation (ADR-0001 §AD-5) ────────────────────────────────
    if (mode != .off) {
        if (schema) |store| {
            schema_store.validate(store, lua, params_idx, method_tmp) catch |verr| {
                if (mode == .strict) {
                    return error.Validation;
                }
                log.warn(
                    "api call '{s}' failed validation: {s} — sending anyway",
                    .{ method_tmp, @errorName(verr) },
                );
            };
        }
    }

    // ── params present but not a table and not nil → reject ──────────────
    if (!lua.isTable(params_idx) and !lua.isNil(params_idx)) {
        return error.InvalidApiCall;
    }

    // ── delegate to buildApiCall (JSON or multipart) ──────────────────────
    const ctx = lua_api.getCtx(lua);
    return lua_api.buildApiCall(lua, params_idx, method_tmp, ctx);
}

/// Iterate a Lua array-table of `{ method, params }` entries (at `table_idx`)
/// and append each parsed ApiCall to `list`.  Malformed entries — and, in
/// strict mode, entries that fail schema validation — are logged and skipped.
fn appendApiCalls(
    lua: *Lua,
    table_idx: i32,
    allocator: std.mem.Allocator,
    list: *std.ArrayListUnmanaged(types.ApiCall),
    schema: ?*const schema_store.SchemaStore,
    mode: types.ValidationMode,
) !void {
    if (!lua.isTable(table_idx)) return;

    const n = lua.rawLen(table_idx);
    var i: ziglua.Integer = 1;
    while (i <= @as(ziglua.Integer, @intCast(n))) : (i += 1) {
        const pre = lua.getTop();
        _ = lua.rawGetIndex(table_idx, i);
        if (lua.isTable(-1)) {
            const abs = lua.getTop();
            if (parseOneApiCall(lua, abs, allocator, schema, mode)) |call| {
                list.append(allocator, call) catch {
                    types.freeApiCall(call, allocator);
                    lua.setTop(pre);
                    return error.OutOfMemory;
                };
            } else |err| {
                log.warn("skipping api call {d}: {s}", .{ i, @errorName(err) });
            }
        }
        // Restore to exactly pre-rawGetIndex depth regardless of what
        // parseOneApiCall may have left on the stack after an error return.
        lua.setTop(pre);
    }
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
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = allocator, .max_file_bytes = 52428800 };
    const engine = try LuaEngine.init(allocator, &ctx);
    return .{ .engine = engine, .db = db, .ctx = ctx };
}

// Needed because db/ctx are in the returned struct by value;
// we'll manage them manually in each test.

test "AC-6.1: io.open / os.execute / require raise errors; math/string/table/utf8 work" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };
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

// AC-6.2 (typed send_message return parsing) retired in sub-step 13c —
// superseded by AC-13.11 (generic { method, params } return-list).

test "AC-6.3: on_message returning {} → empty slice" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(update) return {} end
    );

    const actions = try engine.callOnMessage(testing.allocator, "{\"update_id\":2}");
    defer types.freeApiCalls(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 0), actions.len);
}

// AC-6.4 (typed mixed-action return parsing) retired in sub-step 13c —
// superseded by AC-13.11 (generic six-method-shape return-list).

test "AC-6.5: on_message error → empty slice logged, next call succeeds" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  error("boom")
        \\end
    );

    const actions = try engine.callOnMessage(testing.allocator, "{\"update_id\":4}");
    defer types.freeApiCalls(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 0), actions.len);

    // Replace the function with a good one; next call must succeed.
    try engine.loadString(
        \\function on_message(u)
        \\  return { { method = "sendMessage", params = { chat_id = 5, text = "ok" } } }
        \\end
    );
    const actions2 = try engine.callOnMessage(testing.allocator, "{\"update_id\":4}");
    defer types.freeApiCalls(actions2, testing.allocator);

    try testing.expectEqual(@as(usize, 1), actions2.len);
}

test "AC-6.6: invalid Lua syntax → loadString returns error; engine still usable" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try testing.expectError(error.LuaError, engine.loadString("function ("));

    // Engine must still be usable after the syntax error.
    try engine.loadString("function on_message(u) return {} end");
    const actions = try engine.callOnMessage(testing.allocator, "{\"update_id\":5}");
    defer types.freeApiCalls(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 0), actions.len);
}

// ---------------------------------------------------------------------------
// Sub-step 13c — generic { method, params } engine path (ADR-0001 §AD-1/AD-2)
//
// `luaTableToJson` iterates Lua tables in unspecified key order, so body
// assertions parse the JSON or test by substring — never byte-compare.
// ---------------------------------------------------------------------------

/// Parse an ApiCall body and assert its `chat_id` and `text` fields.
fn expectMsgBody(body: []const u8, chat_id: i64, text: []const u8) !void {
    const Body = struct { chat_id: i64, text: []const u8 };
    const p = try std.json.parseFromSlice(Body, testing.allocator, body, .{
        .ignore_unknown_fields = true,
    });
    defer p.deinit();
    try testing.expectEqual(chat_id, p.value.chat_id);
    try testing.expectEqualStrings(text, p.value.text);
}

test "AC-13.10: on_message receives a decoded Lua table with nested access" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    // Reads a plain field, a sibling field, and a deeply nested array element.
    try engine.loadString(
        \\function on_message(u)
        \\  local t  = u.message.text
        \\  local d  = u.callback_query.data
        \\  local kb = u.callback_query.message.reply_markup.inline_keyboard[1][2].text
        \\  return { { method = "probe", params = { a = t, b = d, c = kb } } }
        \\end
    );

    const body =
        \\{"message":{"text":"MT"},"callback_query":{"data":"CD","message":{"reply_markup":{"inline_keyboard":[[{"text":"B1"},{"text":"B2"}]]}}}}
    ;
    const actions = try engine.callOnMessage(testing.allocator, body);
    defer types.freeApiCalls(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 1), actions.len);
    try testing.expectEqualStrings("probe", actions[0].method);

    const Fields = struct { a: []const u8, b: []const u8, c: []const u8 };
    const p = try std.json.parseFromSlice(Fields, testing.allocator, actions[0].payload.json, .{});
    defer p.deinit();
    try testing.expectEqualStrings("MT", p.value.a);
    try testing.expectEqualStrings("CD", p.value.b);
    try testing.expectEqualStrings("B2", p.value.c);
}

test "AC-13.11: return-list — six method shapes parse to ApiCalls" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  return {
        \\    { method = "sendMessage",            params = { chat_id = 1, text = "m" } },
        \\    { method = "editMessageText",        params = { chat_id = 1, message_id = 2, text = "e", parse_mode = "HTML" } },
        \\    { method = "editMessageReplyMarkup", params = { chat_id = 1, message_id = 2, reply_markup = { inline_keyboard = { { { text = "B", callback_data = "d" } } } } } },
        \\    { method = "answerCallbackQuery",    params = { callback_query_id = "cq" } },
        \\    { method = "deleteMessage",          params = { chat_id = 1, message_id = 2 } },
        \\    { method = "sendDice",               params = { chat_id = 1 } },
        \\  }
        \\end
    );

    const actions = try engine.callOnMessage(testing.allocator, "{}");
    defer types.freeApiCalls(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 6), actions.len);
    try testing.expectEqualStrings("sendMessage", actions[0].method);
    try testing.expectEqualStrings("editMessageText", actions[1].method);
    try testing.expectEqualStrings("editMessageReplyMarkup", actions[2].method);
    try testing.expectEqualStrings("answerCallbackQuery", actions[3].method);
    try testing.expectEqualStrings("deleteMessage", actions[4].method);
    try testing.expectEqualStrings("sendDice", actions[5].method);

    // Bodies carry the expected fields (key order from luaTableToJson is
    // unspecified — assert by substring).
    try testing.expect(std.mem.indexOf(u8, actions[1].payload.json, "\"parse_mode\":\"HTML\"") != null);
    try testing.expect(std.mem.indexOf(u8, actions[2].payload.json, "\"callback_data\":\"d\"") != null);
    try testing.expect(std.mem.indexOf(u8, actions[2].payload.json, "\"inline_keyboard\"") != null);
    try testing.expect(std.mem.indexOf(u8, actions[3].payload.json, "\"callback_query_id\":\"cq\"") != null);
    try testing.expect(std.mem.indexOf(u8, actions[5].payload.json, "\"chat_id\":1") != null);
}

test "AC-13.12: bot.emit calls precede the return-list, in call order" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  bot.emit{ method = "sendMessage", params = { chat_id = 1, text = "e1" } }
        \\  bot.emit{ method = "sendMessage", params = { chat_id = 2, text = "e2" } }
        \\  return { { method = "sendMessage", params = { chat_id = 3, text = "r1" } } }
        \\end
    );

    const actions = try engine.callOnMessage(testing.allocator, "{}");
    defer types.freeApiCalls(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 3), actions.len);
    try expectMsgBody(actions[0].payload.json, 1, "e1"); // emit #1
    try expectMsgBody(actions[1].payload.json, 2, "e2"); // emit #2
    try expectMsgBody(actions[2].payload.json, 3, "r1"); // return-list
}

// ── Phase 14 — schema validation ─────────────────────────────────────────────


/// Fixture: sendMessage requires chat_id (Integer|String) and text (String).
const SCHEMA_FIX =
    \\{"methods":{"sendMessage":{"fields":[
    \\  {"name":"chat_id","types":["Integer","String"],"required":true},
    \\  {"name":"text","types":["String"],"required":true}
    \\]}},"types":{}}
;

test "AC-14.3: off/warn keep an invalid call; strict drops it" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

    var slot = schema_store.SchemaSlot.init(testing.allocator);
    defer slot.deinit();
    slot.install(try schema_store.SchemaStore.fromSlice(testing.allocator, SCHEMA_FIX));

    const cases = .{
        .{ types.ValidationMode.off, @as(usize, 1) },
        .{ types.ValidationMode.warn, @as(usize, 1) },
        .{ types.ValidationMode.strict, @as(usize, 0) },
    };
    inline for (cases) |c| {
        var engine = try LuaEngine.init(testing.allocator, &ctx);
        defer engine.deinit();
        engine.setValidation(&slot, c[0]);
        try engine.loadString(
            \\function on_message(u)
            \\  return { { method = "sendMessage", params = { chat_id = 1 } } }
            \\end
        ); // invalid: required `text` missing
        const actions = try engine.callOnMessage(testing.allocator, "{}");
        defer types.freeApiCalls(actions, testing.allocator);
        try testing.expectEqual(c[1], actions.len);
    }
}

test "AC-14.9: strict validation covers both bot.emit and the return-list" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

    var slot = schema_store.SchemaSlot.init(testing.allocator);
    defer slot.deinit();
    slot.install(try schema_store.SchemaStore.fromSlice(testing.allocator, SCHEMA_FIX));

    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();
    engine.setValidation(&slot, .strict);
    try engine.loadString(
        \\function on_message(u)
        \\  bot.emit{ method = "sendMessage", params = { chat_id = 1 } }            -- invalid
        \\  bot.emit{ method = "sendMessage", params = { chat_id = 1, text = "k" } } -- valid
        \\  return { { method = "sendMessage", params = { chat_id = 2 } } }          -- invalid
        \\end
    );
    const actions = try engine.callOnMessage(testing.allocator, "{}");
    defer types.freeApiCalls(actions, testing.allocator);
    try testing.expectEqual(@as(usize, 1), actions.len);
    try expectMsgBody(actions[0].payload.json, 1, "k");
}

test "AC-14.6: the schema is shared by pointer across engines (single copy)" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

    var slot = schema_store.SchemaSlot.init(testing.allocator);
    defer slot.deinit();
    slot.install(try schema_store.SchemaStore.fromSlice(testing.allocator, SCHEMA_FIX));

    var e1 = try LuaEngine.init(testing.allocator, &ctx);
    defer e1.deinit();
    var e2 = try LuaEngine.init(testing.allocator, &ctx);
    defer e2.deinit();
    e1.setValidation(&slot, .warn);
    e2.setValidation(&slot, .warn);

    // Both engines reference the one SchemaSlot — and the one SchemaStore
    // inside it. The schema never enters a lua_State, so adding engines
    // (workers) never multiplies its memory.
    try testing.expectEqual(e1.schema_slot.?, e2.schema_slot.?);
    try testing.expectEqual(slot.get().?, e2.schema_slot.?.get().?);
}

test "AC-14.7: an empty schema slot validates nothing (Tier-0)" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };

    var slot = schema_store.SchemaSlot.init(testing.allocator); // never installed
    defer slot.deinit();

    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();
    engine.setValidation(&slot, .strict); // strict, but no schema → no-op
    try engine.loadString(
        \\function on_message(u)
        \\  return { { method = "sendMessage", params = { chat_id = 1 } } }
        \\end
    );
    const actions = try engine.callOnMessage(testing.allocator, "{}");
    defer types.freeApiCalls(actions, testing.allocator);
    try testing.expectEqual(@as(usize, 1), actions.len);
}

test "AC-13.13: shipped rules/rules.lua produces sendMessage calls (generic form)" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = lua_api.ApiCtx{ .db = &db, .allocator = testing.allocator, .max_file_bytes = 52428800 };
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    // Load the actual shipped rules file (cwd is the project root under
    // `zig build test`).
    const raw = try std.fs.cwd().readFileAlloc(testing.allocator, "rules/rules.lua", 64 * 1024);
    defer testing.allocator.free(raw);
    const src = try testing.allocator.dupeZ(u8, raw);
    defer testing.allocator.free(src);
    try engine.loadString(src);

    // /start → one sendMessage; fresh user/global state → message #1.
    {
        const actions = try engine.callOnMessage(testing.allocator,
            \\{"message":{"from":{"id":1},"chat":{"id":9},"text":"/start"}}
        );
        defer types.freeApiCalls(actions, testing.allocator);
        try testing.expectEqual(@as(usize, 1), actions.len);
        try testing.expectEqualStrings("sendMessage", actions[0].method);
        try expectMsgBody(actions[0].payload.json, 9, "Welcome! You are message #1");
    }
    // Plain text → echo.
    {
        const actions = try engine.callOnMessage(testing.allocator,
            \\{"message":{"from":{"id":1},"chat":{"id":9},"text":"hello"}}
        );
        defer types.freeApiCalls(actions, testing.allocator);
        try testing.expectEqual(@as(usize, 1), actions.len);
        try testing.expectEqualStrings("sendMessage", actions[0].method);
        try expectMsgBody(actions[0].payload.json, 9, "Echo: hello");
    }
}
