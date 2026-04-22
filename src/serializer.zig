/// serializer.zig — bidirectional Lua table ↔ JSON conversion
///
/// Two public functions:
///   luaTableToJson  — serialize any Lua value at a stack index → owned JSON []u8
///   jsonToLuaTable  — parse a JSON string → push result onto Lua stack
///
/// Limits (both directions):
///   MAX_DEPTH = 8  — nesting levels; 9th level returns error.MaxDepthExceeded
///   MAX_SIZE  = 64 KiB — serialized output; returns error.MaxSizeExceeded

const std = @import("std");
const ziglua = @import("ziglua");
const Lua = ziglua.Lua;

pub const MAX_DEPTH: u32 = 8;
pub const MAX_SIZE: usize = 64 * 1024;

/// Error set shared by all serializer functions.
pub const SerializeError = error{
    MaxDepthExceeded,
    MaxSizeExceeded,
    UnsupportedKeyType,
    /// Returned by ziglua when a type conversion fails (e.g. toInteger on non-integer).
    LuaError,
    OutOfMemory,
};

/// Error set for JSON → Lua direction.
pub const DeserializeError = error{
    MaxDepthExceeded,
    InvalidJson,
    OutOfMemory,
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Serialize the Lua value at `index` to a JSON string.
/// Caller owns the returned slice (free with the same allocator).
pub fn luaTableToJson(lua: *Lua, index: i32, allocator: std.mem.Allocator) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);
    const abs = lua.absIndex(index);
    try serializeValue(lua, abs, allocator, &buf, 0);
    return buf.toOwnedSlice(allocator);
}

/// Parse `json_str` and push one value onto the Lua stack.
/// On error the stack is restored to its state before the call.
pub fn jsonToLuaTable(lua: *Lua, json_str: []const u8, allocator: std.mem.Allocator) !void {
    const top_before = lua.getTop();
    errdefer lua.setTop(top_before);

    const parsed = std.json.parseFromSlice(
        std.json.Value,
        allocator,
        json_str,
        .{},
    ) catch return error.InvalidJson;
    defer parsed.deinit();

    try pushJsonValue(lua, parsed.value, 0);
}

// ---------------------------------------------------------------------------
// Lua → JSON (private)
// ---------------------------------------------------------------------------

fn serializeValue(
    lua: *Lua,
    index: i32,
    allocator: std.mem.Allocator,
    buf: *std.ArrayListUnmanaged(u8),
    depth: u32,
) SerializeError!void {
    switch (lua.typeOf(index)) {
        .nil, .none => try appendChecked(buf, allocator, "null"),

        .boolean => try appendChecked(buf, allocator, if (lua.toBoolean(index)) "true" else "false"),

        .number => {
            if (lua.isInteger(index)) {
                const n = try lua.toInteger(index);
                try appendFmtChecked(buf, allocator, "{d}", .{n});
            } else {
                const n = try lua.toNumber(index);
                if (!std.math.isFinite(n)) {
                    // NaN / ±Inf are not valid JSON — emit null
                    try appendChecked(buf, allocator, "null");
                } else {
                    try appendFloat(buf, allocator, n);
                }
            }
        },

        .string => {
            const s = try lua.toString(index);
            try appendChecked(buf, allocator, "\"");
            try serializeStringContent(buf, allocator, s);
            try appendChecked(buf, allocator, "\"");
        },

        .table => {
            if (depth >= MAX_DEPTH) return error.MaxDepthExceeded;
            const abs = lua.absIndex(index);
            try serializeTable(lua, abs, allocator, buf, depth);
        },

        // functions, userdata, threads → null (safe default)
        else => try appendChecked(buf, allocator, "null"),
    }
}

fn serializeTable(
    lua: *Lua,
    abs_index: i32,
    allocator: std.mem.Allocator,
    buf: *std.ArrayListUnmanaged(u8),
    depth: u32,
) SerializeError!void {
    if (tableIsArray(lua, abs_index)) {
        try appendChecked(buf, allocator, "[");
        const n = lua.rawLen(abs_index);
        var i: ziglua.Integer = 1;
        while (i <= @as(ziglua.Integer, @intCast(n))) : (i += 1) {
            if (i > 1) try appendChecked(buf, allocator, ",");
            _ = lua.rawGetIndex(abs_index, i);
            try serializeValue(lua, -1, allocator, buf, depth + 1);
            lua.pop(1);
        }
        try appendChecked(buf, allocator, "]");
    } else {
        try appendChecked(buf, allocator, "{");
        var first = true;
        lua.pushNil();
        while (lua.next(abs_index)) {
            // key at -2, value at -1
            const key_type = lua.typeOf(-2);
            switch (key_type) {
                .string => {
                    if (!first) try appendChecked(buf, allocator, ",");
                    first = false;
                    const k = try lua.toString(-2);
                    try appendChecked(buf, allocator, "\"");
                    try serializeStringContent(buf, allocator, k);
                    try appendChecked(buf, allocator, "\":");
                    try serializeValue(lua, -1, allocator, buf, depth + 1);
                },
                .number => {
                    if (!first) try appendChecked(buf, allocator, ",");
                    first = false;
                    // Numeric key → quoted string representation in JSON
                    try appendChecked(buf, allocator, "\"");
                    if (lua.isInteger(-2)) {
                        const k = try lua.toInteger(-2);
                        try appendFmtChecked(buf, allocator, "{d}", .{k});
                    } else {
                        const k = try lua.toNumber(-2);
                        try appendFloat(buf, allocator, k);
                    }
                    try appendChecked(buf, allocator, "\":");
                    try serializeValue(lua, -1, allocator, buf, depth + 1);
                },
                else => {
                    // Boolean keys, table keys, etc. are not supported
                    lua.pop(2);
                    return error.UnsupportedKeyType;
                },
            }
            lua.pop(1); // pop value, keep key for next()
        }
        try appendChecked(buf, allocator, "}");
    }
}

/// Returns true iff the table at `abs_index` looks like a Lua sequence:
/// all keys are integers in [1, rawLen] with no gaps or extras.
fn tableIsArray(lua: *Lua, abs_index: i32) bool {
    const n = lua.rawLen(abs_index);
    if (n == 0) return false;

    var count: usize = 0;
    lua.pushNil();
    while (lua.next(abs_index)) {
        count += 1;
        if (!lua.isInteger(-2)) {
            lua.pop(2);
            return false;
        }
        const k = lua.toInteger(-2) catch {
            lua.pop(2);
            return false;
        };
        if (k < 1 or k > @as(ziglua.Integer, @intCast(n))) {
            lua.pop(2);
            return false;
        }
        lua.pop(1); // pop value, keep key
    }
    return count == n;
}

/// Append `s` to buf, checking MAX_SIZE before writing.
fn appendChecked(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    s: []const u8,
) SerializeError!void {
    if (buf.items.len + s.len > MAX_SIZE) return error.MaxSizeExceeded;
    try buf.appendSlice(allocator, s);
}

/// Format `args` with `fmt` into a stack buffer then appendChecked.
fn appendFmtChecked(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    comptime fmt: []const u8,
    args: anytype,
) SerializeError!void {
    var tmp: [128]u8 = undefined;
    // 128 bytes is always enough for any integer or float representation.
    const s = std.fmt.bufPrint(&tmp, fmt, args) catch unreachable;
    return appendChecked(buf, allocator, s);
}

/// Serialize a float ensuring the output always contains a decimal indicator
/// so it round-trips as a float (not re-parsed as integer).
fn appendFloat(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    n: f64,
) SerializeError!void {
    var tmp: [128]u8 = undefined;
    // 128 bytes is always enough for any finite f64 decimal representation.
    const s = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch unreachable;
    try appendChecked(buf, allocator, s);
    // Ensure at least one decimal indicator so JSON parsers don't
    // re-parse it as an integer on the way back.
    const has_point = std.mem.indexOfAny(u8, s, ".eEnN") != null;
    if (!has_point) try appendChecked(buf, allocator, ".0");
}

/// Write a JSON-escaped version of `s` into buf (without surrounding quotes).
fn serializeStringContent(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    s: [:0]const u8,
) SerializeError!void {
    for (s) |byte| {
        // Reserve up to 6 bytes for \uXXXX escapes
        if (buf.items.len + 6 > MAX_SIZE) return error.MaxSizeExceeded;
        switch (byte) {
            '"'  => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            0x08 => try buf.appendSlice(allocator, "\\b"),
            0x0C => try buf.appendSlice(allocator, "\\f"),
            0x00...0x07, 0x0B, 0x0E...0x1F => {
                var esc: [6]u8 = undefined;
                // \uXXXX is always exactly 6 bytes.
                const escaped = std.fmt.bufPrint(&esc, "\\u{X:0>4}", .{byte}) catch unreachable;
                try buf.appendSlice(allocator, escaped);
            },
            else => try buf.append(allocator, byte),
        }
    }
}

// ---------------------------------------------------------------------------
// JSON → Lua (private)
// ---------------------------------------------------------------------------

fn pushJsonValue(lua: *Lua, value: std.json.Value, depth: u32) DeserializeError!void {
    switch (value) {
        .null => lua.pushNil(),
        .bool => |b| lua.pushBoolean(b),
        .integer => |n| lua.pushInteger(n),
        .float => |n| lua.pushNumber(n),
        // number_string: huge numbers that don't fit i64/f64 — push as string
        .number_string => |s| _ = lua.pushString(s),
        .string => |s| _ = lua.pushString(s),

        .array => |arr| {
            if (depth >= MAX_DEPTH) return error.MaxDepthExceeded;
            lua.createTable(@intCast(arr.items.len), 0);
            for (arr.items, 1..) |item, i| {
                try pushJsonValue(lua, item, depth + 1);
                lua.rawSetIndex(-2, @intCast(i));
            }
        },

        .object => |obj| {
            if (depth >= MAX_DEPTH) return error.MaxDepthExceeded;
            lua.createTable(0, @intCast(obj.count()));
            var it = obj.iterator();
            while (it.next()) |entry| {
                _ = lua.pushString(entry.key_ptr.*);
                try pushJsonValue(lua, entry.value_ptr.*, depth + 1);
                lua.rawSetTable(-3);
            }
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Helper: init a fresh Lua state for tests (no stdlib needed for serializer).
fn newLua(allocator: std.mem.Allocator) !*Lua {
    return try Lua.init(allocator);
}

test "AC-2.1: nil round-trips as JSON null" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    lua.pushNil();
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try testing.expectEqualStrings("null", json);

    // null → Lua nil
    try jsonToLuaTable(lua, "null", testing.allocator);
    try testing.expect(lua.isNil(-1));
    lua.pop(1);
    try testing.expectEqual(@as(i32, 0), lua.getTop());
}

test "AC-2.1: boolean true round-trips" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    lua.pushBoolean(true);
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try testing.expectEqualStrings("true", json);

    try jsonToLuaTable(lua, json, testing.allocator);
    try testing.expect(lua.toBoolean(-1) == true);
    lua.pop(1);
    try testing.expectEqual(@as(i32, 0), lua.getTop());
}

test "AC-2.1: boolean false round-trips" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    lua.pushBoolean(false);
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try testing.expectEqualStrings("false", json);

    try jsonToLuaTable(lua, json, testing.allocator);
    try testing.expect(lua.toBoolean(-1) == false);
    lua.pop(1);
}

test "AC-2.1: integer round-trips" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    lua.pushInteger(42);
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try testing.expectEqualStrings("42", json);

    try jsonToLuaTable(lua, json, testing.allocator);
    try testing.expect(lua.isInteger(-1));
    try testing.expectEqual(@as(ziglua.Integer, 42), try lua.toInteger(-1));
    lua.pop(1);
}

test "AC-2.1: float round-trips" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    lua.pushNumber(3.14);
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    // Must have a decimal indicator so it comes back as float
    const has_point = std.mem.indexOfAny(u8, json, ".eE") != null;
    try testing.expect(has_point);

    try jsonToLuaTable(lua, json, testing.allocator);
    try testing.expect(!lua.isInteger(-1)); // must be float
    const v = try lua.toNumber(-1);
    try testing.expectApproxEqRel(3.14, v, 1e-9);
    lua.pop(1);
}

test "AC-2.1: string round-trips" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    _ = lua.pushString("hello, world");
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try testing.expectEqualStrings("\"hello, world\"", json);

    try jsonToLuaTable(lua, json, testing.allocator);
    const s = try lua.toString(-1);
    try testing.expectEqualStrings("hello, world", s);
    lua.pop(1);
}

test "AC-2.1: string with special chars escapes correctly" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    _ = lua.pushString("line1\nline2\ttab\"quote\\back");
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try testing.expectEqualStrings(
        "\"line1\\nline2\\ttab\\\"quote\\\\back\"",
        json,
    );
}

test "AC-2.2: array table (1..N integer keys) serializes to JSON array" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // {10, 20, 30}
    lua.createTable(3, 0);
    lua.pushInteger(10);
    lua.rawSetIndex(-2, 1);
    lua.pushInteger(20);
    lua.rawSetIndex(-2, 2);
    lua.pushInteger(30);
    lua.rawSetIndex(-2, 3);

    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try testing.expectEqualStrings("[10,20,30]", json);
}

test "AC-2.2: JSON array round-trips back to Lua sequence" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    try jsonToLuaTable(lua, "[1,2,3]", testing.allocator);
    try testing.expect(lua.isTable(-1));

    _ = lua.rawGetIndex(-1, 1);
    try testing.expectEqual(@as(ziglua.Integer, 1), try lua.toInteger(-1));
    lua.pop(1);

    _ = lua.rawGetIndex(-1, 2);
    try testing.expectEqual(@as(ziglua.Integer, 2), try lua.toInteger(-1));
    lua.pop(1);

    _ = lua.rawGetIndex(-1, 3);
    try testing.expectEqual(@as(ziglua.Integer, 3), try lua.toInteger(-1));
    lua.pop(1);

    lua.pop(1);
}

test "AC-2.3: object table (string keys) serializes to JSON object" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // { name = "bob" }
    lua.createTable(0, 1);
    _ = lua.pushString("name");
    _ = lua.pushString("bob");
    lua.rawSetTable(-3);

    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try testing.expectEqualStrings("{\"name\":\"bob\"}", json);
}

test "AC-2.3: non-consecutive integer keys → object, not array" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // { [1] = "a", [3] = "c" }  — gap at 2, not a sequence
    lua.createTable(0, 2);
    lua.pushInteger(1);
    _ = lua.pushString("a");
    lua.rawSetTable(-3);
    lua.pushInteger(3);
    _ = lua.pushString("c");
    lua.rawSetTable(-3);

    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    // Must be an object, not "[...]"
    try testing.expect(json[0] == '{');
}

test "AC-2.4: nesting depth 8 succeeds" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // Build 8 levels: {{{{{{{{}}}}}}}}}  (each wrapping the previous)
    lua.createTable(0, 0); // level 8 (innermost, empty)
    var level: u32 = 7;
    while (level > 0) : (level -= 1) {
        const inner = lua.getTop();
        lua.createTable(0, 1);
        _ = lua.pushString("inner");
        lua.pushValue(inner); // push the inner table again
        lua.rawSetTable(-3);  // outer["inner"] = inner_table
        // Remove the original inner from stack
        lua.remove(inner);
    }

    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    // JSON must start with { and contain nested objects
    try testing.expect(json[0] == '{');
}

test "AC-2.4: nesting depth 9 returns error.MaxDepthExceeded" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // Build 9 levels of nesting
    lua.createTable(0, 0);
    var level: u32 = 8;
    while (level > 0) : (level -= 1) {
        const inner = lua.getTop();
        lua.createTable(0, 1);
        _ = lua.pushString("inner");
        lua.pushValue(inner);
        lua.rawSetTable(-3);
        lua.remove(inner);
    }

    const top_before = lua.getTop();
    const result = luaTableToJson(lua, -1, testing.allocator);
    try testing.expectError(error.MaxDepthExceeded, result);
    lua.pop(1);
    _ = top_before;
}

test "AC-2.4: JSON array 8 levels deep succeeds" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();
    // [[[[[[[[]]]]]]]]  — 8 opening brackets
    try jsonToLuaTable(lua, "[[[[[[[[]]]]]]]]", testing.allocator);
    try testing.expect(lua.isTable(-1));
    lua.pop(1);
}

test "AC-2.4: JSON array 9 levels deep returns error.MaxDepthExceeded" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();
    // [[[[[[[[[]]]]]]]]]  — 9 opening brackets
    const top_before = lua.getTop();
    const result = jsonToLuaTable(lua, "[[[[[[[[[]]]]]]]]]", testing.allocator);
    try testing.expectError(error.MaxDepthExceeded, result);
    try testing.expectEqual(top_before, lua.getTop());
}

test "AC-2.5: serializing table exceeding 64 KiB returns error.MaxSizeExceeded" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // Build an array of long strings totalling > 64 KiB
    lua.createTable(10, 0);
    var buf: [8000]u8 = undefined;
    @memset(&buf, 'x');
    var i: ziglua.Integer = 1;
    while (i <= 10) : (i += 1) {
        _ = lua.pushString(&buf); // 8000 bytes each → 80 KiB total
        lua.rawSetIndex(-2, i);
    }

    const result = luaTableToJson(lua, -1, testing.allocator);
    try testing.expectError(error.MaxSizeExceeded, result);
    lua.pop(1);
}

test "AC-2.6: empty table serializes to {}" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    lua.createTable(0, 0);
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try testing.expectEqualStrings("{}", json);
}

test "AC-2.7: JSON null deserializes to Lua nil" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    try jsonToLuaTable(lua, "null", testing.allocator);
    try testing.expect(lua.isNil(-1));
    lua.pop(1);
    try testing.expectEqual(@as(i32, 0), lua.getTop());
}

test "AC-2.8: large integer (2^53) round-trips without loss" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    const big: ziglua.Integer = 9007199254740992; // 2^53
    lua.pushInteger(big);
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try jsonToLuaTable(lua, json, testing.allocator);
    try testing.expect(lua.isInteger(-1));
    try testing.expectEqual(big, try lua.toInteger(-1));
    lua.pop(1);
}

test "AC-2.8: max Lua integer (2^63-1) round-trips without loss" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    const max_int: ziglua.Integer = std.math.maxInt(i64);
    lua.pushInteger(max_int);
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try jsonToLuaTable(lua, json, testing.allocator);
    try testing.expect(lua.isInteger(-1));
    try testing.expectEqual(max_int, try lua.toInteger(-1));
    lua.pop(1);
}

test "AC-2.8: negative integer round-trips" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    const n: ziglua.Integer = std.math.minInt(i64);
    lua.pushInteger(n);
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    try jsonToLuaTable(lua, json, testing.allocator);
    try testing.expectEqual(n, try lua.toInteger(-1));
    lua.pop(1);
}

test "AC-2.9: invalid JSON returns error.InvalidJson without stack mutation" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    const top_before = lua.getTop();
    const result = jsonToLuaTable(lua, "{invalid}", testing.allocator);
    try testing.expectError(error.InvalidJson, result);
    try testing.expectEqual(top_before, lua.getTop());
}

test "AC-2.9: truncated JSON returns error.InvalidJson" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    const top_before = lua.getTop();
    const result = jsonToLuaTable(lua, "{\"key\":", testing.allocator);
    try testing.expectError(error.InvalidJson, result);
    try testing.expectEqual(top_before, lua.getTop());
}

test "AC-2.10: stack hygiene — luaTableToJson leaves stack unchanged" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // Push some sentinel values first
    lua.pushInteger(999);
    lua.pushBoolean(true);

    // Push a table to serialize
    lua.createTable(0, 1);
    _ = lua.pushString("k");
    lua.pushInteger(1);
    lua.rawSetTable(-3);

    const top_before = lua.getTop();
    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);

    try testing.expectEqual(top_before, lua.getTop());

    // Sentinels still intact
    try testing.expect(lua.toBoolean(-2) == true);
    try testing.expectEqual(@as(ziglua.Integer, 999), try lua.toInteger(-3));
    lua.pop(3);
}

test "AC-2.10: stack hygiene — jsonToLuaTable pushes exactly one value on success" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    lua.pushInteger(42); // sentinel
    const top_before = lua.getTop();

    try jsonToLuaTable(lua, "{\"a\":1}", testing.allocator);

    try testing.expectEqual(top_before + 1, lua.getTop());
    try testing.expect(lua.isTable(-1));
    lua.pop(1);

    try testing.expectEqual(@as(ziglua.Integer, 42), try lua.toInteger(-1));
    lua.pop(1);
}

test "AC-2.10: stack hygiene — jsonToLuaTable restores stack on error" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    lua.pushInteger(77); // sentinel
    const top_before = lua.getTop();

    const result = jsonToLuaTable(lua, "!!!bad json!!!", testing.allocator);
    try testing.expectError(error.InvalidJson, result);
    try testing.expectEqual(top_before, lua.getTop());
    try testing.expectEqual(@as(ziglua.Integer, 77), try lua.toInteger(-1));
    lua.pop(1);
}

test "AC-2.11: fuzz — mixed integer and string keys in object" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // { [1] = "a", name = "b" }  — mixed keys, not a sequence
    lua.createTable(0, 2);
    lua.pushInteger(1);
    _ = lua.pushString("a");
    lua.rawSetTable(-3);
    _ = lua.pushString("name");
    _ = lua.pushString("b");
    lua.rawSetTable(-3);

    const json = try luaTableToJson(lua, -1, testing.allocator);
    defer testing.allocator.free(json);
    lua.pop(1);

    // Must produce a JSON object
    try testing.expect(json[0] == '{');
}

test "AC-2.11: fuzz — boolean key returns error.UnsupportedKeyType" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // { [true] = 1 }
    lua.createTable(0, 1);
    lua.pushBoolean(true);
    lua.pushInteger(1);
    lua.rawSetTable(-3);

    const result = luaTableToJson(lua, -1, testing.allocator);
    try testing.expectError(error.UnsupportedKeyType, result);
    lua.pop(1);
}

test "AC-2.11: fuzz — JSON nested arrays at depth 9 return MaxDepthExceeded" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    const top_before = lua.getTop();
    const result = jsonToLuaTable(lua, "[[[[[[[[[]]]]]]]]]", testing.allocator);
    try testing.expectError(error.MaxDepthExceeded, result);
    try testing.expectEqual(top_before, lua.getTop());
}

test "AC-2.11: fuzz — deeply nested object at depth 9 returns MaxDepthExceeded" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // 9 levels: {"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{}}}}}}}}}}}
    const deep = "{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{}}}}}}}}}}";
    const top_before = lua.getTop();
    const result = jsonToLuaTable(lua, deep, testing.allocator);
    try testing.expectError(error.MaxDepthExceeded, result);
    try testing.expectEqual(top_before, lua.getTop());
}
