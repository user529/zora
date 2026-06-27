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
    MaxSizeExceeded,
    InvalidJson,
    OutOfMemory,
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Serialize the Lua value at `index` to a JSON string (64 KiB cap).
/// Caller owns the returned slice (free with the same allocator).
pub fn luaTableToJson(lua: *Lua, index: i32, allocator: std.mem.Allocator) ![]u8 {
    return luaTableToJsonCapped(lua, index, allocator, MAX_SIZE);
}

/// Like luaTableToJson but with a caller-supplied size cap.
pub fn luaTableToJsonCapped(lua: *Lua, index: i32, allocator: std.mem.Allocator, max_size: usize) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);
    const abs = lua.absIndex(index);
    try serializeValue(lua, abs, allocator, &buf, 0, max_size);
    return buf.toOwnedSlice(allocator);
}

/// Parse `json_str` and push one value onto the Lua stack.
/// On error it restores the stack to its state before the call.
/// No input-size limit — required for webhook body decoding up to 1 MB.
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

/// Like jsonToLuaTable but checks input length against max_size first.
/// Returns error.MaxSizeExceeded if json_str.len > max_size.
pub fn jsonToLuaTableCapped(lua: *Lua, json_str: []const u8, allocator: std.mem.Allocator, max_size: usize) !void {
    if (json_str.len > max_size) return error.MaxSizeExceeded;
    return jsonToLuaTable(lua, json_str, allocator);
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
    max_size: usize,
) SerializeError!void {
    switch (lua.typeOf(index)) {
        .nil, .none => try appendChecked(buf, allocator, "null", max_size),

        .boolean => try appendChecked(buf, allocator, if (lua.toBoolean(index)) "true" else "false", max_size),

        .number => {
            if (lua.isInteger(index)) {
                // isInteger is true here, so toInteger cannot fail.
                const n = lua.toInteger(index) catch unreachable;
                try appendFmtChecked(buf, allocator, "{d}", .{n}, max_size);
            } else {
                // The value is a number but not an integer, so toNumber cannot fail.
                const n = lua.toNumber(index) catch unreachable;
                if (!std.math.isFinite(n)) {
                    // NaN / ±Inf are not valid JSON — emit null
                    try appendChecked(buf, allocator, "null", max_size);
                } else {
                    try appendFloat(buf, allocator, n, max_size);
                }
            }
        },

        .string => {
            // The switch guarantees a string value, so toString cannot fail.
            const s = lua.toString(index) catch unreachable;
            try appendChecked(buf, allocator, "\"", max_size);
            try serializeStringContent(buf, allocator, s, max_size);
            try appendChecked(buf, allocator, "\"", max_size);
        },

        .table => {
            if (depth >= MAX_DEPTH) return error.MaxDepthExceeded;
            const abs = lua.absIndex(index);
            try serializeTable(lua, abs, allocator, buf, depth, max_size);
        },

        // functions, userdata, threads → null (safe default)
        else => try appendChecked(buf, allocator, "null", max_size),
    }
}

fn serializeTable(
    lua: *Lua,
    abs_index: i32,
    allocator: std.mem.Allocator,
    buf: *std.ArrayListUnmanaged(u8),
    depth: u32,
    max_size: usize,
) SerializeError!void {
    if (tableIsArray(lua, abs_index)) {
        try appendChecked(buf, allocator, "[", max_size);
        const n = lua.lenRaw(abs_index);
        var i: ziglua.Integer = 1;
        while (i <= @as(ziglua.Integer, @intCast(n))) : (i += 1) {
            if (i > 1) try appendChecked(buf, allocator, ",", max_size);
            _ = lua.getIndexRaw(abs_index, i);
            try serializeValue(lua, -1, allocator, buf, depth + 1, max_size);
            lua.pop(1);
        }
        try appendChecked(buf, allocator, "]", max_size);
    } else {
        try appendChecked(buf, allocator, "{", max_size);
        var first = true;
        lua.pushNil();
        while (lua.next(abs_index)) {
            // key at -2, value at -1
            const key_type = lua.typeOf(-2);
            switch (key_type) {
                .string => {
                    if (!first) try appendChecked(buf, allocator, ",", max_size);
                    first = false;
                    // key_type is .string, so toString cannot fail.
                    const k = lua.toString(-2) catch unreachable;
                    try appendChecked(buf, allocator, "\"", max_size);
                    try serializeStringContent(buf, allocator, k, max_size);
                    try appendChecked(buf, allocator, "\":", max_size);
                    try serializeValue(lua, -1, allocator, buf, depth + 1, max_size);
                },
                .number => {
                    if (!first) try appendChecked(buf, allocator, ",", max_size);
                    first = false;
                    // Numeric key → quoted string representation in JSON
                    try appendChecked(buf, allocator, "\"", max_size);
                    if (lua.isInteger(-2)) {
                        // Guarded by isInteger; toInteger cannot fail.
                        const k = lua.toInteger(-2) catch unreachable;
                        try appendFmtChecked(buf, allocator, "{d}", .{k}, max_size);
                    } else {
                        // A non-integer number key; toNumber cannot fail.
                        const k = lua.toNumber(-2) catch unreachable;
                        try appendFloat(buf, allocator, k, max_size);
                    }
                    try appendChecked(buf, allocator, "\":", max_size);
                    try serializeValue(lua, -1, allocator, buf, depth + 1, max_size);
                },
                else => {
                    // Boolean keys, table keys, etc. are not supported
                    lua.pop(2);
                    return error.UnsupportedKeyType;
                },
            }
            lua.pop(1); // pop value, keep key for next()
        }
        try appendChecked(buf, allocator, "}", max_size);
    }
}

/// Returns true iff the table at `abs_index` looks like a Lua sequence:
/// all keys are integers in [1, rawLen] with no gaps or extras.
fn tableIsArray(lua: *Lua, abs_index: i32) bool {
    const n = lua.lenRaw(abs_index);
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

/// Append `s` to buf, checking `max_size` before writing.
fn appendChecked(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    s: []const u8,
    max_size: usize,
) SerializeError!void {
    if (buf.items.len + s.len > max_size) return error.MaxSizeExceeded;
    try buf.appendSlice(allocator, s);
}

/// Format `args` with `fmt` into a stack buffer then appendChecked.
fn appendFmtChecked(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    comptime fmt: []const u8,
    args: anytype,
    max_size: usize,
) SerializeError!void {
    var tmp: [128]u8 = undefined;
    // 128 bytes is always enough for any integer or float representation.
    const s = std.fmt.bufPrint(&tmp, fmt, args) catch unreachable;
    return appendChecked(buf, allocator, s, max_size);
}

/// Serialize a float ensuring the output always contains a decimal indicator
/// so it round-trips as a float (not re-parsed as integer).
fn appendFloat(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    n: f64,
    max_size: usize,
) SerializeError!void {
    var tmp: [128]u8 = undefined;
    // 128 bytes is always enough for any finite f64 decimal representation.
    const s = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch unreachable;
    try appendChecked(buf, allocator, s, max_size);
    // Ensure at least one decimal indicator so JSON parsers don't
    // re-parse it as an integer on the way back.
    const has_point = std.mem.indexOfAny(u8, s, ".eEnN") != null;
    if (!has_point) try appendChecked(buf, allocator, ".0", max_size);
}

/// Write a JSON-escaped version of `s` into buf (without surrounding quotes).
fn serializeStringContent(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    s: [:0]const u8,
    max_size: usize,
) SerializeError!void {
    for (s) |byte| {
        // Reserve up to 6 bytes for \uXXXX escapes
        if (buf.items.len + 6 > max_size) return error.MaxSizeExceeded;
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
                lua.setIndexRaw(-2, @intCast(i));
            }
        },

        .object => |obj| {
            if (depth >= MAX_DEPTH) return error.MaxDepthExceeded;
            lua.createTable(0, @intCast(obj.count()));
            var it = obj.iterator();
            while (it.next()) |entry| {
                _ = lua.pushString(entry.key_ptr.*);
                try pushJsonValue(lua, entry.value_ptr.*, depth + 1);
                lua.setTableRaw(-3);
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

/// Helper: push a table nested `levels` deep onto the stack. Each level holds
/// one string key "inner" pointing at the next, so the value is an object.
fn pushNested(lua: *Lua, levels: u32) void {
    lua.createTable(0, 0); // innermost
    var n = levels;
    while (n > 1) : (n -= 1) {
        const inner = lua.getTop();
        lua.createTable(0, 1);
        _ = lua.pushString("inner");
        lua.pushValue(inner);
        lua.setTableRaw(-3);
        lua.remove(inner);
    }
}

test "scalar values round-trip through JSON" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // nil → "null" → nil; the stack returns to empty afterwards.
    {
        lua.pushNil();
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expectEqualStrings("null", json);

        try jsonToLuaTable(lua, "null", testing.allocator);
        try testing.expect(lua.isNil(-1));
        lua.pop(1);
        try testing.expectEqual(@as(i32, 0), lua.getTop());
    }
    // boolean true.
    {
        lua.pushBoolean(true);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expectEqualStrings("true", json);
        try jsonToLuaTable(lua, json, testing.allocator);
        try testing.expect(lua.toBoolean(-1) == true);
        lua.pop(1);
    }
    // boolean false.
    {
        lua.pushBoolean(false);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expectEqualStrings("false", json);
        try jsonToLuaTable(lua, json, testing.allocator);
        try testing.expect(lua.toBoolean(-1) == false);
        lua.pop(1);
    }
    // integer.
    {
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
    // float — must carry a decimal indicator so it returns as a float.
    {
        lua.pushNumber(3.14);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expect(std.mem.indexOfAny(u8, json, ".eE") != null);
        try jsonToLuaTable(lua, json, testing.allocator);
        try testing.expect(!lua.isInteger(-1));
        try testing.expectApproxEqRel(@as(f64, 3.14), try lua.toNumber(-1), 1e-9);
        lua.pop(1);
    }
    // string.
    {
        _ = lua.pushString("hello, world");
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expectEqualStrings("\"hello, world\"", json);
        try jsonToLuaTable(lua, json, testing.allocator);
        try testing.expectEqualStrings("hello, world", try lua.toString(-1));
        lua.pop(1);
    }
    // large integer 2^53 — no precision loss through JSON.
    {
        const big: ziglua.Integer = 9007199254740992;
        lua.pushInteger(big);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try jsonToLuaTable(lua, json, testing.allocator);
        try testing.expect(lua.isInteger(-1));
        try testing.expectEqual(big, try lua.toInteger(-1));
        lua.pop(1);
    }
    // max i64.
    {
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
    // min i64 (negative).
    {
        const n: ziglua.Integer = std.math.minInt(i64);
        lua.pushInteger(n);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try jsonToLuaTable(lua, json, testing.allocator);
        try testing.expectEqual(n, try lua.toInteger(-1));
        lua.pop(1);
    }
}

test "string with special chars escapes correctly" {
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

test "table shape maps to a JSON array or object" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // Sequence {10,20,30} → JSON array.
    {
        lua.createTable(3, 0);
        lua.pushInteger(10); lua.setIndexRaw(-2, 1);
        lua.pushInteger(20); lua.setIndexRaw(-2, 2);
        lua.pushInteger(30); lua.setIndexRaw(-2, 3);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expectEqualStrings("[10,20,30]", json);
    }
    // JSON array → Lua sequence with values at 1..3.
    {
        try jsonToLuaTable(lua, "[1,2,3]", testing.allocator);
        try testing.expect(lua.isTable(-1));
        var i: ziglua.Integer = 1;
        while (i <= 3) : (i += 1) {
            _ = lua.getIndexRaw(-1, i);
            try testing.expectEqual(i, try lua.toInteger(-1));
            lua.pop(1);
        }
        lua.pop(1);
    }
    // Object {name="bob"} → JSON object.
    {
        lua.createTable(0, 1);
        _ = lua.pushString("name");
        _ = lua.pushString("bob");
        lua.setTableRaw(-3);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expectEqualStrings("{\"name\":\"bob\"}", json);
    }
    // Non-consecutive integer keys {[1]="a",[3]="c"} → object, not array.
    // Numeric keys serialize as quoted strings; key order is undefined, so
    // parse the result back and assert each key maps to its value.
    {
        lua.createTable(0, 2);
        lua.pushInteger(1); _ = lua.pushString("a"); lua.setTableRaw(-3);
        lua.pushInteger(3); _ = lua.pushString("c"); lua.setTableRaw(-3);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expect(json[0] == '{');

        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, json, .{});
        defer parsed.deinit();
        const obj = parsed.value.object;
        try testing.expectEqual(@as(usize, 2), obj.count());
        try testing.expectEqualStrings("a", obj.get("1").?.string);
        try testing.expectEqualStrings("c", obj.get("3").?.string);
    }
    // Mixed integer and string keys {[1]="a", name="b"} → object.
    // Assert both keys and their distinct values survive, not just the brace.
    {
        lua.createTable(0, 2);
        lua.pushInteger(1); _ = lua.pushString("a"); lua.setTableRaw(-3);
        _ = lua.pushString("name"); _ = lua.pushString("b"); lua.setTableRaw(-3);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expect(json[0] == '{');

        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, json, .{});
        defer parsed.deinit();
        const obj = parsed.value.object;
        try testing.expectEqual(@as(usize, 2), obj.count());
        try testing.expectEqualStrings("a", obj.get("1").?.string);
        try testing.expectEqualStrings("b", obj.get("name").?.string);
    }
    // Empty table → "{}".
    {
        lua.createTable(0, 0);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expectEqualStrings("{}", json);
    }
}

test "rejects nesting past the 8-level limit" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // A failed serialize leaves the partially-pushed values on the Lua stack,
    // so each case resets the stack with setTop(0) before the next to avoid a
    // stack overflow during the next deep traversal.

    // Lua → JSON: depth 8 serializes, depth 9 errors.
    {
        pushNested(lua, 8);
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        try testing.expect(json[0] == '{');
        lua.setTop(0);
    }
    {
        pushNested(lua, 9);
        try testing.expectError(error.MaxDepthExceeded, luaTableToJson(lua, -1, testing.allocator));
        lua.setTop(0);
    }
    // The capped entry point propagates the same depth error.
    {
        pushNested(lua, 9);
        try testing.expectError(
            error.MaxDepthExceeded,
            luaTableToJsonCapped(lua, -1, testing.allocator, 1024 * 1024),
        );
        lua.setTop(0);
    }
    // JSON → Lua: depth 8 parses, depth 9 errors with the stack restored.
    {
        try jsonToLuaTable(lua, "[[[[[[[[]]]]]]]]", testing.allocator); // 8 levels
        try testing.expect(lua.isTable(-1));
        lua.setTop(0);
    }
    {
        const top_before = lua.getTop();
        try testing.expectError(
            error.MaxDepthExceeded,
            jsonToLuaTable(lua, "[[[[[[[[[]]]]]]]]]", testing.allocator), // 9 levels
        );
        try testing.expectEqual(top_before, lua.getTop());
    }
    {
        // 9-level object nesting.
        const deep = "{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{\"a\":{}}}}}}}}}}";
        const top_before = lua.getTop();
        try testing.expectError(error.MaxDepthExceeded, jsonToLuaTable(lua, deep, testing.allocator));
        try testing.expectEqual(top_before, lua.getTop());
    }
}

test "enforces the size cap" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // Default 64 KiB cap: a small value passes.
    {
        _ = lua.pushString("ok");
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expectEqualStrings("\"ok\"", json);
    }
    // Default cap: an 80 KiB array is rejected.
    {
        lua.createTable(10, 0);
        var buf: [8000]u8 = undefined;
        @memset(&buf, 'x');
        var i: ziglua.Integer = 1;
        while (i <= 10) : (i += 1) {
            _ = lua.pushString(&buf); // 8000 bytes each → 80 KiB total
            lua.setIndexRaw(-2, i);
        }
        try testing.expectError(error.MaxSizeExceeded, luaTableToJson(lua, -1, testing.allocator));
        lua.pop(1);
    }
    // Caller-supplied cap: within passes, over fails.
    {
        _ = lua.pushString("hello");
        const json = try luaTableToJsonCapped(lua, -1, testing.allocator, 16);
        defer testing.allocator.free(json);
        lua.pop(1);
        try testing.expectEqualStrings("\"hello\"", json);
    }
    {
        _ = lua.pushString("hello world");
        try testing.expectError(
            error.MaxSizeExceeded,
            luaTableToJsonCapped(lua, -1, testing.allocator, 5),
        );
        lua.pop(1);
    }
    // jsonToLuaTableCapped checks input length; over the cap errors, stack intact.
    {
        const top_before = lua.getTop();
        try testing.expectError(
            error.MaxSizeExceeded,
            jsonToLuaTableCapped(lua, "{\"ok\":true}", testing.allocator, 5),
        );
        try testing.expectEqual(top_before, lua.getTop());
    }
    {
        try jsonToLuaTableCapped(lua, "42", testing.allocator, 100);
        try testing.expect(lua.isInteger(-1));
        try testing.expectEqual(@as(ziglua.Integer, 42), try lua.toInteger(-1));
        lua.pop(1);
    }
    // jsonToLuaTable itself imposes no input-size limit.
    {
        try jsonToLuaTable(lua, "{\"n\":1}", testing.allocator);
        try testing.expect(lua.isTable(-1));
        lua.pop(1);
    }
}

test "stack stays balanced across success and error" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // Invalid and truncated JSON return InvalidJson without touching the stack.
    {
        const top_before = lua.getTop();
        try testing.expectError(error.InvalidJson, jsonToLuaTable(lua, "{invalid}", testing.allocator));
        try testing.expectEqual(top_before, lua.getTop());
    }
    {
        const top_before = lua.getTop();
        try testing.expectError(error.InvalidJson, jsonToLuaTable(lua, "{\"key\":", testing.allocator));
        try testing.expectEqual(top_before, lua.getTop());
    }
    // luaTableToJson leaves the stack and the values below it untouched.
    {
        lua.pushInteger(999);
        lua.pushBoolean(true);
        lua.createTable(0, 1);
        _ = lua.pushString("k");
        lua.pushInteger(1);
        lua.setTableRaw(-3);

        const top_before = lua.getTop();
        const json = try luaTableToJson(lua, -1, testing.allocator);
        defer testing.allocator.free(json);
        try testing.expectEqual(top_before, lua.getTop());
        try testing.expect(lua.toBoolean(-2) == true);
        try testing.expectEqual(@as(ziglua.Integer, 999), try lua.toInteger(-3));
        lua.pop(3);
    }
    // jsonToLuaTable pushes exactly one value on success.
    {
        lua.pushInteger(42); // sentinel
        const top_before = lua.getTop();
        try jsonToLuaTable(lua, "{\"a\":1}", testing.allocator);
        try testing.expectEqual(top_before + 1, lua.getTop());
        try testing.expect(lua.isTable(-1));
        lua.pop(1);
        try testing.expectEqual(@as(ziglua.Integer, 42), try lua.toInteger(-1));
        lua.pop(1);
    }
    // jsonToLuaTable restores the stack on error.
    {
        lua.pushInteger(77); // sentinel
        const top_before = lua.getTop();
        try testing.expectError(error.InvalidJson, jsonToLuaTable(lua, "!!!bad json!!!", testing.allocator));
        try testing.expectEqual(top_before, lua.getTop());
        try testing.expectEqual(@as(ziglua.Integer, 77), try lua.toInteger(-1));
        lua.pop(1);
    }
}

test "boolean key returns error.UnsupportedKeyType" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // { [true] = 1 } — a boolean key cannot be represented in JSON.
    lua.createTable(0, 1);
    lua.pushBoolean(true);
    lua.pushInteger(1);
    lua.setTableRaw(-3);

    try testing.expectError(error.UnsupportedKeyType, luaTableToJson(lua, -1, testing.allocator));
    lua.pop(1);
}

test "JSON integer beyond i64 range pushes the literal as a string" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // 99999999999999999999 exceeds i64 and loses precision as f64, so std.json
    // reports it as .number_string. pushJsonValue must push the exact literal
    // as a Lua string rather than a truncated or overflowed number.
    const literal = "99999999999999999999";
    try jsonToLuaTable(lua, literal, testing.allocator);
    // typeOf, not isNumber: Lua 5.4's isNumber coerces a numeric string, so it
    // would report true even though the value is genuinely a string. The point
    // is that the value was stored as a string literal, not a parsed number.
    try testing.expectEqual(ziglua.LuaType.string, lua.typeOf(-1));
    try testing.expectEqualStrings(literal, try lua.toString(-1));
    lua.pop(1);
}

test "luaTableToJson keeps the stack balanced on an error" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // A boolean key forces error.UnsupportedKeyType partway through table
    // traversal; the stack must return to its prior depth despite the abort.
    lua.createTable(0, 1);
    lua.pushBoolean(true);
    lua.pushInteger(1);
    lua.setTableRaw(-3);

    const top_before = lua.getTop();
    try testing.expectError(error.UnsupportedKeyType, luaTableToJson(lua, -1, testing.allocator));
    try testing.expectEqual(top_before, lua.getTop());
    lua.pop(1);
}

test "JSON string escapes decode to their literal bytes" {
    var lua = try newLua(testing.allocator);
    defer lua.deinit();

    // The JSON source carries \n, \t, \" and \\ escapes; the Lua value must
    // hold the actual newline, tab, quote and backslash bytes.
    try jsonToLuaTable(lua, "\"a\\nb\\tc\\\"d\\\\e\"", testing.allocator);
    try testing.expect(lua.isString(-1));
    try testing.expectEqualStrings("a\nb\tc\"d\\e", try lua.toString(-1));
    lua.pop(1);
}
