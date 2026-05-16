/// state_store.zig — SQLite-backed persistence for user, chat, and global state.
///
/// One StateStore per worker thread (owns its own SQLite connection).
/// WAL mode allows concurrent readers across connections on the same file.
/// Schema is applied idempotently on open(); version mismatch aborts startup.

const std = @import("std");

const c = @cImport({
    @cInclude("sqlite3.h");
});

pub const SCHEMA_VERSION: u32 = 1;

const PRAGMA_SQL: [:0]const u8 = @embedFile("pragma.sql");
const SCHEMA_SQL: [:0]const u8 = @embedFile("schema.sql");

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub const StateStore = struct {
    db: *c.sqlite3,
    allocator: std.mem.Allocator,

    /// Open (or create) the database at `path`.
    /// For in-memory databases use path = ":memory:".
    /// Open schema and verifies schema_version on every open,
    /// applies schema as fallback.
    pub fn open(allocator: std.mem.Allocator, path: [:0]const u8) !StateStore {
        var raw: ?*c.sqlite3 = null;
        const flags = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE;
        if (c.sqlite3_open_v2(path.ptr, &raw, flags, null) != c.SQLITE_OK) {
            if (raw) |db| _ = c.sqlite3_close(db);
            return error.OpenFailed;
        }
        var store = StateStore{ .db = raw.?, .allocator = allocator };
        store.applyPragma() catch |err| {
           _ = c.sqlite3_close(store.db);
           return err;
        };
        store.checkSchemaVersion() catch |err| {
            _ = c.sqlite3_close(store.db);
            return err;
        };
        return store;
    }

    pub fn close(self: *StateStore) void {
        _ = c.sqlite3_close(self.db);
    }

    // -----------------------------------------------------------------------
    // User state
    // -----------------------------------------------------------------------

    /// Returns the stored JSON blob for `user_id`, or "{}" if not yet set.
    /// Caller owns the returned slice.
    pub fn getUserState(self: *StateStore, user_id: i64) ![]u8 {
        return self.getStateById(
            "SELECT data FROM user_state WHERE user_id = ?",
            user_id,
        );
    }

    /// Upsert the JSON blob for `user_id`.
    pub fn setUserState(self: *StateStore, user_id: i64, data: []const u8) !void {
        return self.setStateById(
            "INSERT OR REPLACE INTO user_state (user_id, data) VALUES (?, ?)",
            user_id,
            data,
        );
    }

    // -----------------------------------------------------------------------
    // Chat state
    // -----------------------------------------------------------------------

    pub fn getChatState(self: *StateStore, chat_id: i64) ![]u8 {
        return self.getStateById(
            "SELECT data FROM chat_state WHERE chat_id = ?",
            chat_id,
        );
    }

    pub fn setChatState(self: *StateStore, chat_id: i64, data: []const u8) !void {
        return self.setStateById(
            "INSERT OR REPLACE INTO chat_state (chat_id, data) VALUES (?, ?)",
            chat_id,
            data,
        );
    }

    // -----------------------------------------------------------------------
    // Global key-value store
    // -----------------------------------------------------------------------

    /// Returns the value for `key`, or null if absent.
    /// Caller owns the returned slice (free with the same allocator).
    pub fn getGlobal(self: *StateStore, key: []const u8) !?[]u8 {
        const stmt = try self.prepare("SELECT value FROM global_state WHERE key = ?");
        defer _ = c.sqlite3_finalize(stmt);

        try sqliteOk(c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), null));

        return switch (c.sqlite3_step(stmt)) {
            c.SQLITE_ROW => blk: {
                const text = c.sqlite3_column_text(stmt, 0);
                const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
                break :blk if (text != null)
                    try self.allocator.dupe(u8, text[0..len])
                else
                    null;
            },
            c.SQLITE_DONE => null,
            else => error.SqliteError,
        };
    }

    /// Upsert a global key-value pair.
    pub fn setGlobal(self: *StateStore, key: []const u8, value: []const u8) !void {
        const stmt = try self.prepare(
            "INSERT OR REPLACE INTO global_state (key, value) VALUES (?, ?)",
        );
        defer _ = c.sqlite3_finalize(stmt);

        try sqliteOk(c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), null));
        try sqliteOk(c.sqlite3_bind_text(stmt, 2, value.ptr, @intCast(value.len), null));
        try sqliteDone(c.sqlite3_step(stmt));
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn applyPragma(self: *StateStore) !void {
        try sqliteOk(c.sqlite3_exec(self.db, PRAGMA_SQL.ptr, null, null, null));
    }

    fn applySchema(self: *StateStore) !void {
        try sqliteOk(c.sqlite3_exec(self.db, SCHEMA_SQL.ptr, null, null, null));
    }

    fn checkSchemaExistence(self: *StateStore) !bool {
        const schema_stmt = try self.prepare(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' and name = 'meta'",
        );
        defer _ = c.sqlite3_finalize(schema_stmt);

        if (c.sqlite3_step(schema_stmt) != c.SQLITE_ROW) return error.SchemaError;
        const cnt = c.sqlite3_column_int(schema_stmt, 0);
        return cnt == 1;
    }

    fn checkSchemaVersion(self: *StateStore) !void {
        if (!try self.checkSchemaExistence()) try self.applySchema();

        const version_stmt = try self.prepare(
            "SELECT value FROM meta WHERE key = 'schema_version'",
        );
        defer _ = c.sqlite3_finalize(version_stmt);

        if (c.sqlite3_step(version_stmt) != c.SQLITE_ROW) return error.SchemaError;

        const text = c.sqlite3_column_text(version_stmt, 0) orelse return error.SchemaError;
        const len: usize = @intCast(c.sqlite3_column_bytes(version_stmt, 0));
        const version_str = text[0..len];

        const version = std.fmt.parseInt(u32, version_str, 10) catch return error.SchemaError;
        if (version != SCHEMA_VERSION) return error.SchemaMismatch;
    }

    fn prepare(self: *StateStore, sql: [:0]const u8) !*c.sqlite3_stmt {
        var stmt: ?*c.sqlite3_stmt = null;
        try sqliteOk(c.sqlite3_prepare_v2(self.db, sql.ptr, -1, &stmt, null));
        return stmt.?;
    }

    /// Generic SELECT-by-integer-id returning a JSON blob.
    /// Returns "{}" when no row found (matching column DEFAULT).
    fn getStateById(self: *StateStore, sql: [:0]const u8, id: i64) ![]u8 {
        const stmt = try self.prepare(sql);
        defer _ = c.sqlite3_finalize(stmt);

        try sqliteOk(c.sqlite3_bind_int64(stmt, 1, id));

        return switch (c.sqlite3_step(stmt)) {
            c.SQLITE_ROW => blk: {
                const text = c.sqlite3_column_text(stmt, 0);
                const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
                break :blk if (text != null)
                    try self.allocator.dupe(u8, text[0..len])
                else
                    try self.allocator.dupe(u8, "{}");
            },
            c.SQLITE_DONE => try self.allocator.dupe(u8, "{}"),
            else => error.SqliteError,
        };
    }

    /// Generic INSERT OR REPLACE with (integer_id, text_data) parameters.
    fn setStateById(self: *StateStore, sql: [:0]const u8, id: i64, data: []const u8) !void {
        const stmt = try self.prepare(sql);
        defer _ = c.sqlite3_finalize(stmt);

        try sqliteOk(c.sqlite3_bind_int64(stmt, 1, id));
        try sqliteOk(c.sqlite3_bind_text(stmt, 2, data.ptr, @intCast(data.len), null));
        try sqliteDone(c.sqlite3_step(stmt));
    }

    inline fn sqliteOk(rc: c_int) !void {
        if (rc != c.SQLITE_OK) return error.SqliteError;
    }

    inline fn sqliteDone(rc: c_int) !void {
        if (rc != c.SQLITE_DONE) return error.SqliteError;
    }
    //
    // try sqliteOk(c.sqlite3_bind_int64(stmt, 1, id));
    // try sqliteOk(c.sqlite3_bind_text(stmt, 2, data.ptr, @intCast(data.len), null));
    // try sqliteDone(c.sqlite3_step(stmt));
    //
};


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Open an in-memory StateStore for a single test.
fn openMem() !StateStore {
    return StateStore.open(testing.allocator, ":memory:");
}

test "AC-5.1: fresh in-memory DB creates all tables without error" {
    var store = try openMem();
    defer store.close();

    // Verify each table exists by querying sqlite_master
    const tables = [_][:0]const u8{
        "SELECT 1 FROM meta LIMIT 1",
        "SELECT 1 FROM user_state LIMIT 1",
        "SELECT 1 FROM chat_state LIMIT 1",
        "SELECT 1 FROM global_state LIMIT 1",
    };
    for (tables) |sql| {
        const stmt = try store.prepare(sql);
        _ = c.sqlite3_step(stmt);
        _ = c.sqlite3_finalize(stmt);
    }
}

test "AC-5.2: after schema, meta contains schema_version = '1'" {
    var store = try openMem();
    defer store.close();

    const stmt = try store.prepare("SELECT value FROM meta WHERE key = 'schema_version'");
    defer _ = c.sqlite3_finalize(stmt);

    try testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    const text = c.sqlite3_column_text(stmt, 0);
    const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
    try testing.expect(text != null);
    try testing.expectEqualStrings("1", text[0..len]);
}

test "AC-5.3: opening a DB with matching schema_version succeeds" {
    // open() implicitly checks — if it returns Ok, version matches
    var store = try openMem();
    store.close();
}

test "AC-5.4: opening a DB with mismatched schema_version returns SchemaMismatch" {
    // Create a file-based DB and manually corrupt the schema_version.
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = try tmp.dir.realpath(".", &path_buf);
    var db_path_buf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const db_path = try std.fmt.bufPrintZ(&db_path_buf, "{s}/v.db", .{dir_path});

    // First open: creates and seeds schema_version = '1'
    var store = try StateStore.open(testing.allocator, db_path);

    // Corrupt schema_version to '99'
    const upd = try store.prepare("UPDATE meta SET value = '99' WHERE key = 'schema_version'");
    _ = c.sqlite3_step(upd);
    _ = c.sqlite3_finalize(upd);
    store.close();

    // Second open: should detect mismatch and return error
    const result = StateStore.open(testing.allocator, db_path);
    try testing.expectError(error.SchemaMismatch, result);
}

test "AC-5.5: setUserState / getUserState round-trip" {
    var store = try openMem();
    defer store.close();

    const data = "{\"count\":7}";
    try store.setUserState(42, data);
    const got = try store.getUserState(42);
    defer testing.allocator.free(got);

    try testing.expectEqualStrings(data, got);
}

test "AC-5.6: getUserState for unknown user_id returns '{}'" {
    var store = try openMem();
    defer store.close();

    const got = try store.getUserState(999_999);
    defer testing.allocator.free(got);

    try testing.expectEqualStrings("{}", got);
}

test "AC-5.7: setChatState / getChatState round-trip" {
    var store = try openMem();
    defer store.close();

    try store.setChatState(-100, "{\"muted\":true}");
    const got = try store.getChatState(-100);
    defer testing.allocator.free(got);

    try testing.expectEqualStrings("{\"muted\":true}", got);
}

test "AC-5.7: getChatState for unknown chat_id returns '{}'" {
    var store = try openMem();
    defer store.close();

    const got = try store.getChatState(1);
    defer testing.allocator.free(got);

    try testing.expectEqualStrings("{}", got);
}

test "AC-5.8: setGlobal / getGlobal round-trip" {
    var store = try openMem();
    defer store.close();

    try store.setGlobal("total", "99");
    const got = try store.getGlobal("total");
    defer if (got) |v| testing.allocator.free(v);

    try testing.expect(got != null);
    try testing.expectEqualStrings("99", got.?);
}

test "AC-5.8: getGlobal for missing key returns null" {
    var store = try openMem();
    defer store.close();

    const got = try store.getGlobal("no_such_key");
    try testing.expectEqual(@as(?[]u8, null), got);
}

test "AC-5.9: 3 connections on same file-based DB, concurrent reads, WAL confirmed" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = try tmp.dir.realpath(".", &path_buf);
    var db_path_buf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const db_path = try std.fmt.bufPrintZ(&db_path_buf, "{s}/wal.db", .{dir_path});

    // Seed via first connection
    var s1 = try StateStore.open(testing.allocator, db_path);
    try s1.setUserState(1, "{\"x\":1}");
    try s1.setUserState(2, "{\"x\":2}");

    // Open two more connections
    var s2 = try StateStore.open(testing.allocator, db_path);
    var s3 = try StateStore.open(testing.allocator, db_path);
    defer s1.close();
    defer s2.close();
    defer s3.close();

    // Concurrent reads from all three
    const r1 = try s1.getUserState(1);
    defer testing.allocator.free(r1);
    const r2 = try s2.getUserState(2);
    defer testing.allocator.free(r2);
    const r3 = try s3.getUserState(1);
    defer testing.allocator.free(r3);

    try testing.expectEqualStrings("{\"x\":1}", r1);
    try testing.expectEqualStrings("{\"x\":2}", r2);
    try testing.expectEqualStrings("{\"x\":1}", r3);

    // Verify WAL mode is active (file-based DB only — in-memory always reports "memory")
    const jm_stmt = try s1.prepare("PRAGMA journal_mode");
    defer _ = c.sqlite3_finalize(jm_stmt);
    try testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(jm_stmt));
    const jm_text = c.sqlite3_column_text(jm_stmt, 0);
    const jm_len: usize = @intCast(c.sqlite3_column_bytes(jm_stmt, 0));
    try testing.expect(jm_text != null);
    try testing.expectEqualStrings("wal", jm_text[0..jm_len]);
}

test "AC-5.10: setUserState upserts — second write overwrites first" {
    var store = try openMem();
    defer store.close();

    try store.setUserState(1, "{\"v\":\"A\"}");
    try store.setUserState(1, "{\"v\":\"B\"}");

    const got = try store.getUserState(1);
    defer testing.allocator.free(got);

    try testing.expectEqualStrings("{\"v\":\"B\"}", got);
}

test "AC-5.10: setGlobal upserts — second write overwrites first" {
    var store = try openMem();
    defer store.close();

    try store.setGlobal("k", "first");
    try store.setGlobal("k", "second");

    const got = try store.getGlobal("k");
    defer if (got) |v| testing.allocator.free(v);

    try testing.expectEqualStrings("second", got.?);
}

test "AC-5.11: busy_timeout set — concurrent writers complete without BUSY error" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = try tmp.dir.realpath(".", &path_buf);
    var db_path_buf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const db_path = try std.fmt.bufPrintZ(&db_path_buf, "{s}/busy.db", .{dir_path});

    var s1 = try StateStore.open(testing.allocator, db_path);
    var s2 = try StateStore.open(testing.allocator, db_path);
    defer s1.close();
    defer s2.close();

    // Interleaved writes from two connections — busy_timeout lets them retry
    // rather than immediately returning SQLITE_BUSY.
    for (0..50) |i| {
        try s1.setUserState(@intCast(i), "{}");
        try s2.setUserState(@intCast(i + 1000), "{}");
    }

    // Verify busy_timeout pragma is 5000
    const stmt1 = try s1.prepare("PRAGMA busy_timeout");
    defer _ = c.sqlite3_finalize(stmt1);
    try testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt1));
    const timeout1 = c.sqlite3_column_int64(stmt1, 0);
    try testing.expectEqual(@as(i64, 5000), timeout1);

    const stmt2 = try s1.prepare("PRAGMA busy_timeout");
    defer _ = c.sqlite3_finalize(stmt2);
    try testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt2));
    const timeout2 = c.sqlite3_column_int64(stmt2, 0);
    try testing.expectEqual(@as(i64, 5000), timeout2);
}
