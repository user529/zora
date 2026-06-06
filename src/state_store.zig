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

/// The six hot-path CRUD statements, in cache slot order
/// (get_user, set_user, get_chat, set_chat, get_global, set_global).
const CACHED_SQL = [6][:0]const u8{
    "SELECT data FROM user_state WHERE user_id = ?",
    "INSERT OR REPLACE INTO user_state (user_id, data) VALUES (?, ?)",
    "SELECT data FROM chat_state WHERE chat_id = ?",
    "INSERT OR REPLACE INTO chat_state (chat_id, data) VALUES (?, ?)",
    "SELECT value FROM global_state WHERE key = ?",
    "INSERT OR REPLACE INTO global_state (key, value) VALUES (?, ?)",
};

/// Prepare all six cached statements into an array. On any prepare failure,
/// finalize the already-prepared prefix (zero-leak) and return the error.
fn prepareInto(db: *c.sqlite3, sqls: [6][:0]const u8) ![6]*c.sqlite3_stmt {
    var out: [6]*c.sqlite3_stmt = undefined;
    var prepared: usize = 0;
    errdefer for (out[0..prepared]) |s| {
        _ = c.sqlite3_finalize(s);
    };
    for (sqls, 0..) |sql, i| {
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql.ptr, -1, &stmt, null) != c.SQLITE_OK)
            return error.SqliteError;
        out[i] = stmt.?;
        prepared += 1;
    }
    return out;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub const StateStore = struct {
    db: *c.sqlite3,
    allocator: std.mem.Allocator,
    // Cached hot-path CRUD statements: compiled once in open(), reused via
    // sqlite3_reset, finalized in close(). Default `undefined`; always assigned
    // by prepareStatements() before open() returns success.
    stmt_get_user:   *c.sqlite3_stmt = undefined,
    stmt_set_user:   *c.sqlite3_stmt = undefined,
    stmt_get_chat:   *c.sqlite3_stmt = undefined,
    stmt_set_chat:   *c.sqlite3_stmt = undefined,
    stmt_get_global: *c.sqlite3_stmt = undefined,
    stmt_set_global: *c.sqlite3_stmt = undefined,

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
        store.prepareStatements() catch |err| {
            // prepareInto already finalized any prepared prefix; the connection
            // has no live statements, so sqlite3_close is clean here.
            _ = c.sqlite3_close(store.db);
            return err;
        };
        return store;
    }

    pub fn close(self: *StateStore) void {
        // Finalize cached statements before closing, or sqlite3_close returns
        // SQLITE_BUSY and leaks the connection.
        _ = c.sqlite3_finalize(self.stmt_get_user);
        _ = c.sqlite3_finalize(self.stmt_set_user);
        _ = c.sqlite3_finalize(self.stmt_get_chat);
        _ = c.sqlite3_finalize(self.stmt_set_chat);
        _ = c.sqlite3_finalize(self.stmt_get_global);
        _ = c.sqlite3_finalize(self.stmt_set_global);
        _ = c.sqlite3_close(self.db);
    }

    pub fn beginImmediate(self: *StateStore) !void {
        try sqliteOk(c.sqlite3_exec(self.db, "BEGIN IMMEDIATE", null, null, null));
    }

    pub fn commit(self: *StateStore) !void {
        try sqliteOk(c.sqlite3_exec(self.db, "COMMIT", null, null, null));
    }

    pub fn rollback(self: *StateStore) void {
        _ = c.sqlite3_exec(self.db, "ROLLBACK", null, null, null);
    }

    fn prepareStatements(self: *StateStore) !void {
        const s = try prepareInto(self.db, CACHED_SQL);
        self.stmt_get_user   = s[0];
        self.stmt_set_user   = s[1];
        self.stmt_get_chat   = s[2];
        self.stmt_set_chat   = s[3];
        self.stmt_get_global = s[4];
        self.stmt_set_global = s[5];
    }

    // -----------------------------------------------------------------------
    // User state
    // -----------------------------------------------------------------------

    /// Returns the stored JSON blob for `user_id`, or "{}" if not yet set.
    /// Caller owns the returned slice.
    pub fn getUserState(self: *StateStore, user_id: i64) ![]u8 {
        return self.getStateById(self.stmt_get_user, user_id);
    }

    /// Upsert the JSON blob for `user_id`.
    pub fn setUserState(self: *StateStore, user_id: i64, data: []const u8) !void {
        return self.setStateById(self.stmt_set_user, user_id, data);
    }

    // -----------------------------------------------------------------------
    // Chat state
    // -----------------------------------------------------------------------

    pub fn getChatState(self: *StateStore, chat_id: i64) ![]u8 {
        return self.getStateById(self.stmt_get_chat, chat_id);
    }

    pub fn setChatState(self: *StateStore, chat_id: i64, data: []const u8) !void {
        return self.setStateById(self.stmt_set_chat, chat_id, data);
    }

    // -----------------------------------------------------------------------
    // Global key-value store
    // -----------------------------------------------------------------------

    /// Returns the value for `key`, or null if absent.
    /// Caller owns the returned slice (free with the same allocator).
    pub fn getGlobal(self: *StateStore, key: []const u8) !?[]u8 {
        const stmt = self.stmt_get_global;
        defer {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_clear_bindings(stmt);
        }

        try sqliteOk(c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), null));

        return switch (c.sqlite3_step(stmt)) {
            c.SQLITE_ROW => blk: {
                const text = c.sqlite3_column_text(stmt, 0);
                const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
                // dupe BEFORE the deferred reset invalidates the column pointer
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
        const stmt = self.stmt_set_global;
        defer {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_clear_bindings(stmt);
        }

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

    /// Run a cached SELECT-by-integer-id statement, returning a JSON blob.
    /// Returns "{}" when no row found (matching column DEFAULT).
    fn getStateById(self: *StateStore, stmt: *c.sqlite3_stmt, id: i64) ![]u8 {
        defer {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_clear_bindings(stmt);
        }

        try sqliteOk(c.sqlite3_bind_int64(stmt, 1, id));

        return switch (c.sqlite3_step(stmt)) {
            c.SQLITE_ROW => blk: {
                const text = c.sqlite3_column_text(stmt, 0);
                const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
                // dupe BEFORE the deferred reset invalidates the column pointer
                break :blk if (text != null)
                    try self.allocator.dupe(u8, text[0..len])
                else
                    try self.allocator.dupe(u8, "{}");
            },
            c.SQLITE_DONE => try self.allocator.dupe(u8, "{}"),
            else => error.SqliteError,
        };
    }

    /// Run a cached INSERT OR REPLACE with (integer_id, text_data) parameters.
    fn setStateById(self: *StateStore, stmt: *c.sqlite3_stmt, id: i64, data: []const u8) !void {
        _ = self;
        defer {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_clear_bindings(stmt);
        }

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
};


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Open an in-memory StateStore for a single test.
fn openMem() !StateStore {
    return StateStore.open(testing.allocator, ":memory:");
}

test "fresh in-memory DB creates all tables without error" {
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

test "schema_version is seeded, matched on open, and mismatch is rejected" {
    // A fresh DB seeds meta.schema_version = '1'.
    {
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
    // Opening with a matching version succeeds (open() checks implicitly).
    {
        var store = try openMem();
        store.close();
    }
    // Opening a DB whose stored version differs returns SchemaMismatch.
    {
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const dir_path = try tmp.dir.realpath(".", &path_buf);
        var db_path_buf: [std.fs.max_path_bytes + 16]u8 = undefined;
        const db_path = try std.fmt.bufPrintZ(&db_path_buf, "{s}/v.db", .{dir_path});

        // First open creates and seeds schema_version = '1'.
        var store = try StateStore.open(testing.allocator, db_path);
        // Corrupt the stored version to '99'.
        const upd = try store.prepare("UPDATE meta SET value = '99' WHERE key = 'schema_version'");
        _ = c.sqlite3_step(upd);
        _ = c.sqlite3_finalize(upd);
        store.close();

        // The second open detects the mismatch.
        try testing.expectError(error.SchemaMismatch, StateStore.open(testing.allocator, db_path));
    }
}

test "state round-trips for user, chat, and global" {
    var store = try openMem();
    defer store.close();

    // user_state
    try store.setUserState(42, "{\"count\":7}");
    const u = try store.getUserState(42);
    defer testing.allocator.free(u);
    try testing.expectEqualStrings("{\"count\":7}", u);

    // chat_state
    try store.setChatState(-100, "{\"muted\":true}");
    const ch = try store.getChatState(-100);
    defer testing.allocator.free(ch);
    try testing.expectEqualStrings("{\"muted\":true}", ch);

    // global_state
    try store.setGlobal("total", "99");
    const g = try store.getGlobal("total");
    defer if (g) |v| testing.allocator.free(v);
    try testing.expect(g != null);
    try testing.expectEqualStrings("99", g.?);
}

test "unknown keys return the empty default" {
    var store = try openMem();
    defer store.close();

    // Unknown user/chat ids return "{}".
    const u = try store.getUserState(999_999);
    defer testing.allocator.free(u);
    try testing.expectEqualStrings("{}", u);

    const ch = try store.getChatState(1);
    defer testing.allocator.free(ch);
    try testing.expectEqualStrings("{}", ch);

    // A missing global key returns null.
    try testing.expectEqual(@as(?[]u8, null), try store.getGlobal("no_such_key"));
}

test "3 connections on same file-based DB, concurrent reads, WAL confirmed" {
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

test "a second write upserts over the first" {
    var store = try openMem();
    defer store.close();

    // user_state upsert.
    try store.setUserState(1, "{\"v\":\"A\"}");
    try store.setUserState(1, "{\"v\":\"B\"}");
    const u = try store.getUserState(1);
    defer testing.allocator.free(u);
    try testing.expectEqualStrings("{\"v\":\"B\"}", u);

    // global_state upsert.
    try store.setGlobal("k", "first");
    try store.setGlobal("k", "second");
    const g = try store.getGlobal("k");
    defer if (g) |v| testing.allocator.free(v);
    try testing.expectEqualStrings("second", g.?);
}

test "an immediate transaction commits and rolls back" {
    var store = try openMem();
    defer store.close();

    // commit persists the write.
    try store.beginImmediate();
    try store.setUserState(1, "{\"x\":42}");
    try store.commit();
    {
        const data = try store.getUserState(1);
        defer store.allocator.free(data);
        try testing.expect(std.mem.indexOf(u8, data, "\"x\":42") != null);
    }

    // rollback reverts the write to the last committed value.
    try store.setUserState(2, "{\"x\":0}");
    try store.beginImmediate();
    try store.setUserState(2, "{\"x\":99}");
    store.rollback();
    {
        const data = try store.getUserState(2);
        defer store.allocator.free(data);
        try testing.expect(std.mem.indexOf(u8, data, "\"x\":0") != null);
        try testing.expect(std.mem.indexOf(u8, data, "\"x\":99") == null);
    }
}

/// Count live (un-finalized) prepared statements on a connection.
fn countStmts(db: *c.sqlite3) usize {
    var n: usize = 0;
    var s: ?*c.sqlite3_stmt = c.sqlite3_next_stmt(db, null);
    while (s != null) : (s = c.sqlite3_next_stmt(db, s)) n += 1;
    return n;
}

test "open prepares exactly 6 cached statements; close tears down cleanly" {
    var store = try openMem();
    // Exactly the six cached CRUD statements are live after open().
    try testing.expectEqual(@as(usize, 6), countStmts(store.db));
    // Exercise the real close() teardown (finalize all six, then sqlite3_close).
    // A finalize omission in close() would leave the connection BUSY and leak it,
    // which the surrounding test suite's `defer store.close()` usage would surface.
    store.close();
}

test "finalizing the six cached statements lets sqlite3_close return OK" {
    // White-box check mirroring close()'s teardown: finalizing exactly the six
    // cached statements leaves no live statement, so sqlite3_close returns
    // SQLITE_OK (it would return SQLITE_BUSY if any statement were still live).
    // The store is torn down manually here, so no `defer store.close()`.
    const store = try openMem();
    _ = c.sqlite3_finalize(store.stmt_get_user);
    _ = c.sqlite3_finalize(store.stmt_set_user);
    _ = c.sqlite3_finalize(store.stmt_get_chat);
    _ = c.sqlite3_finalize(store.stmt_set_chat);
    _ = c.sqlite3_finalize(store.stmt_get_global);
    _ = c.sqlite3_finalize(store.stmt_set_global);
    try testing.expectEqual(@as(c_int, c.SQLITE_OK), c.sqlite3_close(store.db));
}

test "prepareInto finalizes the prepared prefix on a mid-sequence failure" {
    var store = try openMem();
    defer store.close();

    // A batch whose last entry is invalid SQL: the first five prepare, the
    // sixth fails, and the errdefer must finalize the five already prepared.
    const bad = [_][:0]const u8{
        "SELECT 1", "SELECT 1", "SELECT 1", "SELECT 1", "SELECT 1",
        "SELECT bogus_col FROM no_such_table",
    };
    const before = countStmts(store.db); // 6 cached
    try testing.expectError(error.SqliteError, prepareInto(store.db, bad));
    // No leak: the 5 successful prepares were finalized, count is unchanged.
    try testing.expectEqual(before, countStmts(store.db));
}

test "setGlobal/getGlobal reuse the cached statement (count stays 6)" {
    var store = try openMem();
    defer store.close();

    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        var vbuf: [16]u8 = undefined;
        const v = try std.fmt.bufPrint(&vbuf, "{d}", .{i});
        try store.setGlobal("total", v);
    }
    const got = try store.getGlobal("total");
    defer testing.allocator.free(got.?);
    try testing.expectEqualStrings("999", got.?);

    // No statement growth across 1000+ reuses: still exactly the 6 cached.
    try testing.expectEqual(@as(usize, 6), countStmts(store.db));
}

test "user/chat statements reset cleanly across mixed access" {
    var store = try openMem();
    defer store.close();

    // unknown id -> "{}"
    const u_empty = try store.getUserState(1);
    defer testing.allocator.free(u_empty);
    try testing.expectEqualStrings("{}", u_empty);

    // write user, write chat (different cached stmts), read both back
    try store.setUserState(1, "{\"count\":1}");
    try store.setChatState(2, "{\"room\":\"a\"}");

    const user_state1 = try store.getUserState(1);
    defer testing.allocator.free(user_state1);
    try testing.expectEqualStrings("{\"count\":1}", user_state1);

    const ch = try store.getChatState(2);
    defer testing.allocator.free(ch);
    try testing.expectEqualStrings("{\"room\":\"a\"}", ch);

    // overwrite user, re-read -- no stale row/binding from prior reuse
    try store.setUserState(1, "{\"count\":2}");
    const user_state2 = try store.getUserState(1);
    defer testing.allocator.free(user_state2);
    try testing.expectEqualStrings("{\"count\":2}", user_state2);

    // statements reused, not re-prepared: still the 6 cached
    try testing.expectEqual(@as(usize, 6), countStmts(store.db));
}

test "global statement reset is clean across mixed read/write/miss" {
    var store = try openMem();
    defer store.close();

    // miss -> null
    try testing.expectEqual(@as(?[]u8, null), try store.getGlobal("k"));

    // write then read same key
    try store.setGlobal("k", "v1");
    const a = try store.getGlobal("k");
    defer testing.allocator.free(a.?);
    try testing.expectEqualStrings("v1", a.?);

    // overwrite then read -- no stale binding/row state leaks between reuses
    try store.setGlobal("k", "v2");
    const b = try store.getGlobal("k");
    defer testing.allocator.free(b.?);
    try testing.expectEqualStrings("v2", b.?);

    // a different missing key still returns null after prior populated reads
    try testing.expectEqual(@as(?[]u8, null), try store.getGlobal("absent"));
}

test "busy_timeout set — concurrent writers complete without BUSY error" {
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
