/// state_store.zig — SQLite-backed persistence for user, chat, and global state.
///
/// One StateStore per worker thread (owns its own SQLite connection).
/// WAL mode allows concurrent readers across connections on the same file.
/// open() applies the schema idempotently; a version mismatch aborts startup.

const std = @import("std");

const c = @cImport({
    @cInclude("sqlite3.h");
});

const state_crypto = @import("state_crypto.zig");
const log = std.log.scoped(.state_store);

pub const EncryptionConfig = struct { passphrase: []const u8, io: std.Io };
pub const OpenOptions = struct { encryption: ?EncryptionConfig = null };

pub const SCHEMA_VERSION: u32 = 2;

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

    /// Test-only fault injection. When true, the next `commit()` returns
    /// `error.SqliteError` instead of issuing COMMIT, leaving the open
    /// transaction unresolved (the real failure shape: the caller must
    /// `rollback()`). The flag auto-clears so only one commit fails. Default
    /// false — inert in production; no path sets it except tests.
    fail_next_commit: bool = false,
    /// Set when the database is in encrypted mode; null in plaintext mode.
    /// Established by openWithOptions and torn down by close().
    cipher: ?state_crypto.Cipher = null,
    /// Reusable scratch for encrypt-on-write; grown as needed, freed in close().
    enc_buf: std.ArrayListUnmanaged(u8) = .empty,

    /// Open (or create) the database at `path` in plaintext mode.
    /// For in-memory databases use path = ":memory:".
    pub fn open(allocator: std.mem.Allocator, path: [:0]const u8) !StateStore {
        return openWithOptions(allocator, path, .{});
    }

    /// Open (or create) the database, reconciling encryption mode.
    /// Applies pragmas and schema, verifies schema_version, then either seeds
    /// (fresh DB) or reconciles (existing DB) the encryption marker against
    /// `opts`. A configured/stored mode mismatch returns
    /// error.EncryptionModeMismatch; a wrong passphrase returns
    /// error.WrongEncryptionKey. On success a non-null `cipher` means
    /// encrypted mode.
    pub fn openWithOptions(allocator: std.mem.Allocator, path: [:0]const u8, opts: OpenOptions) !StateStore {
        var raw: ?*c.sqlite3 = null;
        const flags = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE;
        if (c.sqlite3_open_v2(path.ptr, &raw, flags, null) != c.SQLITE_OK) {
            if (raw) |db| _ = c.sqlite3_close(db);
            return error.OpenFailed;
        }
        var store = StateStore{ .db = raw.?, .allocator = allocator };
        errdefer {
            if (store.cipher) |*cph| cph.deinit();
            _ = c.sqlite3_close(store.db);
        }

        try store.applyPragma();
        const fresh = !try store.checkSchemaExistence();
        if (fresh) try store.applySchema();
        try store.checkSchemaVersion();
        if (fresh) try store.seedEncryption(opts) else try store.reconcileEncryption(opts);
        try store.prepareStatements();
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
        self.enc_buf.deinit(self.allocator);
        if (self.cipher) |*cph| cph.deinit();
        _ = c.sqlite3_close(self.db);
    }

    pub fn beginImmediate(self: *StateStore) !void {
        try sqliteOk(c.sqlite3_exec(self.db, "BEGIN IMMEDIATE", null, null, null));
    }

    /// Begin a deferred transaction: the write lock is taken lazily, on the
    /// first write. A read-only or send-only transaction therefore never blocks
    /// another worker's writer. Used to wrap a resumed coroutine segment, which
    /// is frequently read-only.
    pub fn beginDeferred(self: *StateStore) !void {
        try sqliteOk(c.sqlite3_exec(self.db, "BEGIN DEFERRED", null, null, null));
    }

    pub fn commit(self: *StateStore) !void {
        // Test-only fault injection: simulate a COMMIT failure without issuing
        // it, leaving the transaction open for the caller to roll back. Inert
        // unless a test sets `fail_next_commit`.
        if (self.fail_next_commit) {
            self.fail_next_commit = false;
            return error.SqliteError;
        }
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
                if (self.cipher != null) break :blk try self.decodePayload(stmt, 0);
                const text = c.sqlite3_column_text(stmt, 0);
                const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
                break :blk if (text != null) try self.allocator.dupe(u8, text[0..len]) else null;
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
        try self.bindPayload(stmt, 2, value);
        try sqliteDone(c.sqlite3_step(stmt));
    }

    // -----------------------------------------------------------------------
    // Scheduler queries — cold path: prepared per call, never cached.
    // -----------------------------------------------------------------------
    //
    // The schedule path is not hot, so these statements do not join the six
    // cached CRUD statements. Reuse-first (PRINCIPLESv2 §1): all four methods
    // funnel through prepareBound/execParams instead of repeating the
    // prepare -> bind -> step -> finalize dance.

    /// One positional bind value for a cold-path statement. A `text` or `blob`
    /// slice binds with SQLITE_STATIC (the `null` destructor), so its bytes must
    /// stay valid until the statement is stepped and finalized — true for every
    /// caller here, which binds slices that outlive the statement.
    const Param = union(enum) { int: i64, text: []const u8, blob: []const u8 };

    /// Prepare `sql` and bind `params` positionally (1-based); return the bound,
    /// not-yet-stepped statement — the shared prepare+bind prefix for every
    /// cold-path scheduler query. Ownership passes to the caller, which MUST
    /// `defer _ = c.sqlite3_finalize(stmt)` and then step as its query needs
    /// (writes step once to DONE; reads loop over rows). A bind failure finalizes
    /// the statement here before the error propagates (zero-leak).
    fn prepareBound(self: *StateStore, sql: [:0]const u8, params: []const Param) !*c.sqlite3_stmt {
        const stmt = try self.prepare(sql);
        errdefer _ = c.sqlite3_finalize(stmt);
        for (params, 1..) |p, i| {
            const idx: c_int = @intCast(i);
            switch (p) {
                .int  => |v| try sqliteOk(c.sqlite3_bind_int64(stmt, idx, v)),
                .text => |v| try sqliteOk(c.sqlite3_bind_text(stmt, idx, v.ptr, @intCast(v.len), null)),
                .blob => |v| try sqliteOk(c.sqlite3_bind_blob(stmt, idx, v.ptr, @intCast(v.len), null)),
            }
        }
        return stmt;
    }

    /// Prepare+bind `sql`, step once expecting SQLITE_DONE, finalize. The
    /// no-result write path (INSERT/DELETE) on the cold scheduler path.
    fn execParams(self: *StateStore, sql: [:0]const u8, params: []const Param) !void {
        const stmt = try self.prepareBound(sql, params);
        defer _ = c.sqlite3_finalize(stmt);
        try sqliteDone(c.sqlite3_step(stmt));
    }

    /// One claimed scheduler row. `payload` is owned by the caller.
    pub const ClaimedJob = struct { id: i64, payload: []u8 };

    /// Insert a job; returns its rowid.
    pub fn scheduleInsert(self: *StateStore, fire_at_ms: i64, payload: []const u8) !i64 {
        const sql = "INSERT INTO schedule (fire_at_ms, payload) VALUES (?, ?)";
        if (self.cipher) |cph| {
            const buf = try self.allocator.alloc(u8, payload.len + state_crypto.overhead);
            defer self.allocator.free(buf);
            const blob = cph.encryptInto(buf, payload);
            try self.execParams(sql, &.{ .{ .int = fire_at_ms }, .{ .blob = blob } });
        } else {
            try self.execParams(sql, &.{ .{ .int = fire_at_ms }, .{ .text = payload } });
        }
        return c.sqlite3_last_insert_rowid(self.db);
    }

    /// Delete a job by id. Returns true if a row was removed.
    pub fn scheduleDelete(self: *StateStore, id: i64) !bool {
        try self.execParams("DELETE FROM schedule WHERE id = ?", &.{.{ .int = id }});
        return c.sqlite3_changes(self.db) > 0;
    }

    /// Smallest fire_at_ms among claimable rows (unclaimed, or lease older than
    /// `reclaim_cutoff_ms`). Null when no claimable row exists.
    pub fn scheduleMinFire(self: *StateStore, reclaim_cutoff_ms: i64) !?i64 {
        const stmt = try self.prepareBound(
            "SELECT MIN(fire_at_ms) FROM schedule " ++
                "WHERE claimed_at_ms IS NULL OR claimed_at_ms < ?",
            &.{.{ .int = reclaim_cutoff_ms }},
        );
        defer _ = c.sqlite3_finalize(stmt);
        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.SqliteError;
        // MIN over an empty set yields SQL NULL → column_type is NULL.
        if (c.sqlite3_column_type(stmt, 0) == c.SQLITE_NULL) return null;
        return c.sqlite3_column_int64(stmt, 0);
    }

    /// Atomically claim up to `max_batch` due rows: stamp claimed_at_ms = now_ms
    /// on the earliest rows whose fire_at_ms <= now that are unclaimed (or whose
    /// lease predates reclaim_cutoff_ms), and return them. One bulk
    /// UPDATE ... RETURNING does the claim in a single atomic statement, so a row
    /// is never handed out twice — no explicit transaction, no per-row update.
    /// Caller owns each payload and the slice.
    pub fn scheduleClaimDue(
        self: *StateStore,
        now_ms: i64,
        reclaim_cutoff_ms: i64,
        max_batch: u32,
        allocator: std.mem.Allocator,
    ) ![]ClaimedJob {
        // SQLite does not accept ORDER BY/LIMIT directly on UPDATE, so the
        // earliest-N rows are chosen by a subquery and stamped in bulk; RETURNING
        // streams the claimed rows back. Their order is unspecified — the
        // scheduler round-robins, so batch order does not matter.
        const stmt = try self.prepareBound(
            "UPDATE schedule SET claimed_at_ms = ? " ++
                "WHERE id IN (SELECT id FROM schedule " ++
                "WHERE fire_at_ms <= ? AND (claimed_at_ms IS NULL OR claimed_at_ms < ?) " ++
                "ORDER BY fire_at_ms LIMIT ?) " ++
                "RETURNING id, payload",
            &.{
                .{ .int = now_ms },            // SET claimed_at_ms
                .{ .int = now_ms },            // fire_at_ms <= now
                .{ .int = reclaim_cutoff_ms },
                .{ .int = @intCast(max_batch) },
            },
        );
        defer _ = c.sqlite3_finalize(stmt);

        var list: std.ArrayListUnmanaged(ClaimedJob) = .empty;
        errdefer {
            for (list.items) |j| allocator.free(j.payload);
            list.deinit(allocator);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const id = c.sqlite3_column_int64(stmt, 0);
            const payload = if (self.cipher) |cph| blk: {
                const raw = c.sqlite3_column_blob(stmt, 1);
                const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 1));
                if (raw == null) break :blk try allocator.dupe(u8, "{}");
                const bytes = @as([*]const u8, @ptrCast(raw))[0..len];
                break :blk cph.decryptAlloc(allocator, bytes) catch |e| {
                    log.warn("schedule payload decrypt failed: {s}", .{@errorName(e)});
                    return error.DecryptFailed;
                };
            } else blk: {
                const text = c.sqlite3_column_text(stmt, 1);
                const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 1));
                break :blk if (text != null) try allocator.dupe(u8, text[0..len]) else try allocator.dupe(u8, "{}");
            };
            errdefer allocator.free(payload);
            try list.append(allocator, .{ .id = id, .payload = payload });
        }

        return list.toOwnedSlice(allocator);
    }

    /// Insert one schedule row with its original identity preserved — the
    /// migration path. Unlike scheduleInsert (fresh rowid, NULL lease), this
    /// writes the given id and claimed_at_ms verbatim, so rowids that rules may
    /// have stored and in-flight leases both survive a v1 -> v2 migration. The
    /// payload encrypts on write when a cipher is set.
    pub fn insertScheduleRowRaw(
        self: *StateStore,
        id: i64,
        fire_at_ms: i64,
        claimed_at_ms: ?i64,
        payload: []const u8,
    ) !void {
        const stmt = try self.prepare(
            "INSERT INTO schedule (id, fire_at_ms, payload, claimed_at_ms) VALUES (?, ?, ?, ?)",
        );
        defer _ = c.sqlite3_finalize(stmt);
        try sqliteOk(c.sqlite3_bind_int64(stmt, 1, id));
        try sqliteOk(c.sqlite3_bind_int64(stmt, 2, fire_at_ms));
        try self.bindPayload(stmt, 3, payload);
        if (claimed_at_ms) |ca|
            try sqliteOk(c.sqlite3_bind_int64(stmt, 4, ca))
        else
            try sqliteOk(c.sqlite3_bind_null(stmt, 4));
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

    /// Read a meta value by key. Caller owns the returned slice; null if absent.
    pub fn getMeta(self: *StateStore, allocator: std.mem.Allocator, key: [:0]const u8) !?[]u8 {
        const stmt = try self.prepare("SELECT value FROM meta WHERE key = ?");
        defer _ = c.sqlite3_finalize(stmt);
        try sqliteOk(c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), null));
        return switch (c.sqlite3_step(stmt)) {
            c.SQLITE_ROW => blk: {
                const text = c.sqlite3_column_text(stmt, 0);
                const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
                break :blk if (text != null) try allocator.dupe(u8, text[0..len]) else null;
            },
            c.SQLITE_DONE => null,
            else => error.SqliteError,
        };
    }

    fn putMeta(self: *StateStore, key: []const u8, value: []const u8) !void {
        try self.execParams(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            &.{ .{ .text = key }, .{ .text = value } },
        );
    }

    /// Fresh-DB path: write the encryption marker and, in encrypted mode, the
    /// salt, params, and verifier; establish the cipher.
    fn seedEncryption(self: *StateStore, opts: OpenOptions) !void {
        const enc = opts.encryption orelse {
            try self.putMeta("encryption", "none");
            return;
        };
        const salt = state_crypto.randomSalt(enc.io);
        const params = state_crypto.default_argon;
        var cph = try state_crypto.deriveCipher(self.allocator, enc.io, enc.passphrase, salt, params);
        errdefer cph.deinit();

        var salt_hex: [state_crypto.salt_len * 2]u8 = undefined;
        _ = std.fmt.bufPrint(&salt_hex, "{x}", .{salt[0..]}) catch unreachable;
        var pbuf: [64]u8 = undefined;
        const pstr = try state_crypto.encodeParams(&pbuf, params);
        const vhex = try state_crypto.makeVerifierHex(cph, self.allocator);
        defer self.allocator.free(vhex);

        try self.putMeta("encryption", "xchacha20poly1305");
        try self.putMeta("kdf_salt", &salt_hex);
        try self.putMeta("kdf_params", pstr);
        try self.putMeta("enc_verifier", vhex);
        self.cipher = cph;
    }

    /// Existing-DB path: the stored marker must match the configured mode. In
    /// encrypted mode, derive the key from the stored salt/params and verify it
    /// against the stored verifier.
    fn reconcileEncryption(self: *StateStore, opts: OpenOptions) !void {
        const mode = (try self.getMeta(self.allocator, "encryption")) orelse
            try self.allocator.dupe(u8, "none");
        defer self.allocator.free(mode);

        const want_enc = opts.encryption != null;
        const is_enc = std.mem.eql(u8, mode, "xchacha20poly1305");
        if (want_enc != is_enc) return error.EncryptionModeMismatch;
        if (!want_enc) return;

        const salt_hex = (try self.getMeta(self.allocator, "kdf_salt")) orelse return error.SchemaError;
        defer self.allocator.free(salt_hex);
        var salt: [state_crypto.salt_len]u8 = undefined;
        _ = std.fmt.hexToBytes(&salt, salt_hex) catch return error.SchemaError;

        const pstr = (try self.getMeta(self.allocator, "kdf_params")) orelse return error.SchemaError;
        defer self.allocator.free(pstr);
        const params = state_crypto.parseParams(pstr) catch return error.SchemaError;

        var cph = try state_crypto.deriveCipher(self.allocator, opts.encryption.?.io, opts.encryption.?.passphrase, salt, params);
        errdefer cph.deinit();

        const vhex = (try self.getMeta(self.allocator, "enc_verifier")) orelse return error.SchemaError;
        defer self.allocator.free(vhex);
        try state_crypto.checkVerifierHex(cph, self.allocator, vhex);
        self.cipher = cph;
    }

    fn prepare(self: *StateStore, sql: [:0]const u8) !*c.sqlite3_stmt {
        var stmt: ?*c.sqlite3_stmt = null;
        try sqliteOk(c.sqlite3_prepare_v2(self.db, sql.ptr, -1, &stmt, null));
        return stmt.?;
    }

    /// Bind a payload at parameter `idx`: ciphertext BLOB when a cipher is set,
    /// plaintext TEXT otherwise. The encrypted bytes live in self.enc_buf, which
    /// stays valid until the statement is stepped (callers step immediately).
    fn bindPayload(self: *StateStore, stmt: *c.sqlite3_stmt, idx: c_int, data: []const u8) !void {
        if (self.cipher) |cph| {
            const n = data.len + state_crypto.overhead;
            try self.enc_buf.ensureTotalCapacity(self.allocator, n);
            self.enc_buf.items.len = n;
            _ = cph.encryptInto(self.enc_buf.items, data);
            try sqliteOk(c.sqlite3_bind_blob(stmt, idx, self.enc_buf.items.ptr, @intCast(n), null));
        } else {
            try sqliteOk(c.sqlite3_bind_text(stmt, idx, data.ptr, @intCast(data.len), null));
        }
    }

    /// Decode a payload column read from `stmt` at `col` into owned plaintext:
    /// decrypt the BLOB when a cipher is set, else dupe the TEXT. Returns
    /// error.DecryptFailed on a bad ciphertext (logged at warn).
    fn decodePayload(self: *StateStore, stmt: *c.sqlite3_stmt, col: c_int) ![]u8 {
        if (self.cipher) |cph| {
            const raw = c.sqlite3_column_blob(stmt, col);
            const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
            if (raw == null) return error.DecryptFailed;
            const bytes = @as([*]const u8, @ptrCast(raw))[0..len];
            return cph.decryptAlloc(self.allocator, bytes) catch |e| {
                log.warn("state payload decrypt failed: {s}", .{@errorName(e)});
                return error.DecryptFailed;
            };
        }
        const text = c.sqlite3_column_text(stmt, col);
        const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
        return if (text != null) self.allocator.dupe(u8, text[0..len]) else self.allocator.dupe(u8, "{}");
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
            c.SQLITE_ROW => try self.decodePayload(stmt, 0),
            c.SQLITE_DONE => try self.allocator.dupe(u8, "{}"),
            else => error.SqliteError,
        };
    }

    /// Run a cached INSERT OR REPLACE with (integer_id, payload) parameters.
    fn setStateById(self: *StateStore, stmt: *c.sqlite3_stmt, id: i64, data: []const u8) !void {
        defer {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_clear_bindings(stmt);
        }

        try sqliteOk(c.sqlite3_bind_int64(stmt, 1, id));
        try self.bindPayload(stmt, 2, data);
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

/// Read back an integer-valued PRAGMA on `store`'s connection. Shared by the
/// pragma-verification tests (synchronous, foreign_keys, busy_timeout) so each
/// asserts the effective value rather than trusting the open() path.
fn readPragmaInt(store: *StateStore, pragma: [:0]const u8) !i64 {
    const stmt = try store.prepare(pragma);
    defer _ = c.sqlite3_finalize(stmt);
    try testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    return c.sqlite3_column_int64(stmt, 0);
}

test "fresh in-memory DB creates all tables without error" {
    var store = try openMem();
    defer store.close();

    // Verify each table exists by querying it. A missing table makes prepare()
    // fail ('no such table'); an empty table steps to SQLITE_DONE. Either is a
    // clean signal the table is present, so assert the step return is one of
    // the two — not silently discarded.
    const tables = [_][:0]const u8{
        "SELECT 1 FROM meta LIMIT 1",
        "SELECT 1 FROM user_state LIMIT 1",
        "SELECT 1 FROM chat_state LIMIT 1",
        "SELECT 1 FROM global_state LIMIT 1",
        "SELECT 1 FROM schedule LIMIT 1",
    };
    for (tables) |sql| {
        const stmt = try store.prepare(sql);
        defer _ = c.sqlite3_finalize(stmt);
        const rc = c.sqlite3_step(stmt);
        try testing.expect(rc == c.SQLITE_ROW or rc == c.SQLITE_DONE);
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
        try testing.expectEqualStrings("2", text[0..len]);
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
        const dir_path_len = try tmp.dir.realPath(testing.io, &path_buf);
        const dir_path = path_buf[0..dir_path_len];
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
    const dir_path_len = try tmp.dir.realPath(testing.io, &path_buf);
    const dir_path = path_buf[0..dir_path_len];
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

    // The other required pragmas must also be in effect on the
    // connection (synchronous=NORMAL == 1, foreign_keys=ON == 1).
    try testing.expectEqual(@as(i64, 1), try readPragmaInt(&s1, "PRAGMA synchronous"));
    try testing.expectEqual(@as(i64, 1), try readPragmaInt(&s1, "PRAGMA foreign_keys"));
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
    // close() discards sqlite3_close's return code by design, so this test cannot
    // assert SQLITE_OK directly. The teardown's success is asserted by the
    // companion white-box test "finalizing the six cached statements lets
    // sqlite3_close return OK", which mirrors close()'s exact finalize sequence
    // and checks the SQLITE_OK return. A finalize omission here would leave the
    // connection BUSY and leak it — surfaced by countStmts being non-zero and by
    // the suite-wide `defer store.close()` usage.
    try testing.expectEqual(@as(usize, 6), countStmts(store.db));
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
    const dir_path_len = try tmp.dir.realPath(testing.io, &path_buf);
    const dir_path = path_buf[0..dir_path_len];
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

    // busy_timeout must be 5000 on BOTH connections (the second query reads
    // s2, not s1 again).
    try testing.expectEqual(@as(i64, 5000), try readPragmaInt(&s1, "PRAGMA busy_timeout"));
    try testing.expectEqual(@as(i64, 5000), try readPragmaInt(&s2, "PRAGMA busy_timeout"));

    // The other required pragmas hold on both connections too.
    try testing.expectEqual(@as(i64, 1), try readPragmaInt(&s1, "PRAGMA synchronous"));
    try testing.expectEqual(@as(i64, 1), try readPragmaInt(&s2, "PRAGMA synchronous"));
    try testing.expectEqual(@as(i64, 1), try readPragmaInt(&s1, "PRAGMA foreign_keys"));
    try testing.expectEqual(@as(i64, 1), try readPragmaInt(&s2, "PRAGMA foreign_keys"));
}

test "scheduleInsert returns rowid; scheduleDelete reports removal" {
    var store = try StateStore.open(std.testing.allocator, ":memory:");
    defer store.close();

    const id1 = try store.scheduleInsert(1000, "{\"a\":1}");
    const id2 = try store.scheduleInsert(2000, "{}");
    try std.testing.expect(id2 > id1);

    try std.testing.expect(try store.scheduleDelete(id1)); // removed
    try std.testing.expect(!try store.scheduleDelete(id1)); // already gone
    try std.testing.expect(try store.scheduleDelete(id2));
}

test "scheduleMinFire ignores freshly-claimed rows" {
    var store = try StateStore.open(std.testing.allocator, ":memory:");
    defer store.close();
    _ = try store.scheduleInsert(5000, "{}");
    _ = try store.scheduleInsert(3000, "{}");

    // Reclaim cutoff far in the past → nothing reclaimable; both unclaimed.
    try std.testing.expectEqual(@as(?i64, 3000), try store.scheduleMinFire(0));

    // Claim everything due at now=10000 → both leased at 10000.
    const claimed = try store.scheduleClaimDue(10_000, 0, 16, std.testing.allocator);
    defer {
        for (claimed) |j| std.testing.allocator.free(j.payload);
        std.testing.allocator.free(claimed);
    }
    try std.testing.expectEqual(@as(usize, 2), claimed.len);

    // With cutoff=0, leased rows (claimed_at_ms=10000) are NOT reclaimable → null.
    try std.testing.expectEqual(@as(?i64, null), try store.scheduleMinFire(0));
    // With cutoff=20000 (> lease stamp), they become reclaimable again.
    try std.testing.expectEqual(@as(?i64, 3000), try store.scheduleMinFire(20_000));
}

test "scheduleClaimDue selects only due rows, ordered, capped, and stamps lease" {
    var store = try StateStore.open(std.testing.allocator, ":memory:");
    defer store.close();
    _ = try store.scheduleInsert(100, "{\"n\":1}");
    _ = try store.scheduleInsert(200, "{\"n\":2}");
    _ = try store.scheduleInsert(900, "{\"n\":9}"); // not due at now=300

    const claimed = try store.scheduleClaimDue(300, 0, 1, std.testing.allocator); // cap 1
    defer {
        for (claimed) |j| std.testing.allocator.free(j.payload);
        std.testing.allocator.free(claimed);
    }
    // cap=1 → only the earliest due row (fire_at_ms=100).
    try std.testing.expectEqual(@as(usize, 1), claimed.len);
    try std.testing.expectEqualStrings("{\"n\":1}", claimed[0].payload);

    // A second claim at the same now returns the next due row (first is leased).
    const claimed2 = try store.scheduleClaimDue(300, 0, 16, std.testing.allocator);
    defer {
        for (claimed2) |j| std.testing.allocator.free(j.payload);
        std.testing.allocator.free(claimed2);
    }
    try std.testing.expectEqual(@as(usize, 1), claimed2.len);
    try std.testing.expectEqualStrings("{\"n\":2}", claimed2[0].payload);
}

test "schedule table and index exist after open" {
    var store = try StateStore.open(std.testing.allocator, ":memory:");
    defer store.close();

    // The table is queryable (would error 'no such table' if absent).
    const tbl = try store.prepare("SELECT count(*) FROM schedule");
    defer _ = c.sqlite3_finalize(tbl);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(tbl));
    try std.testing.expectEqual(@as(c_int, 0), c.sqlite3_column_int(tbl, 0));

    // The fire-time index exists.
    const idx = try store.prepare(
        "SELECT count(*) FROM sqlite_schema WHERE type='index' AND name='idx_schedule_fire'",
    );
    defer _ = c.sqlite3_finalize(idx);
    try std.testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(idx));
    try std.testing.expectEqual(@as(c_int, 1), c.sqlite3_column_int(idx, 0));
}

test "open applies synchronous=NORMAL and foreign_keys=ON" {
    // pragma.sql sets synchronous=NORMAL and foreign_keys=ON, both
    // mandatory. A regression dropping either pragma is
    // otherwise undetected. The readback values are SQLite's canonical
    // encodings: synchronous NORMAL == 1, foreign_keys ON == 1.
    var store = try openMem();
    defer store.close();
    try testing.expectEqual(@as(i64, 1), try readPragmaInt(&store, "PRAGMA synchronous"));
    try testing.expectEqual(@as(i64, 1), try readPragmaInt(&store, "PRAGMA foreign_keys"));
}

test "beginDeferred commits a write and rolls one back" {
    // P4-7: the worker's resumed-coroutine path wraps each post-yield segment in
    // beginDeferred (lazy write lock). Commit must persist the segment's write;
    // rollback must discard it, leaving the last committed value intact.
    var store = try openMem();
    defer store.close();

    // Commit path: write under a deferred txn, commit, read back the new value.
    try store.beginDeferred();
    try store.setUserState(1, "{\"v\":\"committed\"}");
    try store.commit();
    {
        const data = try store.getUserState(1);
        defer testing.allocator.free(data);
        try testing.expectEqualStrings("{\"v\":\"committed\"}", data);
    }

    // Rollback path: write under a second deferred txn, roll back, confirm the
    // committed value is unchanged.
    try store.beginDeferred();
    try store.setUserState(1, "{\"v\":\"discarded\"}");
    store.rollback();
    {
        const data = try store.getUserState(1);
        defer testing.allocator.free(data);
        try testing.expectEqualStrings("{\"v\":\"committed\"}", data);
    }
}

test "scheduleClaimDue substitutes {} for a NULL payload" {
    // P4-27: scheduleClaimDue defensively duplicates "{}" when a claimed row's
    // payload column is SQL NULL. The shipped schema marks payload NOT NULL, so
    // the only way to reach that branch is to bypass the constraint. Recreate
    // the schedule table without NOT NULL (raw test-local SQL), insert a row
    // with a NULL payload, claim it, and assert the fallback fired.
    var store = try openMem();
    defer store.close();

    try testing.expectEqual(c.SQLITE_OK, c.sqlite3_exec(store.db, "DROP TABLE schedule", null, null, null));
    try testing.expectEqual(c.SQLITE_OK, c.sqlite3_exec(store.db,
        "CREATE TABLE schedule (id INTEGER PRIMARY KEY, fire_at_ms INTEGER NOT NULL, " ++
            "payload TEXT, claimed_at_ms INTEGER) STRICT", null, null, null));
    try testing.expectEqual(c.SQLITE_OK, c.sqlite3_exec(store.db,
        "INSERT INTO schedule (id, fire_at_ms, payload, claimed_at_ms) VALUES (1, 100, NULL, NULL)",
        null, null, null));

    const claimed = try store.scheduleClaimDue(200, 0, 16, testing.allocator);
    defer {
        for (claimed) |j| testing.allocator.free(j.payload);
        testing.allocator.free(claimed);
    }
    try testing.expectEqual(@as(usize, 1), claimed.len);
    try testing.expectEqualStrings("{}", claimed[0].payload);
}

test "fail_next_commit seam makes one commit fail, then auto-resets" {
    // Test-only fault seam used by the worker resume-path test (B4b): set
    // fail_next_commit, run a txn + write, and the next commit() errors without
    // issuing COMMIT (transaction left open for rollback). The flag clears
    // itself, so the following commit succeeds normally.
    var store = try openMem();
    defer store.close();

    store.fail_next_commit = true;
    try store.beginDeferred();
    try store.setUserState(1, "{\"v\":\"A\"}");
    try testing.expectError(error.SqliteError, store.commit());
    // The seam cleared itself.
    try testing.expect(!store.fail_next_commit);
    // The transaction is still open (commit was never issued); roll it back so
    // the write is discarded — the real failure-handling shape.
    store.rollback();
    {
        const data = try store.getUserState(1);
        defer testing.allocator.free(data);
        try testing.expectEqualStrings("{}", data);
    }

    // A subsequent normal commit succeeds: the seam is one-shot.
    try store.beginDeferred();
    try store.setUserState(1, "{\"v\":\"B\"}");
    try store.commit();
    {
        const data = try store.getUserState(1);
        defer testing.allocator.free(data);
        try testing.expectEqualStrings("{\"v\":\"B\"}", data);
    }
}

test "fresh plaintext DB records encryption=none" {
    var store = try StateStore.open(testing.allocator, ":memory:");
    defer store.close();
    const mode = try store.getMeta(testing.allocator, "encryption");
    defer if (mode) |m| testing.allocator.free(m);
    try testing.expect(mode != null);
    try testing.expectEqualStrings("none", mode.?);
}

test "fresh encrypted DB seeds marker, salt, params, verifier and sets cipher" {
    var store = try StateStore.openWithOptions(testing.allocator, ":memory:", .{
        .encryption = .{ .passphrase = "pw", .io = testing.io },
    });
    defer store.close();
    try testing.expect(store.cipher != null);
    const mode = try store.getMeta(testing.allocator, "encryption");
    defer if (mode) |m| testing.allocator.free(m);
    try testing.expectEqualStrings("xchacha20poly1305", mode.?);
    inline for (.{ "kdf_salt", "kdf_params", "enc_verifier" }) |k| {
        const v = try store.getMeta(testing.allocator, k);
        defer if (v) |x| testing.allocator.free(x);
        try testing.expect(v != null);
    }
}

test "reopening an encrypted DB with the right passphrase succeeds" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(testing.io, &path_buf);
    var db_buf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const db_path = try std.fmt.bufPrintZ(&db_buf, "{s}/e.db", .{path_buf[0..dir_len]});

    {
        var s = try StateStore.openWithOptions(testing.allocator, db_path, .{ .encryption = .{ .passphrase = "pw", .io = testing.io } });
        s.close();
    }
    var s2 = try StateStore.openWithOptions(testing.allocator, db_path, .{ .encryption = .{ .passphrase = "pw", .io = testing.io } });
    defer s2.close();
    try testing.expect(s2.cipher != null);
}

test "reopening an encrypted DB with the wrong passphrase aborts" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(testing.io, &path_buf);
    var db_buf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const db_path = try std.fmt.bufPrintZ(&db_buf, "{s}/e.db", .{path_buf[0..dir_len]});

    {
        var s = try StateStore.openWithOptions(testing.allocator, db_path, .{ .encryption = .{ .passphrase = "right", .io = testing.io } });
        s.close();
    }
    try testing.expectError(error.WrongEncryptionKey, StateStore.openWithOptions(testing.allocator, db_path, .{ .encryption = .{ .passphrase = "wrong", .io = testing.io } }));
}

test "mode mismatch aborts in both directions" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(testing.io, &path_buf);
    var db_buf: [std.fs.max_path_bytes + 16]u8 = undefined;

    // Encrypted DB opened without a passphrase → mismatch.
    const enc_path = try std.fmt.bufPrintZ(&db_buf, "{s}/m1.db", .{path_buf[0..dir_len]});
    {
        var s = try StateStore.openWithOptions(testing.allocator, enc_path, .{ .encryption = .{ .passphrase = "pw", .io = testing.io } });
        s.close();
    }
    try testing.expectError(error.EncryptionModeMismatch, StateStore.open(testing.allocator, enc_path));

    // Plaintext DB opened with a passphrase → mismatch.
    var db_buf2: [std.fs.max_path_bytes + 16]u8 = undefined;
    const plain_path = try std.fmt.bufPrintZ(&db_buf2, "{s}/m2.db", .{path_buf[0..dir_len]});
    {
        var s = try StateStore.open(testing.allocator, plain_path);
        s.close();
    }
    try testing.expectError(error.EncryptionModeMismatch, StateStore.openWithOptions(testing.allocator, plain_path, .{ .encryption = .{ .passphrase = "pw", .io = testing.io } }));
}

test "encrypted mode round-trips user/chat/global/schedule" {
    var store = try StateStore.openWithOptions(testing.allocator, ":memory:", .{ .encryption = .{ .passphrase = "pw", .io = testing.io } });
    defer store.close();

    try store.setUserState(1, "{\"count\":7}");
    const u = try store.getUserState(1);
    defer testing.allocator.free(u);
    try testing.expectEqualStrings("{\"count\":7}", u);

    try store.setChatState(-100, "{\"muted\":true}");
    const ch = try store.getChatState(-100);
    defer testing.allocator.free(ch);
    try testing.expectEqualStrings("{\"muted\":true}", ch);

    try store.setGlobal("total", "99");
    const g = try store.getGlobal("total");
    defer if (g) |v| testing.allocator.free(v);
    try testing.expectEqualStrings("99", g.?);

    const id = try store.scheduleInsert(1000, "{\"job\":1}");
    const claimed = try store.scheduleClaimDue(2000, 0, 16, testing.allocator);
    defer {
        for (claimed) |j| testing.allocator.free(j.payload);
        testing.allocator.free(claimed);
    }
    try testing.expectEqual(@as(usize, 1), claimed.len);
    try testing.expectEqual(id, claimed[0].id);
    try testing.expectEqualStrings("{\"job\":1}", claimed[0].payload);
}

test "on-disk bytes are not the plaintext in encrypted mode" {
    var store = try StateStore.openWithOptions(testing.allocator, ":memory:", .{ .encryption = .{ .passphrase = "pw", .io = testing.io } });
    defer store.close();
    try store.setUserState(1, "{\"secret\":\"swordfish\"}");

    // Read the raw stored column directly (bypassing decryption).
    const stmt = try store.prepare("SELECT data FROM user_state WHERE user_id = 1");
    defer _ = c.sqlite3_finalize(stmt);
    try testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    try testing.expectEqual(c.SQLITE_BLOB, c.sqlite3_column_type(stmt, 0));
    const raw = c.sqlite3_column_blob(stmt, 0);
    const len: usize = @intCast(c.sqlite3_column_bytes(stmt, 0));
    const bytes = @as([*]const u8, @ptrCast(raw))[0..len];
    try testing.expect(std.mem.indexOf(u8, bytes, "swordfish") == null);
    try testing.expect(len >= state_crypto.overhead);
}

test "a corrupted encrypted row reports DecryptFailed" {
    var store = try StateStore.openWithOptions(testing.allocator, ":memory:", .{ .encryption = .{ .passphrase = "pw", .io = testing.io } });
    defer store.close();
    try store.setUserState(1, "{\"x\":1}");
    // Corrupt the stored blob in place.
    try testing.expectEqual(c.SQLITE_OK, c.sqlite3_exec(store.db,
        "UPDATE user_state SET data = X'00000000000000000000000000000000000000000000000000000000' WHERE user_id = 1",
        null, null, null));
    try testing.expectError(error.DecryptFailed, store.getUserState(1));
}

test "insertScheduleRowRaw preserves id and claimed_at_ms (plaintext)" {
    var store = try openMem();
    defer store.close();

    try store.insertScheduleRowRaw(5, 1000, null, "{\"job\":\"a\"}");
    try store.insertScheduleRowRaw(6, 500, 1234, "{\"job\":\"b\"}");

    // Raw read-back on the same connection: ids, lease, and (plaintext) payload.
    const stmt = try store.prepare(
        "SELECT id, fire_at_ms, payload, claimed_at_ms FROM schedule ORDER BY id",
    );
    defer _ = c.sqlite3_finalize(stmt);

    try testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    try testing.expectEqual(@as(i64, 5), c.sqlite3_column_int64(stmt, 0));
    try testing.expectEqual(@as(i64, 1000), c.sqlite3_column_int64(stmt, 1));
    try testing.expectEqual(c.SQLITE_NULL, c.sqlite3_column_type(stmt, 3));

    try testing.expectEqual(c.SQLITE_ROW, c.sqlite3_step(stmt));
    try testing.expectEqual(@as(i64, 6), c.sqlite3_column_int64(stmt, 0));
    try testing.expectEqual(@as(i64, 1234), c.sqlite3_column_int64(stmt, 3));
}

test "insertScheduleRowRaw payload round-trips under encryption" {
    var store = try StateStore.openWithOptions(
        testing.allocator,
        ":memory:",
        .{ .encryption = .{ .passphrase = "pw", .io = testing.io } },
    );
    defer store.close();

    try store.insertScheduleRowRaw(7, 50, null, "{\"x\":true}");

    // scheduleClaimDue is the only read path that decrypts a schedule payload.
    const jobs = try store.scheduleClaimDue(100, 0, 10, testing.allocator);
    defer {
        for (jobs) |j| testing.allocator.free(j.payload);
        testing.allocator.free(jobs);
    }
    try testing.expectEqual(@as(usize, 1), jobs.len);
    try testing.expectEqual(@as(i64, 7), jobs[0].id);
    try testing.expectEqualStrings("{\"x\":true}", jobs[0].payload);
}
