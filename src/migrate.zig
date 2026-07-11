/// migrate.zig — zora-migrate: upgrade a v1 plaintext state database to v2.
///
/// Reads every row from a v1 (TEXT-column, schema_version='1') database and
/// writes it into a freshly-built v2 database via StateStore, which encrypts on
/// write when STATE_ENCRYPTION_KEY is set. The output is built in a temp file and
/// renamed only after a self-verify reopen succeeds; the source is never touched.
const std = @import("std");
const builtin = @import("builtin");

const state_store = @import("state_store.zig");

// The tool's own SQLite binding for the read-only source connection. This is a
// distinct type from state_store's internal `c`: this file reads the source and
// writes the output only through StateStore's public API — the two never mix.
const c = @cImport({
    @cInclude("sqlite3.h");
});

const log = std.log.scoped(.migrate);

/// Row counts copied per table, for the summary line.
const Counts = struct { user: u64 = 0, chat: u64 = 0, global: u64 = 0, schedule: u64 = 0 };

const ParsedArgs = struct { in_path: [:0]const u8, out_path: [:0]const u8 };

const ArgError = error{ MissingIn, MissingOut, UnknownFlag };

/// Parse `--in <path> --out <path>` from argv (argv[0] is the program name).
/// Returns an ArgError on a missing value, missing flag, or unknown flag; the
/// caller prints the usage line.
fn parseArgs(argv: []const [:0]const u8) ArgError!ParsedArgs {
    var in_path: ?[:0]const u8 = null;
    var out_path: ?[:0]const u8 = null;
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const a = argv[i];
        if (std.mem.eql(u8, a, "--in")) {
            i += 1;
            if (i >= argv.len) return ArgError.MissingIn;
            in_path = argv[i];
        } else if (std.mem.eql(u8, a, "--out")) {
            i += 1;
            if (i >= argv.len) return ArgError.MissingOut;
            out_path = argv[i];
        } else return ArgError.UnknownFlag;
    }
    return .{
        .in_path = in_path orelse return ArgError.MissingIn,
        .out_path = out_path orelse return ArgError.MissingOut,
    };
}

/// Best-effort process hardening, run before the tool reads a secret: keep the
/// passphrase, derived key, and transient plaintext out of swap and core dumps.
/// Each measure logs at warn and continues on failure — a missing lock is a
/// hardening shortfall, not a correctness failure.
fn harden() void {
    if (std.c.mlockall(.{ .CURRENT = true, .FUTURE = true }) != 0)
        log.warn("mlockall failed — sensitive data may reach swap", .{});

    std.posix.setrlimit(.CORE, .{ .cur = 0, .max = 0 }) catch
        log.warn("setrlimit(RLIMIT_CORE, 0) failed — core dumps not suppressed", .{});

    if (builtin.os.tag == .linux) {
        const rc = std.os.linux.prctl(@intFromEnum(std.os.linux.PR.SET_DUMPABLE), 0, 0, 0, 0);
        if (rc != 0) log.warn("prctl(PR_SET_DUMPABLE, 0) failed", .{});
    }
}

const MigrateError = error{ OutputExists, SourceNotV1, SqliteError };

/// First key seen in each per-key table, captured during the copy so self-verify
/// can decrypt-read one real row per table (the verifier alone only proves the
/// key decrypts a known constant). global_key is owned; the caller frees it.
const Samples = struct {
    user_id: ?i64 = null,
    chat_id: ?i64 = null,
    global_key: ?[]u8 = null,
};

/// Prepare a statement on the raw source connection.
fn prep(db: *c.sqlite3, sql: [:0]const u8) MigrateError!*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, sql.ptr, -1, &stmt, null) != c.SQLITE_OK)
        return MigrateError.SqliteError;
    return stmt.?;
}

/// Step to the next row: true when a row is ready, false at end of table.
/// Any step code other than SQLITE_ROW or SQLITE_DONE is a real error and must
/// not be treated as end-of-table (a silent partial copy).
fn nextRow(stmt: *c.sqlite3_stmt) MigrateError!bool {
    return switch (c.sqlite3_step(stmt)) {
        c.SQLITE_ROW => true,
        c.SQLITE_DONE => false,
        else => MigrateError.SqliteError,
    };
}

/// Read a TEXT column as a slice valid until the statement is re-stepped. A NULL
/// column reads as "{}" (matching the v1 column default), so a copied row is
/// never empty.
fn colText(stmt: *c.sqlite3_stmt, col: c_int) []const u8 {
    const txt = c.sqlite3_column_text(stmt, col);
    const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
    return if (txt != null) txt[0..len] else "{}";
}

/// Open `path` read-only and confirm it is a schema-v1 database. Returns the open
/// connection (caller closes) or an error. Diagnostics log at warn (the caller
/// handles the error), never err. The connection is never written.
fn openSourceV1(path: [:0]const u8) MigrateError!*c.sqlite3 {
    var raw: ?*c.sqlite3 = null;
    if (c.sqlite3_open_v2(path.ptr, &raw, c.SQLITE_OPEN_READONLY, null) != c.SQLITE_OK) {
        if (raw) |db| _ = c.sqlite3_close(db);
        log.warn("cannot open source database '{s}'", .{path});
        return MigrateError.SourceNotV1;
    }
    errdefer _ = c.sqlite3_close(raw.?);

    const stmt = try prep(raw.?, "SELECT value FROM meta WHERE key = 'schema_version'");
    defer _ = c.sqlite3_finalize(stmt);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) {
        log.warn("source '{s}' has no schema_version — not a v1 zora database", .{path});
        return MigrateError.SourceNotV1;
    }
    const ver = colText(stmt, 0);
    if (!std.mem.eql(u8, ver, "1")) {
        log.warn("source schema_version is '{s}', expected '1' — nothing to migrate", .{ver});
        return MigrateError.SourceNotV1;
    }
    return raw.?;
}

/// Copy every payload row from the v1 source into the v2 store inside one
/// transaction, capturing the first key per table into `samples`.
fn copyAll(
    gpa: std.mem.Allocator,
    out: *state_store.StateStore,
    src: *c.sqlite3,
    counts: *Counts,
    samples: *Samples,
) !void {
    try out.beginImmediate();
    errdefer out.rollback();

    {
        const stmt = try prep(src, "SELECT user_id, data FROM user_state");
        defer _ = c.sqlite3_finalize(stmt);
        while (try nextRow(stmt)) {
            const id = c.sqlite3_column_int64(stmt, 0);
            try out.setUserState(id, colText(stmt, 1));
            if (samples.user_id == null) samples.user_id = id;
            counts.user += 1;
        }
    }
    {
        const stmt = try prep(src, "SELECT chat_id, data FROM chat_state");
        defer _ = c.sqlite3_finalize(stmt);
        while (try nextRow(stmt)) {
            const id = c.sqlite3_column_int64(stmt, 0);
            try out.setChatState(id, colText(stmt, 1));
            if (samples.chat_id == null) samples.chat_id = id;
            counts.chat += 1;
        }
    }
    {
        const stmt = try prep(src, "SELECT key, value FROM global_state");
        defer _ = c.sqlite3_finalize(stmt);
        while (try nextRow(stmt)) {
            const key = colText(stmt, 0);
            try out.setGlobal(key, colText(stmt, 1));
            if (samples.global_key == null) samples.global_key = try gpa.dupe(u8, key);
            counts.global += 1;
        }
    }
    {
        const stmt = try prep(src, "SELECT id, fire_at_ms, payload, claimed_at_ms FROM schedule");
        defer _ = c.sqlite3_finalize(stmt);
        while (try nextRow(stmt)) {
            const id = c.sqlite3_column_int64(stmt, 0);
            const fire = c.sqlite3_column_int64(stmt, 1);
            const payload = colText(stmt, 2);
            const claimed: ?i64 = if (c.sqlite3_column_type(stmt, 3) == c.SQLITE_NULL)
                null
            else
                c.sqlite3_column_int64(stmt, 3);
            try out.insertScheduleRowRaw(id, fire, claimed, payload);
            counts.schedule += 1;
        }
    }

    try out.commit();
}

/// Reopen the freshly-written database with the same options to confirm the
/// server could open it: the reopen re-runs the v2 schema-version check and, in
/// encrypted mode, the verifier check. Then decrypt-read one real row from each
/// per-key table (when present): the verifier is a fixed constant written once,
/// so this catches a write-path or corruption fault in actual payload data that
/// the verifier alone would miss. Schedule rows are not read here — the only
/// decrypt path for them claims (mutates) the row, and the verified file must be
/// the file that ships.
fn selfVerify(
    gpa: std.mem.Allocator,
    path: [:0]const u8,
    opts: state_store.OpenOptions,
    samples: Samples,
) !void {
    var s = try state_store.StateStore.openWithOptions(gpa, path, opts);
    defer s.close();
    if (samples.user_id) |id| gpa.free(try s.getUserState(id));
    if (samples.chat_id) |id| gpa.free(try s.getChatState(id));
    if (samples.global_key) |k| {
        if (try s.getGlobal(k)) |v| gpa.free(v);
    }
}

/// Convert the v1 plaintext database at `in_path` into a v2 database at
/// `out_path`. When `key` is non-null the output is encrypted; otherwise it is
/// plaintext v2. The source is never modified. Returns per-table row counts; on
/// any failure no file is left at `out_path` (a temp sibling is removed).
pub fn migrate(
    gpa: std.mem.Allocator,
    io: std.Io,
    in_path: [:0]const u8,
    out_path: [:0]const u8,
    key: ?[]const u8,
) !Counts {
    const cwd = std.Io.Dir.cwd();

    // Refuse to clobber an existing destination.
    if (statExists(cwd, io, out_path)) {
        log.warn("output '{s}' already exists — refusing to overwrite", .{out_path});
        return MigrateError.OutputExists;
    }

    // Validate the source before creating anything.
    const src = try openSourceV1(in_path);
    defer _ = c.sqlite3_close(src);

    // Build to a temp sibling; rename only on full success.
    var tmp_buf: [std.fs.max_path_bytes + 8]u8 = undefined;
    const out_tmp = try std.fmt.bufPrintZ(&tmp_buf, "{s}.tmp", .{out_path});
    deleteTempSet(cwd, io, out_tmp); // clear any stale temp (and WAL sidecars) from a prior run
    errdefer deleteTempSet(cwd, io, out_tmp);

    const opts: state_store.OpenOptions = if (key) |k|
        .{ .encryption = .{ .passphrase = k, .io = io } }
    else
        .{};

    var counts = Counts{};
    var samples = Samples{};
    defer if (samples.global_key) |k| gpa.free(k);

    var out = try state_store.StateStore.openWithOptions(gpa, out_tmp, opts);
    copyAll(gpa, &out, src, &counts, &samples) catch |e| {
        out.close();
        return e;
    };
    out.close();

    try selfVerify(gpa, out_tmp, opts, samples);
    try cwd.rename(out_tmp, cwd, out_path, io);
    return counts;
}

/// True when `path` exists (a successful stat). Used only to refuse clobbering.
fn statExists(dir: std.Io.Dir, io: std.Io, path: [:0]const u8) bool {
    _ = dir.statFile(io, path, .{}) catch return false;
    return true;
}

/// Best-effort removal of the temp database and its WAL sidecars. In WAL mode
/// SQLite writes `<path>-wal` and `<path>-shm` beside the file, and a crash mid-
/// migration can leave them; cleanup must clear all three so no orphaned sidecar
/// outlives a failed run. A clean close checkpoints and removes the sidecars on
/// its own, so on the success path these deletes are no-ops.
fn deleteTempSet(dir: std.Io.Dir, io: std.Io, out_tmp: []const u8) void {
    dir.deleteFile(io, out_tmp) catch {};
    var buf: [std.fs.max_path_bytes + 12]u8 = undefined;
    for ([_][]const u8{ "-wal", "-shm" }) |suffix| {
        const p = std.fmt.bufPrint(&buf, "{s}{s}", .{ out_tmp, suffix }) catch continue;
        dir.deleteFile(io, p) catch {};
    }
}

/// Entry point. Hardens the process first, then parses argv, owns the
/// passphrase (zeroed on every exit path), runs the migration, and prints a
/// summary line. This is the only site in `migrate.zig` that logs at `err`;
/// it is never unit-tested, so its error logs cannot trip the build-fail rule.
pub fn main(init: std.process.Init) u8 {
    // Harden before reading any secret: no passphrase, key, or plaintext should
    // ever exist in a swappable or dumpable state.
    harden();

    const gpa = std.heap.c_allocator;
    const io = init.io;

    const argv = init.minimal.args.toSlice(init.arena.allocator()) catch {
        log.err("cannot read command-line arguments", .{});
        return 1;
    };

    const args = parseArgs(argv) catch {
        log.err("usage: zora-migrate --in <v1.db> --out <v2.db>  (STATE_ENCRYPTION_KEY enables encryption)", .{});
        return 2;
    };

    // Own the passphrase so it can be zeroed after the last key derivation. The
    // presence of STATE_ENCRYPTION_KEY selects the output mode, exactly as the
    // server gates encryption.
    const key: ?[]u8 = if (init.environ_map.get("STATE_ENCRYPTION_KEY")) |v|
        gpa.dupe(u8, v) catch {
            log.err("out of memory", .{});
            return 1;
        }
    else
        null;
    defer if (key) |k| {
        std.crypto.secureZero(u8, k);
        gpa.free(k);
    };

    const counts = migrate(gpa, io, args.in_path, args.out_path, key) catch |e| {
        log.err("migration failed: {s}", .{@errorName(e)});
        return 1;
    };

    log.info("migrated user_state={d} chat_state={d} global_state={d} schedule={d}", .{
        counts.user, counts.chat, counts.global, counts.schedule,
    });
    log.info("wrote '{s}' (mode={s})", .{
        args.out_path,
        if (key != null) "encrypted" else "plaintext",
    });
    return 0;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "parseArgs reads --in and --out" {
    const argv = [_][:0]const u8{ "zora-migrate", "--in", "a.db", "--out", "b.db" };
    const a = try parseArgs(&argv);
    try testing.expectEqualStrings("a.db", a.in_path);
    try testing.expectEqualStrings("b.db", a.out_path);
}

test "parseArgs errors on a missing --out" {
    const argv = [_][:0]const u8{ "zora-migrate", "--in", "a.db" };
    try testing.expectError(ArgError.MissingOut, parseArgs(&argv));
}

test "parseArgs errors on an unknown flag" {
    const argv = [_][:0]const u8{ "zora-migrate", "--wat" };
    try testing.expectError(ArgError.UnknownFlag, parseArgs(&argv));
}

/// Write a v1 (TEXT-column, schema_version='1') database at `path` with rows in
/// every table: one user, one chat, one global, and two schedule rows — id 5
/// unclaimed at fire 1000, id 6 claimed (lease 1234) at the *earlier* fire 500.
/// The earlier fire on the claimed row lets a test prove the lease survived: a
/// row wrongly migrated as unclaimed would lower scheduleMinFire(0).
fn writeV1Fixture(path: [:0]const u8) !void {
    var raw: ?*c.sqlite3 = null;
    try testing.expectEqual(c.SQLITE_OK, c.sqlite3_open_v2(
        path.ptr,
        &raw,
        c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE,
        null,
    ));
    defer _ = c.sqlite3_close(raw.?);
    const ddl =
        \\CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
        \\CREATE TABLE user_state (user_id INTEGER PRIMARY KEY, data TEXT NOT NULL DEFAULT '{}') STRICT;
        \\CREATE TABLE chat_state (chat_id INTEGER PRIMARY KEY, data TEXT NOT NULL DEFAULT '{}') STRICT;
        \\CREATE TABLE global_state (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
        \\CREATE TABLE schedule (id INTEGER PRIMARY KEY, fire_at_ms INTEGER NOT NULL, payload TEXT NOT NULL DEFAULT '{}', claimed_at_ms INTEGER) STRICT;
        \\INSERT INTO meta VALUES ('schema_version', '1');
        \\INSERT INTO user_state VALUES (42, '{"count":7}');
        \\INSERT INTO chat_state VALUES (-100, '{"muted":true}');
        \\INSERT INTO global_state VALUES ('total', '"99"');
        \\INSERT INTO schedule VALUES (5, 1000, '{"job":"a"}', NULL);
        \\INSERT INTO schedule VALUES (6, 500, '{"job":"b"}', 1234);
    ;
    try testing.expectEqual(c.SQLITE_OK, c.sqlite3_exec(raw.?, ddl, null, null, null));
}

/// Resolve a tmpDir to an absolute path and join `name` onto it, NUL-terminated.
fn joinZ(buf: []u8, dir_path: []const u8, name: []const u8) [:0]const u8 {
    return std.fmt.bufPrintZ(buf, "{s}/{s}", .{ dir_path, name }) catch unreachable;
}

test "migrate v1 plaintext -> v2 plaintext preserves every row" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var dbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dlen = try tmp.dir.realPath(testing.io, &dbuf);
    const dir = dbuf[0..dlen];

    var ibuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    var obuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const in_path = joinZ(&ibuf, dir, "v1.db");
    const out_path = joinZ(&obuf, dir, "v2.db");

    try writeV1Fixture(in_path);
    const counts = try migrate(testing.allocator, testing.io, in_path, out_path, null);
    try testing.expectEqual(@as(u64, 1), counts.user);
    try testing.expectEqual(@as(u64, 1), counts.chat);
    try testing.expectEqual(@as(u64, 1), counts.global);
    try testing.expectEqual(@as(u64, 2), counts.schedule);

    // The new binary opens it (schema v2, encryption=none) and reads it back.
    var s = try state_store.StateStore.open(testing.allocator, out_path);
    defer s.close();

    const u = try s.getUserState(42);
    defer testing.allocator.free(u);
    try testing.expectEqualStrings("{\"count\":7}", u);

    const g = (try s.getGlobal("total")).?;
    defer testing.allocator.free(g);
    try testing.expectEqualStrings("\"99\"", g);

    const mode = (try s.getMeta(testing.allocator, "encryption")).?;
    defer testing.allocator.free(mode);
    try testing.expectEqualStrings("none", mode);

    // Lease preserved: only the unclaimed row (id 5, fire 1000) is claimable at
    // cutoff 0; if id 6 had lost its lease, min fire would be 500.
    try testing.expectEqual(@as(?i64, 1000), try s.scheduleMinFire(0));

    // Both schedule rows migrated with ids and payloads intact (cutoff 2000
    // reclaims id 6's lease of 1234 so both come back).
    const jobs = try s.scheduleClaimDue(3000, 2000, 10, testing.allocator);
    defer {
        for (jobs) |j| testing.allocator.free(j.payload);
        testing.allocator.free(jobs);
    }
    try testing.expectEqual(@as(usize, 2), jobs.len);
}

test "migrate v1 plaintext -> v2 encrypted; new binary reads it back" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var dbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dlen = try tmp.dir.realPath(testing.io, &dbuf);
    const dir = dbuf[0..dlen];

    var ibuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    var obuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const in_path = joinZ(&ibuf, dir, "v1.db");
    const out_path = joinZ(&obuf, dir, "v2enc.db");

    try writeV1Fixture(in_path);
    _ = try migrate(testing.allocator, testing.io, in_path, out_path, "correct horse");

    // Right passphrase reads the data back.
    var s = try state_store.StateStore.openWithOptions(
        testing.allocator,
        out_path,
        .{ .encryption = .{ .passphrase = "correct horse", .io = testing.io } },
    );
    defer s.close();
    const u = try s.getUserState(42);
    defer testing.allocator.free(u);
    try testing.expectEqualStrings("{\"count\":7}", u);

    // Schedule payloads decrypt too.
    const jobs = try s.scheduleClaimDue(3000, 2000, 10, testing.allocator);
    defer {
        for (jobs) |j| testing.allocator.free(j.payload);
        testing.allocator.free(jobs);
    }
    try testing.expectEqual(@as(usize, 2), jobs.len);

    // Wrong passphrase is rejected by the verifier.
    try testing.expectError(error.WrongEncryptionKey, state_store.StateStore.openWithOptions(
        testing.allocator,
        out_path,
        .{ .encryption = .{ .passphrase = "wrong", .io = testing.io } },
    ));
}

test "migrate refuses to overwrite an existing output" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var dbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dlen = try tmp.dir.realPath(testing.io, &dbuf);
    const dir = dbuf[0..dlen];

    var ibuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    var obuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const in_path = joinZ(&ibuf, dir, "v1.db");
    const out_path = joinZ(&obuf, dir, "exists.db");

    try writeV1Fixture(in_path);
    try writeV1Fixture(out_path); // out already exists
    try testing.expectError(MigrateError.OutputExists, migrate(testing.allocator, testing.io, in_path, out_path, null));
}

test "migrate refuses a source that is not v1" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var dbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dlen = try tmp.dir.realPath(testing.io, &dbuf);
    const dir = dbuf[0..dlen];

    var ibuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    var obuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const in_path = joinZ(&ibuf, dir, "v2src.db");
    const out_path = joinZ(&obuf, dir, "out.db");

    // A v2 database (StateStore seeds schema_version='2').
    var s = try state_store.StateStore.open(testing.allocator, in_path);
    s.close();

    try testing.expectError(MigrateError.SourceNotV1, migrate(testing.allocator, testing.io, in_path, out_path, null));
}

test "migrate handles an empty v1 database" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var dbuf: [std.fs.max_path_bytes]u8 = undefined;
    const dlen = try tmp.dir.realPath(testing.io, &dbuf);
    const dir = dbuf[0..dlen];

    var ibuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    var obuf: [std.fs.max_path_bytes + 16]u8 = undefined;
    const in_path = joinZ(&ibuf, dir, "empty.db");
    const out_path = joinZ(&obuf, dir, "emptyout.db");

    // v1 schema, schema_version='1', but no data rows.
    var raw: ?*c.sqlite3 = null;
    try testing.expectEqual(c.SQLITE_OK, c.sqlite3_open_v2(
        in_path.ptr,
        &raw,
        c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE,
        null,
    ));
    const ddl =
        \\CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
        \\CREATE TABLE user_state (user_id INTEGER PRIMARY KEY, data TEXT NOT NULL DEFAULT '{}') STRICT;
        \\CREATE TABLE chat_state (chat_id INTEGER PRIMARY KEY, data TEXT NOT NULL DEFAULT '{}') STRICT;
        \\CREATE TABLE global_state (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
        \\CREATE TABLE schedule (id INTEGER PRIMARY KEY, fire_at_ms INTEGER NOT NULL, payload TEXT NOT NULL DEFAULT '{}', claimed_at_ms INTEGER) STRICT;
        \\INSERT INTO meta VALUES ('schema_version', '1');
    ;
    try testing.expectEqual(c.SQLITE_OK, c.sqlite3_exec(raw.?, ddl, null, null, null));
    _ = c.sqlite3_close(raw.?);

    const counts = try migrate(testing.allocator, testing.io, in_path, out_path, null);
    try testing.expectEqual(@as(u64, 0), counts.user);

    var s = try state_store.StateStore.open(testing.allocator, out_path);
    s.close();
}
