/// tg_schema.zig — Telegram Bot API schema: parsing, validation, hot-watcher.
///
/// The schema is the *data* side of outgoing-call validation.
/// It is a vendored JSON file (`schema/botapi.json`, PaulSonOfLars format):
///
///   { "methods": { "<name>": { "fields": [ { name, types, required }, ... ] } } }
///
/// Each process parses one immutable `SchemaStore`. `SchemaSlot` holds it
/// behind an atomic pointer so the watcher can swap in a re-parsed copy
/// without locking the read path. The validator (`validate`) is the *logic*
/// side — it lives here, in the Zig core, because the structural-check
/// algorithm is stable.

const std = @import("std");
const builtin = @import("builtin");
const ziglua = @import("ziglua");
const watcher = @import("watcher.zig");
const rt = @import("rt.zig");

const Lua = ziglua.Lua;
const log = std.log.scoped(.schema);

/// Hard cap on the schema file size — guards against a runaway file.
const MAX_SCHEMA_BYTES: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// JSON shape (PaulSonOfLars api.json) — only the fields in use are declared;
// `ignore_unknown_fields` drops name/href/description/returns etc.
// ---------------------------------------------------------------------------

const FieldJson = struct {
    name: []const u8,
    types: [][]const u8 = &.{},
    required: bool = false,
};

pub const MethodJson = struct {
    fields: []FieldJson = &.{},
};

const SchemaJson = struct {
    methods: std.json.ArrayHashMap(MethodJson),
};

// ---------------------------------------------------------------------------
// SchemaStore — one immutable parsed schema
// ---------------------------------------------------------------------------

pub const SchemaStore = struct {
    parsed: std.json.Parsed(SchemaJson),

    /// Parse JSON bytes into a heap-allocated SchemaStore.
    /// Caller owns the result; release it with `destroy`.
    pub fn fromSlice(gpa: std.mem.Allocator, bytes: []const u8) !*SchemaStore {
        const self = try gpa.create(SchemaStore);
        errdefer gpa.destroy(self);
        self.parsed = try std.json.parseFromSlice(
            SchemaJson,
            gpa,
            bytes,
            .{ .ignore_unknown_fields = true, .allocate = .alloc_always },
        );
        return self;
    }

    /// Read and parse `path` into a heap-allocated SchemaStore.
    pub fn fromFile(gpa: std.mem.Allocator, io: std.Io, path: []const u8) !*SchemaStore {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, gpa, .limited(MAX_SCHEMA_BYTES));
        defer gpa.free(bytes);
        return fromSlice(gpa, bytes);
    }

    pub fn destroy(self: *SchemaStore, gpa: std.mem.Allocator) void {
        self.parsed.deinit();
        gpa.destroy(self);
    }

    /// Look up a method's spec by name. Returns null for unknown methods.
    pub fn method(self: *const SchemaStore, name: []const u8) ?MethodJson {
        return self.parsed.value.methods.map.get(name);
    }
};

// ---------------------------------------------------------------------------
// Validation — structural only: known method, required params present,
// primitive types match. Never semantic.
// ---------------------------------------------------------------------------

pub const ValidationError = error{
    UnknownMethod,
    MissingRequired,
    TypeMismatch,
};

/// Primitive class a schema field accepts. `compound` = object/array;
/// `any` = a union of differing primitives or an unrecognised type name —
/// such fields are not type-checked (conservative: never a false rejection).
const ParamKind = enum { string, integer, float, boolean, compound, any };

fn classifyOne(t: []const u8) ParamKind {
    if (std.mem.eql(u8, t, "Integer")) return .integer;
    if (std.mem.eql(u8, t, "String")) return .string;
    if (std.mem.eql(u8, t, "Boolean")) return .boolean;
    if (std.mem.eql(u8, t, "True")) return .boolean;
    if (std.mem.eql(u8, t, "Float")) return .float;
    if (std.mem.eql(u8, t, "Float number")) return .float;
    // InputFile accepts both a string (file_id / URL) and a binary upload — treat
    // it as unclassifiable so the validator never produces a false-positive TypeMismatch when
    // a rule passes a string file_id.
    if (std.mem.eql(u8, t, "InputFile")) return .any;
    // "Array of ...", concrete object type names, etc.
    return .compound;
}

/// Reduce a field's `types` array to one ParamKind. A union of differing
/// kinds collapses to `any` (unclassifiable → not checked).
fn classify(types_arr: [][]const u8) ParamKind {
    if (types_arr.len == 0) return .any;
    var acc: ?ParamKind = null;
    for (types_arr) |t| {
        const k = classifyOne(t);
        if (acc == null) {
            acc = k;
        } else if (acc.? != k) {
            return .any;
        }
    }
    return acc.?;
}

fn kindMatchesType(kind: ParamKind, vt: ziglua.LuaType) bool {
    return switch (kind) {
        .any => true,
        .string => vt == .string,
        .integer, .float => vt == .number,
        .boolean => vt == .boolean,
        .compound => vt == .table,
    };
}

/// Validate one outgoing call structurally. `params_idx` is the absolute
/// stack index of the call's `params` value (a table, or nil/absent).
/// Net stack effect: zero. Never raises a Lua error, never panics.
pub fn validate(
    store: *const SchemaStore,
    lua: *Lua,
    params_idx: i32,
    method_name: []const u8,
) ValidationError!void {
    const m = store.method(method_name) orelse return error.UnknownMethod;
    const params_is_table = lua.isTable(params_idx);

    for (m.fields) |f| {
        if (!params_is_table) {
            // No params table at all: every required field is missing.
            if (f.required) return error.MissingRequired;
            continue;
        }

        var namebuf: [128]u8 = undefined;
        const key: [:0]const u8 = std.mem.printSentinel(&namebuf, "{s}", .{f.name}, 0) catch {
            // Implausible for a real Telegram API name; skip conservatively
            // (no false rejections) but log so it's visible if it ever fires.
            log.warn("validate: field name '{s}' exceeds buffer, skipping check", .{f.name});
            continue;
        };

        const vt: ziglua.LuaType = lua.getField(params_idx, key);
        defer lua.pop(1);

        if (vt == .nil) {
            if (f.required) return error.MissingRequired;
            continue;
        }
        if (!kindMatchesType(classify(f.types), vt)) return error.TypeMismatch;
    }
}

// ---------------------------------------------------------------------------
// SchemaSlot — the hot-reloadable holder. One per process.
//
// `current` is read lock-free by workers. The watcher swaps it under `mutex`
// and pushes the displaced store onto `retired`. Retired stores are freed
// only by `deinit` — never during the run — so a reader holding a stale
// pointer can never use-after-free.
// ---------------------------------------------------------------------------

pub const SchemaSlot = struct {
    gpa: std.mem.Allocator,
    current: std.atomic.Value(?*SchemaStore) = .init(null),
    version: std.atomic.Value(u64) = .init(0),
    failures: std.atomic.Value(u64) = .init(0),
    mutex: std.Io.Mutex = .init,
    io: std.Io,
    retired: std.ArrayListUnmanaged(*SchemaStore) = .empty,

    pub fn init(gpa: std.mem.Allocator, io: std.Io) SchemaSlot {
        return .{ .gpa = gpa, .io = io };
    }

    /// Current schema, or null when none is loaded (Tier-0). Lock-free.
    pub fn get(self: *SchemaSlot) ?*const SchemaStore {
        return self.current.load(.acquire);
    }

    /// Swap in `new_store`, retire the previous one, bump `version`.
    pub fn install(self: *SchemaSlot, new_store: *SchemaStore) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const old = self.current.swap(new_store, .release);
        if (old) |o| self.retired.append(self.gpa, o) catch {
            // OOM: cannot defer this free to deinit; free it immediately so
            // the displaced store does not leak for the process lifetime.
            log.err("schema slot: OOM tracking retired store — freeing it immediately", .{});
            o.destroy(self.gpa);
        };
        _ = self.version.fetchAdd(1, .release);
    }

    /// Free the current store and every retired store. Call once, at
    /// shutdown, after all readers and the watcher have stopped.
    pub fn deinit(self: *SchemaSlot) void {
        // Swap current to null under the mutex so a concurrent install() that
        // hasn't been stopped yet cannot displace the same pointer into retired
        // after deinit has already freed it (double-free).
        self.mutex.lockUncancelable(self.io);
        const c = self.current.swap(null, .release);
        self.mutex.unlock(self.io);
        if (c) |store| store.destroy(self.gpa);
        for (self.retired.items) |r| r.destroy(self.gpa);
        self.retired.deinit(self.gpa);
    }
};

/// Parse `path` at startup and install it. On any failure, log a warning and
/// leave the slot empty — the process runs in Tier-0 (no validation).
pub fn loadInitial(slot: *SchemaSlot, path: []const u8) void {
    const store = SchemaStore.fromFile(slot.gpa, slot.io, path) catch |err| {
        log.warn(
            "schema '{s}': {s} — running without validation (Tier-0)",
            .{ path, @errorName(err) },
        );
        return;
    };
    slot.install(store);
    log.info("schema loaded from '{s}'", .{path});
}

// ---------------------------------------------------------------------------
// Schema file watcher — re-parses SCHEMA_FILE on change and swaps the slot.
// A parse failure increments `failures` and keeps the previous schema.
// Reuses watcher.zig's kernel watchers.
// ---------------------------------------------------------------------------

const SchemaWatchCtx = struct {
    slot: *SchemaSlot,
    path: []const u8,
};

fn onSchemaChange(context: *anyopaque) void {
    const ctx: *SchemaWatchCtx = @alignCast(@ptrCast(context));
    const store = SchemaStore.fromFile(ctx.slot.gpa, ctx.slot.io, ctx.path) catch |err| {
        // fetchAdd returns the previous value; +1 is the count including this failure.
        const n_failures = ctx.slot.failures.fetchAdd(1, .release) + 1;
        log.warn(
            "schema reload failed ({s}) — keeping previous schema ({d} failed reloads since start)",
            .{ @errorName(err), n_failures },
        );
        return;
    };
    ctx.slot.install(store);
    log.info("schema reloaded (version {d})", .{ctx.slot.version.load(.acquire)});
}

pub const SchemaWatcherArgs = struct {
    schema_file: []const u8,
    slot: *SchemaSlot,
    allocator: std.mem.Allocator,
};

/// Watcher thread entry point. Normally never returns (kernel watcher loops
/// forever). Duplicates schema_file so the caller can free its copy immediately
/// after spawning. The defer executes only if the underlying watcher returns
/// early due to a system error (e.g. inotify fd exhaustion) — in the normal
/// infinite-loop case it is unreachable.
pub fn schemaWatcherThread(args: SchemaWatcherArgs) void {
    const owned_path = args.allocator.dupe(u8, args.schema_file) catch {
        log.err("schemaWatcherThread: OOM duplicating schema path — watcher not started", .{});
        return;
    };
    defer args.allocator.free(owned_path);
    var ctx = SchemaWatchCtx{ .slot = args.slot, .path = owned_path };
    const target = watcher.WatchTarget{
        .path = owned_path,
        .context = &ctx,
        .on_write = onSchemaChange,
    };
    switch (builtin.os.tag) {
        .linux => watcher.watchInotify(target),
        .freebsd => watcher.watchKqueue(target),
        // Linux and FreeBSD are the only supported targets; fail the build
        // rather than ship a watcher that silently does nothing.
        else => @compileError("schema watcher: unsupported OS (Linux and FreeBSD only)"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// A minimal valid schema used across tg_schema tests.
const FIXTURE_SCHEMA =
    \\{"methods":{
    \\  "sendMessage":{"fields":[
    \\    {"name":"chat_id","types":["Integer","String"],"required":true},
    \\    {"name":"text","types":["String"],"required":true},
    \\    {"name":"parse_mode","types":["String"],"required":false}
    \\  ]},
    \\  "sendDice":{"fields":[
    \\    {"name":"chat_id","types":["Integer","String"],"required":true}
    \\  ]}
    \\},"types":{}}
;

test "fromSlice parses a schema and rejects malformed JSON" {
    // A valid schema exposes its methods and fields; unknown methods are null.
    {
        const store = try SchemaStore.fromSlice(testing.allocator, FIXTURE_SCHEMA);
        defer store.destroy(testing.allocator);

        const sm = store.method("sendMessage") orelse return error.TestUnexpectedNull;
        try testing.expectEqual(@as(usize, 3), sm.fields.len);
        try testing.expectEqualStrings("chat_id", sm.fields[0].name);
        try testing.expect(sm.fields[0].required);
        try testing.expect(!sm.fields[2].required);
        try testing.expect(store.method("sendDice") != null);
        try testing.expect(store.method("noSuchMethod") == null);
    }
    // Malformed JSON is a SyntaxError.
    try testing.expectError(
        error.SyntaxError,
        SchemaStore.fromSlice(testing.allocator, "{not json"),
    );
}

/// Build a Lua state with a single global table `p` from a Lua table literal,
/// and return the absolute stack index of `p` (pushed on top).
fn pushParams(lua: *Lua, table_literal: [:0]const u8) !i32 {
    try lua.doString(table_literal); // e.g. "p = { chat_id = 1, text = 'x' }"
    _ = lua.getGlobal("p");
    return lua.getTop();
}

test "validate accepts valid calls and rejects bad ones" {
    const store = try SchemaStore.fromSlice(testing.allocator, FIXTURE_SCHEMA);
    defer store.destroy(testing.allocator);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();
    lua.openBase();

    // Well-formed call passes.
    {
        const idx = try pushParams(lua, "p = { chat_id = 1, text = 'hi' }");
        try validate(store, lua, idx, "sendMessage");
        lua.pop(1);
    }
    // Missing required param → MissingRequired.
    {
        const idx = try pushParams(lua, "p = { text = 'hi' }"); // no chat_id
        try testing.expectError(error.MissingRequired, validate(store, lua, idx, "sendMessage"));
        lua.pop(1);
    }
    // Unknown method → UnknownMethod.
    {
        const idx = try pushParams(lua, "p = { chat_id = 1 }");
        try testing.expectError(error.UnknownMethod, validate(store, lua, idx, "sendUnicorn"));
        lua.pop(1);
    }
    // `text` is String-only; a table value is a TypeMismatch.
    {
        const idx = try pushParams(lua, "p = { chat_id = 1, text = {} }");
        try testing.expectError(error.TypeMismatch, validate(store, lua, idx, "sendMessage"));
        lua.pop(1);
    }
    // `chat_id` is Integer|String (a mixed union → kind `any`): a table value is
    // not type-checked — the validator never rejects what it cannot classify.
    {
        const idx = try pushParams(lua, "p = { chat_id = {}, text = 'ok' }");
        try validate(store, lua, idx, "sendMessage");
        lua.pop(1);
    }

    // InputFile fields accept a string (file_id or URL) and unclassifiable values.
    {
        const schema =
            \\{"methods":{"sendPhoto":{"fields":[
            \\  {"name":"chat_id","types":["Integer","String"],"required":true},
            \\  {"name":"photo","types":["InputFile"],"required":true}
            \\]}},"types":{}}
        ;
        const photo_store = try SchemaStore.fromSlice(testing.allocator, schema);
        defer photo_store.destroy(testing.allocator);

        // String file_id passes — not a TypeMismatch.
        const idx = try pushParams(lua, "p = { chat_id = 1, photo = 'AgACAgIAAxk' }");
        try validate(photo_store, lua, idx, "sendPhoto");
        lua.pop(1);

        // Table value also passes (InputFile is .any — unclassifiable).
        const idx2 = try pushParams(lua, "p = { chat_id = 1, photo = {} }");
        try validate(photo_store, lua, idx2, "sendPhoto");
        lua.pop(1);
    }
}

test "SchemaSlot with no schema → get() is null (Tier-0)" {
    var slot = SchemaSlot.init(testing.allocator, testing.io);
    defer slot.deinit();
    try testing.expect(slot.get() == null);
    try testing.expectEqual(@as(u64, 0), slot.version.load(.acquire));
}

test "loadInitial reads a file; install swaps and retires" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "s.json", .data = FIXTURE_SCHEMA });
    }
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPathFile(testing.io, "s.json", &path_buf);
    const path = path_buf[0..path_len];

    var slot = SchemaSlot.init(testing.allocator, testing.io);
    defer slot.deinit();

    loadInitial(&slot, path);
    try testing.expectEqual(@as(u64, 1), slot.version.load(.acquire));
    const s1 = slot.get() orelse return error.TestUnexpectedNull;
    try testing.expect(s1.method("sendMessage") != null);

    // A second install retires the first store (freed by deinit, not now).
    const s2 = try SchemaStore.fromSlice(testing.allocator, FIXTURE_SCHEMA);
    slot.install(s2);
    try testing.expectEqual(@as(u64, 2), slot.version.load(.acquire));
    try testing.expectEqual(@as(usize, 1), slot.retired.items.len);

    // The newly installed store is now live: get() returns s2, not the old s1.
    try testing.expect(slot.get() == s2);
    // The displaced store was retired — not the wrong pointer, and not freed:
    // it is exactly s1, and its methods are still reachable through it.
    try testing.expect(slot.retired.items[0] == s1);
    try testing.expect(slot.retired.items[0].method("sendMessage") != null);
}

test "loadInitial on a missing file leaves the slot empty" {
    var slot = SchemaSlot.init(testing.allocator, testing.io);
    defer slot.deinit();
    loadInitial(&slot, "/nonexistent/zora-no-such-schema.json");
    try testing.expect(slot.get() == null);
    try testing.expectEqual(@as(u64, 0), slot.version.load(.acquire));
}

test "schema watcher hot-reloads; a broken file is rejected" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // Initial schema: sendMessage only.
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "s.json", .data = FIXTURE_SCHEMA });
    }
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPathFile(testing.io, "s.json", &path_buf);
    const path = path_buf[0..path_len];

    var slot = SchemaSlot.init(testing.allocator, testing.io);
    defer slot.deinit();
    loadInitial(&slot, path);

    var ctx = SchemaWatchCtx{ .slot = &slot, .path = path };
    var stop = std.atomic.Value(bool).init(false);
    const t = try std.Thread.spawn(.{}, struct {
        fn run(target: watcher.WatchTarget, stp: *std.atomic.Value(bool)) void {
            watcher.watchPoll(target, testing.io, 50, stp);
        }
    }.run, .{
        watcher.WatchTarget{ .path = path, .context = &ctx, .on_write = onSchemaChange },
        &stop,
    });
    defer {
        stop.store(true, .release);
        t.join();
    }

    // Establish a deterministic mtime baseline before touching the file.
    // The poll watcher records the initial mtime on its first iteration and
    // only fires on a *change* thereafter. A fixed sleep races the watcher's
    // first poll under parallel-suite load: write too early and the new mtime
    // becomes the baseline, so the reload is missed. Instead, wait until
    // `version` has held steady at 1 across several full poll intervals
    // (poll_ms = 50). A stable 1 proves the watcher has captured its baseline
    // and produced no spurious reload — only then is it safe to write.
    {
        const poll_ms = 50;
        const required_stable_polls = 3;
        const deadline = rt.nowMs(rt.io()) + 3000;
        var stable_polls: u32 = 0;
        while (stable_polls < required_stable_polls) {
            rt.sleepNs(rt.io(), poll_ms * std.time.ns_per_ms);
            const v = slot.version.load(.acquire);
            // A premature reload would push version past 1 — never weaken this.
            try testing.expectEqual(@as(u64, 1), v);
            stable_polls += 1;
            if (rt.nowMs(rt.io()) >= deadline) return error.TestBaselineTimeout;
        }
    }

    // Valid reload: add a method.
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "s.json", .data =
            \\{"methods":{"sendMessage":{"fields":[]},"newMethod":{"fields":[]}},"types":{}}
        });
    }
    {
        const deadline = rt.nowMs(rt.io()) + 2000;
        while (slot.version.load(.acquire) < 2) {
            if (rt.nowMs(rt.io()) >= deadline) return error.TestReloadTimeout;
            rt.sleepNs(rt.io(), 10 * std.time.ns_per_ms);
        }
    }
    try testing.expect(slot.get().?.method("newMethod") != null);

    // Broken reload: failures bump, version stays, old schema retained.
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "s.json", .data = "{ this is not json" });
    }
    {
        const deadline = rt.nowMs(rt.io()) + 2000;
        while (slot.failures.load(.acquire) < 1) {
            if (rt.nowMs(rt.io()) >= deadline) return error.TestFailureTimeout;
            rt.sleepNs(rt.io(), 10 * std.time.ns_per_ms);
        }
    }
    try testing.expectEqual(@as(u64, 2), slot.version.load(.acquire));
    try testing.expect(slot.get().?.method("newMethod") != null);
}

test "onSchemaChange on a missing file bumps failures, not version" {
    // Drive the bad-reload path directly, without the poll watcher — the
    // failures counter and the version-unchanged invariant are exercised in
    // isolation, free of timing flake.
    var slot = SchemaSlot.init(testing.allocator, testing.io);
    defer slot.deinit();

    var ctx = SchemaWatchCtx{ .slot = &slot, .path = "/nonexistent/zora-no-such-schema.json" };
    onSchemaChange(&ctx);

    // A failed reload increments failures and leaves the version untouched.
    try testing.expectEqual(@as(u64, 1), slot.failures.load(.acquire));
    try testing.expectEqual(@as(u64, 0), slot.version.load(.acquire));
    // No store was installed: the slot stays empty (Tier-0).
    try testing.expect(slot.get() == null);
}

test "validate with nil params passes when no field is required" {
    // A method whose every field is optional accepts an absent params table:
    // nothing is required, so nil params is valid.
    const schema =
        \\{"methods":{"getMe":{"fields":[
        \\  {"name":"timeout","types":["Integer"],"required":false}
        \\]}},"types":{}}
    ;
    const store = try SchemaStore.fromSlice(testing.allocator, schema);
    defer store.destroy(testing.allocator);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();
    lua.openBase();

    // Push a nil at a known absolute index and validate against it.
    lua.pushNil();
    const idx = lua.getTop();
    try testing.expect(!lua.isTable(idx));
    try validate(store, lua, idx, "getMe");
    lua.pop(1);
}
