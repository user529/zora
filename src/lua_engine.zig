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
/// `{ method = "...", params = {...} }`.  Rules may also push
/// fire-and-forget calls mid-handler with `bot.emit{...}`; those are collected
/// ahead of the return-list.

const std = @import("std");
const ziglua = @import("ziglua");
const types = @import("types.zig");
const serializer = @import("serializer.zig");
const lua_api = @import("lua_api.zig");
const tg_schema = @import("tg_schema.zig");
const io_pool = @import("io_pool.zig");

const Lua = ziglua.Lua;
const log = std.log.scoped(.lua_engine);

// ---------------------------------------------------------------------------
// Version constant — increment on breaking bot.* API changes
// ---------------------------------------------------------------------------

pub const RULES_API_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Coroutine handle and outcome
// ---------------------------------------------------------------------------

/// A live coroutine handler. Owned by the worker's inflight map while parked.
/// Unref'd (and GC-eligible) when the coroutine finishes or is reaped.
pub const CoroHandle = struct {
    thread:    *Lua,
    /// Registry reference keeping the thread alive (prevents GC).
    lua_ref:   i32,
    coro_id:   u32,
    worker_id: u8,
};

/// Result of startHandler or resumeHandler.
pub const CoroOutcome = union(enum) {
    /// Coroutine finished. Caller owns the slice; free with types.freeApiCalls.
    done:    []types.ApiCall,
    /// Coroutine yielded. Caller submits pending_job.io_job to the io_pool
    /// and stores the CoroHandle in the inflight entry.
    yielded: struct {
        handle:      CoroHandle,
        pending_job: lua_api.PendingJob,
    },
    /// Lua error or malformed body. Logged; no actions.
    err,
};

/// Release a live coroutine handle: clear the per-thread emit accumulator
/// from the Lua registry, then drop the integer registry ref.
///
/// Must be called on every code path that ends a coroutine's lifetime
/// EXCEPT processResume's .ok branch (which calls pushEmitBatch explicitly
/// before collecting actions).  Forgetting this causes a registry[thread]
/// entry to keep the Lua thread object alive indefinitely.
pub fn teardownCoro(main: *Lua, handle: CoroHandle) void {
    // Retrieve the thread object via its integer ref and use it as a key to
    // clear registry[thread_obj] from the main state — safe even for dead
    // (errored) coroutines whose internal call-info stack is unwound.
    _ = main.getIndexRaw(ziglua.registry_index, @as(ziglua.Integer, handle.lua_ref));
    main.pushNil();
    main.setTableRaw(ziglua.registry_index); // registry[thread_obj] = nil
    main.unref(ziglua.registry_index, handle.lua_ref);
}

/// Instruction hook — fires after 10M instructions per resume.
fn hookCountLimit(lua: *Lua, _: ziglua.Event, _: *ziglua.DebugInfo) void {
    lua.raiseErrorStr("instruction limit exceeded", .{});
}

const INSTRUCTION_CAP: i32 = 10_000_000;

// ---------------------------------------------------------------------------
// LuaEngine
// ---------------------------------------------------------------------------

pub const LuaEngine = struct {
    lua: *Lua,
    allocator: std.mem.Allocator,
    /// Hot-reloadable schema for outgoing-call validation. null → no schema.
    schema_slot: ?*tg_schema.SchemaSlot = null,
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
        slot: ?*tg_schema.SchemaSlot,
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

    pub fn callOnMessage(
        self:      *LuaEngine,
        allocator: std.mem.Allocator,
        body:      []const u8,
    ) ![]types.ApiCall {
        const outcome = try self.startHandler(body, 0, 0, allocator, "on_message", null);
        switch (outcome) {
            .done    => |actions| return actions,
            .yielded => |y| {
                switch (y.pending_job) {
                    .io           => |io|   lua_api.freeOwnedStrings(io.owned_strings, allocator),
                    .tracked_send => |call| types.freeApiCall(call, allocator),
                }
                teardownCoro(self.lua, y.handle);
                log.err("callOnMessage: rule called a yield-ing function — no io_pool wired", .{});
                return try allocator.alloc(types.ApiCall, 0);
            },
            .err => return try allocator.alloc(types.ApiCall, 0),
        }
    }

    /// Start a new coroutine handler for `body`.
    /// Creates a child Lua thread, decodes the JSON body, calls the first resumeThread.
    /// Returns CoroOutcome.done (fast path — no yield), .yielded, or .err.
    ///
    /// `handler` is the name of the Lua global to call (e.g. "on_message",
    /// "on_schedule"). When `extra_int` is non-null it is pushed as a second Lua
    /// argument after the decoded body table (nargs becomes 2).
    pub fn startHandler(
        self:      *LuaEngine,
        body:      []const u8,
        coro_id:   u32,
        worker_id: u8,
        allocator: std.mem.Allocator,
        handler:   [:0]const u8,
        extra_int: ?i64,
    ) !CoroOutcome {
        const main = self.lua;

        // Create child thread and pin it in the registry so GC can't collect it.
        const thread = main.newThread();    // pushes thread on main stack
        const lua_ref = main.ref(ziglua.registry_index); // pops, stores, returns ref

        const handle = CoroHandle{
            .thread    = thread,
            .lua_ref   = lua_ref,
            .coro_id   = coro_id,
            .worker_id = worker_id,
        };

        // Initialise per-coroutine emit accumulator (registry[thread] = {}).
        lua_api.beginEmitBatch(thread);

        // Push the selected handler. A missing or non-function handler logs and
        // returns .err (the caller treats .err as a completed no-op).
        const fn_type = thread.getGlobal(handler);
        if (fn_type != .function) {
            thread.pop(1);
            if (fn_type == .nil) {
                log.warn("{s} not found", .{handler});
            } else {
                log.warn("{s} is not a function", .{handler});
            }
            teardownCoro(main, handle);
            return .err;
        }

        // Decode JSON body → Lua table (the first handler argument).
        serializer.jsonToLuaTable(thread, body, allocator) catch |e| {
            log.err("startHandler: body decode failed: {s}", .{@errorName(e)});
            thread.pop(1); // pop handler
            teardownCoro(main, handle);
            return .err;
        };

        // Optional second argument (the schedule id for on_schedule).
        var nargs: i32 = 1;
        if (extra_int) |v| {
            thread.pushInteger(@intCast(v));
            nargs = 2;
        }

        // Set per-resume instruction cap.
        thread.setHook(ziglua.wrap(hookCountLimit), .{ .count = true }, INSTRUCTION_CAP);

        // First resume: call handler(body_table[, extra_int]).
        var nres: i32 = 0;
        const status = thread.resumeThread(main, nargs, &nres) catch |e| {
            log.warn("startHandler resumeThread error: {s}", .{@errorName(e)});
            teardownCoro(main, handle);
            return .err;
        };

        return self.processResume(handle, main, thread, status, nres, allocator);
    }

    /// Resume a parked coroutine with the result of a completed I/O job.
    /// Frees owned_strings (io_pool is done with them by the time this is called).
    pub fn resumeHandler(
        self:             *LuaEngine,
        handle:           CoroHandle,
        result:           io_pool.IoResult,
        owned_strings:    lua_api.OwnedStrings,
        is_tracked_send:  bool,
        allocator:        std.mem.Allocator,
    ) !CoroOutcome {
        const main = self.lua;
        const thread = handle.thread;

        // Safe to free now — io_pool has already finished with the strings.
        lua_api.freeOwnedStrings(owned_strings, allocator);

        if (is_tracked_send) {
            pushTrackedSendResult(thread, result);
        } else {
            pushIoResult(thread, result);
        }

        // Reset instruction cap for this resume.
        thread.setHook(ziglua.wrap(hookCountLimit), .{ .count = true }, INSTRUCTION_CAP);

        var nres: i32 = 0;
        const status = thread.resumeThread(main, 1, &nres) catch |e| {
            log.warn("resumeHandler resumeThread error: {s}", .{@errorName(e)});
            teardownCoro(main, handle);
            return .err;
        };

        return self.processResume(handle, main, thread, status, nres, allocator);
    }

    /// Shared post-resumeThread logic: collect actions on .ok, read pending job on .yield.
    fn processResume(
        self:      *LuaEngine,
        handle:    CoroHandle,
        main:      *Lua,
        thread:    *Lua,
        status:    ziglua.ResumeStatus,
        nres:      i32,
        allocator: std.mem.Allocator,
    ) !CoroOutcome {
        switch (status) {
            .ok => {
                defer main.unref(ziglua.registry_index, handle.lua_ref);

                var list: std.ArrayListUnmanaged(types.ApiCall) = .empty;
                errdefer {
                    for (list.items) |c| types.freeApiCall(c, allocator);
                    list.deinit(allocator);
                }

                const store: ?*const tg_schema.SchemaStore =
                    if (self.schema_slot) |s| s.get() else null;

                // Emit batch first.
                lua_api.pushEmitBatch(thread);
                errdefer thread.pop(1); // pop on OOM before returning error
                try appendApiCalls(thread, thread.getTop(), allocator, &list, store, self.validation);
                thread.pop(1); // pop on success so return-list uses correct stack top

                // Then the return-list.
                if (nres > 1) {
                    log.warn("on_message returned {d} values; only the last is collected", .{nres});
                }
                if (nres > 0) {
                    try appendApiCalls(thread, thread.getTop(), allocator, &list, store, self.validation);
                    thread.pop(@intCast(nres));
                }

                return .{ .done = try list.toOwnedSlice(allocator) };
            },
            .yield => {
                var pj = self.apiCtxPendingJob() orelse {
                    log.err("coroutine yielded but pending_job is null (coro_id={d})", .{handle.coro_id});
                    teardownCoro(main, handle);
                    return .err;
                };
                switch (pj) {
                    .io => |*io| {
                        io.io_job.worker_id = handle.worker_id;
                        io.io_job.coro_id   = handle.coro_id;
                    },
                    .tracked_send => |*call| {
                        if (call.tracking) |*t| {
                            t.worker_id = handle.worker_id;
                            t.coro_id   = handle.coro_id;
                        }
                    },
                }
                return .{ .yielded = .{ .handle = handle, .pending_job = pj } };
            },
        }
    }

    /// Read and clear ApiCtx.pending_job.
    fn apiCtxPendingJob(self: *LuaEngine) ?lua_api.PendingJob {
        const ctx = lua_api.getCtx(self.lua);
        const pj = ctx.pending_job;
        ctx.pending_job = null;
        return pj;
    }
};

// ---------------------------------------------------------------------------
// pushIoResult — translate an IoResult into a Lua value on the thread's stack
// ---------------------------------------------------------------------------

/// Push one Lua value representing an IoResult onto the thread stack.
/// For http: pushes {status=N, body="..."}
/// For proc: pushes {exit_code=N, stdout="..."}
/// For err:  pushes a combined table {status=0, body=msg, exit_code=-1, stdout=msg}
fn pushIoResult(thread: *Lua, result: io_pool.IoResult) void {
    switch (result.outcome) {
        .http => |h| {
            thread.createTable(0, 3);
            thread.pushInteger(@intCast(h.status));
            thread.setField(-2, "status");
            _ = thread.pushString(h.body);
            thread.setField(-2, "body");

            // headers subtable: verbatim server names as keys, case-insensitive
            // lookup via the shared metatable. setTable (not setField) because
            // names are not NUL-terminated.
            thread.createTable(0, @intCast(h.headers.len));
            for (h.headers) |hdr| {
                _ = thread.pushString(hdr.name);  // key
                _ = thread.pushString(hdr.value); // value
                thread.setTable(-3);
            }
            lua_api.attachHeaderMetatable(thread);
            thread.setField(-2, "headers");
        },
        .proc => |p| {
            thread.createTable(0, 2);
            thread.pushInteger(@intCast(p.exit_code));
            thread.setField(-2, "exit_code");
            _ = thread.pushString(p.stdout);
            thread.setField(-2, "stdout");
        },
        .send => |s| {
            thread.pushInteger(s.message_id);
        },
        .err => |msg| {
            thread.createTable(0, 5);
            thread.pushInteger(0);
            thread.setField(-2, "status");
            _ = thread.pushString(msg);
            thread.setField(-2, "body");
            thread.pushInteger(-1);
            thread.setField(-2, "exit_code");
            _ = thread.pushString(msg);
            thread.setField(-2, "stdout");

            // Stable, empty headers table (same shape as the .http arm) so
            // rules can index resp.headers unconditionally on transport errors.
            thread.createTable(0, 0);
            lua_api.attachHeaderMetatable(thread);
            thread.setField(-2, "headers");
        },
    }
}

fn pushTrackedSendResult(thread: *Lua, result: io_pool.IoResult) void {
    switch (result.outcome) {
        .send => |s| thread.pushInteger(s.message_id),
        .err  => |msg| _ = thread.pushString(msg),
        else  => _ = thread.pushString("unexpected result type for tracked send"),
    }
}

// ---------------------------------------------------------------------------
// ApiCall parsing — { method, params } Lua tables → types.ApiCall
// ---------------------------------------------------------------------------

/// Parse one `{ method = "...", params = {...} }` Lua table (at absolute
/// `table_idx`) into an ApiCall.  `method` is required; `params` is optional.
/// When `mode != .off` and `schema` is non-null, params are schema-validated.
/// lua_api.buildApiCall builds the payload (JSON vs multipart).
fn parseOneApiCall(
    lua:       *Lua,
    table_idx: i32,
    schema:    ?*const tg_schema.SchemaStore,
    mode:      types.ValidationMode,
) !types.ApiCall {
    // ── method (required string) ──────────────────────────────────────────
    _ = lua.getField(table_idx, "method");
    const method_tmp = lua.toString(-1) catch {
        lua.pop(1);
        return error.InvalidApiCall;
    };
    lua.pop(1);

    // ── params value on the stack ─────────────────────────────────────────
    _ = lua.getField(table_idx, "params");
    const params_idx = lua.getTop();
    defer lua.pop(1); // always pop params

    // ── schema validation ─────────────────────────────────────────────────
    if (mode != .off) {
        if (schema) |store| {
            tg_schema.validate(store, lua, params_idx, method_tmp) catch |verr| {
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
    schema: ?*const tg_schema.SchemaStore,
    mode: types.ValidationMode,
) !void {
    if (!lua.isTable(table_idx)) return;

    const n = lua.lenRaw(table_idx);
    var i: ziglua.Integer = 1;
    while (i <= @as(ziglua.Integer, @intCast(n))) : (i += 1) {
        const pre = lua.getTop();
        _ = lua.getIndexRaw(table_idx, i);
        if (lua.isTable(-1)) {
            const abs = lua.getTop();
            if (parseOneApiCall(lua, abs, schema, mode)) |call| {
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

/// Default test ApiCtx for `db`: 50 MB file cap, 1 MB JSON cap, testing allocator.
fn testCtx(db: *state_store.StateStore) lua_api.ApiCtx {
    return .{ .db = db, .allocator = testing.allocator, .io = testing.io, .max_file_bytes = 52428800, .json_max_bytes = 1048576 };
}

test "io.open / os.execute / require raise errors; math/string/table/utf8 work" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
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

test "dofile and loadfile globals are nil → calling them raises a Lua error" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    // init() nils both after openBase, so they are no longer callable: invoking
    // a nil value is a runtime error surfaced as error.LuaError.
    try testing.expectError(error.LuaError, engine.loadString("dofile('x')"));
    try testing.expectError(error.LuaError, engine.loadString("loadfile('x')"));

    // The globals themselves read back as nil.
    try engine.loadString("assert(dofile == nil); assert(loadfile == nil)");
}

test "on_message returning {} → empty slice" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(update) return {} end
    );

    const actions = try engine.callOnMessage(testing.allocator, "{\"update_id\":2}");
    defer types.freeApiCalls(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 0), actions.len);
}

test "on_message error → empty slice logged, next call succeeds" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
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

test "invalid Lua syntax → loadString returns error; engine still usable" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
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
// Generic { method, params } engine path
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

test "on_message receives a decoded Lua table with nested access" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
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

test "return-list — six method shapes parse to ApiCalls" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
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
    // unspecified — assert by substring, or parse for the {chat_id,text} shape).
    try expectMsgBody(actions[0].payload.json, 1, "m"); // sendMessage chat_id+text
    try testing.expect(std.mem.indexOf(u8, actions[1].payload.json, "\"parse_mode\":\"HTML\"") != null);
    try testing.expect(std.mem.indexOf(u8, actions[1].payload.json, "\"message_id\":2") != null);
    try testing.expect(std.mem.indexOf(u8, actions[2].payload.json, "\"callback_data\":\"d\"") != null);
    try testing.expect(std.mem.indexOf(u8, actions[2].payload.json, "\"inline_keyboard\"") != null);
    try testing.expect(std.mem.indexOf(u8, actions[3].payload.json, "\"callback_query_id\":\"cq\"") != null);
    // deleteMessage carries chat_id and message_id (no text field).
    try testing.expect(std.mem.indexOf(u8, actions[4].payload.json, "\"chat_id\":1") != null);
    try testing.expect(std.mem.indexOf(u8, actions[4].payload.json, "\"message_id\":2") != null);
    try testing.expect(std.mem.indexOf(u8, actions[5].payload.json, "\"chat_id\":1") != null);
}

test "bot.emit calls precede the return-list, in call order" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
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

// ── schema validation ────────────────────────────────────────────────────────

/// Fixture: sendMessage requires chat_id (Integer|String) and text (String).
const SCHEMA_FIX =
    \\{"methods":{"sendMessage":{"fields":[
    \\  {"name":"chat_id","types":["Integer","String"],"required":true},
    \\  {"name":"text","types":["String"],"required":true}
    \\]}},"types":{}}
;

test "validation mode governs both bot.emit and the return-list" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);

    var slot = tg_schema.SchemaSlot.init(testing.allocator, testing.io);
    defer slot.deinit();
    slot.install(try tg_schema.SchemaStore.fromSlice(testing.allocator, SCHEMA_FIX));

    // off and warn keep an invalid return-list call; strict drops it.
    {
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
    // strict validates both the emit path and the return-list; of three calls
    // only the one valid emit survives.
    {
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
}

test "schema slot is shared by pointer and an empty slot validates nothing" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);

    // One installed slot is referenced by pointer from every engine. The schema
    // never enters a lua_State, so adding engines (workers) never multiplies its
    // memory.
    {
        var slot = tg_schema.SchemaSlot.init(testing.allocator, testing.io);
        defer slot.deinit();
        slot.install(try tg_schema.SchemaStore.fromSlice(testing.allocator, SCHEMA_FIX));

        var e1 = try LuaEngine.init(testing.allocator, &ctx);
        defer e1.deinit();
        var e2 = try LuaEngine.init(testing.allocator, &ctx);
        defer e2.deinit();
        e1.setValidation(&slot, .warn);
        e2.setValidation(&slot, .warn);

        try testing.expectEqual(e1.schema_slot.?, e2.schema_slot.?);
        try testing.expectEqual(slot.get().?, e2.schema_slot.?.get().?);
    }
    // An empty slot under strict mode is a no-op: nothing is dropped (Tier-0).
    {
        var slot = tg_schema.SchemaSlot.init(testing.allocator, testing.io); // never installed
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
}

test "shipped rules/rules.lua produces sendMessage calls (generic form)" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    // Load the actual shipped rules file (cwd is the project root under
    // `zig build test`).
    const raw = try std.Io.Dir.cwd().readFileAlloc(testing.io, "rules/rules.lua", testing.allocator, .limited(64 * 1024));
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
        try expectMsgBody(actions[0].payload.json, 9, "Welcome! You are message #1.");
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
    // /stats → the rule's `string.format("Your messages: %d | Total: %d", ...)`.
    // State persists across the calls above for user 1: this is the third message
    // (/start, hello, /stats), so per-user count and global total are both 3.
    {
        const actions = try engine.callOnMessage(testing.allocator,
            \\{"message":{"from":{"id":1},"chat":{"id":9},"text":"/stats"}}
        );
        defer types.freeApiCalls(actions, testing.allocator);
        try testing.expectEqual(@as(usize, 1), actions.len);
        try testing.expectEqualStrings("sendMessage", actions[0].method);
        try expectMsgBody(actions[0].payload.json, 9, "Your messages: 3 | Total: 3");
    }
    // /remind <secs> <text> → schedules a job (bot.schedule_after, synchronous
    // insert into the StateStore) and confirms with one sendMessage.
    {
        const actions = try engine.callOnMessage(testing.allocator,
            \\{"message":{"from":{"id":1},"chat":{"id":9},"text":"/remind 5 hello"}}
        );
        defer types.freeApiCalls(actions, testing.allocator);
        try testing.expectEqual(@as(usize, 1), actions.len);
        try testing.expectEqualStrings("sendMessage", actions[0].method);
        try expectMsgBody(actions[0].payload.json, 9, "Reminder set for 5s.");
    }
    // on_schedule(payload, id) → one sendMessage echoing the scheduled payload,
    // mirroring the /remind job above ({ chat = chat_id, text = rest }).
    {
        const outcome = try engine.startHandler(
            \\{"chat":9,"text":"hello"}
        , 0, 0, testing.allocator, "on_schedule", @as(?i64, 1));
        switch (outcome) {
            .done => |actions| {
                defer types.freeApiCalls(actions, testing.allocator);
                try testing.expectEqual(@as(usize, 1), actions.len);
                try testing.expectEqualStrings("sendMessage", actions[0].method);
                try expectMsgBody(actions[0].payload.json, 9, "hello");
            },
            else => return error.UnexpectedOutcome,
        }
    }
}

// ── coroutine engine tests ───────────────────────────────────────────────────

test "sync rule (no yield) via startHandler → .done with actions" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  return { { method = "sendMessage", params = { chat_id = 7, text = "hi" } } }
        \\end
    );

    const outcome = try engine.startHandler("{\"update_id\":1}", 42, 0, testing.allocator, "on_message", null);
    switch (outcome) {
        .done => |actions| {
            defer types.freeApiCalls(actions, testing.allocator);
            try testing.expectEqual(@as(usize, 1), actions.len);
            try testing.expectEqualStrings("sendMessage", actions[0].method);
        },
        else => return error.UnexpectedOutcome,
    }
}

test "per-resume instruction cap aborts infinite loop" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  while true do end
        \\end
    );

    const outcome = try engine.startHandler("{}", 1, 0, testing.allocator, "on_message", null);
    switch (outcome) {
        .err => {},
        else => return error.ExpectedError,
    }
}

test "bounded heavy computation in one resume is not falsely aborted" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  local s = 0
        \\  for i = 1, 1000000 do s = s + i end
        \\  return { { method = "result", params = { n = s } } }
        \\end
    );

    const outcome = try engine.startHandler("{}", 2, 0, testing.allocator, "on_message", null);
    switch (outcome) {
        .done => |actions| {
            defer types.freeApiCalls(actions, testing.allocator);
            try testing.expectEqual(@as(usize, 1), actions.len);
        },
        else => return error.FalselyAborted,
    }
}

test "startHandler dispatches on_schedule with payload and id" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var engine = try LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_schedule(payload, id)
        \\  return { { method = "noteId", params = { got = id, n = payload.n } } }
        \\end
    );

    const outcome = try engine.startHandler("{\"n\":5}", 0, 0, testing.allocator, "on_schedule", @as(?i64, 77));
    switch (outcome) {
        .done => |actions| {
            defer {
                for (actions) |a| types.freeApiCall(a, testing.allocator);
                testing.allocator.free(actions);
            }
            try testing.expectEqual(@as(usize, 1), actions.len);
            try testing.expectEqualStrings("noteId", actions[0].method);
            try testing.expect(std.mem.indexOf(u8, actions[0].payload.json, "\"got\":77") != null);
            try testing.expect(std.mem.indexOf(u8, actions[0].payload.json, "\"n\":5") != null);
        },
        else => return error.UnexpectedOutcome,
    }
}
