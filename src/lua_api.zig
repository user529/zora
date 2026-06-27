/// lua_api.zig — Lua-callable functions registered as bot.*
///
/// Registers the following Lua globals when `register()` is called:
///
///   bot.get_user_state(user_id: integer) -> table
///   bot.set_user_state(user_id: integer, data: table)
///   bot.get_chat_state(chat_id: integer) -> table
///   bot.set_chat_state(chat_id: integer, data: table)
///   bot.get_global(key: string) -> string|nil
///   bot.set_global(key: string, value: string)
///   bot.log(level: string, message: string)
///   bot.rules_api_version (integer, read-only by convention)
///
/// The ApiCtx pointer is stored in the Lua registry under "_zora_ctx" and
/// retrieved by each C function.  It must remain live for the entire lifetime
/// of the Lua state.

const std = @import("std");
const ziglua = @import("ziglua");
const state_store = @import("state_store.zig");
const serializer = @import("serializer.zig");
const types = @import("types.zig");
const io_pool = @import("io_pool.zig");
const rt = @import("rt.zig");
const scheduler = @import("scheduler.zig");

const Lua = ziglua.Lua;

// ---------------------------------------------------------------------------
// ApiCtx — context shared by all bot.* functions via the Lua registry
// ---------------------------------------------------------------------------

/// Per-engine context stored in the Lua registry.
/// The db allocator is used for state_store operations;
/// the api allocator is used for temporary serializer buffers.
pub const ApiCtx = struct {
    db: *state_store.StateStore,
    /// Allocator for temporary allocations inside bot.* functions
    /// (e.g., JSON intermediate buffers).  Must be an arena or GPA —
    /// every allocation is freed before the C function returns.
    allocator:      std.mem.Allocator,
    /// Runtime for blocking I/O (reading file-upload descriptors).
    io:             std.Io,
    /// Maximum file size in bytes for multipart upload descriptors.
    max_file_bytes: usize,
    /// Maximum bytes for json.decode input / json.encode output.
    json_max_bytes: usize,
    /// Set by a yield-ing C function immediately before calling lua.yield(0).
    /// Read and cleared by lua_engine.startHandler / resumeHandler after
    /// resumeThread returns .yield.  Null at all other times.
    pending_job: ?PendingJob = null,
    /// Set by main.zig so bot.schedule_* can wake the timer after inserting a
    /// job. Null in unit tests — the insert still works, the timer just relies
    /// on its wait cap to notice the new row.
    scheduler: ?*scheduler.Scheduler = null,
};

const REGISTRY_KEY: [:0]const u8 = "_zora_ctx";

/// Registry key for the shared, case-insensitive response-header metatable.
const HEADER_MT_KEY: [:0]const u8 = "zora.header_mt";

// ---------------------------------------------------------------------------
// Coroutine I/O types — shared between lua_api, lua_engine, and worker
// ---------------------------------------------------------------------------

/// Heap-allocated copies of an IoJob's payload strings.
/// The IoJob references these slices — they must outlive the io_pool job.
/// Free with freeOwnedStrings when the coroutine finishes, errors, or is reaped.
pub const OwnedStrings = union(enum) {
    http: struct { method: []u8, url: []u8, headers: []std.http.Header, body: []u8 },
    exec: struct { argv: [][]u8 },  // each element is a duped arg; the slice itself is also allocated
    shell: struct { command: []u8 },
    none,
};

/// Returned from startHandler/resumeHandler when the coroutine yields.
pub const PendingJob = union(enum) {
    /// Submitted to io_pool. owned_strings freed when coroutine finishes, errors, or is reaped.
    io: struct { io_job: io_pool.IoJob, owned_strings: OwnedStrings },
    /// Pushed to dispatcher_queue. Dispatcher takes ownership of ApiCall strings after push.
    tracked_send: types.ApiCall,
};

/// Free all heap-allocated strings in an OwnedStrings value.
pub fn freeOwnedStrings(strings: OwnedStrings, allocator: std.mem.Allocator) void {
    switch (strings) {
        .http => |h| {
            allocator.free(h.method);
            allocator.free(h.url);
            for (h.headers) |hdr| {
                allocator.free(hdr.name);
                allocator.free(hdr.value);
            }
            allocator.free(h.headers);
            allocator.free(h.body);
        },
        .exec => |e| {
            for (e.argv) |arg| allocator.free(arg);
            allocator.free(e.argv);
        },
        .shell => |s| allocator.free(s.command),
        .none  => {},
    }
}

/// The tg.* ergonomic facade.  `tg.<method>{params}` is
/// exactly `bot.emit{ method = "<method>", params = {params} }` — pure sugar;
/// the call is schema-validated downstream regardless of which form is used.
const TG_FACADE_LUA: [:0]const u8 =
    \\tg = setmetatable({}, { __index = function(_, method)
    \\  return function(params)
    \\    return bot.emit{ method = method, params = params }
    \\  end
    \\end })
;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Register all bot.* functions and bot.rules_api_version into `lua`.
/// `ctx` must remain live for the entire lifetime of `lua`.
/// `rules_api_version` is exposed as the read-only Lua integer
/// `bot.rules_api_version`.
pub fn register(lua: *Lua, ctx: *ApiCtx, rules_api_version: u32) void {
    // Store ApiCtx pointer in the registry so CFns can retrieve it.
    lua.pushLightUserdata(@ptrCast(ctx));
    lua.setField(ziglua.registry_index, REGISTRY_KEY);

    // Create the `bot` table.
    lua.newTable();

    // Register each function as bot.<name>.
    const fns = [_]struct { name: [:0]const u8, func: ziglua.CFn }{
        .{ .name = "get_user_state", .func = botGetUserState },
        .{ .name = "set_user_state", .func = botSetUserState },
        .{ .name = "get_chat_state", .func = botGetChatState },
        .{ .name = "set_chat_state", .func = botSetChatState },
        .{ .name = "get_global",     .func = botGetGlobal },
        .{ .name = "set_global",     .func = botSetGlobal },
        .{ .name = "log",            .func = botLog },
        .{ .name = "now_ms",         .func = botNowMs },
        .{ .name = "schedule_at",    .func = botScheduleAt },
        .{ .name = "schedule_after", .func = botScheduleAfter },
        .{ .name = "unschedule",     .func = botUnschedule },
        .{ .name = "emit",           .func = botEmit },
        .{ .name = "url_encode",     .func = botUrlEncode },
        .{ .name = "shell_quote",    .func = botShellQuote },
        .{ .name = "http_request",   .func = botHttpRequest },
        .{ .name = "exec",           .func = botExec },
        .{ .name = "shell",          .func = botShell },
        .{ .name = "send_message",   .func = botSendMessageTracked },
    };
    for (fns) |f| {
        lua.pushFunction(f.func);
        lua.setField(-2, f.name);
    }

    // bot.rules_api_version = rules_api_version
    lua.pushInteger(@intCast(rules_api_version));
    lua.setField(-2, "rules_api_version");

    // Stash the shared response-header metatable for pushIoResult to attach.
    installHeaderMetatable(lua);

    // _G.bot = bot_table
    lua.setGlobal("bot");

    // _G.json = { decode = luaJsonDecode, encode = luaJsonEncode }
    lua.newTable();
    lua.pushFunction(luaJsonDecode);
    lua.setField(-2, "decode");
    lua.pushFunction(luaJsonEncode);
    lua.setField(-2, "encode");
    lua.setGlobal("json");

    // Install an initial (empty) emit accumulator so the registry slot is
    // always a table; callOnMessage replaces it per invocation.
    beginEmitBatch(lua);

    // Install the tg.* facade.  The snippet is a fixed constant — a failure
    // here would be a build-time bug, not a runtime condition.
    lua.doString(TG_FACADE_LUA) catch unreachable;
}

// ---------------------------------------------------------------------------
// Private: retrieve ApiCtx from the Lua registry
// ---------------------------------------------------------------------------

pub fn getCtx(lua: *Lua) *ApiCtx {
    _ = lua.getField(ziglua.registry_index, REGISTRY_KEY);
    const ptr = lua.toPointer(-1) orelse unreachable; // always a light userdata
    lua.pop(1);
    return @constCast(@alignCast(@ptrCast(ptr)));
}

// ---------------------------------------------------------------------------
// Public: buildApiCall — build an ApiCall from a Lua params table
// ---------------------------------------------------------------------------

/// Build an ApiCall from a Lua params table at stack index `params_idx`.
/// Single scan: if any value is a file-descriptor table → multipart payload;
/// otherwise → JSON payload. `method` is duped into the returned ApiCall.
///
/// File-descriptor tables:
///   { __file = "/abs/path" }                      — read from disk
///   { __file_bytes = "...", filename = "name" }   — inline bytes
///
/// Errors: error.FileTooLarge, error.MissingFilename, file-read errors, OOM.
/// Read an integer `chat_id` from a Lua params table, if present. Leaves the
/// stack balanced. Returns null when absent or non-integer.
fn extractChatId(lua: *Lua, params_idx: i32) ?i64 {
    if (!lua.isTable(params_idx)) return null;
    _ = lua.getField(params_idx, "chat_id");
    defer lua.pop(1);
    if (!lua.isInteger(-1)) return null;
    return lua.toInteger(-1) catch null;
}

pub fn buildApiCall(
    lua:        *Lua,
    params_idx: i32,
    method:     []const u8,
    ctx:        *ApiCtx,
) !types.ApiCall {
    const alloc = ctx.allocator;
    const method_owned = try alloc.dupe(u8, method);
    errdefer alloc.free(method_owned);

    const chat_id: ?i64 = extractChatId(lua, params_idx);

    // No params → JSON "{}"
    if (!lua.isTable(params_idx)) {
        return .{
            .method  = method_owned,
            .payload = .{ .json = try alloc.dupe(u8, "{}") },
            .route   = if (chat_id) |c| .{ .chat_id = c } else null,
        };
    }

    // Single-pass scan over the params table.
    var parts: std.ArrayListUnmanaged(types.MultipartPart) = .empty;
    errdefer {
        for (parts.items) |p| {
            alloc.free(p.name);
            alloc.free(p.content);
            if (p.filename) |f| alloc.free(f);
        }
        parts.deinit(alloc);
    }
    var has_files = false;

    lua.pushNil(); // initial key for lua.next()
    while (lua.next(params_idx)) {
        // Stack: [..., params_table, key, value]
        // defer pops value at end of every iteration (including continue/return).
        defer lua.pop(1);

        const key_type = lua.typeOf(-2);
        if (key_type != .string and key_type != .number) continue;
        const key_z = lua.toString(-2) catch continue;
        const key = try alloc.dupe(u8, key_z);
        // key_in_parts: true once key is transferred to parts (so defer must not free it).
        var key_in_parts = false;
        defer if (!key_in_parts) alloc.free(key);

        if (lua.isTable(-1)) {
            // Check for __file (file-path descriptor)
            const ft = lua.getField(-1, "__file");
            if (ft == .string) {
                const path_z = lua.toString(-1) catch {
                    lua.pop(1); // pop __file value
                    return error.InvalidDescriptor; // defer frees key
                };
                lua.pop(1); // pop __file value

                const bytes = std.Io.Dir.cwd().readFileAlloc(
                    ctx.io, path_z, alloc, .limited(ctx.max_file_bytes +| 1),
                ) catch |err| {
                    return if (err == error.StreamTooLong) error.FileTooLarge else err; // defer frees key
                };
                if (bytes.len > ctx.max_file_bytes) {
                    alloc.free(bytes);
                    return error.FileTooLarge; // defer frees key
                }

                const fname = alloc.dupe(u8, std.fs.path.basename(path_z)) catch |err| {
                    alloc.free(bytes);
                    return err; // defer frees key
                };
                parts.append(alloc, .{ .name = key, .content = bytes, .filename = fname }) catch |err| {
                    alloc.free(fname);
                    alloc.free(bytes);
                    return err; // defer frees key
                };
                key_in_parts = true; // parts owns key; defer must not free it
                has_files = true;
                continue;
            }
            lua.pop(1); // pop nil __file result

            // Check for __file_bytes (inline-bytes descriptor)
            const fbt = lua.getField(-1, "__file_bytes");
            if (fbt == .string) {
                const src = lua.toString(-1) catch {
                    lua.pop(1); // pop __file_bytes value
                    return error.InvalidDescriptor; // defer frees key
                };
                if (src.len > ctx.max_file_bytes) {
                    lua.pop(1); // pop __file_bytes value
                    return error.FileTooLarge; // defer frees key
                }
                const bytes = alloc.dupe(u8, src) catch |err| {
                    lua.pop(1); // pop __file_bytes value
                    return err; // defer frees key
                };
                lua.pop(1); // pop __file_bytes value

                // filename sub-key is required
                const fn_type = lua.getField(-1, "filename");
                if (fn_type != .string) {
                    lua.pop(1); // pop nil filename result
                    alloc.free(bytes);
                    return error.MissingFilename; // defer frees key
                }
                const fname_z = lua.toString(-1) catch unreachable;
                const fname   = alloc.dupe(u8, fname_z) catch |err| {
                    lua.pop(1); // pop filename value
                    alloc.free(bytes);
                    return err; // defer frees key
                };
                lua.pop(1); // pop filename value

                parts.append(alloc, .{ .name = key, .content = bytes, .filename = fname }) catch |err| {
                    alloc.free(fname);
                    alloc.free(bytes);
                    return err; // defer frees key
                };
                key_in_parts = true; // parts owns key; defer must not free it
                has_files = true;
                continue;
            }
            lua.pop(1); // pop nil __file_bytes result
            // Fall through: table value that is not a descriptor → skip as scalar
        }

        // Scalar value — stringify into a text part, or skip unsupported types.
        const val_type = lua.typeOf(-1);
        const scalar: []const u8 = switch (val_type) {
            .string  => alloc.dupe(u8, lua.toString(-1) catch "") catch |err| return err,
            .number  => blk: {
                if (lua.isInteger(-1)) {
                    const n = lua.toInteger(-1) catch 0;
                    break :blk std.fmt.allocPrint(alloc, "{d}", .{n}) catch |err| return err;
                } else {
                    const f = lua.toNumber(-1) catch 0;
                    break :blk std.fmt.allocPrint(alloc, "{d}", .{f}) catch |err| return err;
                }
            },
            .boolean => blk: {
                const b = lua.toBoolean(-1);
                break :blk alloc.dupe(u8, if (b) "true" else "false") catch |err| return err;
            },
            else => continue, // skip nil, tables, etc. — defer frees key
        };

        parts.append(alloc, .{ .name = key, .content = scalar, .filename = null }) catch |err| {
            alloc.free(scalar);
            return err; // defer frees key
        };
        key_in_parts = true; // parts owns key; defer must not free it
    }

    if (has_files) {
        return .{
            .method  = method_owned,
            .payload = .{ .multipart = try parts.toOwnedSlice(alloc) },
            .route   = if (chat_id) |c| .{ .chat_id = c } else null,
        };
    }

    // No file parts — free accumulated scalar parts; serialize table as JSON.
    for (parts.items) |p| {
        alloc.free(p.name);
        alloc.free(p.content);
        // filename is always null for scalar parts
    }
    parts.deinit(alloc);

    const body = try serializer.luaTableToJson(lua, params_idx, alloc);
    return .{
        .method  = method_owned,
        .payload = .{ .json = body },
        .route   = if (chat_id) |c| .{ .chat_id = c } else null,
    };
}

// ---------------------------------------------------------------------------
// Private: getStateImpl
// ---------------------------------------------------------------------------
//
fn getStateImpl(
    lua: *Lua,
    ctx: *ApiCtx,
    id: ziglua.Integer,
    comptime dbFn: fn (*state_store.StateStore, i64) anyerror![]u8,
    comptime errPrefix: []const u8,
) c_int {
    const json = dbFn(ctx.db, id) catch |err| {
        lua.raiseErrorStr(errPrefix ++ " db error: %s", .{@errorName(err).ptr});
    };
    defer ctx.db.allocator.free(json);
    serializer.jsonToLuaTable(lua, json, ctx.allocator) catch |err| {
        lua.raiseErrorStr(errPrefix ++ " deserialize error: %s", .{@errorName(err).ptr});
    };
    return 1;
}

fn setStateImpl(
    lua: *Lua,
    ctx: *ApiCtx,
    id: ziglua.Integer,
    comptime dbFn: fn (*state_store.StateStore, i64, []const u8) anyerror!void,
    comptime errPrefix: []const u8,
) c_int {
    lua.checkType(2, .table);

    const json = serializer.luaTableToJson(lua, 2, ctx.allocator) catch |err| {
        lua.raiseErrorStr(errPrefix ++ " serialize error: %s", .{@errorName(err).ptr});
    };
    defer ctx.allocator.free(json);

    dbFn(ctx.db, id, json) catch |err| {
        lua.raiseErrorStr(errPrefix ++ " db error: %s", .{@errorName(err).ptr});
    };
    return 0;
}

// ---------------------------------------------------------------------------
// bot.get_user_state(user_id: integer) -> table
// ---------------------------------------------------------------------------

fn botGetUserState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const user_id = lua.checkInteger(1);

    return getStateImpl(lua, ctx, user_id, state_store.StateStore.getUserState, "getUserState");
}

// ---------------------------------------------------------------------------
// bot.set_user_state(user_id: integer, data: table)
// ---------------------------------------------------------------------------

fn botSetUserState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const user_id = lua.checkInteger(1);
    return setStateImpl(lua, ctx, user_id, state_store.StateStore.setUserState, "setUserState");
}

// ---------------------------------------------------------------------------
// bot.get_chat_state(chat_id: integer) -> table
// ---------------------------------------------------------------------------

fn botGetChatState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const chat_id = lua.checkInteger(1);

    return getStateImpl(lua, ctx, chat_id, state_store.StateStore.getChatState, "getChatState");
}

// ---------------------------------------------------------------------------
// bot.set_chat_state(chat_id: integer, data: table)
// ---------------------------------------------------------------------------

fn botSetChatState(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const chat_id = lua.checkInteger(1);

    return setStateImpl(lua, ctx, chat_id, state_store.StateStore.setChatState, "setChatState");
}

// ---------------------------------------------------------------------------
// bot.get_global(key: string) -> string|nil
// ---------------------------------------------------------------------------

fn botGetGlobal(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const key = lua.checkString(1);

    const maybe_val = ctx.db.getGlobal(key) catch |err| {
        lua.raiseErrorStr("get_global db error: %s", .{@errorName(err).ptr});
    };

    if (maybe_val) |val| {
        defer ctx.db.allocator.free(val);
        _ = lua.pushString(val);
    } else {
        lua.pushNil();
    }
    return 1;
}

// ---------------------------------------------------------------------------
// bot.set_global(key: string, value: string)
// ---------------------------------------------------------------------------

fn botSetGlobal(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const key = lua.checkString(1);
    const val = lua.checkString(2);

    ctx.db.setGlobal(key, val) catch |err| {
        lua.raiseErrorStr("set_global db error: %s", .{@errorName(err).ptr});
    };
    return 0;
}

// ---------------------------------------------------------------------------
// bot.now_ms() -> integer  (epoch ms; the sandbox has no os.time)
// ---------------------------------------------------------------------------

fn botNowMs(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    lua.pushInteger(@intCast(rt.nowMs(ctx.io)));
    return 1;
}

/// Serialize the `payload` field of the arg table at stack index 1, insert a
/// row with `fire_at_ms`, wake the scheduler if wired, and push the new id.
/// Shared by botScheduleAt and botScheduleAfter.
///
/// json is freed explicitly on both the error path (before raiseErrorStr) and
/// the success path — never via defer, because raiseErrorStr calls lua_error
/// (longjmp) which skips any Zig defer in this frame.
fn scheduleInsertCommon(lua: *Lua, ctx: *ApiCtx, fire_at_ms: i64) c_int {
    // payload is optional; default to "{}" when absent or nil.
    // The three raiseErrorStr calls in this expression fire BEFORE json exists,
    // so no allocation is held at those points — no free needed there.
    const ptype = lua.getField(1, "payload");
    const json: []u8 = if (ptype == .table)
        serializer.luaTableToJsonCapped(lua, lua.getTop(), ctx.allocator, ctx.json_max_bytes) catch |err| {
            lua.raiseErrorStr("schedule: payload serialize error: %s", .{@errorName(err).ptr});
        }
    else if (ptype == .nil)
        ctx.allocator.dupe(u8, "{}") catch lua.raiseErrorStr("schedule: OOM", .{})
    else
        lua.raiseErrorStr("schedule: payload must be a table", .{});
    lua.pop(1); // pop the payload field

    const id = ctx.db.scheduleInsert(fire_at_ms, json) catch |err| {
        ctx.allocator.free(json); // free before longjmp; defer would be skipped
        lua.raiseErrorStr("schedule: db error: %s", .{@errorName(err).ptr});
    };
    ctx.allocator.free(json); // success path (raiseErrorStr above is noreturn)
    if (ctx.scheduler) |s| s.wakeUp();
    lua.pushInteger(@intCast(id));
    return 1;
}

// ---------------------------------------------------------------------------
// bot.schedule_at{ at_ms = integer, payload = table? } -> id
// ---------------------------------------------------------------------------

fn botScheduleAt(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table);
    const ctx = getCtx(lua);
    _ = lua.getField(1, "at_ms");
    const at_ms = lua.toInteger(-1) catch lua.raiseErrorStr("schedule_at: at_ms must be an integer", .{});
    lua.pop(1);
    return scheduleInsertCommon(lua, ctx, at_ms);
}

// ---------------------------------------------------------------------------
// bot.schedule_after{ seconds = number(>=0), payload = table? } -> id
// ---------------------------------------------------------------------------

fn botScheduleAfter(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table);
    const ctx = getCtx(lua);
    _ = lua.getField(1, "seconds");
    const seconds = lua.toNumber(-1) catch lua.raiseErrorStr("schedule_after: seconds must be a number", .{});
    lua.pop(1);
    if (!std.math.isFinite(seconds) or seconds < 0)
        lua.raiseErrorStr("schedule_after: seconds must be a finite number >= 0", .{});
    const fire_at_ms = rt.nowMs(ctx.io) +| std.math.lossyCast(i64, seconds * 1000.0);
    return scheduleInsertCommon(lua, ctx, fire_at_ms);
}

// ---------------------------------------------------------------------------
// bot.unschedule(id: integer) -> boolean
// ---------------------------------------------------------------------------

fn botUnschedule(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const id = lua.checkInteger(1);
    const removed = ctx.db.scheduleDelete(@intCast(id)) catch |err| {
        lua.raiseErrorStr("unschedule: db error: %s", .{@errorName(err).ptr});
    };
    lua.pushBoolean(removed);
    return 1;
}

// ---------------------------------------------------------------------------
// bot.log(level: string, message: string)
// ---------------------------------------------------------------------------

fn botLog(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const level = lua.checkString(1);
    const message = lua.checkString(2);

    const log = std.log.scoped(.lua);
    if (std.mem.eql(u8, level, "info")) {
        log.info("{s}", .{message});
    } else if (std.mem.eql(u8, level, "warn")) {
        log.warn("{s}", .{message});
    } else if (std.mem.eql(u8, level, "error")) {
        log.err("{s}", .{message});
    } else {
        lua.raiseErrorStr(
            "bot.log: invalid level '%s'; expected info, warn, or error",
            .{level.ptr},
        );
    }
    return 0;
}

// ---------------------------------------------------------------------------
// bot.emit — fire-and-forget API-call accumulator
//
// `bot.emit{ method = ..., params = {...} }` appends an API-call table to a
// per-invocation accumulator held in the Lua registry.  lua_engine drains the
// accumulator after on_message returns, dispatching the emitted calls in call
// order, before the on_message return-list.
// ---------------------------------------------------------------------------

/// Install a fresh, empty emit accumulator keyed by the calling thread.
/// Call once before each on_message resumeThread.
pub fn beginEmitBatch(lua: *Lua) void {
    _ = lua.pushThread();                       // key = this thread
    lua.newTable();                             // value = {}
    lua.setTableRaw(ziglua.registry_index);     // registry[thread] = {}
}

/// Push the emit accumulator onto the stack and clear the registry entry.
/// After this call: stack has the accumulator table on top; registry entry is nil.
pub fn pushEmitBatch(lua: *Lua) void {
    _ = lua.pushThread();
    _ = lua.getTableRaw(ziglua.registry_index); // stack: [..., accum_table]
    // Clear the registry entry.
    _ = lua.pushThread();
    lua.pushNil();
    lua.setTableRaw(ziglua.registry_index);     // registry[thread] = nil
    // accum_table remains on stack for caller
}

fn botEmit(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table);

    _ = lua.pushThread();
    _ = lua.getTableRaw(ziglua.registry_index); // [arg1, batch]
    const next: ziglua.Integer = @intCast(lua.lenRaw(-1) + 1);
    lua.pushValue(1);           // [arg1, batch, arg1]
    lua.setIndexRaw(-2, next);  // batch[next] = arg1 → [arg1, batch]
    lua.pop(1);                 // [arg1]
    return 0;
}

// ---------------------------------------------------------------------------
// json.decode(s) — parse JSON string → Lua value
// ---------------------------------------------------------------------------

fn luaJsonDecode(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const s = lua.checkString(1);

    serializer.jsonToLuaTableCapped(lua, s, ctx.allocator, ctx.json_max_bytes) catch |err| {
        lua.raiseErrorStr("json.decode: %s", .{@errorName(err).ptr});
    };
    return 1;
}

// ---------------------------------------------------------------------------
// json.encode(v) — serialize Lua value → JSON string
// ---------------------------------------------------------------------------

fn luaJsonEncode(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);

    const json = serializer.luaTableToJsonCapped(lua, 1, ctx.allocator, ctx.json_max_bytes) catch |err| {
        lua.raiseErrorStr("json.encode: %s", .{@errorName(err).ptr});
    };
    defer ctx.allocator.free(json);
    _ = lua.pushString(json);
    return 1;
}

// ---------------------------------------------------------------------------
// bot.url_encode(s) — RFC 3986 percent-encoding
// ---------------------------------------------------------------------------

fn botUrlEncode(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const s = lua.checkString(1);

    // Worst case: every byte becomes 3 chars (%XX).
    var out = std.ArrayListUnmanaged(u8).initCapacity(ctx.allocator, s.len * 3) catch {
        lua.raiseErrorStr("url_encode: out of memory", .{});
    };
    defer out.deinit(ctx.allocator);

    for (s) |byte| {
        switch (byte) {
            'A'...'Z', 'a'...'z', '0'...'9', '-', '_', '.', '~' => {
                out.appendAssumeCapacity(byte);
            },
            else => {
                var enc: [3]u8 = undefined;
                _ = std.fmt.bufPrint(&enc, "%{X:0>2}", .{byte}) catch unreachable;
                out.appendSliceAssumeCapacity(&enc);
            },
        }
    }

    _ = lua.pushString(out.items);
    return 1;
}

// ---------------------------------------------------------------------------
// bot.shell_quote(s) — POSIX single-quote wrapping
// ---------------------------------------------------------------------------

fn botShellQuote(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    const ctx = getCtx(lua);
    const s = lua.checkString(1);

    // Worst case: every char is a single-quote → s.len * 4 + 2 (surrounding quotes).
    var out = std.ArrayListUnmanaged(u8).initCapacity(ctx.allocator, s.len * 4 + 2) catch {
        lua.raiseErrorStr("shell_quote: out of memory", .{});
    };
    defer out.deinit(ctx.allocator);

    out.appendAssumeCapacity('\'');
    for (s) |byte| {
        if (byte == '\'') {
            // End single-quote, append escaped single-quote, reopen single-quote.
            out.appendSliceAssumeCapacity("'\\''");
        } else {
            out.appendAssumeCapacity(byte);
        }
    }
    out.appendAssumeCapacity('\'');

    _ = lua.pushString(out.items);
    return 1;
}

// ---------------------------------------------------------------------------
// bot.http_request{ method, url, headers?, body? } — yield-ing HTTP
// ---------------------------------------------------------------------------

const HeaderError = error{ EmptyName, BadNameByte, BadValueByte };

/// A header name must be a non-empty RFC-7230-ish token: every byte printable
/// ASCII (0x21..0x7e) and not ':'. Rejects controls, space, CRLF, NUL, DEL,
/// high bytes — the set std.http would assert on, plus stricter cleanliness.
/// A plain byte loop avoids relying on std.mem.indexOf* so it is immune to
/// stdlib naming changes in that family of helpers.
fn validateHeaderName(name: []const u8) HeaderError!void {
    if (name.len == 0) return error.EmptyName;
    for (name) |c| {
        if (c < 0x21 or c > 0x7e or c == ':') return error.BadNameByte;
    }
}

/// A header value may hold most bytes but never CR, LF, or NUL (header
/// injection / request smuggling).
fn validateHeaderValue(value: []const u8) HeaderError!void {
    for (value) |c| {
        if (c == '\r' or c == '\n' or c == 0) return error.BadValueByte;
    }
}

/// Free an owned []http.Header slice (each name+value, then the slice).
fn freeHeaderSlice(allocator: std.mem.Allocator, headers: []std.http.Header) void {
    for (headers) |hdr| {
        allocator.free(hdr.name);
        allocator.free(hdr.value);
    }
    allocator.free(headers);
}

/// Free a partially-built header list (used on the error unwinding paths).
fn freeHeaderList(allocator: std.mem.Allocator, list: *std.ArrayListUnmanaged(std.http.Header)) void {
    for (list.items) |hdr| {
        allocator.free(hdr.name);
        allocator.free(hdr.value);
    }
    list.deinit(allocator);
}

fn botHttpRequest(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table);
    const ctx = getCtx(lua);
    const alloc = ctx.allocator;

    // Read method and url from the argument table.
    _ = lua.getField(1, "method");
    const method_raw = lua.checkString(-1);
    _ = lua.getField(1, "url");
    const url_raw = lua.checkString(-1);

    _ = lua.getField(1, "body");
    const body_raw: []const u8 = if (lua.isString(-1)) lua.checkString(-1) else "";

    // Read the optional headers map. A string (the old, dropped form) is now
    // an error so the new table contract is unambiguous.
    _ = lua.getField(1, "headers");
    const headers_idx = lua.getTop();
    const htype = lua.typeOf(headers_idx);
    if (htype == .string)
        lua.raiseErrorStr("http_request: headers must be a table", .{});

    // Validate, cap, and dupe each header into an owned []http.Header.
    var hdr_list: std.ArrayListUnmanaged(std.http.Header) = .empty;
    if (htype == .table) {
        var total_bytes: usize = 0;
        lua.pushNil();
        while (lua.next(headers_idx)) {
            // key at -2, value at -1
            if (lua.typeOf(-2) != .string or lua.typeOf(-1) != .string) {
                freeHeaderList(alloc, &hdr_list);
                lua.raiseErrorStr("http_request: header name and value must be strings", .{});
            }
            const nm = lua.toString(-2) catch unreachable;
            const vl = lua.toString(-1) catch unreachable;
            validateHeaderName(nm) catch {
                freeHeaderList(alloc, &hdr_list);
                lua.raiseErrorStr("http_request: invalid header name", .{});
            };
            validateHeaderValue(vl) catch {
                freeHeaderList(alloc, &hdr_list);
                lua.raiseErrorStr("http_request: invalid header value", .{});
            };
            if (hdr_list.items.len >= 64) {
                freeHeaderList(alloc, &hdr_list);
                lua.raiseErrorStr("http_request: too many headers (max 64)", .{});
            }
            total_bytes += nm.len + vl.len;
            if (total_bytes > 16 * 1024) {
                freeHeaderList(alloc, &hdr_list);
                lua.raiseErrorStr("http_request: headers too large (max 16 KB)", .{});
            }
            const nm_d = alloc.dupe(u8, nm) catch {
                freeHeaderList(alloc, &hdr_list);
                lua.raiseErrorStr("http_request: OOM", .{});
            };
            const vl_d = alloc.dupe(u8, vl) catch {
                alloc.free(nm_d);
                freeHeaderList(alloc, &hdr_list);
                lua.raiseErrorStr("http_request: OOM", .{});
            };
            hdr_list.append(alloc, .{ .name = nm_d, .value = vl_d }) catch {
                alloc.free(nm_d);
                alloc.free(vl_d);
                freeHeaderList(alloc, &hdr_list);
                lua.raiseErrorStr("http_request: OOM", .{});
            };
            lua.pop(1); // pop value, keep key for next()
        }
    }
    // toOwnedSlice failure leaves the list buffer intact, so freeHeaderList here is not a double-free.
    const headers = hdr_list.toOwnedSlice(alloc) catch {
        freeHeaderList(alloc, &hdr_list);
        lua.raiseErrorStr("http_request: OOM", .{});
    };

    // Dupe the scalar payload strings. On OOM, also free the header slice.
    const method = alloc.dupe(u8, method_raw) catch {
        freeHeaderSlice(alloc, headers);
        lua.raiseErrorStr("http_request: OOM", .{});
    };
    const url = alloc.dupe(u8, url_raw) catch {
        alloc.free(method);
        freeHeaderSlice(alloc, headers);
        lua.raiseErrorStr("http_request: OOM", .{});
    };
    const body = alloc.dupe(u8, body_raw) catch {
        alloc.free(method);
        alloc.free(url);
        freeHeaderSlice(alloc, headers);
        lua.raiseErrorStr("http_request: OOM", .{});
    };

    const owned = OwnedStrings{ .http = .{
        .method  = method,
        .url     = url,
        .headers = headers,
        .body    = body,
    }};

    ctx.pending_job = .{ .io = .{
        .io_job = io_pool.IoJob{
            .worker_id   = 0,   // filled in by lua_engine.startHandler/resumeHandler
            .coro_id     = 0,   // filled in by lua_engine.startHandler/resumeHandler
            .payload = .{ .http_request = .{
                .method  = method,
                .url     = url,
                .headers = headers,
                .body    = body,
            }},
        },
        .owned_strings = owned,
    }};

    lua.yield(0); // noreturn — parks here; worker pushes 1 result before next resumeThread
}

// ---------------------------------------------------------------------------
// bot.exec{ argv = { ... } } — yield-ing subprocess (no shell)
// ---------------------------------------------------------------------------

fn botExec(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table);
    const ctx = getCtx(lua);
    const alloc = ctx.allocator;

    _ = lua.getField(1, "argv");
    lua.checkType(-1, .table);
    const argc: usize = @intCast(lua.lenRaw(-1));
    if (argc == 0) lua.raiseErrorStr("bot.exec: argv must be non-empty", .{});

    // Dupe each argv element.
    const argv = alloc.alloc([]u8, argc) catch lua.raiseErrorStr("exec: OOM", .{});
    var n_duped: usize = 0;
    for (argv, 0..) |*slot, i| {
        _ = lua.getIndexRaw(-1, @intCast(i + 1));
        const arg = lua.checkString(-1);
        slot.* = alloc.dupe(u8, arg) catch {
            for (argv[0..n_duped]) |a| alloc.free(a);
            alloc.free(argv);
            lua.raiseErrorStr("exec: OOM", .{});
        };
        n_duped += 1;
        lua.pop(1);
    }

    // Build a const-pointer slice for IoJob (the mutable slice IS the owned copy).
    const argv_const: []const []const u8 = @ptrCast(argv);

    ctx.pending_job = .{ .io = .{
        .io_job = io_pool.IoJob{
            .worker_id = 0, .coro_id = 0,
            .payload = .{ .exec = .{ .argv = argv_const } },
        },
        .owned_strings = OwnedStrings{ .exec = .{ .argv = argv } },
    }};

    lua.yield(0);
}

// ---------------------------------------------------------------------------
// bot.shell{ command = "..." } — yield-ing subprocess via sh -c
// ---------------------------------------------------------------------------

fn botShell(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table);
    const ctx = getCtx(lua);
    const alloc = ctx.allocator;

    _ = lua.getField(1, "command");
    const cmd_raw = lua.checkString(-1);
    const command = alloc.dupe(u8, cmd_raw) catch lua.raiseErrorStr("shell: OOM", .{});

    ctx.pending_job = .{ .io = .{
        .io_job = io_pool.IoJob{
            .worker_id = 0, .coro_id = 0,
            .payload = .{ .shell = .{ .command = command } },
        },
        .owned_strings = OwnedStrings{ .shell = .{ .command = command } },
    }};

    lua.yield(0);
}

// ---------------------------------------------------------------------------
// bot.send_message{ chat_id, text, opts? } — yield-ing tracked send
// ---------------------------------------------------------------------------

fn trackedSendCont(
    state: ?*ziglua.LuaState,
    _: c_int,
    _: ziglua.Context,
) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    // Resume argument at top of stack (-1): integer → success (return it), string → error (raise it).
    if (lua.isInteger(-1)) return 1;
    lua.raiseError();
}

fn botSendMessageTracked(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table);
    const ctx = getCtx(lua);
    const alloc = ctx.allocator;

    // Build a flat params table: {chat_id, text, ...opts}
    lua.newTable(); // params at stack index 2

    // chat_id (required integer)
    _ = lua.getField(1, "chat_id");
    if (!lua.isInteger(-1)) {
        lua.pop(2);
        lua.raiseErrorStr("bot.send_message: chat_id must be an integer", .{});
    }
    lua.setField(2, "chat_id");

    // text (required string)
    _ = lua.getField(1, "text");
    if (!lua.isString(-1)) {
        lua.pop(2);
        lua.raiseErrorStr("bot.send_message: text must be a string", .{});
    }
    lua.setField(2, "text");

    // opts (optional table) — merge all keys into params
    _ = lua.getField(1, "opts");
    if (lua.isTable(-1)) {
        lua.pushNil();
        while (lua.next(-2)) {
            // stack: [arg(1), params(2), opts(3), key(-2), value(-1)]
            lua.pushValue(-2); // dup key
            lua.pushValue(-2); // dup value
            lua.setTableRaw(2);
            lua.pop(1); // pop value, keep key for next()
        }
    }
    lua.pop(1); // pop opts or nil

    // JSON-encode the params table (index 2)
    const body = serializer.luaTableToJson(lua, 2, alloc) catch |err| {
        lua.pop(1);
        lua.raiseErrorStr("bot.send_message: JSON encode failed: %s", .{@errorName(err).ptr});
    };
    lua.pop(1); // pop params

    // Build the tracked ApiCall. tracking zeros are stamped by processResume.
    const method = alloc.dupe(u8, "sendMessage") catch {
        alloc.free(body);
        lua.raiseErrorStr("bot.send_message: OOM", .{});
    };

    ctx.pending_job = .{ .tracked_send = types.ApiCall{
        .method   = method,
        .payload  = .{ .json = body },
        .tracking = .{ .worker_id = 0, .coro_id = 0 },
        .route    = if (extractChatId(lua, 1)) |c| .{ .chat_id = c } else null,
    }};

    lua.yieldCont(0, 0, trackedSendCont);
}

// ---------------------------------------------------------------------------
// Response-header table: case-insensitive lookup via a shared __index metatable
//
// pushIoResult builds resp.headers with the server's VERBATIM header names as
// keys. A shared metatable (one per lua_State, stashed in the registry) gives
// the table an __index that falls back to a case-insensitive scan, so
// resp.headers["content-type"] finds a "Content-Type" key. Iteration with
// pairs() still yields the verbatim keys.
// ---------------------------------------------------------------------------

/// True when `a` equals `b_lower` under ASCII case folding. `b_lower` is
/// assumed already lower-cased; only `a` is folded. This differs from
/// `std.ascii.eqlIgnoreCase`, which folds both sides on every call. The
/// caller lower-cases the query key once, so folding only `a` here avoids
/// re-folding that key on each `next()` iteration of the header scan.
fn asciiEqlLowerRhs(a: []const u8, b_lower: []const u8) bool {
    if (a.len != b_lower.len) return false;
    for (a, b_lower) |ca, cb| {
        if (std.ascii.toLower(ca) != cb) return false;
    }
    return true;
}

/// __index(table, key): case-insensitive lookup over the verbatim-keyed
/// response-header table. Returns the matching value or nil. Always returns
/// exactly one value. Keys longer than 256 bytes are treated as absent and
/// never match.
fn headerIndex(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    // table at 1, key at 2.
    if (lua.typeOf(1) != .table or lua.typeOf(2) != .string) {
        lua.pushNil();
        return 1;
    }
    const key = lua.toString(2) catch {
        lua.pushNil();
        return 1;
    };
    var kbuf: [256]u8 = undefined;
    if (key.len > kbuf.len) {
        lua.pushNil();
        return 1;
    }
    for (key, 0..) |c, i| kbuf[i] = std.ascii.toLower(c);
    const kq = kbuf[0..key.len];

    lua.pushNil(); // first key for next()
    while (lua.next(1)) {
        // stored key at -2, value at -1
        if (lua.typeOf(-2) == .string) {
            const sk = lua.toString(-2) catch {
                lua.pop(1); // pop value, keep key for next()
                continue;
            };
            if (asciiEqlLowerRhs(sk, kq)) {
                // Leave the value (-1) on top; Lua takes it as the result.
                return 1;
            }
        }
        lua.pop(1); // pop value, keep key for next()
    }
    lua.pushNil();
    return 1;
}

/// Build and stash the shared header metatable in the registry. Called once
/// per lua_State from register().
fn installHeaderMetatable(lua: *Lua) void {
    lua.newTable();                                     // mt
    lua.pushFunction(headerIndex);
    lua.setField(-2, "__index");                        // mt.__index = headerIndex
    lua.setField(ziglua.registry_index, HEADER_MT_KEY); // registry[KEY] = mt (pops mt)
}

/// Attach the shared header metatable to the table on top of `lua`'s stack.
/// No-op (leaves the table as-is) if register() never ran on this state.
pub fn attachHeaderMetatable(lua: *Lua) void {
    const t = lua.getField(ziglua.registry_index, HEADER_MT_KEY); // pushes mt or nil
    if (t == .nil) {
        lua.pop(1);
        return;
    }
    lua.setMetatable(-2); // pops mt, sets it on the table below
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "validateHeaderName accepts tokens, rejects empty/colon/control" {
    try validateHeaderName("Content-Type");
    try validateHeaderName("X-Custom_Header.v2");
    try testing.expectError(error.EmptyName, validateHeaderName(""));
    try testing.expectError(error.BadNameByte, validateHeaderName("Bad:Name"));
    try testing.expectError(error.BadNameByte, validateHeaderName("Bad Name"));
    try testing.expectError(error.BadNameByte, validateHeaderName("Bad\r\n"));
    try testing.expectError(error.BadNameByte, validateHeaderName("Bad\x7f"));
}

test "validateHeaderValue accepts text, rejects CR/LF/NUL" {
    try validateHeaderValue("application/json");
    try validateHeaderValue("Bearer abc.def-ghi with spaces");
    try testing.expectError(error.BadValueByte, validateHeaderValue("a\rb"));
    try testing.expectError(error.BadValueByte, validateHeaderValue("a\nb"));
    try testing.expectError(error.BadValueByte, validateHeaderValue("a\x00b"));
}

test "bot.schedule_after / schedule_at / unschedule / now_ms" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // schedule_after returns an integer id and inserts a row.
    try lua.doString(
        \\id = bot.schedule_after{ seconds = 0, payload = { n = 1 } }
        \\assert(type(id) == "number", "id must be a number")
        \\assert(bot.unschedule(id) == true, "unschedule should remove the row")
        \\assert(bot.unschedule(id) == false, "second unschedule should be false")
    );

    // Negative seconds must be rejected with a Lua error.
    {
        const result = lua.doString(
            \\bot.schedule_after{ seconds = -1, payload = {} }
        );
        if (result) |_| return error.TestExpectedLuaError else |_| {}
    }

    // Non-finite seconds: NaN (0/0) must be rejected — not a crash.
    {
        const r = lua.doString("bot.schedule_after{ seconds = 0/0, payload = {} }");
        if (r) |_| return error.TestExpectedLuaError else |_| {}
    }

    // Non-finite seconds: +inf (1/0) must be rejected — not a crash.
    {
        const r = lua.doString("bot.schedule_after{ seconds = 1/0, payload = {} }");
        if (r) |_| return error.TestExpectedLuaError else |_| {}
    }

    // schedule_at accepts a past instant; now_ms returns a usable integer.
    try lua.doString(
        \\local a = bot.schedule_at{ at_ms = bot.now_ms() - 5000, payload = {} }
        \\assert(type(a) == "number")
    );
}

test "state round-trips through bot.*" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // User, chat, and global state each round-trip; a missing key reads back nil.
    try lua.doString(
        \\bot.set_user_state(1, {count = 3})
        \\local u = bot.get_user_state(1)
        \\assert(u.count == 3, "expected count=3 got " .. tostring(u.count))
        \\
        \\bot.set_chat_state(42, {step = "started"})
        \\local c = bot.get_chat_state(42)
        \\assert(c.step == "started", "expected step=started got " .. tostring(c.step))
        \\
        \\bot.set_global("counter", "42")
        \\assert(bot.get_global("counter") == "42", "global round-trip failed")
        \\assert(bot.get_global("__no_such_key__") == nil, "expected nil for missing key")
    );
}

test "bot.log valid levels succeed; invalid level errors" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // Valid levels info and warn should not error.
    try lua.doString(
        \\bot.log("info",  "info message")
        \\bot.log("warn",  "warn message")
    );

    // Invalid level should raise a Lua error (any error variant is fine).
    const result = lua.doString(
        \\bot.log("bad_level", "oops")
    );
    if (result) |_| return error.TestExpectedLuaError else |_| {}
}

test "bot.rules_api_version matches Zig constant" {
    const lua_engine = @import("lua_engine.zig");

    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, lua_engine.RULES_API_VERSION);

    // Read the version from Lua and compare.
    try lua.doString(
        \\assert(type(bot.rules_api_version) == "number",
        \\       "rules_api_version should be a number")
    );

    _ = lua.getGlobal("bot");
    _ = lua.getField(-1, "rules_api_version");
    const ver = try lua.toInteger(-1);
    lua.pop(2);

    try testing.expectEqual(@as(ziglua.Integer, lua_engine.RULES_API_VERSION), ver);
}

test "bot.get_user_state with non-integer arg → Lua error" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    const result = lua.doString(
        \\bot.get_user_state("not_an_int")
    );
    if (result) |_| return error.TestExpectedLuaError else |_| {}
}

test "tg.<method>{...} == bot.emit{method=...,params=...}" {
    const lua_engine = @import("lua_engine.zig");
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var engine = try lua_engine.LuaEngine.init(testing.allocator, &ctx);
    defer engine.deinit();

    try engine.loadString(
        \\function on_message(u)
        \\  tg.sendMessage{ chat_id = 7, text = "via facade" }
        \\  return {}
        \\end
    );
    const actions = try engine.callOnMessage(testing.allocator, "{}");
    defer types.freeApiCalls(actions, testing.allocator);

    try testing.expectEqual(@as(usize, 1), actions.len);
    try testing.expectEqualStrings("sendMessage", actions[0].method);
    const json_body = actions[0].payload.json;
    try testing.expect(std.mem.indexOf(u8, json_body, "\"chat_id\":7") != null);
    try testing.expect(std.mem.indexOf(u8, json_body, "\"text\":\"via facade\"") != null);
    try testing.expect(actions[0].route != null);
    try testing.expectEqual(@as(i64, 7), actions[0].route.?.chat_id);
}

test "two engines have independent ApiCtx (no registry aliasing)" {
    const lua_engine = @import("lua_engine.zig");

    var db1 = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db1.close();
    var db2 = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db2.close();

    var ctx1 = testCtx(&db1);
    var ctx2 = testCtx(&db2);

    var engine1 = try lua_engine.LuaEngine.init(testing.allocator, &ctx1);
    defer engine1.deinit();
    var engine2 = try lua_engine.LuaEngine.init(testing.allocator, &ctx2);
    defer engine2.deinit();

    // Write to engine1's db via engine1.
    try engine1.lua.doString(
        \\bot.set_global("x", "from_engine1")
    );

    // engine2 should NOT see the value written by engine1 (separate db).
    try engine2.lua.doString(
        \\local v = bot.get_global("x")
        \\assert(v == nil, "engine2 should not see engine1 state: " .. tostring(v))
    );

    // Confirm engine1 still has its value.
    try engine1.lua.doString(
        \\local v = bot.get_global("x")
        \\assert(v == "from_engine1", "engine1 lost its state")
    );
}

// ---------------------------------------------------------------------------
// buildApiCall detects __file and returns multipart;
// a file larger than max_file_bytes → error.FileTooLarge
// ---------------------------------------------------------------------------

test "buildApiCall builds multipart and enforces the file limit" {
    const alloc = testing.allocator;
    var db = try state_store.StateStore.open(alloc, ":memory:");
    defer db.close();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var lua = try Lua.init(alloc);
    defer lua.deinit();
    lua.openBase();

    // A __file descriptor under the limit becomes a multipart part.
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "img.jpg", .data = "\xff\xd8\xff" });
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path_len = try tmp.dir.realPathFile(testing.io, "img.jpg", &path_buf);
        const path = path_buf[0..path_len];

        var ctx = ApiCtx{ .db = &db, .allocator = alloc, .io = testing.io, .max_file_bytes = 1024, .json_max_bytes = 1048576 };
        register(lua, &ctx, 1);

        // Build params table: { photo = { __file = path } }
        lua.newTable();             // params
        lua.newTable();             // descriptor
        _ = lua.pushString(path);
        lua.setField(-2, "__file"); // descriptor.__file = path
        lua.setField(-2, "photo");  // params.photo = descriptor
        const params_idx: i32 = lua.getTop();

        const call = try buildApiCall(lua, params_idx, "sendPhoto", &ctx);
        defer types.freeApiCall(call, alloc);
        lua.setTop(0);

        try testing.expectEqualStrings("sendPhoto", call.method);
        switch (call.payload) {
            .multipart => |parts| {
                try testing.expectEqual(@as(usize, 1), parts.len);
                try testing.expectEqualStrings("photo", parts[0].name);
                try testing.expectEqualStrings("\xff\xd8\xff", parts[0].content);
                try testing.expectEqualStrings("img.jpg", parts[0].filename.?);
            },
            .json => return error.ExpectedMultipart,
        }
    }
    // A file larger than the limit → FileTooLarge (11 bytes, limit 10).
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "big.bin", .data = "hello world" });
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path_len = try tmp.dir.realPathFile(testing.io, "big.bin", &path_buf);
        const path = path_buf[0..path_len];

        var ctx = ApiCtx{ .db = &db, .allocator = alloc, .io = testing.io, .max_file_bytes = 10, .json_max_bytes = 1048576 };
        register(lua, &ctx, 1);

        lua.newTable();
        lua.newTable();
        _ = lua.pushString(path);
        lua.setField(-2, "__file");
        lua.setField(-2, "photo");
        const params_idx: i32 = lua.getTop();

        try testing.expectError(error.FileTooLarge, buildApiCall(lua, params_idx, "sendPhoto", &ctx));
        lua.setTop(0);
    }
}

// ---------------------------------------------------------------------------
// json.* global and bot.url_encode / bot.shell_quote
// ---------------------------------------------------------------------------

/// Test ApiCtx for `db` with a custom JSON encode/decode cap (50 MB file cap).
fn testCtxCap(db: *state_store.StateStore, json_max: usize) ApiCtx {
    return .{
        .db             = db,
        .allocator      = testing.allocator,
        .io             = testing.io,
        .max_file_bytes = 52428800,
        .json_max_bytes = json_max,
    };
}

/// Default test ApiCtx for `db`: 50 MB file cap, 1 MB JSON cap.
fn testCtx(db: *state_store.StateStore) ApiCtx {
    return testCtxCap(db, 1048576);
}

/// Run `src` and assert it raises a Lua error. Fails the test if the chunk
/// runs cleanly. Shared by the bad-arg validation tests, which all assert a
/// rejected argument surfaces as a Lua-level error (not a Zig log.err).
fn expectLuaError(lua: *Lua, src: [:0]const u8) !void {
    if (lua.doString(src)) |_| return error.TestExpectedLuaError else |_| {}
}

test "json.decode handles valid, oversized, and malformed input" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();
    lua.openBase();

    // Valid JSON decodes to a Lua table (default 1 MB cap).
    {
        var ctx = testCtx(&db);
        register(lua, &ctx, 1);
        try lua.doString(
            \\local t = json.decode('{"ok":true,"n":42}')
            \\assert(t.ok == true,  "expected ok=true got " .. tostring(t.ok))
            \\assert(t.n  == 42,    "expected n=42 got "   .. tostring(t.n))
        );
    }
    // Oversized input raises a Lua error; the state survives for later code.
    {
        var ctx = testCtxCap(&db, 4); // tiny cap
        register(lua, &ctx, 1);
        const result = lua.doString("json.decode('{\"ok\":true}')");
        if (result) |_| return error.TestExpectedLuaError else |_| {}
        try lua.doString("local x = 1 + 1");
    }
    // Malformed JSON raises a Lua error (doString restores the stack).
    {
        var ctx = testCtx(&db);
        register(lua, &ctx, 1);
        const result = lua.doString("json.decode('{bad json}')");
        if (result) |_| return error.TestExpectedLuaError else |_| {}
    }
}

test "json.encode enforces size and depth limits" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();
    lua.openBase();

    // A small table encodes to a string that decodes back (default cap).
    {
        var ctx = testCtx(&db);
        register(lua, &ctx, 1);
        try lua.doString(
            \\local s = json.encode({a = 1, b = "x"})
            \\assert(type(s) == "string", "expected string got " .. type(s))
            \\local t = json.decode(s)
            \\assert(t.a == 1,   "expected a=1 got " .. tostring(t.a))
            \\assert(t.b == "x", "expected b=x got " .. tostring(t.b))
        );
    }
    // A table over json_max_bytes raises a Lua error.
    {
        var ctx = testCtxCap(&db, 10); // tiny cap
        register(lua, &ctx, 1);
        const result = lua.doString(
            \\json.encode({key = "this is a long value that exceeds the cap"})
        );
        if (result) |_| return error.TestExpectedLuaError else |_| {}
    }
    // A table nested past the depth limit raises a Lua error.
    {
        var ctx = testCtx(&db);
        register(lua, &ctx, 1);
        const result = lua.doString(
            \\local t = {}
            \\local cur = t
            \\for i = 1, 9 do cur.inner = {}; cur = cur.inner end
            \\json.encode(t)
        );
        if (result) |_| return error.TestExpectedLuaError else |_| {}
    }
}

test "json.decode(json.encode(t)) round-trips" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();
    lua.openBase();
    register(lua, &ctx, 1);

    try lua.doString(
        \\local orig = {name = "alice", count = 7, nested = {x = true}}
        \\local t = json.decode(json.encode(orig))
        \\assert(t.name == "alice",     "name mismatch: " .. tostring(t.name))
        \\assert(t.count == 7,          "count mismatch: " .. tostring(t.count))
        \\assert(t.nested.x == true,    "nested.x mismatch")
    );
}

test "bot.url_encode encodes reserved and passes unreserved chars" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();
    lua.openBase();
    register(lua, &ctx, 1);

    try lua.doString(
        \\assert(bot.url_encode("hello world&q=1") == "hello%20world%26q%3D1",
        \\       "reserved: " .. bot.url_encode("hello world&q=1"))
        \\assert(bot.url_encode("Az09-_.~") == "Az09-_.~",
        \\       "unreserved chars changed: " .. bot.url_encode("Az09-_.~"))
    );
}

test "bot.shell_quote wraps in single quotes" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();
    lua.openBase();
    register(lua, &ctx, 1);

    try lua.doString(
        \\assert(bot.shell_quote("hello world") == "'hello world'")
        \\assert(bot.shell_quote("it's")        == "'it'\\''s'")
        \\assert(bot.shell_quote("")             == "''")
    );
}

test "json.* and bot.url_encode/shell_quote available without require" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();
    var ctx = testCtx(&db);
    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();
    lua.openBase();
    register(lua, &ctx, 1);

    try lua.doString(
        \\assert(type(json) == "table",          "json not a table")
        \\assert(type(json.decode) == "function","json.decode missing")
        \\assert(type(json.encode) == "function","json.encode missing")
        \\assert(type(bot.url_encode) == "function", "bot.url_encode missing")
        \\assert(type(bot.shell_quote) == "function", "bot.shell_quote missing")
    );
}

test "bot.send_message produces a tracked send with merged opts" {
    const lua_engine_mod = @import("lua_engine.zig");

    // A bare send yields a .tracked_send stamped with the worker/coro ids.
    {
        var db = try state_store.StateStore.open(testing.allocator, ":memory:");
        defer db.close();
        var ctx = testCtx(&db);
        var engine = try lua_engine_mod.LuaEngine.init(testing.allocator, &ctx);
        defer engine.deinit();

        try engine.loadString(
            \\function on_message(u)
            \\  local mid = bot.send_message{ chat_id = 1, text = "hi" }
            \\  return {}
            \\end
        );

        const outcome = try engine.startHandler("{}", 7, 3, testing.allocator, "on_message", null);
        switch (outcome) {
            .yielded => |y| {
                defer lua_engine_mod.teardownCoro(engine.lua, y.handle);
                switch (y.pending_job) {
                    .tracked_send => |call| {
                        defer types.freeApiCall(call, testing.allocator);
                        try testing.expectEqualStrings("sendMessage", call.method);
                        try testing.expect(call.tracking != null);
                        try testing.expectEqual(@as(u8, 3),  call.tracking.?.worker_id);
                        try testing.expectEqual(@as(u32, 7), call.tracking.?.coro_id);
                        try testing.expect(std.mem.indexOf(u8, call.payload.json, "\"chat_id\":1") != null);
                        try testing.expect(std.mem.indexOf(u8, call.payload.json, "\"text\":\"hi\"") != null);
                        try testing.expect(call.route != null);
                        try testing.expectEqual(@as(i64, 1), call.route.?.chat_id);
                    },
                    .io => return error.ExpectedTrackedSend,
                }
            },
            .done => return error.ExpectedYield,
            .err  => return error.ExpectedYield,
        }
    }
    // opts are merged into the JSON body.
    {
        var db = try state_store.StateStore.open(testing.allocator, ":memory:");
        defer db.close();
        var ctx = testCtx(&db);
        var engine = try lua_engine_mod.LuaEngine.init(testing.allocator, &ctx);
        defer engine.deinit();

        try engine.loadString(
            \\function on_message(u)
            \\  local mid = bot.send_message{
            \\    chat_id = 5,
            \\    text    = "hello",
            \\    opts    = { parse_mode = "HTML" }
            \\  }
            \\  return {}
            \\end
        );

        const outcome = try engine.startHandler("{}", 1, 0, testing.allocator, "on_message", null);
        switch (outcome) {
            .yielded => |y| {
                defer lua_engine_mod.teardownCoro(engine.lua, y.handle);
                switch (y.pending_job) {
                    .tracked_send => |call| {
                        defer types.freeApiCall(call, testing.allocator);
                        try testing.expect(std.mem.indexOf(u8, call.payload.json, "\"parse_mode\":\"HTML\"") != null);
                        try testing.expect(std.mem.indexOf(u8, call.payload.json, "\"chat_id\":5") != null);
                    },
                    .io => return error.ExpectedTrackedSend,
                }
            },
            else => return error.ExpectedYield,
        }
    }
}

test "state setters/getters reject bad arguments with a Lua error" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // Non-integer ids: checkInteger rejects a string.
    try expectLuaError(lua, "bot.set_user_state(\"x\", {})");
    try expectLuaError(lua, "bot.get_chat_state(\"x\")");
    try expectLuaError(lua, "bot.set_chat_state(\"x\", {})");

    // Setters require a table as the second argument: checkType rejects a string.
    try expectLuaError(lua, "bot.set_user_state(1, \"not a table\")");
    try expectLuaError(lua, "bot.set_chat_state(1, \"not a table\")");

    // Global key/value must be strings: checkString rejects nil/absent args.
    try expectLuaError(lua, "bot.get_global()");
    try expectLuaError(lua, "bot.set_global(\"k\")");
    try expectLuaError(lua, "bot.set_global()");
}

test "buildApiCall handles __file_bytes and requires filename" {
    const alloc = testing.allocator;
    var db = try state_store.StateStore.open(alloc, ":memory:");
    defer db.close();

    var lua = try Lua.init(alloc);
    defer lua.deinit();
    lua.openBase();

    var ctx = testCtx(&db);
    register(lua, &ctx, 1);

    // { photo = { __file_bytes = "...", filename = "x.bin" } } → one multipart part.
    {
        lua.newTable();                       // params
        lua.newTable();                       // descriptor
        _ = lua.pushString("\x01\x02\x03");
        lua.setField(-2, "__file_bytes");     // descriptor.__file_bytes
        _ = lua.pushString("x.bin");
        lua.setField(-2, "filename");         // descriptor.filename
        lua.setField(-2, "photo");            // params.photo = descriptor
        const params_idx: i32 = lua.getTop();

        const call = try buildApiCall(lua, params_idx, "sendDocument", &ctx);
        defer types.freeApiCall(call, alloc);
        lua.setTop(0);

        try testing.expectEqualStrings("sendDocument", call.method);
        switch (call.payload) {
            .multipart => |parts| {
                try testing.expectEqual(@as(usize, 1), parts.len);
                try testing.expectEqualStrings("photo", parts[0].name);
                try testing.expectEqualStrings("\x01\x02\x03", parts[0].content);
                try testing.expectEqualStrings("x.bin", parts[0].filename.?);
            },
            .json => return error.ExpectedMultipart,
        }
    }
    // __file_bytes without filename → error.MissingFilename.
    {
        lua.newTable();                       // params
        lua.newTable();                       // descriptor (no filename)
        _ = lua.pushString("\x01\x02\x03");
        lua.setField(-2, "__file_bytes");
        lua.setField(-2, "photo");
        const params_idx: i32 = lua.getTop();

        try testing.expectError(error.MissingFilename, buildApiCall(lua, params_idx, "sendDocument", &ctx));
        lua.setTop(0);
    }
}

test "headerIndex resolves response headers case-insensitively" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // Build a verbatim-keyed table and attach the shared header metatable, then
    // confirm a differently-cased lookup hits the same value and a missing key
    // reads back nil.
    lua.newTable();
    _ = lua.pushString("application/json");
    lua.setField(-2, "Content-Type");
    attachHeaderMetatable(lua);
    lua.setGlobal("h");

    try lua.doString(
        \\assert(h["content-type"] == "application/json",
        \\       "case-insensitive lookup failed: " .. tostring(h["content-type"]))
        \\assert(h["CONTENT-TYPE"] == "application/json", "upper-case lookup failed")
        \\assert(h["Content-Type"] == "application/json", "verbatim lookup failed")
        \\assert(h["X-Missing"] == nil, "missing header should read nil")
    );
}

test "bot.send_message rejects missing required fields" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // Missing chat_id and missing text each raise a Lua error. The send yields
    // on success, so reaching the yield would mean the field passed validation;
    // a raised error before the yield is the rejection we want.
    try expectLuaError(lua, "bot.send_message{ text = \"x\" }");
    try expectLuaError(lua, "bot.send_message{ chat_id = 1 }");
}

test "http_request/exec/shell reject bad arguments" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // bot.exec: an empty argv is rejected before any subprocess is built.
    try expectLuaError(lua, "bot.exec{ argv = {} }");

    // bot.http_request: a string headers map is the dropped form and is rejected.
    try expectLuaError(lua,
        "bot.http_request{ method = \"GET\", url = \"http://x\", headers = \"oops\" }",
    );

    // bot.shell: a non-string command fails checkString. A table never coerces
    // to a string (unlike a number, which checkString accepts), so it rejects
    // before any command is duped or yield is reached.
    try expectLuaError(lua, "bot.shell{ command = {} }");
}

test "bot.schedule_after without payload returns a valid id" {
    var db = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer db.close();

    var ctx = testCtx(&db);

    var lua = try Lua.init(testing.allocator);
    defer lua.deinit();

    lua.openBase();
    register(lua, &ctx, 1);

    // No payload key at all → the nil-payload branch dupes "{}" and inserts a
    // row. openBase does not load `math`, so integer-ness is checked with `% 1`
    // rather than math.type.
    try lua.doString(
        \\local id = bot.schedule_after{ seconds = 0 }
        \\assert(type(id) == "number", "id must be a number, got " .. type(id))
        \\assert(id % 1 == 0, "id must be an integer, got " .. tostring(id))
        \\assert(id > 0, "id must be positive, got " .. tostring(id))
    );
}
