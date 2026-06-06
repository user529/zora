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
    /// Maximum file size in bytes for multipart upload descriptors.
    max_file_bytes: usize,
    /// Maximum bytes for json.decode input / json.encode output.
    json_max_bytes: usize,
    /// Set by a yield-ing C function immediately before calling lua.yield(0).
    /// Read and cleared by lua_engine.startHandler / resumeHandler after
    /// resumeThread returns .yield.  Null at all other times.
    pending_job: ?PendingJob = null,
};

const REGISTRY_KEY: [:0]const u8 = "_zora_ctx";

// ---------------------------------------------------------------------------
// Coroutine I/O types — shared between lua_api, lua_engine, and worker
// ---------------------------------------------------------------------------

/// Heap-allocated copies of an IoJob's payload strings.
/// The IoJob references these slices — they must outlive the io_pool job.
/// Free with freeOwnedStrings when the coroutine finishes, errors, or is reaped.
pub const OwnedStrings = union(enum) {
    http: struct { method: []u8, url: []u8, headers: []u8, body: []u8 },
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
    const ptr = lua.toPointer(-1) catch unreachable; // always a light userdata
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

                const bytes = std.fs.cwd().readFileAlloc(
                    alloc, path_z, ctx.max_file_bytes +| 1,
                ) catch |err| {
                    return if (err == error.FileTooBig) error.FileTooLarge else err; // defer frees key
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
    lua.checkType(2, .table);

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
// accumulator after on_message returns: emitted calls are dispatched in call
// order, before the on_message return-list.
// ---------------------------------------------------------------------------

/// Install a fresh, empty emit accumulator keyed by the calling thread.
/// Call once before each on_message resumeThread.
pub fn beginEmitBatch(lua: *Lua) void {
    _ = lua.pushThread();                       // key = this thread
    lua.newTable();                             // value = {}
    lua.rawSetTable(ziglua.registry_index);     // registry[thread] = {}
}

/// Push the emit accumulator onto the stack and clear the registry entry.
/// After this call: stack has the accumulator table on top; registry entry is nil.
pub fn pushEmitBatch(lua: *Lua) void {
    _ = lua.pushThread();
    _ = lua.rawGetTable(ziglua.registry_index); // stack: [..., accum_table]
    // Clear the registry entry.
    _ = lua.pushThread();
    lua.pushNil();
    lua.rawSetTable(ziglua.registry_index);     // registry[thread] = nil
    // accum_table remains on stack for caller
}

fn botEmit(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table);

    _ = lua.pushThread();
    _ = lua.rawGetTable(ziglua.registry_index); // [arg1, batch]
    const next: ziglua.Integer = @intCast(lua.rawLen(-1) + 1);
    lua.pushValue(1);           // [arg1, batch, arg1]
    lua.rawSetIndex(-2, next);  // batch[next] = arg1 → [arg1, batch]
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

fn botHttpRequest(state: ?*ziglua.LuaState) callconv(.c) c_int {
    const lua: *Lua = @ptrCast(state.?);
    lua.checkType(1, .table);
    const ctx = getCtx(lua);
    const alloc = ctx.allocator;

    // Read fields from the argument table.
    _ = lua.getField(1, "method");
    const method_raw = lua.checkString(-1);
    _ = lua.getField(1, "url");
    const url_raw = lua.checkString(-1);
    _ = lua.getField(1, "headers");
    const headers_raw: []const u8 = if (lua.isString(-1)) lua.checkString(-1) else "";
    _ = lua.getField(1, "body");
    const body_raw: []const u8 = if (lua.isString(-1)) lua.checkString(-1) else "";

    // Dupe all strings — they must outlive the coroutine yield.
    const method  = alloc.dupe(u8, method_raw)  catch lua.raiseErrorStr("http_request: OOM", .{});
    const url     = alloc.dupe(u8, url_raw)     catch { alloc.free(method); lua.raiseErrorStr("http_request: OOM", .{}); };
    const headers = alloc.dupe(u8, headers_raw) catch { alloc.free(method); alloc.free(url); lua.raiseErrorStr("http_request: OOM", .{}); };
    const body    = alloc.dupe(u8, body_raw)    catch { alloc.free(method); alloc.free(url); alloc.free(headers); lua.raiseErrorStr("http_request: OOM", .{}); };

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
            .deadline_ms = -1,
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
    const argc: usize = @intCast(lua.rawLen(-1));
    if (argc == 0) lua.raiseErrorStr("bot.exec: argv must be non-empty", .{});

    // Dupe each argv element.
    const argv = alloc.alloc([]u8, argc) catch lua.raiseErrorStr("exec: OOM", .{});
    var n_duped: usize = 0;
    for (argv, 0..) |*slot, i| {
        _ = lua.rawGetIndex(-1, @intCast(i + 1));
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
            .worker_id = 0, .coro_id = 0, .deadline_ms = -1,
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
            .worker_id = 0, .coro_id = 0, .deadline_ms = -1,
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
            lua.rawSetTable(2);
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
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

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

    _ = try lua.getGlobal("bot");
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
        try tmp.dir.writeFile(.{ .sub_path = "img.jpg", .data = "\xff\xd8\xff" });
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try tmp.dir.realpath("img.jpg", &path_buf);

        var ctx = ApiCtx{ .db = &db, .allocator = alloc, .max_file_bytes = 1024, .json_max_bytes = 1048576 };
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
        try tmp.dir.writeFile(.{ .sub_path = "big.bin", .data = "hello world" });
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try tmp.dir.realpath("big.bin", &path_buf);

        var ctx = ApiCtx{ .db = &db, .allocator = alloc, .max_file_bytes = 10, .json_max_bytes = 1048576 };
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
        .max_file_bytes = 52428800,
        .json_max_bytes = json_max,
    };
}

/// Default test ApiCtx for `db`: 50 MB file cap, 1 MB JSON cap.
fn testCtx(db: *state_store.StateStore) ApiCtx {
    return testCtxCap(db, 1048576);
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

        const outcome = try engine.startHandler("{}", 7, 3, testing.allocator);
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

        const outcome = try engine.startHandler("{}", 1, 0, testing.allocator);
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
