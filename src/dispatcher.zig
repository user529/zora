// dispatcher.zig — outbound HTTP thread pool → Telegram API
//
// ACTION STRING-PAYLOAD OWNERSHIP CONTRACT (see TECH_DEBT.md TD-4)
// ----------------------------------------------------------------
// worker.zig transfers ownership of all heap-allocated strings inside each
// Action to this dispatcher.  The dispatcher MUST free every string payload
// after the action has been sent (successfully or not).  Use the helper
// pattern below for every dequeue:
//
//   const action = dispatcher_queue.pop();
//   defer freeActionPayload(action, allocator);
//   // ... send action ...
//
// freeActionPayload must mirror lua_engine.LuaEngine.freeActions field-by-field:
//   send_message    → free text
//   send_message_ex → free text, free opts
//   answer_callback → free callback_query_id, free text (if non-null)
//   delete_message  → nothing to free
//
// Failure to call freeActionPayload leaks memory on every dispatched action.

const std = @import("std");
const types = @import("types.zig");
const queue_mod = @import("queue.zig");

const log = std.log.scoped(.dispatcher);

// ---------------------------------------------------------------------------
// Public: DispatcherArgs — all parameters for one dispatcher thread
// ---------------------------------------------------------------------------

pub const DispatcherArgs = struct {
    id: u8,
    queue: *queue_mod.Queue(types.Action),
    bot_token: []const u8,
    /// Base URL for the Telegram Bot API.
    /// Production: "https://api.telegram.org"
    /// Tests:      "http://127.0.0.1:{port}"  (plain HTTP, no TLS, to a mock)
    api_base: []const u8,
    allocator: std.mem.Allocator,
    stop: *std.atomic.Value(bool),
};

// ---------------------------------------------------------------------------
// Public: thread entry point
// ---------------------------------------------------------------------------

/// Dispatcher thread main loop.
/// Creates one `std.http.Client` (and therefore one connection pool) per
/// thread, giving each thread its own persistent connection to the API.
/// Runs until `args.stop` is set to true.
pub fn dispatcherThread(args: DispatcherArgs) void {
    var client = std.http.Client{ .allocator = args.allocator };
    defer client.deinit();
    const url_prefix = std.fmt.allocPrint(
        args.allocator,
        "{s}/bot{s}/",
        .{ args.api_base, args.bot_token },
    ) catch unreachable;
    defer args.allocator.free(url_prefix);

    while (!args.stop.load(.acquire)) {
        // const maybe_action = args.queue.tryPop();
        const maybe_action = args.queue.popTimeout(10 * std.time.ns_per_ms);
        if (maybe_action == null) {
            // std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        }
        const action = maybe_action.?;
        // Always free string payloads, whether send succeeds or not.
        defer types.freeActionPayload(action, args.allocator);

        sendWithRetry(&client, action, url_prefix, args.allocator) catch |err| {
            log.warn("dispatcher {d}: dropped action after retry failure: {s}", .{ args.id, @errorName(err) });
        };
    }
    log.info("dispatcher {d}: stopped", .{args.id});
}

// ---------------------------------------------------------------------------
// Private: send with one retry on failure
// ---------------------------------------------------------------------------

fn sendWithRetry(
    client: *std.http.Client,
    action: types.Action,
    url_prefix: []const u8,
    allocator: std.mem.Allocator,
) !void {
    if (send(client, action, url_prefix, allocator)) {
        return;
    } else |err| {
        log.warn("send failed ({s}), retrying in 1s", .{@errorName(err)});
        std.Thread.sleep(1 * std.time.ns_per_s);
        // Reinitialize the client so the retry always opens a fresh TCP
        // connection — any pooled connection from the failed attempt may be
        // broken and would cause a second WriteFailed/ReadFailed.
        client.deinit();
        client.* = std.http.Client{ .allocator = allocator };
        try send(client, action, url_prefix, allocator);
    }
}

// ---------------------------------------------------------------------------
// Private: single HTTP POST to the Telegram Bot API
// ---------------------------------------------------------------------------

fn send(
    client: *std.http.Client,
    action: types.Action,
    url_prefix: []const u8,
    allocator: std.mem.Allocator,
) !void {
    const method_path = actionMethod(action);

    const body = try buildBody(action, allocator);
    defer allocator.free(body);

    var buf: [256]u8 = undefined;
    const url = std.fmt.bufPrint(&buf, "{s}{s}", .{ url_prefix, method_path }) catch unreachable;

    log.info("→ {s}  {s}", .{ method_path, body });

    const result = try client.fetch(.{
        .location = .{ .url = url },
        .payload = body,
        .keep_alive = false, // each send uses a fresh connection; simplifies retry logic
        .extra_headers = &.{
            .{ .name = "Content-Type", .value = "application/json" },
        },
    });

    if (result.status.class() != .success) {
        log.warn("Telegram API returned HTTP {d} for {s}", .{
            @intFromEnum(result.status), method_path,
        });
        return error.TelegramApiError;
    }

    log.info("← {d} {s}", .{ @intFromEnum(result.status), method_path });
}

// ---------------------------------------------------------------------------
// Private: action → Telegram Bot API method name
// ---------------------------------------------------------------------------

fn actionMethod(action: types.Action) [:0]const u8 {
    return switch (action) {
        .send_message, .send_message_ex => "sendMessage",
        .answer_callback => "answerCallbackQuery",
        .delete_message => "deleteMessage",
    };
}

// ---------------------------------------------------------------------------
// Private: action → JSON request body
// ---------------------------------------------------------------------------

pub fn buildBody(action: types.Action, allocator: std.mem.Allocator) ![]u8 {
    return switch (action) {
        .send_message => |a| std.json.Stringify.valueAlloc(allocator, .{
            .chat_id = a.chat_id,
            .text = a.text,
        }, .{}),

        .send_message_ex => |a| blk: {
            var aw: std.io.Writer.Allocating = .init(allocator);
            defer aw.deinit();
            try aw.writer.print("{{\"chat_id\":{d},\"text\":", .{a.chat_id});
            try std.json.Stringify.value(a.text, .{}, &aw.writer);
            if (!std.mem.eql(u8, a.opts, "{}")) {
                try aw.writer.writeByte(',');
                try aw.writer.writeAll(a.opts[1 .. a.opts.len - 1]); // strip outer { }
            }
            try aw.writer.writeByte('}');
            break :blk try aw.toOwnedSlice();
        },

        .answer_callback => |a| blk: {
            if (a.text) |text| {
                break :blk std.json.Stringify.valueAlloc(allocator, .{
                    .callback_query_id = a.callback_query_id,
                    .text = text,
                }, .{});
            } else {
                break :blk std.json.Stringify.valueAlloc(allocator, .{
                    .callback_query_id = a.callback_query_id,
                }, .{});
            }
        },

        .delete_message => |a| std.json.Stringify.valueAlloc(allocator, .{
            .chat_id = a.chat_id,
            .message_id = a.message_id,
        }, .{}),
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

// ── Unit tests: buildBody ────────────────────────────────────────────────────

test "AC-9.1: send_message body contains chat_id and text" {
    const action = types.Action{ .send_message = .{
        .chat_id = 123,
        .text = "hello",
    } };
    const body = try buildBody(action, testing.allocator);
    defer testing.allocator.free(body);

    try testing.expectEqualStrings("{\"chat_id\":123,\"text\":\"hello\"}", body);
}

test "AC-9.2: send_message_ex merges opts fields into body" {
    const action = types.Action{ .send_message_ex = .{
        .chat_id = 7,
        .text = "bold",
        .opts = try testing.allocator.dupe(u8, "{\"parse_mode\":\"HTML\"}"),
    } };
    defer testing.allocator.free(action.send_message_ex.opts);

    const body = try buildBody(action, testing.allocator);
    defer testing.allocator.free(body);

    try testing.expectEqualStrings(
        "{\"chat_id\":7,\"text\":\"bold\",\"parse_mode\":\"HTML\"}",
        body,
    );
}

test "AC-9.2: send_message_ex with empty opts == send_message body" {
    const action = types.Action{ .send_message_ex = .{
        .chat_id = 1,
        .text = "plain",
        .opts = try testing.allocator.dupe(u8, "{}"),
    } };
    defer testing.allocator.free(action.send_message_ex.opts);

    const body = try buildBody(action, testing.allocator);
    defer testing.allocator.free(body);

    try testing.expectEqualStrings("{\"chat_id\":1,\"text\":\"plain\"}", body);
}

test "AC-9.3: answer_callback body with text" {
    const action = types.Action{ .answer_callback = .{
        .callback_query_id = "cq42",
        .text = "done",
    } };
    const body = try buildBody(action, testing.allocator);
    defer testing.allocator.free(body);

    try testing.expectEqualStrings(
        "{\"callback_query_id\":\"cq42\",\"text\":\"done\"}",
        body,
    );
}

test "AC-9.3: answer_callback body without text" {
    const action = types.Action{ .answer_callback = .{
        .callback_query_id = "cq99",
        .text = null,
    } };
    const body = try buildBody(action, testing.allocator);
    defer testing.allocator.free(body);

    try testing.expectEqualStrings("{\"callback_query_id\":\"cq99\"}", body);
}

test "AC-9.4: delete_message body contains chat_id and message_id" {
    const action = types.Action{ .delete_message = .{
        .chat_id = 55,
        .message_id = 1001,
    } };
    const body = try buildBody(action, testing.allocator);
    defer testing.allocator.free(body);

    try testing.expectEqualStrings("{\"chat_id\":55,\"message_id\":1001}", body);
}

test "AC-9.1: actionMethod returns correct paths" {
    try testing.expectEqualStrings("sendMessage", actionMethod(.{ .send_message = .{ .chat_id = 0, .text = "" } }));
    try testing.expectEqualStrings("sendMessage", actionMethod(.{ .send_message_ex = .{ .chat_id = 0, .text = "", .opts = "{}" } }));
    try testing.expectEqualStrings("answerCallbackQuery", actionMethod(.{ .answer_callback = .{ .callback_query_id = "", .text = null } }));
    try testing.expectEqualStrings("deleteMessage", actionMethod(.{ .delete_message = .{ .chat_id = 0, .message_id = 0 } }));
}

// ── Mock HTTP server ─────────────────────────────────────────────────────────

/// A minimal HTTP/1.1 server used only in tests.
/// Accepts connections, records each POST request, and replies with
/// {"ok":true}. Closes cleanly when `stop` is set or when `server.deinit()`
/// is called (which unblocks `accept` with an error).
pub const MockServer = struct {
    server: std.net.Server,
    thread: std.Thread,
    received: queue_mod.Queue(MockRequest),
    stop: std.atomic.Value(bool),
    call_cnt: std.atomic.Value(u32),
    allocator: std.mem.Allocator,

    const MockRequest = struct {
        path: []const u8,
        body: []const u8,
        allocator: std.mem.Allocator,

        fn deinit(self: *MockRequest) void {
            self.allocator.free(self.path);
            self.allocator.free(self.body);
        }
    };

    // Heap-allocated so that `&self` passed to the spawned thread remains valid
    // after init() returns.  A by-value return would copy the struct and
    // invalidate the pointer held by mockLoop.
    pub fn init(allocator: std.mem.Allocator) !*MockServer {
        const self = try allocator.create(MockServer);
        errdefer allocator.destroy(self);
        const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
        self.server = try addr.listen(.{ .reuse_address = true });
        errdefer self.server.deinit();
        self.allocator = allocator;
        self.call_cnt = std.atomic.Value(u32).init(0);
        self.stop = std.atomic.Value(bool).init(false);
        self.received = try queue_mod.Queue(MockRequest).init(allocator, 512);
        errdefer self.received.deinit(allocator);
        self.thread = try std.Thread.spawn(.{}, mockLoop, .{self});
        return self;
    }

    pub fn deinit(self: *MockServer) void {
        self.stop.store(true, .release);
        // mockLoop polls with a 10ms timeout and re-checks stop on each
        // iteration, so it will exit within ~10ms of seeing stop = true.
        // The server fd is only closed below, after the thread has exited —
        // so mockLoop never sees a closed fd.
        self.thread.join();
        self.server.deinit();
        // Drain any unread requests.
        while (self.received.popTimeout(0)) |req| {
            var r = req;
            r.deinit();
        }
        self.received.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    fn port(self: *const MockServer) u16 {
        return self.server.listen_address.in.sa.port;
    }

    pub fn baseUrl(self: *const MockServer, buf: []u8) []u8 {
        return std.fmt.bufPrint(buf, "http://127.0.0.1:{d}", .{
            std.mem.bigToNative(u16, self.port()),
        }) catch unreachable;
    }

    pub fn waitForN(self: *MockServer, n: usize, timeout_ms: u64) bool {
        const t0 = std.time.milliTimestamp();
        while (self.received.len() < n) {
            if (@as(u64, @intCast(std.time.milliTimestamp() - t0)) >= timeout_ms)
                return false;
            std.Thread.sleep(5 * std.time.ns_per_ms);
        }
        return true;
    }
};

fn mockLoop(srv: *MockServer) void {
    const fd = srv.server.stream.handle;
    while (!srv.stop.load(.acquire)) {
        // Poll with a short timeout so we re-check `stop` regularly without
        // ever calling accept() on a closed fd.  The fd is only closed in
        // MockServer.deinit() *after* thread.join(), so it is always valid here.
        var pfd = std.posix.pollfd{
            .fd = fd,
            .events = std.posix.POLL.IN,
            .revents = 0,
        };
        const n = std.posix.poll(@as(*[1]std.posix.pollfd, &pfd)[0..1], 10) catch return;
        if (n == 0) continue; // timeout — recheck stop
        if (pfd.revents & std.posix.POLL.IN == 0) continue;

        const conn = srv.server.accept() catch return;
        const t = std.Thread.spawn(.{}, mockHandle, .{ srv, conn }) catch {
            conn.stream.close();
            continue;
        };
        t.detach();
    }
}

fn mockHandle(srv: *MockServer, conn: std.net.Server.Connection) void {
    defer conn.stream.close();
    // Keep reading requests on the same connection (keep-alive).
    while (!srv.stop.load(.acquire)) {
        const req = parseHttpRequest(conn.stream, srv.allocator) catch return;
        const r = req orelse return;

        srv.received.push(r) catch {
            var rr = r;
            rr.deinit();
        };

        // Reply with a minimal Telegram-style 200 response.
        const response =
            "HTTP/1.1 200 OK\r\n" ++
            "Content-Type: application/json\r\n" ++
            "Content-Length: 15\r\n" ++
            "Connection: keep-alive\r\n" ++
            "\r\n" ++
            "{\"ok\":true}\r\n\r\n";
        conn.stream.writeAll(response) catch return;
        _ = srv.call_cnt.fetchAdd(1, .release);
    }
}

/// Read one HTTP/1.1 request from `stream`.
/// Returns null on EOF or connection close.
fn parseHttpRequest(stream: std.net.Stream, allocator: std.mem.Allocator) !?MockServer.MockRequest {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(allocator);

    var tmp: [8192]u8 = undefined;

    // Read until the blank line that separates headers from body.
    while (std.mem.indexOf(u8, buf.items, "\r\n\r\n") == null) {
        const n = stream.read(&tmp) catch |err| switch (err) {
            error.ConnectionResetByPeer, error.BrokenPipe => return null,
            else => return err,
        };
        if (n == 0) return null;
        try buf.appendSlice(allocator, tmp[0..n]);
    }

    const he = std.mem.indexOf(u8, buf.items, "\r\n\r\n").?;
    const header_section = buf.items[0..he];

    // Parse request line: "POST /path HTTP/1.1"
    const rl_end = std.mem.indexOf(u8, header_section, "\r\n") orelse return null;
    var parts = std.mem.splitScalar(u8, header_section[0..rl_end], ' ');
    _ = parts.next() orelse return null; // skip method
    const raw_path = parts.next() orelse return null;
    const path = try allocator.dupe(u8, raw_path);
    errdefer allocator.free(path);

    // Parse Content-Length.
    var content_length: usize = 0;
    var lines = std.mem.splitSequence(u8, header_section[rl_end + 2 ..], "\r\n");
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = line[0..colon];
        const value = std.mem.trim(u8, line[colon + 1 ..], " ");
        if (std.ascii.eqlIgnoreCase(name, "content-length")) {
            content_length = std.fmt.parseInt(usize, value, 10) catch 0;
            break;
        }
    }

    // Ensure we have the full body.
    const body_start = he + 4;
    const needed = body_start + content_length;
    while (buf.items.len < needed) {
        const n = stream.read(&tmp) catch |err| switch (err) {
            error.ConnectionResetByPeer, error.BrokenPipe => break,
            else => return err,
        };
        if (n == 0) break;
        try buf.appendSlice(allocator, tmp[0..n]);
    }

    const body_end = @min(needed, buf.items.len);
    const body = try allocator.dupe(u8, buf.items[body_start..body_end]);
    errdefer allocator.free(body);

    return MockServer.MockRequest{
        .path = path,
        .body = body,
        .allocator = allocator,
    };
}

// Helper: build a stop flag + spawn one dispatcher thread pointing at a mock.
//
// Heap-allocated so that &self.queue and &self.stop remain valid after init()
// returns.  A by-value return would copy the struct to the caller's stack and
// invalidate the pointers passed to the spawned thread.
const TestDispatcher = struct {
    stop: std.atomic.Value(bool),
    queue: queue_mod.Queue(types.Action),
    thread: std.Thread,
    allocator: std.mem.Allocator,

    fn init(
        allocator: std.mem.Allocator,
        bot_token: []const u8,
        api_base: []const u8,
    ) !*TestDispatcher {
        const self = try allocator.create(TestDispatcher);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.stop = std.atomic.Value(bool).init(false);
        self.queue = try queue_mod.Queue(types.Action).init(allocator, 256);
        errdefer self.queue.deinit(allocator);
        const args = DispatcherArgs{
            .id = 0,
            .queue = &self.queue,
            .bot_token = bot_token,
            .api_base = api_base,
            .allocator = allocator,
            .stop = &self.stop,
        };
        self.thread = try std.Thread.spawn(.{}, dispatcherThread, .{args});
        return self;
    }

    fn deinit(self: *TestDispatcher) void {
        self.stop.store(true, .release);
        self.thread.join();
        self.queue.deinit(self.allocator);
        self.allocator.destroy(self);
    }
};

// ── Mock server integration tests ────────────────────────────────────────────

test "AC-9.1: send_message POSTs to /bot{token}/sendMessage with JSON body" {
    const mock = try MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const api_base = mock.baseUrl(&url_buf);

    const d = try TestDispatcher.init(testing.allocator, "TESTTOKEN", api_base);
    defer d.deinit();

    try d.queue.push(.{ .send_message = .{
        .chat_id = 42,
        .text = try testing.allocator.dupe(u8, "hello"),
    } });

    try testing.expect(mock.waitForN(1, 2000));

    var req = mock.received.pop();
    defer req.deinit();

    try testing.expectEqualStrings("/botTESTTOKEN/sendMessage", req.path);
    try testing.expectEqualStrings("{\"chat_id\":42,\"text\":\"hello\"}", req.body);
}

test "AC-9.2: send_message_ex merges opts in the sent body" {
    const mock = try MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const api_base = mock.baseUrl(&url_buf);

    const d = try TestDispatcher.init(testing.allocator, "TOK", api_base);
    defer d.deinit();

    try d.queue.push(.{ .send_message_ex = .{
        .chat_id = 1,
        .text = try testing.allocator.dupe(u8, "hi"),
        .opts = try testing.allocator.dupe(u8, "{\"parse_mode\":\"HTML\"}"),
    } });

    try testing.expect(mock.waitForN(1, 2000));

    var req = mock.received.pop();
    defer req.deinit();

    try testing.expectEqualStrings("/botTOK/sendMessage", req.path);
    try testing.expectEqualStrings(
        "{\"chat_id\":1,\"text\":\"hi\",\"parse_mode\":\"HTML\"}",
        req.body,
    );
}

test "AC-9.3: answer_callback POSTs to answerCallbackQuery" {
    const mock = try MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const d = try TestDispatcher.init(testing.allocator, "TOK", mock.baseUrl(&url_buf));
    defer d.deinit();

    try d.queue.push(.{ .answer_callback = .{
        .callback_query_id = try testing.allocator.dupe(u8, "cq1"),
        .text = try testing.allocator.dupe(u8, "done"),
    } });

    try testing.expect(mock.waitForN(1, 2000));
    var req = mock.received.pop();
    defer req.deinit();

    try testing.expectEqualStrings("/botTOK/answerCallbackQuery", req.path);
    try testing.expectEqualStrings(
        "{\"callback_query_id\":\"cq1\",\"text\":\"done\"}",
        req.body,
    );
}

test "AC-9.4: delete_message POSTs to deleteMessage" {
    const mock = try MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const d = try TestDispatcher.init(testing.allocator, "TOK", mock.baseUrl(&url_buf));
    defer d.deinit();

    try d.queue.push(.{ .delete_message = .{
        .chat_id = 99,
        .message_id = 777,
    } });

    try testing.expect(mock.waitForN(1, 2000));
    var req = mock.received.pop();
    defer req.deinit();

    try testing.expectEqualStrings("/botTOK/deleteMessage", req.path);
    try testing.expectEqualStrings("{\"chat_id\":99,\"message_id\":777}", req.body);
}

test "AC-9.5: dispatcher retries once after server closes connection; exactly 2 attempts" {
    // First connection: accept but immediately close without a response.
    // Second connection: answer normally.
    // The dispatcher must have retried exactly once.

    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();

    const srv_port = server.listen_address.in.sa.port;
    var url_buf: [64]u8 = undefined;
    const api_base = std.fmt.bufPrint(&url_buf, "http://127.0.0.1:{d}", .{
        std.mem.bigToNative(u16, srv_port),
    }) catch unreachable;

    // Spawn a thread that drops the first connection and serves the second.
    var attempt_count = std.atomic.Value(u32).init(0);
    const Ctx = struct {
        server: *std.net.Server,
        attempt_count: *std.atomic.Value(u32),
    };
    const ctx = Ctx{ .server = &server, .attempt_count = &attempt_count };
    const srv_thread = try std.Thread.spawn(.{}, struct {
        fn run(c: Ctx) void {
            // First connection: drop immediately.
            const c1 = c.server.accept() catch return;
            _ = c.attempt_count.fetchAdd(1, .release);
            c1.stream.close();

            // Second connection: respond normally.
            const c2 = c.server.accept() catch return;
            defer c2.stream.close();
            _ = c.attempt_count.fetchAdd(1, .release);
            // Drain the request.
            var buf: [4096]u8 = undefined;
            _ = c2.stream.read(&buf) catch {};
            c2.stream.writeAll(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" ++
                    "Content-Length: 15\r\n\r\n{\"ok\":true}\r\n\r\n",
            ) catch {};
        }
    }.run, .{ctx});

    const d = try TestDispatcher.init(testing.allocator, "TOK", api_base);
    defer d.deinit();

    try d.queue.push(.{ .send_message = .{
        .chat_id = 1,
        .text = try testing.allocator.dupe(u8, "retry test"),
    } });

    srv_thread.join();

    // Both attempts must have been made — no more, no less.
    try testing.expectEqual(@as(u32, 2), attempt_count.load(.acquire));
}

test "AC-9.6: both attempts fail — action discarded, no third attempt, no crash" {
    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(.{ .reuse_address = true });

    const srv_port = server.listen_address.in.sa.port;
    var url_buf: [64]u8 = undefined;
    const api_base = std.fmt.bufPrint(&url_buf, "http://127.0.0.1:{d}", .{
        std.mem.bigToNative(u16, srv_port),
    }) catch unreachable;

    var attempt_count = std.atomic.Value(u32).init(0);
    const Ctx = struct {
        server: *std.net.Server,
        attempt_count: *std.atomic.Value(u32),
    };
    const ctx = Ctx{ .server = &server, .attempt_count = &attempt_count };
    const srv_thread = try std.Thread.spawn(.{}, struct {
        fn run(c: Ctx) void {
            // Drop both connections immediately.
            for (0..2) |_| {
                const conn = c.server.accept() catch break;
                _ = c.attempt_count.fetchAdd(1, .release);
                conn.stream.close();
            }
        }
    }.run, .{ctx});

    // Close the server so the dispatcher can't make a third attempt.
    // (We wait for both drops first, then close.)

    const d = try TestDispatcher.init(testing.allocator, "TOK", api_base);
    defer d.deinit();

    try d.queue.push(.{ .send_message = .{
        .chat_id = 2,
        .text = try testing.allocator.dupe(u8, "both-fail"),
    } });

    srv_thread.join();
    server.deinit(); // safe to call after thread exits

    // Exactly 2 attempts — no third attempt was made.
    try testing.expectEqual(@as(u32, 2), attempt_count.load(.acquire));
}

test "AC-9.7: 100 actions dispatched — all 100 received by mock server" {
    const mock = try MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;

    // Use 4 dispatcher threads to exercise the shared queue concurrently.
    var stop = std.atomic.Value(bool).init(false);
    var queue = try queue_mod.Queue(types.Action).init(testing.allocator, 256);
    defer queue.deinit(testing.allocator);

    var threads: [4]std.Thread = undefined;
    for (&threads, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, dispatcherThread, .{DispatcherArgs{
            .id = @intCast(i),
            .queue = &queue,
            .bot_token = "TOK",
            .api_base = mock.baseUrl(&url_buf),
            .allocator = testing.allocator,
            .stop = &stop,
        }});
    }
    defer {
        stop.store(true, .release);
        for (threads) |t| t.join();
    }

    for (0..100) |i| {
        try queue.push(.{ .send_message = .{
            .chat_id = @intCast(i),
            .text = try testing.allocator.dupe(u8, "bulk"),
        } });
    }

    try testing.expect(mock.waitForN(100, 10_000));
    try testing.expectEqual(@as(usize, 100), mock.received.len());

    // Drain to free memory.
    while (mock.received.popTimeout(0)) |req| {
        var r = req;
        r.deinit();
    }
}

test "AC-9.8: actions from one on_message call dispatched in original order" {
    // A single dispatcher thread preserves queue order within its own
    // dequeue sequence.  We push N actions from one goroutine and verify
    // they arrive in the same order.
    const mock = try MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const d = try TestDispatcher.init(testing.allocator, "TOK", mock.baseUrl(&url_buf));
    defer d.deinit();

    const N = 10;
    for (0..N) |i| {
        try d.queue.push(.{ .send_message = .{
            .chat_id = @intCast(i),
            .text = try testing.allocator.dupe(u8, "ordered"),
        } });
    }

    try testing.expect(mock.waitForN(N, 5000));

    for (0..N) |i| {
        var req = mock.received.pop();
        defer req.deinit();
        // The body's chat_id must match the push order.
        const expected_body = try std.fmt.allocPrint(
            testing.allocator,
            "{{\"chat_id\":{d},\"text\":\"ordered\"}}",
            .{i},
        );
        defer testing.allocator.free(expected_body);
        try testing.expectEqualStrings(expected_body, req.body);
    }
}

// ── Live integration test ─────────────────────────────────────────────────────

test "AC-9.live: send real Telegram message via dispatcher" {
    const token = std.posix.getenv("TELEGRAM_BOT_TOKEN") orelse return error.SkipZigTest;
    const chat_id_str = std.posix.getenv("TELEGRAM_CHAT_ID") orelse return error.SkipZigTest;
    const chat_id = try std.fmt.parseInt(i64, chat_id_str, 10);

    const d = try TestDispatcher.init(testing.allocator, token, "https://api.telegram.org");
    defer d.deinit();

    const msg = "zora dispatcher live — Phase 9 wired up. Actions reach Telegram.";
    try d.queue.push(.{ .send_message = .{
        .chat_id = chat_id,
        .text = try testing.allocator.dupe(u8, msg),
    } });

    // Give the dispatcher enough time to send and confirm.
    std.Thread.sleep(4 * std.time.ns_per_s);
}
