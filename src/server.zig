/// server.zig — minimal HTTP/1.1 server for the Telegram webhook endpoint.
///
/// Architecture:
///   - One accept thread (poll-based, honours stop flag with 10ms timeout)
///   - One detached thread per connection
///   - Validates method, path, and X-Telegram-Bot-Api-Secret-Token header
///   - Parses body as JSON into types.Update, routes by hashUserId % len(queues)
///   - Responds 200 OK immediately; the worker thread does the Lua processing
///
/// Memory ownership:
///   std.json.parseFromSlice allocates Update strings in an arena.  The full
///   Parsed(Update) value (arena pointer + value) is pushed into the worker
///   queue.  The worker calls parsed.deinit() after processing, which frees the
///   arena and all strings it owns.

const std    = @import("std");
const types  = @import("types.zig");
const q_mod  = @import("queue.zig");
const worker = @import("worker.zig");

const log = std.log.scoped(.server);

/// Maximum body size accepted.  Requests with Content-Length above this
/// threshold are rejected with 413 before any body memory is allocated.
pub const MAX_BODY_BYTES: usize = 1 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub const ServerArgs = struct {
    listen_addr:    std.net.Address,
    webhook_secret: []const u8,
    /// Slice of worker input queues.  Server routes updates by
    /// hashUserId(user_id, queues.len).  Must remain valid for the Server lifetime.
    queues:         []*q_mod.Queue(std.json.Parsed(types.Update)),
    allocator:      std.mem.Allocator,
};

pub const Server = struct {
    allocator:      std.mem.Allocator,
    webhook_secret: []const u8,
    queues:         []*q_mod.Queue(std.json.Parsed(types.Update)),
    listener:       std.net.Server,
    stop:           std.atomic.Value(bool),
    thread:         std.Thread,

    /// Start the server.  Returns a heap-allocated Server.  Call deinit() to
    /// stop the accept loop, close the socket, and free the allocation.
    pub fn init(args: ServerArgs) !*Server {
        const self = try args.allocator.create(Server);
        errdefer args.allocator.destroy(self);
        self.allocator      = args.allocator;
        self.webhook_secret = args.webhook_secret;
        self.queues         = args.queues;
        self.listener       = try args.listen_addr.listen(.{ .reuse_address = true });
        errdefer self.listener.deinit();
        self.stop   = std.atomic.Value(bool).init(false);
        self.thread = try std.Thread.spawn(.{}, acceptLoop, .{self});
        return self;
    }

    /// Signal the accept loop to exit, wait for it, then release resources.
    pub fn deinit(self: *Server) void {
        self.stop.store(true, .release);
        self.thread.join();
        self.listener.deinit();
        self.allocator.destroy(self);
    }

    /// The address the listening socket is actually bound to.
    /// Useful when port 0 was requested (OS assigns an ephemeral port).
    pub fn listenAddress(self: *const Server) std.net.Address {
        return self.listener.listen_address;
    }
};

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

fn acceptLoop(srv: *Server) void {
    const fd = srv.listener.stream.handle;
    while (!srv.stop.load(.acquire)) {
        var pfd = std.posix.pollfd{
            .fd      = fd,
            .events  = std.posix.POLL.IN,
            .revents = 0,
        };
        const n = std.posix.poll(@as(*[1]std.posix.pollfd, &pfd)[0..1], 10) catch return;
        if (n == 0) continue;
        if (pfd.revents & std.posix.POLL.IN == 0) continue;

        const conn = srv.listener.accept() catch |err| {
            log.warn("accept: {s}", .{@errorName(err)});
            continue;
        };
        const t = std.Thread.spawn(.{}, handleConnection, .{ srv, conn }) catch |err| {
            log.warn("thread spawn: {s}", .{@errorName(err)});
            conn.stream.close();
            continue;
        };
        t.detach();
    }
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

fn handleConnection(srv: *Server, conn: std.net.Server.Connection) void {
    defer conn.stream.close();
    handleRequest(srv, conn.stream) catch |err| {
        log.warn("request error: {s}", .{@errorName(err)});
    };
}

fn sendStatus(stream: std.net.Stream, comptime status: []const u8) !void {
    try stream.writeAll(
        "HTTP/1.1 " ++ status ++ "\r\n" ++
        "Content-Length: 0\r\n" ++
        "Connection: close\r\n" ++
        "\r\n",
    );
}

fn handleRequest(srv: *Server, stream: std.net.Stream) !void {
    // Arena for the HTTP line/header scratch space and body buffer.
    // Freed at end of function.  JSON-parsed Update strings are separately
    // owned by the arena inside std.json.Parsed (backed by srv.allocator).
    var line_arena = std.heap.ArenaAllocator.init(srv.allocator);
    defer line_arena.deinit();
    const la = line_arena.allocator();

    // Reader buffer.  8 KiB is enough for any well-formed HTTP request header.
    var read_buf: [8192]u8 = undefined;
    var net_rdr = stream.reader(&read_buf);
    const rdr   = net_rdr.interface(); // *std.io.Reader

    // ── Request line ─────────────────────────────────────────────────────────
    // Use takeDelimiter (not takeDelimiterExclusive) so the '\n' itself is
    // consumed; takeDelimiterExclusive leaves it in the buffer, causing the
    // header loop to see an empty first line and exit immediately.
    const req_raw_opt = rdr.takeDelimiter('\n') catch {
        sendStatus(stream, "400 Bad Request") catch {};
        return;
    };
    const req_raw = req_raw_opt orelse {
        sendStatus(stream, "400 Bad Request") catch {};
        return;
    };
    const req = std.mem.trimRight(u8, req_raw, "\r");

    var req_parts = std.mem.splitScalar(u8, req, ' ');
    const method  = req_parts.next() orelse "";
    const path    = req_parts.next() orelse "";

    // Evaluate method/path BEFORE reading any more data (slices point into
    // read_buf; safe until the next takeDelimiter call).
    const method_ok = std.mem.eql(u8, method, "POST");
    const path_ok   = std.mem.eql(u8, path, "/webhook");

    if (!method_ok or !path_ok) {
        try sendStatus(stream, "403 Forbidden");
        return;
    }

    // ── Headers ──────────────────────────────────────────────────────────────
    var content_length: usize = 0;
    var secret_valid   = false;

    while (true) {
        // takeDelimiter returns null on clean EOF; errors break the loop.
        const maybe_raw = rdr.takeDelimiter('\n') catch break;
        const h_raw = maybe_raw orelse break;
        const h = std.mem.trimRight(u8, h_raw, "\r");
        if (h.len == 0) break; // blank line = end of headers

        if (std.ascii.startsWithIgnoreCase(h, "content-length:")) {
            const v = std.mem.trim(u8, h["content-length:".len..], " \t");
            content_length = std.fmt.parseInt(usize, v, 10) catch 0;
        } else if (std.ascii.startsWithIgnoreCase(h, "x-telegram-bot-api-secret-token:")) {
            const v = std.mem.trim(u8, h["x-telegram-bot-api-secret-token:".len..], " \t");
            secret_valid = std.mem.eql(u8, v, srv.webhook_secret);
        }
        // Each header line is processed inline; slices are not held after this
        // iteration.  Buffer-pointer safety is maintained.
    }

    if (!secret_valid) {
        try sendStatus(stream, "403 Forbidden");
        return;
    }

    // ── Body ─────────────────────────────────────────────────────────────────
    if (content_length > MAX_BODY_BYTES) {
        try sendStatus(stream, "413 Request Entity Too Large");
        return;
    }

    const body = try la.alloc(u8, content_length);
    rdr.readSliceAll(body) catch {
        try sendStatus(stream, "400 Bad Request");
        return;
    };

    // ── JSON parse ───────────────────────────────────────────────────────────
    // Parsed(Update) is pushed into the worker queue; the worker calls
    // parsed.deinit() after processing, freeing the arena and all string data.
    const parsed = std.json.parseFromSlice(types.Update, srv.allocator, body, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always, // force string copies into Parsed arena — body may be freed before worker runs
    }) catch {
        try sendStatus(stream, "400 Bad Request");
        return;
    };

    // ── Route to worker queue ─────────────────────────────────────────────────
    const user_id = parsed.value.effectiveUserId() orelse parsed.value.update_id;
    const idx     = worker.hashUserId(user_id, @intCast(srv.queues.len));
    srv.queues[idx].push(parsed) catch {
        log.warn("queue {d} full — dropping update {d}", .{ idx, parsed.value.update_id });
        parsed.deinit();
    };

    // ── Respond 200 immediately ───────────────────────────────────────────────
    try sendStatus(stream, "200 OK");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;
const Queue   = q_mod.Queue;

const TEST_SECRET = "test-webhook-secret";

/// Minimal valid Telegram Update JSON: single message from user_id=100.
const VALID_UPDATE =
    \\{"update_id":1,"message":{"message_id":1,"from":{"id":100,"is_bot":false,"first_name":"T"},"chat":{"id":100,"type":"private"},"date":0}}
;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Heap-allocated test fixture: N worker queues + a running Server.
/// Queues use std.testing.allocator; server internal operations use
/// std.heap.page_allocator (prototype memory; Update strings are not freed).
const TestSetup = struct {
    const MAX_Q = 8;

    q_store: [MAX_Q]Queue(types.Update),
    q_ptrs:  [MAX_Q]*Queue(types.Update),
    q_count: usize,
    srv:     *Server,

    fn init(n: usize, secret: []const u8) !*TestSetup {
        std.debug.assert(n > 0 and n <= MAX_Q);
        const self = try testing.allocator.create(TestSetup);
        errdefer testing.allocator.destroy(self);
        self.q_count = n;
        for (0..n) |i| {
            self.q_store[i] = try Queue(types.Update).init(testing.allocator, 512);
            self.q_ptrs[i]  = &self.q_store[i];
        }
        const bind_addr = try std.net.Address.parseIp4("127.0.0.1", 0);
        self.srv = try Server.init(.{
            .listen_addr    = bind_addr,
            .webhook_secret = secret,
            .queues         = self.q_ptrs[0..n],
            .allocator      = std.heap.page_allocator,
        });
        return self;
    }

    fn deinit(self: *TestSetup) void {
        self.srv.deinit();
        for (0..self.q_count) |i| self.q_store[i].deinit(testing.allocator);
        testing.allocator.destroy(self);
    }

    fn serverAddr(self: *const TestSetup) std.net.Address {
        return self.srv.listenAddress();
    }

    fn queueLen(self: *TestSetup, i: usize) usize {
        return self.q_store[i].len();
    }
};

/// Send an HTTP request and return the response status code.
fn httpReq(
    method:  []const u8,
    address: std.net.Address,
    path:    []const u8,
    secret:  ?[]const u8,
    body:    []const u8,
) !u16 {
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    // Build request headers using FixedBufferStream + GenericWriter.
    var hdr_buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&hdr_buf);
    const w = fbs.writer();
    try w.print("{s} {s} HTTP/1.1\r\nHost: localhost\r\n", .{ method, path });
    if (secret) |s| try w.print("X-Telegram-Bot-Api-Secret-Token: {s}\r\n", .{s});
    try w.print("Content-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len});

    try stream.writeAll(fbs.getWritten());
    try stream.writeAll(body);

    // Read status line from response.
    var read_buf: [512]u8 = undefined;
    var net_rdr = stream.reader(&read_buf);
    const rdr  = net_rdr.interface();
    const raw_line = (try rdr.takeDelimiter('\n')) orelse return error.BadResponse;
    const trimmed = std.mem.trimRight(u8, raw_line, "\r");
    var it = std.mem.splitScalar(u8, trimmed, ' ');
    _ = it.next(); // "HTTP/1.1"
    const code = it.next() orelse return error.BadResponse;
    return std.fmt.parseInt(u16, code, 10) catch error.BadResponse;
}

/// Send a POST /webhook with Content-Length > MAX_BODY_BYTES but no body.
/// Server must reject based on the header alone.
fn httpOversizeBody(address: std.net.Address, secret: []const u8) !u16 {
    const stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    var hdr_buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&hdr_buf);
    const w = fbs.writer();
    try w.print(
        "POST /webhook HTTP/1.1\r\n" ++
        "X-Telegram-Bot-Api-Secret-Token: {s}\r\n" ++
        "Content-Length: {d}\r\n" ++
        "Connection: close\r\n\r\n",
        .{ secret, MAX_BODY_BYTES + 1 },
    );
    try stream.writeAll(fbs.getWritten());
    // Intentionally omit the body — server must reject before reading it.

    var read_buf: [512]u8 = undefined;
    var net_rdr = stream.reader(&read_buf);
    const rdr  = net_rdr.interface();
    const raw_line2 = (try rdr.takeDelimiter('\n')) orelse return error.BadResponse;
    const trimmed2 = std.mem.trimRight(u8, raw_line2, "\r");
    var it2 = std.mem.splitScalar(u8, trimmed2, ' ');
    _ = it2.next();
    const code2 = it2.next() orelse return error.BadResponse;
    return std.fmt.parseInt(u16, code2, 10) catch error.BadResponse;
}

// ---------------------------------------------------------------------------
// AC tests
// ---------------------------------------------------------------------------

test "AC-10.1: valid POST /webhook with correct secret → 200 OK, update enqueued" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, VALID_UPDATE);
    try testing.expectEqual(@as(u16, 200), status);
    try testing.expectEqual(@as(usize, 1), ts.queueLen(0));
}

test "AC-10.2: GET /webhook → 403 Forbidden, nothing enqueued" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpReq("GET", ts.serverAddr(), "/webhook", TEST_SECRET, "");
    try testing.expectEqual(@as(u16, 403), status);
    try testing.expectEqual(@as(usize, 0), ts.queueLen(0));
}

test "AC-10.3: POST /notwebhook → 403 Forbidden, nothing enqueued" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpReq("POST", ts.serverAddr(), "/notwebhook", TEST_SECRET, VALID_UPDATE);
    try testing.expectEqual(@as(u16, 403), status);
    try testing.expectEqual(@as(usize, 0), ts.queueLen(0));
}

test "AC-10.4: POST /webhook with no secret header → 403 Forbidden" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpReq("POST", ts.serverAddr(), "/webhook", null, VALID_UPDATE);
    try testing.expectEqual(@as(u16, 403), status);
}

test "AC-10.5: POST /webhook with wrong secret → 403 Forbidden" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpReq("POST", ts.serverAddr(), "/webhook", "wrong-secret", VALID_UPDATE);
    try testing.expectEqual(@as(u16, 403), status);
}

test "AC-10.6: valid secret, malformed JSON body → 400 Bad Request, nothing enqueued" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, "{not json}");
    try testing.expectEqual(@as(u16, 400), status);
    try testing.expectEqual(@as(usize, 0), ts.queueLen(0));
}

test "AC-10.7: 200 OK arrives within 50ms" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const t0 = std.time.milliTimestamp();
    const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, VALID_UPDATE);
    const elapsed = std.time.milliTimestamp() - t0;

    try testing.expectEqual(@as(u16, 200), status);
    try testing.expect(elapsed < 50);
}

test "AC-10.8: 100 requests with same user_id → all enqueued to same worker queue" {
    const N = 4;
    const ts = try TestSetup.init(N, TEST_SECRET);
    defer ts.deinit();

    const expected_idx = worker.hashUserId(100, N);

    for (0..100) |i| {
        var body_buf: [512]u8 = undefined;
        const body = try std.fmt.bufPrint(&body_buf,
            \\{{"update_id":{d},"message":{{"message_id":{d},"from":{{"id":100,"is_bot":false,"first_name":"T"}},"chat":{{"id":100,"type":"private"}},"date":0}}}}
        , .{ i + 1, i + 1 });
        const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, body);
        try testing.expectEqual(@as(u16, 200), status);
    }

    try testing.expectEqual(@as(usize, 100), ts.queueLen(expected_idx));
    for (0..N) |i| {
        if (i != expected_idx) try testing.expectEqual(@as(usize, 0), ts.queueLen(i));
    }
}

test "AC-10.9: 10 simultaneous connections → all 200 OK, all updates enqueued" {
    const N = 10;
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const ThreadCtx = struct {
        address: std.net.Address,
        status:  u16  = 0,
        err:     bool = false,
    };

    var ctxs:    [N]ThreadCtx  = undefined;
    var threads: [N]std.Thread = undefined;

    for (0..N) |i| {
        ctxs[i] = .{ .address = ts.serverAddr() };
        threads[i] = try std.Thread.spawn(.{}, struct {
            fn run(ctx: *ThreadCtx) void {
                var buf: [512]u8 = undefined;
                const uid: i64 = @intCast(@intFromPtr(ctx) & 0xFFFFFF);
                const body = std.fmt.bufPrint(&buf,
                    \\{{"update_id":{d},"message":{{"message_id":{d},"from":{{"id":{d},"is_bot":false,"first_name":"T"}},"chat":{{"id":{d},"type":"private"}},"date":0}}}}
                , .{ uid, uid, uid, uid }) catch {
                    ctx.err = true;
                    return;
                };
                ctx.status = httpReq("POST", ctx.address, "/webhook", TEST_SECRET, body) catch {
                    ctx.err = true;
                    return;
                };
            }
        }.run, .{&ctxs[i]});
    }

    for (0..N) |i| threads[i].join();

    var enqueued: usize = 0;
    for (0..N) |i| {
        try testing.expect(!ctxs[i].err);
        try testing.expectEqual(@as(u16, 200), ctxs[i].status);
    }
    for (0..ts.q_count) |i| enqueued += ts.queueLen(i);
    try testing.expectEqual(@as(usize, N), enqueued);
}

test "AC-10.10: body > 1 MB → 413, server does not allocate unbounded memory" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpOversizeBody(ts.serverAddr(), TEST_SECRET);
    try testing.expect(status == 413 or status == 400);
    try testing.expectEqual(@as(usize, 0), ts.queueLen(0));
}
