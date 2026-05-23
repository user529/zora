/// server.zig — minimal HTTP/1.1 server for the Telegram webhook endpoint.
///
/// Architecture:
///   - One accept thread (poll-based, honours stop flag with 10ms timeout)
///   - One detached thread per connection
///   - Validates method, path, and X-Telegram-Bot-Api-Secret-Token header
///   - Point-extracts user_id from the raw JSON body (no full parse —
///     ADR-0001 §AD-2), routes by hashUserId % len(queues)
///   - Responds 200 OK immediately; the worker thread does the Lua processing
///
/// Memory ownership:
///   The raw webhook body is duplicated into a heap-allocated WorkItem and
///   pushed to a worker queue.  The worker frees WorkItem.body after
///   callOnMessage returns.  No arena is leaked — this replaces the pre-ADR
///   Parsed(Update) model where each enqueued Update leaked its arena.

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
    queues:         []*q_mod.Queue(types.WorkItem),
    allocator:      std.mem.Allocator,
};

pub const Server = struct {
    allocator:      std.mem.Allocator,
    webhook_secret: []const u8,
    queues:         []*q_mod.Queue(types.WorkItem),
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
            secret_valid = std.mem.eql(u8, srv.webhook_secret, v);
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
        log.warn("rejected oversized body: Content-Length={d} > {d}", .{ content_length, MAX_BODY_BYTES });
        try sendStatus(stream, "413 Request Entity Too Large");
        return;
    }

    const body = try la.alloc(u8, content_length);
    rdr.readSliceAll(body) catch {
        log.warn("rejected request: failed to read body", .{});
        try sendStatus(stream, "400 Bad Request");
        return;
    };

    // ── Point-extract the routing user_id (no full parse — ADR-0001 §AD-2) ───
    // A syntactically malformed body is rejected here; a well-formed body with
    // no identifiable sender routes to worker 0 (user_id 0).
    const user_id = extractUserId(la, body) catch {
        log.warn("rejected request: malformed JSON body", .{});
        try sendStatus(stream, "400 Bad Request");
        return;
    };

    // ── Forward the raw body verbatim to a worker ────────────────────────────
    // The WorkItem owns its body; the worker frees it after callOnMessage.
    const owned_body = srv.allocator.dupe(u8, body) catch {
        log.warn("rejected request: out of memory copying body", .{});
        try sendStatus(stream, "500 Internal Server Error");
        return;
    };
    const item = types.WorkItem{ .body = owned_body, .user_id = user_id };
    const idx  = worker.hashUserId(user_id orelse 0, @intCast(srv.queues.len));
    srv.queues[idx].push(item) catch {
        log.warn("queue {d} full — update dropped", .{idx});
        srv.allocator.free(owned_body);
    };

    // ── Respond 200 immediately ───────────────────────────────────────────────
    try sendStatus(stream, "200 OK");
}

// ---------------------------------------------------------------------------
// user_id extraction (ADR-0001 §AD-2)
//
// The worker-routing key is point-extracted from the raw webhook JSON with a
// streaming scanner — never a full parse.  We navigate to `message.from.id`
// (preferred) or `callback_query.from.id` and stop as soon as an id is found;
// trailing bytes are not scanned.  Telegram lists `message` before
// `callback_query` in the Update object, so first-match equals the old
// `effectiveUserId` precedence.
//
// Returns error.InvalidJson when the body is not syntactically valid JSON up
// to the point an id is found (a fully malformed body is always rejected).
// A well-formed body with no `from.id` yields null.
// ---------------------------------------------------------------------------

const ExtractError = error{InvalidJson};

fn extractUserId(allocator: std.mem.Allocator, body: []const u8) ExtractError!?i64 {
    var scanner = std.json.Scanner.initCompleteInput(allocator, body);
    defer scanner.deinit();

    if ((scanner.next() catch return error.InvalidJson) != .object_begin)
        return error.InvalidJson;

    while (true) {
        const tok = scanner.nextAlloc(allocator, .alloc_if_needed) catch return error.InvalidJson;
        const key = switch (tok) {
            .object_end => return null,
            .string => |s| s,
            .allocated_string => |s| s,
            else => return error.InvalidJson, // an object key must be a string
        };
        const wanted = std.mem.eql(u8, key, "message") or
            std.mem.eql(u8, key, "callback_query");
        if (tok == .allocated_string) allocator.free(tok.allocated_string);

        if (wanted) {
            if (try scanFromId(allocator, &scanner)) |id| return id;
        } else {
            scanner.skipValue() catch return error.InvalidJson;
        }
    }
}

/// Scanner positioned just after a `message`/`callback_query` key.  When the
/// value is an object holding `from.id`, returns that id (the scanner may be
/// left mid-document — the caller stops).  Otherwise consumes the whole value
/// cleanly and returns null.
fn scanFromId(allocator: std.mem.Allocator, scanner: *std.json.Scanner) ExtractError!?i64 {
    if ((scanner.peekNextTokenType() catch return error.InvalidJson) != .object_begin) {
        scanner.skipValue() catch return error.InvalidJson;
        return null;
    }
    _ = scanner.next() catch return error.InvalidJson; // consume object_begin

    while (true) {
        const tok = scanner.nextAlloc(allocator, .alloc_if_needed) catch return error.InvalidJson;
        const key = switch (tok) {
            .object_end => return null,
            .string => |s| s,
            .allocated_string => |s| s,
            else => return error.InvalidJson,
        };
        const is_from = std.mem.eql(u8, key, "from");
        if (tok == .allocated_string) allocator.free(tok.allocated_string);

        if (is_from) {
            if (try scanIdField(allocator, scanner)) |id| return id;
            // `from` carried no usable id — keep consuming the enclosing object.
        } else {
            scanner.skipValue() catch return error.InvalidJson;
        }
    }
}

/// Scanner positioned just after a `from` key.  Returns an integer `id` value
/// if present; otherwise consumes the value cleanly and returns null.
fn scanIdField(allocator: std.mem.Allocator, scanner: *std.json.Scanner) ExtractError!?i64 {
    if ((scanner.peekNextTokenType() catch return error.InvalidJson) != .object_begin) {
        scanner.skipValue() catch return error.InvalidJson;
        return null;
    }
    _ = scanner.next() catch return error.InvalidJson; // consume object_begin

    while (true) {
        const tok = scanner.nextAlloc(allocator, .alloc_if_needed) catch return error.InvalidJson;
        const key = switch (tok) {
            .object_end => return null,
            .string => |s| s,
            .allocated_string => |s| s,
            else => return error.InvalidJson,
        };
        const is_id = std.mem.eql(u8, key, "id");
        if (tok == .allocated_string) allocator.free(tok.allocated_string);

        if (is_id and (scanner.peekNextTokenType() catch return error.InvalidJson) == .number) {
            const num = scanner.nextAlloc(allocator, .alloc_if_needed) catch return error.InvalidJson;
            const slice = switch (num) {
                .number => |s| s,
                .allocated_number => |s| s,
                else => return error.InvalidJson, // peek guaranteed a number
            };
            const parsed = std.fmt.parseInt(i64, slice, 10) catch null;
            if (num == .allocated_number) allocator.free(num.allocated_number);
            // A good integer id wins immediately (early exit).  A non-integer
            // id (float / overflow) falls through: keep consuming the object.
            if (parsed) |id| return id;
        } else {
            scanner.skipValue() catch return error.InvalidJson;
        }
    }
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
/// Everything — queue buffers and server internals — uses std.testing.allocator.
/// WorkItem ownership is clean (no leaked arenas), so the leak detector runs
/// over the whole server path; deinit drains any unconsumed WorkItem bodies.
const TestSetup = struct {
    const MAX_Q = 8;

    q_store: [MAX_Q]Queue(types.WorkItem),
    q_ptrs:  [MAX_Q]*Queue(types.WorkItem),
    q_count: usize,
    srv:     *Server,

    fn init(n: usize, secret: []const u8) !*TestSetup {
        std.debug.assert(n > 0 and n <= MAX_Q);
        const self = try testing.allocator.create(TestSetup);
        errdefer testing.allocator.destroy(self);
        self.q_count = n;
        for (0..n) |i| {
            self.q_store[i] = try Queue(types.WorkItem).init(testing.allocator, 512);
            self.q_ptrs[i]  = &self.q_store[i];
        }
        const bind_addr = try std.net.Address.parseIp4("127.0.0.1", 0);
        self.srv = try Server.init(.{
            .listen_addr    = bind_addr,
            .webhook_secret = secret,
            .queues         = self.q_ptrs[0..n],
            .allocator      = testing.allocator,
        });
        return self;
    }

    fn deinit(self: *TestSetup) void {
        self.srv.deinit();
        for (0..self.q_count) |i| {
            // Free WorkItem bodies enqueued but never consumed (no worker here).
            while (self.q_store[i].popTimeout(0)) |item| testing.allocator.free(item.body);
            self.q_store[i].deinit(testing.allocator);
        }
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

// ---------------------------------------------------------------------------
// Sub-step 13b — raw body forwarding + point-extracted user_id (ADR-0001 §AD-2)
//
// AC-13.8 (oversize / malformed-JSON rejection) is regression-protected by the
// existing AC-10.6 and AC-10.10 above — no separate test.
// ---------------------------------------------------------------------------

test "AC-13.5: server forwards the raw webhook body verbatim to the worker" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, VALID_UPDATE);
    try testing.expectEqual(@as(u16, 200), status);
    try testing.expectEqual(@as(usize, 1), ts.queueLen(0));

    const item = ts.q_store[0].pop();
    defer testing.allocator.free(item.body);
    // The body the worker sees is byte-identical to what the client posted.
    try testing.expectEqualStrings(VALID_UPDATE, item.body);
    try testing.expectEqual(@as(?i64, 100), item.user_id);
}

test "AC-13.6: extractUserId — message / callback_query / edge cases" {
    const A = testing.allocator;

    // message.from.id
    try testing.expectEqual(@as(?i64, 100), try extractUserId(A, VALID_UPDATE));
    // callback_query.from.id, no message
    try testing.expectEqual(@as(?i64, 55), try extractUserId(A,
        \\{"update_id":2,"callback_query":{"id":"c","from":{"id":55,"is_bot":false,"first_name":"X"}}}
    ));
    // both present → message wins (message precedes callback_query)
    try testing.expectEqual(@as(?i64, 7), try extractUserId(A,
        \\{"message":{"from":{"id":7}},"callback_query":{"from":{"id":9}}}
    ));
    // channel post: message present, no `from`
    try testing.expectEqual(@as(?i64, null), try extractUserId(A,
        \\{"update_id":3,"message":{"message_id":1,"chat":{"id":-100,"type":"channel"}}}
    ));
    // no message, no callback_query
    try testing.expectEqual(@as(?i64, null), try extractUserId(A, "{\"update_id\":4}"));
    // empty object
    try testing.expectEqual(@as(?i64, null), try extractUserId(A, "{}"));
    // message present, `from` without `id`
    try testing.expectEqual(@as(?i64, null), try extractUserId(A,
        \\{"message":{"from":{"first_name":"NoId"}}}
    ));
    // message is JSON null
    try testing.expectEqual(@as(?i64, null), try extractUserId(A, "{\"message\":null}"));
    // negative id
    try testing.expectEqual(@as(?i64, -42), try extractUserId(A,
        \\{"message":{"from":{"id":-42}}}
    ));
    // id is a string, not an integer → no routing id
    try testing.expectEqual(@as(?i64, null), try extractUserId(A,
        \\{"message":{"from":{"id":"123"}}}
    ));
    // unrelated keys + nested objects before message are skipped
    try testing.expectEqual(@as(?i64, 88), try extractUserId(A,
        \\{"update_id":9,"x":{"y":{"z":1}},"message":{"chat":{"id":5},"from":{"id":88}}}
    ));
    // fully malformed JSON → error
    try testing.expectError(error.InvalidJson, extractUserId(A, "{not json}"));
    // empty body → error
    try testing.expectError(error.InvalidJson, extractUserId(A, ""));
    // top-level array (not an object) → error
    try testing.expectError(error.InvalidJson, extractUserId(A, "[1,2,3]"));
}

test "AC-13.7: extractUserId early-exits once the routing id is found" {
    // The id is reachable; the bytes after it are invalid JSON.  A full scan
    // would raise error.InvalidJson — returning the id proves the scan stopped
    // as soon as `message.from.id` was captured.
    const body = "{\"message\":{\"from\":{\"id\":777,\"junk\":NOTVALID}}}";
    try testing.expectEqual(@as(?i64, 777), try extractUserId(testing.allocator, body));
}

test "AC-13.9: server→worker WorkItem handoff leaks nothing under testing.allocator" {
    // The server allocates one WorkItem.body per accepted update.  Draining the
    // queues exactly as a worker does (take item, free body) must leave the
    // testing.allocator balanced — proving the server↔worker ownership contract
    // and that the pre-ADR Parsed(Update) arena leak is gone.
    const ts = try TestSetup.init(2, TEST_SECRET);
    defer ts.deinit();

    for (0..20) |i| {
        var buf: [256]u8 = undefined;
        const body = try std.fmt.bufPrint(&buf,
            \\{{"update_id":{d},"message":{{"from":{{"id":{d}}}}}}}
        , .{ i + 1, i + 1 });
        try testing.expectEqual(@as(u16, 200),
            try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, body));
    }

    var drained: usize = 0;
    for (0..ts.q_count) |qi| {
        while (ts.q_store[qi].popTimeout(0)) |item| {
            testing.allocator.free(item.body);
            drained += 1;
        }
    }
    try testing.expectEqual(@as(usize, 20), drained);
}
