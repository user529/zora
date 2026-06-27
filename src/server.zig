/// server.zig — minimal HTTP/1.1 server for the Telegram webhook endpoint.
///
/// Architecture:
///   - One accept thread (poll-based, honours stop flag with 10ms timeout)
///   - A fixed pool of handler threads draining a connection hand-off queue
///   - Validates method, path, and X-Telegram-Bot-Api-Secret-Token header
///   - Point-extracts user_id from the raw JSON body (no full parse),
///     routes by hashUserId % len(queues)
///   - Responds 200 OK immediately; the worker thread does the Lua processing
///
/// Memory ownership:
///   The raw webhook body is duplicated into a heap-allocated WorkItem and
///   pushed to a worker queue.  The worker frees WorkItem.body after
///   callOnMessage returns.  No arena is leaked.
const std = @import("std");
const types = @import("types.zig");
const q_mod = @import("queue.zig");
const worker = @import("worker.zig");
const metrics_mod = @import("metrics.zig");
const rt = @import("rt.zig");

const log = std.log.scoped(.server);

/// Maximum body size accepted.  Requests with Content-Length above this
/// threshold are rejected with 413 before any body memory is allocated.
pub const MAX_BODY_BYTES: usize = 1 * 1024 * 1024;

/// Maximum number of concurrently open connection-handler threads.
/// The accept loop drops connections beyond this limit.
pub const MAX_CONNECTIONS: u32 = 1024;

/// Idle timeout (ms) to wait for a connected client's first byte before giving
/// up, so a peer that connects and sends nothing cannot pin a handler thread.
/// Implemented with poll() — see waitReadable for why SO_RCVTIMEO is unusable
/// under 0.16's blocking-Io read path. 0 disables the guard.
pub const DEFAULT_READ_IDLE_TIMEOUT_MS: u32 = 15_000;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub const ServerArgs = struct {
    listen_addr: std.Io.net.IpAddress,
    webhook_secret: []const u8,
    /// Slice of worker input queues.  Server routes updates by
    /// hashUserId(user_id, queues.len).  Must remain valid for the Server lifetime.
    queues: []*q_mod.Queue(types.WorkItem),
    allocator: std.mem.Allocator,
    /// Runtime for the listener, accepted streams, sleeps, and time reads.
    io: std.Io,
    /// Number of reusable connection-handler threads.
    pool_threads: u8,
    /// Per-recv idle read timeout (SO_RCVTIMEO) for accepted connections.
    /// 0 disables. Defaulted so callers need not set it.
    read_idle_timeout_ms: u32 = DEFAULT_READ_IDLE_TIMEOUT_MS,
    /// Optional metrics sink. Null means "don't count" (tests default to null).
    metrics: ?*metrics_mod.Metrics = null,
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    webhook_secret: []const u8,
    queues: []*q_mod.Queue(types.WorkItem),
    listener: std.Io.net.Server,
    stop: std.atomic.Value(bool),
    accept_thread: std.Thread,
    active: std.atomic.Value(u32),
    /// Fixed handler-thread pool (std.Thread.Pool was removed in 0.16). The
    /// accept thread pushes accepted streams to conn_queue; these threads pop
    /// and serve them, so the pool reuses threads rather than spawning one per connection.
    handler_threads: []std.Thread,
    conn_queue: q_mod.Queue(std.Io.net.Stream),
    read_idle_timeout_ms: u32,
    metrics: ?*metrics_mod.Metrics,

    /// Start the server.  Returns a heap-allocated Server.  Call deinit() to
    /// stop the accept loop, close the socket, and free the allocation.
    pub fn init(args: ServerArgs) !*Server {
        const self = try args.allocator.create(Server);
        errdefer args.allocator.destroy(self);
        self.allocator = args.allocator;
        self.io = args.io;
        self.webhook_secret = args.webhook_secret;
        self.queues = args.queues;
        self.read_idle_timeout_ms = args.read_idle_timeout_ms;
        self.metrics = args.metrics;
        // kernel_backlog 4096 matches somaxconn; default 128 caused SYN-queue
        // exhaustion warnings under burst load.
        self.listener = try args.listen_addr.listen(args.io, .{ .reuse_address = true, .kernel_backlog = 4096 });
        errdefer self.listener.deinit(args.io);
        self.stop = std.atomic.Value(bool).init(false);
        self.active = std.atomic.Value(u32).init(0);

        // Connection hand-off queue: accept thread pushes, handlers pop.
        self.conn_queue = try q_mod.Queue(std.Io.net.Stream).init(args.allocator, args.io, MAX_CONNECTIONS);
        errdefer self.conn_queue.deinit(args.allocator);

        // Handler pool must be running before the accept loop (which feeds it).
        const n_handlers = @max(@as(usize, args.pool_threads), 1);
        self.handler_threads = try args.allocator.alloc(std.Thread, n_handlers);
        errdefer args.allocator.free(self.handler_threads);
        var spawned: usize = 0;
        errdefer {
            self.stop.store(true, .release);
            for (self.handler_threads[0..spawned]) |t| t.join();
        }
        for (self.handler_threads) |*t| {
            t.* = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, handlerLoop, .{self});
            spawned += 1;
        }

        self.accept_thread = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, acceptLoop, .{self});
        return self;
    }

    /// Signal the accept loop to exit, wait for it, drain the handler pool,
    /// then release resources.  Order matters: stop accepting before draining
    /// (no new tasks), drain before freeing (handlers hold *Server).
    pub fn deinit(self: *Server) void {
        self.stop.store(true, .release);
        self.accept_thread.join();
        for (self.handler_threads) |t| t.join();
        self.allocator.free(self.handler_threads);
        // Close any streams accepted but not yet served (handlers drain first,
        // so this is normally empty; the close keeps fds from leaking on a race).
        while (self.conn_queue.popTimeout(0)) |stream| stream.close(self.io);
        self.conn_queue.deinit(self.allocator);
        self.listener.deinit(self.io);
        self.allocator.destroy(self);
    }

    /// The address the listening socket is actually bound to.
    /// Useful when port 0 was requested (OS assigns an ephemeral port).
    /// 0.16's net.Server drops listen_address, so query the socket directly.
    pub fn listenAddress(self: *const Server) std.Io.net.IpAddress {
        var sa: std.posix.sockaddr.in = undefined;
        var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
        _ = std.posix.system.getsockname(self.listener.socket.handle, @ptrCast(&sa), &len);
        return .{ .ip4 = .{
            .bytes = @bitCast(sa.addr),
            .port = std.mem.bigToNative(u16, sa.port),
        } };
    }
};

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

fn acceptLoop(srv: *Server) void {
    const fd = srv.listener.socket.handle;
    while (!srv.stop.load(.acquire)) {
        var pfd = std.posix.pollfd{
            .fd = fd,
            .events = std.posix.POLL.IN,
            .revents = 0,
        };
        const n = std.posix.poll(@as(*[1]std.posix.pollfd, &pfd)[0..1], 10) catch return;
        if (n == 0) continue;
        if (pfd.revents & std.posix.POLL.IN == 0) continue;

        const stream = srv.listener.accept(srv.io) catch |err| {
            log.warn("accept: {s}", .{@errorName(err)});
            continue;
        };
        if (srv.active.load(.acquire) >= MAX_CONNECTIONS) {
            log.warn("connection limit ({d}) reached — dropping", .{MAX_CONNECTIONS});
            stream.close(srv.io);
            continue;
        }
        // Drop connections when every worker queue is at capacity to prevent
        // thread-churn SIGSEGV.
        const all_saturated = blk: {
            for (srv.queues) |q| {
                if (q.len() < q.buf.len) break :blk false;
            }
            break :blk true;
        };
        if (all_saturated) {
            log.warn("all worker queues saturated — dropping connection", .{});
            sendStatus(srv.io, stream, "503 Service Unavailable") catch {};
            stream.close(srv.io);
            continue;
        }
        // active is incremented here and decremented in handleConnection's defer;
        // it bounds both concurrent handlers and the conn_queue backlog.
        _ = srv.active.fetchAdd(1, .acquire);
        srv.conn_queue.push(stream) catch |err| {
            _ = srv.active.fetchSub(1, .release);
            log.warn("connection enqueue: {s}", .{@errorName(err)});
            stream.close(srv.io);
            continue;
        };
    }
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

/// One fixed pool thread: pull accepted streams off conn_queue and serve them.
/// After stop, drain the queue so every accepted connection is answered and its
/// fd released before the thread exits.
fn handlerLoop(srv: *Server) void {
    while (!srv.stop.load(.acquire)) {
        const stream = srv.conn_queue.popTimeout(10 * std.time.ns_per_ms) orelse continue;
        handleConnection(srv, stream);
    }
    while (srv.conn_queue.popTimeout(0)) |stream| handleConnection(srv, stream);
}

fn handleConnection(srv: *Server, stream: std.Io.net.Stream) void {
    defer _ = srv.active.fetchSub(1, .release);
    defer stream.close(srv.io);
    handleRequest(srv, stream) catch |err| {
        log.warn("request error: {s}", .{@errorName(err)});
    };
}

/// Wait up to `timeout_ms` for the socket to become readable, so a client that
/// connects and sends nothing cannot hold a handler thread indefinitely.
/// Returns true if data is ready, false on timeout/error. timeout_ms == 0 means
/// "no idle limit" (wait forever — caller skips the gate).
///
/// 0.16 note: this replaces SO_RCVTIMEO. The Threaded Io does blocking recv on
/// its worker threads and treats an EAGAIN timeout as a programmer bug (panics),
/// so a socket read timeout cannot be set via setsockopt; poll() is the portable
/// idle guard that works with the blocking-Io read path.
fn waitReadable(fd: std.posix.socket_t, timeout_ms: u32) bool {
    var pfd = std.posix.pollfd{ .fd = fd, .events = std.posix.POLL.IN, .revents = 0 };
    const n = std.posix.poll(@as(*[1]std.posix.pollfd, &pfd)[0..1], @intCast(timeout_ms)) catch return false;
    return n > 0 and (pfd.revents & std.posix.POLL.IN) != 0;
}

fn sendStatus(io: std.Io, stream: std.Io.net.Stream, comptime status: []const u8) !void {
    var wbuf: [128]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    try sw.interface.writeAll(
        "HTTP/1.1 " ++ status ++ "\r\n" ++
            "Content-Length: 0\r\n" ++
            "Connection: close\r\n" ++
            "\r\n",
    );
    try sw.interface.flush();
}

fn handleRequest(srv: *Server, stream: std.Io.net.Stream) !void {
    // Arena for the HTTP line/header scratch space, body buffer, and the
    // user_id extraction scanner.  Freed at end of function.  The body copy
    // handed to the worker is allocated separately from srv.allocator.
    var line_arena = std.heap.ArenaAllocator.init(srv.allocator);
    defer line_arena.deinit();
    const la = line_arena.allocator();

    // ── Idle guard ─────────────────────────────────────────────────────────────
    // A client that opens a connection and sends nothing must not pin a handler
    // thread. Wait for the first byte before the (blocking) read below.
    if (srv.read_idle_timeout_ms > 0 and !waitReadable(stream.socket.handle, srv.read_idle_timeout_ms)) {
        try sendStatus(srv.io, stream, "400 Bad Request");
        return;
    }

    // Reader buffer.  8 KiB is enough for any well-formed HTTP request header.
    var read_buf: [8192]u8 = undefined;
    var net_rdr = stream.reader(srv.io, &read_buf);
    const rdr = &net_rdr.interface; // *std.Io.Reader

    // ── Request line ─────────────────────────────────────────────────────────
    // Use takeDelimiter (not takeDelimiterExclusive) so the '\n' itself is
    // consumed; takeDelimiterExclusive leaves it in the buffer, causing the
    // header loop to see an empty first line and exit immediately.
    const req_raw_opt = rdr.takeDelimiter('\n') catch {
        sendStatus(srv.io, stream, "400 Bad Request") catch {};
        return;
    };
    const req_raw = req_raw_opt orelse {
        sendStatus(srv.io, stream, "400 Bad Request") catch {};
        return;
    };
    const req = std.mem.trimEnd(u8, req_raw, "\r");

    var req_parts = std.mem.splitScalar(u8, req, ' ');
    const method = req_parts.next() orelse "";
    const path = req_parts.next() orelse "";

    // Evaluate method/path BEFORE reading any more data (slices point into
    // read_buf; safe until the next takeDelimiter call).
    const method_ok = std.mem.eql(u8, method, "POST");
    const path_ok = std.mem.eql(u8, path, "/webhook");

    if (!method_ok or !path_ok) {
        try sendStatus(srv.io, stream, "403 Forbidden");
        return;
    }

    // ── Headers ──────────────────────────────────────────────────────────────
    var content_length: usize = 0;
    var secret_valid = false;

    while (true) {
        // takeDelimiter returns null on clean EOF; errors break the loop.
        const maybe_raw = rdr.takeDelimiter('\n') catch break;
        const h_raw = maybe_raw orelse break;
        const h = std.mem.trimEnd(u8, h_raw, "\r");
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
        try sendStatus(srv.io, stream, "403 Forbidden");
        return;
    }

    // ── Body ─────────────────────────────────────────────────────────────────
    if (content_length > MAX_BODY_BYTES) {
        log.warn("rejected oversized body: Content-Length={d} > {d}", .{ content_length, MAX_BODY_BYTES });
        try sendStatus(srv.io, stream, "413 Request Entity Too Large");
        return;
    }

    const body = try la.alloc(u8, content_length);
    rdr.readSliceAll(body) catch {
        log.warn("rejected request: failed to read body", .{});
        try sendStatus(srv.io, stream, "400 Bad Request");
        return;
    };

    // ── Point-extract the routing user_id (no full parse) ────────────────────
    // A syntactically malformed body is rejected here; a well-formed body with
    // no identifiable sender routes to worker 0 (user_id 0).
    const user_id = extractUserId(la, body) catch {
        log.warn("rejected request: malformed JSON body", .{});
        try sendStatus(srv.io, stream, "400 Bad Request");
        return;
    };

    // ── Forward the raw body verbatim to a worker ────────────────────────────
    // The WorkItem owns its body; the worker frees it after callOnMessage.
    const owned_body = srv.allocator.dupe(u8, body) catch {
        log.warn("rejected request: out of memory copying body", .{});
        try sendStatus(srv.io, stream, "500 Internal Server Error");
        return;
    };
    const item = types.WorkItem{ .body = owned_body, .user_id = user_id };
    const n = srv.queues.len;
    const primary = worker.hashUserId(user_id orelse 0, @intCast(n));
    var pushed = false;
    for (0..n) |i| {
        const idx = (primary + i) % n;
        srv.queues[idx].push(item) catch continue;
        if (i > 0) {
            // Overflow placement relaxes per-user affinity; count it so the
            // relaxation is measurable, not silent.
            if (srv.metrics) |m| _ = m.route_overflow_total.fetchAdd(1, .monotonic);
            log.debug("worker {d} full — overflowed update to worker {d}", .{ primary, idx });
        }
        pushed = true;
        break;
    }
    if (!pushed) {
        if (srv.metrics) |m| _ = m.route_drop_total.fetchAdd(1, .monotonic);
        log.warn("all queues full — update dropped (primary={d})", .{primary});
        srv.allocator.free(owned_body);
        try sendStatus(srv.io, stream, "503 Service Unavailable");
        return;
    }

    // ── Respond 200 immediately ───────────────────────────────────────────────
    try sendStatus(srv.io, stream, "200 OK");
}

// ---------------------------------------------------------------------------
// user_id extraction
//
// The worker-routing key is point-extracted from the raw webhook JSON with a
// streaming scanner — never a full parse.  The scanner navigates to `message.from.id`
// (preferred) or `callback_query.from.id` and stop as soon as an id is found;
// trailing bytes are not scanned.  Telegram lists `message` before
// `callback_query` in the Update object, so first-match gives `message`
// precedence.
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
const Queue = q_mod.Queue;

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
    q_ptrs: [MAX_Q]*Queue(types.WorkItem),
    q_count: usize,
    srv: *Server,

    fn init(n: usize, secret: []const u8) !*TestSetup {
        return initFull(n, secret, DEFAULT_READ_IDLE_TIMEOUT_MS, 512, null);
    }

    fn initTimeout(n: usize, secret: []const u8, read_idle_timeout_ms: u32) !*TestSetup {
        return initFull(n, secret, read_idle_timeout_ms, 512, null);
    }

    /// Full constructor: N queues of `capacity` items each, optional metrics sink.
    /// Saturation/overflow tests use a tiny capacity (so a queue fills with a
    /// single update) and a metrics pointer (so the route counters are observable).
    fn initFull(
        n: usize,
        secret: []const u8,
        read_idle_timeout_ms: u32,
        capacity: usize,
        metrics: ?*metrics_mod.Metrics,
    ) !*TestSetup {
        std.debug.assert(n > 0 and n <= MAX_Q);
        const self = try testing.allocator.create(TestSetup);
        errdefer testing.allocator.destroy(self);
        self.q_count = n;
        for (0..n) |i| {
            self.q_store[i] = try Queue(types.WorkItem).init(testing.allocator, testing.io, capacity);
            self.q_ptrs[i] = &self.q_store[i];
        }
        const bind_addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
        self.srv = try Server.init(.{
            .listen_addr = bind_addr,
            .webhook_secret = secret,
            .queues = self.q_ptrs[0..n],
            .allocator = testing.allocator,
            .io = testing.io,
            .pool_threads = 2,
            .read_idle_timeout_ms = read_idle_timeout_ms,
            .metrics = metrics,
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

    fn serverAddr(self: *const TestSetup) std.Io.net.IpAddress {
        return self.srv.listenAddress();
    }

    fn queueLen(self: *TestSetup, i: usize) usize {
        return self.q_store[i].len();
    }
};

/// Send an HTTP request and return the response status code.
fn httpReq(
    method: []const u8,
    address: std.Io.net.IpAddress,
    path: []const u8,
    secret: ?[]const u8,
    body: []const u8,
) !u16 {
    const io = testing.io;
    var stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    // Write the request straight to the socket's buffered writer, then flush.
    var wbuf: [1536]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    const w = &sw.interface;
    try w.print("{s} {s} HTTP/1.1\r\nHost: localhost\r\n", .{ method, path });
    if (secret) |s| try w.print("X-Telegram-Bot-Api-Secret-Token: {s}\r\n", .{s});
    try w.print("Content-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len});
    try w.writeAll(body);
    try w.flush();

    // Read status line from response.
    var read_buf: [512]u8 = undefined;
    var net_rdr = stream.reader(io, &read_buf);
    const rdr = &net_rdr.interface;
    const raw_line = (try rdr.takeDelimiter('\n')) orelse return error.BadResponse;
    const trimmed = std.mem.trimEnd(u8, raw_line, "\r");
    var it = std.mem.splitScalar(u8, trimmed, ' ');
    _ = it.next(); // "HTTP/1.1"
    const code = it.next() orelse return error.BadResponse;
    return std.fmt.parseInt(u16, code, 10) catch error.BadResponse;
}

/// Send a POST /webhook with Content-Length > MAX_BODY_BYTES but no body.
/// Server must reject based on the header alone.
fn httpOversizeBody(address: std.Io.net.IpAddress, secret: []const u8) !u16 {
    const io = testing.io;
    var stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var wbuf: [512]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    const w = &sw.interface;
    try w.print(
        "POST /webhook HTTP/1.1\r\n" ++
            "X-Telegram-Bot-Api-Secret-Token: {s}\r\n" ++
            "Content-Length: {d}\r\n" ++
            "Connection: close\r\n\r\n",
        .{ secret, MAX_BODY_BYTES + 1 },
    );
    try w.flush();
    // Intentionally omit the body — server must reject before reading it.

    var read_buf: [512]u8 = undefined;
    var net_rdr = stream.reader(io, &read_buf);
    const rdr = &net_rdr.interface;
    const raw_line2 = (try rdr.takeDelimiter('\n')) orelse return error.BadResponse;
    const trimmed2 = std.mem.trimEnd(u8, raw_line2, "\r");
    var it2 = std.mem.splitScalar(u8, trimmed2, ' ');
    _ = it2.next();
    const code2 = it2.next() orelse return error.BadResponse;
    return std.fmt.parseInt(u16, code2, 10) catch error.BadResponse;
}

// ---------------------------------------------------------------------------
// Webhook endpoint tests
// ---------------------------------------------------------------------------

test "valid POST /webhook with correct secret → 200 OK, update enqueued" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, VALID_UPDATE);
    try testing.expectEqual(@as(u16, 200), status);
    try testing.expectEqual(@as(usize, 1), ts.queueLen(0));
}

test "non-POST, wrong path, or bad secret → 403 Forbidden, nothing enqueued" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    // GET instead of POST.
    try testing.expectEqual(@as(u16, 403), try httpReq("GET", ts.serverAddr(), "/webhook", TEST_SECRET, ""));
    // Wrong path.
    try testing.expectEqual(@as(u16, 403), try httpReq("POST", ts.serverAddr(), "/notwebhook", TEST_SECRET, VALID_UPDATE));
    // Missing secret header.
    try testing.expectEqual(@as(u16, 403), try httpReq("POST", ts.serverAddr(), "/webhook", null, VALID_UPDATE));
    // Wrong secret.
    try testing.expectEqual(@as(u16, 403), try httpReq("POST", ts.serverAddr(), "/webhook", "wrong-secret", VALID_UPDATE));

    // No rejected request enqueued anything.
    try testing.expectEqual(@as(usize, 0), ts.queueLen(0));
}

test "valid secret, malformed JSON body → 400 Bad Request, nothing enqueued" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, "{not json}");
    try testing.expectEqual(@as(u16, 400), status);
    try testing.expectEqual(@as(usize, 0), ts.queueLen(0));
}

test "200 OK arrives within 50ms" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const t0 = rt.nowMs(testing.io);
    const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, VALID_UPDATE);
    const elapsed = rt.nowMs(testing.io) - t0;

    try testing.expectEqual(@as(u16, 200), status);
    try testing.expect(elapsed < 50);
}

test "100 requests with same user_id → all enqueued to same worker queue" {
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

test "10 simultaneous connections → all 200 OK, all updates enqueued" {
    const N = 10;
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const ThreadCtx = struct {
        address: std.Io.net.IpAddress,
        status: u16 = 0,
        err: bool = false,
    };

    var ctxs: [N]ThreadCtx = undefined;
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

test "body > 1 MB → 413, server does not allocate unbounded memory" {
    const ts = try TestSetup.init(1, TEST_SECRET);
    defer ts.deinit();

    const status = try httpOversizeBody(ts.serverAddr(), TEST_SECRET);
    try testing.expectEqual(@as(u16, 413), status);
    try testing.expectEqual(@as(usize, 0), ts.queueLen(0));
}

// ---------------------------------------------------------------------------
// Raw body forwarding + point-extracted user_id
//
// Oversize / malformed-JSON rejection is already covered by the malformed-JSON
// and oversize-body tests above — no separate test.
// ---------------------------------------------------------------------------

test "server forwards the raw webhook body verbatim to the worker" {
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

test "extractUserId — message / callback_query / edge cases" {
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
    // early exit: the id is found before the trailing invalid JSON is reached,
    // so the scan stops and returns the id rather than raising InvalidJson.
    try testing.expectEqual(@as(?i64, 777), try extractUserId(A, "{\"message\":{\"from\":{\"id\":777,\"junk\":NOTVALID}}}"));
}

test "server→worker WorkItem handoff leaks nothing under testing.allocator" {
    // The server allocates one WorkItem.body per accepted update.  Draining the
    // queues exactly as a worker does (take item, free body) must leave the
    // testing.allocator balanced — proving the server↔worker ownership contract.
    const ts = try TestSetup.init(2, TEST_SECRET);
    defer ts.deinit();

    for (0..20) |i| {
        var buf: [256]u8 = undefined;
        const body = try std.fmt.bufPrint(&buf,
            \\{{"update_id":{d},"message":{{"from":{{"id":{d}}}}}}}
        , .{ i + 1, i + 1 });
        try testing.expectEqual(@as(u16, 200), try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, body));
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

test "pool-backed server: 50 sequential connections all 200, deinit drains cleanly" {
    const ts = try TestSetup.init(2, TEST_SECRET);
    defer ts.deinit();

    for (0..50) |_| {
        try testing.expectEqual(@as(u16, 200), try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, VALID_UPDATE));
    }

    // Reaching here (and ts.deinit returning) proves pool.deinit() drains the
    // accepted connections and joins its workers without hanging or leaking.
}

// ---------------------------------------------------------------------------
// Routing under backpressure: affinity-overflow placement and all-full drop
//
// Both paths relax the hash(user_id)%N affinity and record it on Metrics:
//   - overflow: primary queue full, update placed on a later queue → still 200.
//   - drop:     every queue full, update shed → 503.
// hashUserId(2, 2) == 0, so an update from user_id 2 routes primarily to
// queue 0; pre-filling queue 0 forces the chosen path deterministically.
// ---------------------------------------------------------------------------

/// A minimal valid Update from user_id 2 (routes to queue 0 under N=1 and N=2).
const UPDATE_USER_2 =
    \\{"update_id":1,"message":{"message_id":1,"from":{"id":2,"is_bot":false,"first_name":"T"},"chat":{"id":2,"type":"private"},"date":0}}
;

/// Push a placeholder WorkItem (heap-owned body, freed by TestSetup.deinit) to
/// fill a queue slot so the server sees that queue at capacity.
fn fillQueue(ts: *TestSetup, qi: usize) !void {
    const body = try testing.allocator.dupe(u8, "{}");
    try ts.q_store[qi].push(.{ .body = body, .user_id = null });
}

test "primary queue full → affinity-overflow places update on next queue, 200, route_overflow_total++" {
    var metrics = metrics_mod.Metrics{};
    // Two queues, capacity 1 each: filling queue 0 leaves queue 1 free, so the
    // overflow path (place elsewhere + 200) is taken rather than the drop path.
    const ts = try TestSetup.initFull(2, TEST_SECRET, DEFAULT_READ_IDLE_TIMEOUT_MS, 1, &metrics);
    defer ts.deinit();

    try fillQueue(ts, 0); // queue 0 at capacity; primary for user_id 2

    const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, UPDATE_USER_2);
    try testing.expectEqual(@as(u16, 200), status);

    // Update landed on the non-primary queue, not dropped.
    try testing.expectEqual(@as(usize, 1), ts.queueLen(0)); // unchanged (the placeholder)
    try testing.expectEqual(@as(usize, 1), ts.queueLen(1)); // the overflowed update
    try testing.expectEqual(@as(u64, 1), metrics.route_overflow_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), metrics.route_drop_total.load(.monotonic));
}

test "all queues full → update dropped with 503, queue length unchanged" {
    var metrics = metrics_mod.Metrics{};
    // Single queue, capacity 1: once filled, every queue is saturated, so the
    // next update is shed (503) rather than placed.
    const ts = try TestSetup.initFull(1, TEST_SECRET, DEFAULT_READ_IDLE_TIMEOUT_MS, 1, &metrics);
    defer ts.deinit();

    try fillQueue(ts, 0); // the only queue is now at capacity

    const status = try httpReq("POST", ts.serverAddr(), "/webhook", TEST_SECRET, UPDATE_USER_2);
    try testing.expectEqual(@as(u16, 503), status);

    // The dropped update was not enqueued: the queue still holds only the filler.
    try testing.expectEqual(@as(usize, 1), ts.queueLen(0));

    // route_drop_total is NOT incremented here. A steadily-saturated server is
    // shed at the accept loop ("all worker queues saturated — dropping
    // connection"), which sends 503 without touching the counter; the only path
    // that increments route_drop_total is the in-handleRequest push-loop drop,
    // reached only in the narrow race where a queue fills *between* the accept
    // check and the push. So under the very condition the counter names, it
    // stays 0. See task report (B13) — flagged as a possible production bug,
    // not fixed here (this task touches tests only). The assertion below pins
    // the current behavior so a future fix that wires the counter is visible.
    try testing.expectEqual(@as(u64, 0), metrics.route_drop_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), metrics.route_overflow_total.load(.monotonic));
}

test "stalled client (sends nothing) is reclaimed by the read timeout" {
    // Server with a short idle timeout so the test does not wait 15 s.
    const ts = try TestSetup.initTimeout(1, TEST_SECRET, 200);
    defer ts.deinit();

    // Open a connection and send nothing.
    // handleRequest's read-failure path closes the conn.
    const io = testing.io;
    var stream = try ts.serverAddr().connect(io, .{ .mode = .stream });
    defer stream.close(io);

    // Read the response: the server sends "400 Bad Request" then closes, OR the
    // peer closes (EOF). Either proves the stalled connection was reclaimed
    // rather than held open indefinitely. readSliceShort returns short on EOF,
    // so it unblocks as soon as the server closes the stalled connection.
    var rbuf: [128]u8 = undefined;
    var net_rdr = stream.reader(io, &rbuf);
    var dest: [128]u8 = undefined;
    const t0 = rt.nowMs(io);
    const n = net_rdr.interface.readSliceShort(&dest) catch 0; // closed/reset counts as reclaimed
    const elapsed = rt.nowMs(io) - t0;

    try testing.expect(elapsed < 5_000); // reclaimed well within the 15 s default
    _ = n;
}
