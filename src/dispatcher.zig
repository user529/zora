// dispatcher.zig — outbound HTTP thread pool → Telegram API
//
// APICALL STRING-PAYLOAD OWNERSHIP CONTRACT
// ----------------------------------------------------------------
// worker.zig transfers ownership of both heap-allocated strings inside each
// ApiCall (`method` and `body`) to this dispatcher.  The dispatcher MUST free
// them after the call has been sent (successfully or not).  Use the helper
// pattern below for every dequeue:
//
//   const call = dispatcher_queue.pop();
//   defer types.freeApiCall(call, allocator);
//   // ... send call ...
//
// Failure to call types.freeApiCall leaks memory on every dispatched call.
//
// The dispatcher is API-agnostic: it POSTs `body` verbatim
// to /bot<token>/<method> with Content-Type: application/json.  It does not
// know — or need to know — which Telegram method it is calling.

const std = @import("std");
const types = @import("types.zig");
const queue_mod = @import("queue.zig");
const io_pool = @import("io_pool.zig");
const metrics_mod = @import("metrics.zig");
const delay = @import("delay.zig");

const log = std.log.scoped(.dispatcher);

/// Upper bound on a captured Telegram response body. Each dispatcher thread
/// owns one buffer of this size and reuses it for every send, so the hot path
/// makes no per-request heap allocation for the response. A reply larger than
/// this is refused (error.ResponseTooLarge) rather than buffered. Matches the
/// 64 KB state ceiling in serializer.zig; real send* replies are far smaller.
const RESPONSE_CEILING: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Public: DispatcherArgs — all parameters for one dispatcher thread
// ---------------------------------------------------------------------------

pub const DispatcherArgs = struct {
    id: u8,
    queue: *queue_mod.Queue(types.ApiCall),
    bot_token: []const u8,
    /// Base URL for the Telegram Bot API.
    /// Production: "https://api.telegram.org"
    /// Tests:      "http://127.0.0.2:{port}"  (plain HTTP, no TLS, to a mock)
    api_base: []const u8,
    allocator: std.mem.Allocator,
    stop: *std.atomic.Value(bool),
    /// Per-worker result queues indexed by worker_id.
    /// Non-null only when tracked sends are expected. Existing tests pass null.
    io_result_queues: ?[]*queue_mod.Queue(io_pool.IoResult) = null,
    /// Optional metrics sink. Null means "don't count" (existing tests default to null).
    metrics: ?*metrics_mod.Metrics = null,
    /// Park sink for 429'd / blocked-chat calls. Null ⇒ no delayed dispatch
    /// (existing tests default null and keep the single-retry behavior).
    delay_q:       ?*delay.DelayQueue = null,
    /// Shared per-chat block state. Null ⇒ no pre-send divert.
    blocked_until: ?*delay.BlockedMap = null,
    retry_after_default_ms: u64 = 1000,
    retry_after_max_ms:     u64 = 60000,
};

// ---------------------------------------------------------------------------
// Public: thread entry point
// ---------------------------------------------------------------------------

/// If the call's chat is currently blocked, park it in delay_q (ownership moves)
/// and return true. Returns false when not blocked / no throttle configured.
fn divertIfBlocked(call: types.ApiCall, args: DispatcherArgs) bool {
    const bu = args.blocked_until orelse return false;
    const dq = args.delay_q orelse return false;
    const r = call.route orelse return false;
    const until = bu.blockedUntil(r.chat_id, std.time.milliTimestamp()) orelse return false;
    parkAt(dq, until, call, args);
    return true;
}

/// Record the block window (if any) and park the 429'd call.
fn parkRateLimited(call: types.ApiCall, retry_after_ms: u64, args: DispatcherArgs) void {
    if (args.metrics) |m| _ = m.throttle_429_total.fetchAdd(1, .monotonic);
    const ready = std.time.milliTimestamp() + @as(i64, @intCast(retry_after_ms));
    if (call.route) |r| if (args.blocked_until) |bu| bu.block(r.chat_id, ready);
    const dq = args.delay_q orelse {
        // No delay_q (tests): the single attempt already happened; drop.
        types.freeApiCall(call, args.allocator);
        return;
    };
    parkAt(dq, ready, call, args);
}

/// Park `call` in `dq` at `ready`, accounting metrics; free + count shed on overflow.
fn parkAt(dq: *delay.DelayQueue, ready: i64, call: types.ApiCall, args: DispatcherArgs) void {
    dq.push(ready, call) catch {
        if (args.metrics) |m| _ = m.throttle_shed_total.fetchAdd(1, .monotonic);
        log.warn("dispatcher {d}: delay_q full, shed a call", .{args.id});
        types.freeApiCall(call, args.allocator);
        return;
    };
    if (args.metrics) |m| {
        _ = m.throttle_delayed_total.fetchAdd(1, .monotonic);
        _ = m.throttle_delay_depth.fetchAdd(1, .monotonic);
    }
}

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

    // One response buffer per thread, reused across every send (no per-request
    // heap allocation for the reply). Body bytes captured here stay valid only
    // until the next send overwrites them.
    var resp_buf: [RESPONSE_CEILING]u8 = undefined;

    while (!args.stop.load(.acquire)) {
        const maybe_call = args.queue.popTimeout(10 * std.time.ns_per_ms);
        if (maybe_call == null) continue;
        const call = maybe_call.?;

        // If this chat is still inside a 429 window, hold the call instead of
        // sending it.
        if (divertIfBlocked(call, args)) continue;

        var retry_after_ms: u64 = args.retry_after_default_ms;
        if (sendWithRetry(&client, call, url_prefix, args.allocator, args.io_result_queues, args.metrics, &retry_after_ms, args.retry_after_default_ms, args.retry_after_max_ms, &resp_buf)) {
            types.freeApiCall(call, args.allocator); // sent OK
        } else |err| {
            if (err == error.RateLimited) {
                parkRateLimited(call, retry_after_ms, args);
            } else {
                log.warn("dispatcher {d}: dropped call after retry failure: {s}", .{ args.id, @errorName(err) });
                types.freeApiCall(call, args.allocator);
            }
        }
    }
    log.info("dispatcher {d}: stopped", .{args.id});
}

// ---------------------------------------------------------------------------
// Private: send with one retry on failure
// ---------------------------------------------------------------------------

fn sendWithRetry(
    client:          *std.http.Client,
    call:            types.ApiCall,
    url_prefix:      []const u8,
    allocator:       std.mem.Allocator,
    result_queues:   ?[]*queue_mod.Queue(io_pool.IoResult),
    metrics:         ?*metrics_mod.Metrics,
    retry_after_out: *u64,
    default_ms:      u64,
    max_ms:          u64,
    resp_buf:        []u8,
) !void {
    if (send(client, call, url_prefix, allocator, result_queues, metrics, retry_after_out, default_ms, max_ms, resp_buf)) {
        return;
    } else |err| {
        if (err == error.RateLimited) return err; // caller parks; never retry a 429
        log.warn("send failed ({s}), retrying in 200ms", .{@errorName(err)});
        std.Thread.sleep(200 * std.time.ns_per_ms);
        // Reinitialize the client so the retry always opens a fresh TCP
        // connection — any pooled connection from the failed attempt may be
        // broken and would cause a second WriteFailed/ReadFailed.
        client.deinit();
        client.* = std.http.Client{ .allocator = allocator };
        send(client, call, url_prefix, allocator, result_queues, metrics, retry_after_out, default_ms, max_ms, resp_buf) catch |retry_err| {
            if (retry_err == error.RateLimited) return retry_err;
            if (call.tracking) |tracking| {
                pushTrackedErr(result_queues, tracking.worker_id, tracking.coro_id, @errorName(retry_err), allocator);
            }
            return retry_err;
        };
    }
}

// ---------------------------------------------------------------------------
// Private: single HTTP POST to the Telegram Bot API
//
// Generic by construction: payload is switched on the
// ApiCall.payload union — JSON body POSTed verbatim, or multipart built
// from parts.  No per-method branching.
// ---------------------------------------------------------------------------

/// Treat any non-2xx response as a retryable error, logging the status.
/// Returns error.TelegramApiError so sendWithRetry triggers the single retry.
fn checkStatus(status: std.http.Status, method: []const u8) !void {
    if (status.class() != .success) {
        log.warn("Telegram API returned HTTP {d} for {s}", .{ @intFromEnum(status), method });
        return error.TelegramApiError;
    }
}

/// Map a fetch error to error.ResponseTooLarge when it was caused by the reply
/// overflowing the fixed response buffer `fw`; otherwise pass it through. On
/// overflow fixedDrain fills the buffer to capacity before failing, so a full
/// buffer paired with WriteFailed is an unambiguous "body exceeded the ceiling".
fn fetchErr(err: anyerror, fw: *const std.Io.Writer, method: []const u8, metrics: ?*metrics_mod.Metrics) anyerror {
    if (err == error.WriteFailed and fw.end == fw.buffer.len) {
        if (metrics) |m| _ = m.response_oversize_total.fetchAdd(1, .monotonic);
        log.warn("response exceeded {d} KB ceiling for {s}; dropping", .{ fw.buffer.len / 1024, method });
        return error.ResponseTooLarge;
    }
    return err;
}

fn send(
    client:          *std.http.Client,
    call:            types.ApiCall,
    url_prefix:      []const u8,
    allocator:       std.mem.Allocator,
    result_queues:   ?[]*queue_mod.Queue(io_pool.IoResult),
    metrics:         ?*metrics_mod.Metrics,
    retry_after_out: *u64,   // set only when returning error.RateLimited
    default_ms:      u64,
    max_ms:          u64,
    resp_buf:        []u8,    // caller-owned, reused across sends; ceiling = its len
) !void {
    const url = try std.fmt.allocPrint(allocator, "{s}{s}", .{ url_prefix, call.method });
    defer allocator.free(url);

    // Capture the response body for every send: a 429 carries retry_after, and
    // tracked sends need message_id. The fixed buffer is reused across sends
    // (no per-request allocation) and caps the reply at its length.
    var fw = std.Io.Writer.fixed(resp_buf);

    const result = switch (call.payload) {
        .json => |body| blk: {
            log.debug("→ {s} (json) {s}", .{ call.method, body });
            break :blk client.fetch(.{
                .location        = .{ .url = url },
                .payload         = body,
                .keep_alive      = true,
                .extra_headers   = &.{.{ .name = "Content-Type", .value = "application/json" }},
                // Ask for an uncompressed response. With a response_writer set,
                // std.http.Client.fetch heap-allocates a 64 KB flate window per
                // call for any gzip/deflate body and then gunzips it. Telegram's
                // reply bodies are tiny, so identity encoding skips both the
                // per-call allocation and the decompression entirely.
                .headers         = .{ .accept_encoding = .{ .override = "identity" } },
                .response_writer = &fw,
            }) catch |err| return fetchErr(err, &fw, call.method, metrics);
        },
        .multipart => |parts| blk: {
            const mb = try buildMultipartBody(parts, allocator);
            defer allocator.free(mb.bytes);
            log.debug("→ {s} (multipart, {d} parts)", .{ call.method, parts.len });
            const ct = try std.fmt.allocPrint(allocator,
                "multipart/form-data; boundary={s}", .{mb.boundary[0..mb.blen]});
            defer allocator.free(ct);
            break :blk client.fetch(.{
                .location        = .{ .url = url },
                .payload         = mb.bytes,
                .keep_alive      = true,
                .extra_headers   = &.{.{ .name = "Content-Type", .value = ct }},
                // Identity encoding — see the json branch above for the rationale.
                .headers         = .{ .accept_encoding = .{ .override = "identity" } },
                .response_writer = &fw,
            }) catch |err| return fetchErr(err, &fw, call.method, metrics);
        },
    };

    const resp = fw.buffered();

    // 429 → signal RateLimited so the caller parks the call (no inline retry).
    if (result.status == .too_many_requests) {
        retry_after_out.* = parseRetryAfter(resp, default_ms, max_ms, allocator);
        return error.RateLimited;
    }

    try checkStatus(result.status, call.method); // other non-2xx ⇒ retryable error

    log.debug("← {d} {s}", .{ @intFromEnum(result.status), call.method });

    // Tracked sends: extract message_id from the captured body.
    if (call.tracking) |tracking| {
        const message_id = parseMessageId(resp, allocator) catch {
            log.warn("tracked send: missing message_id in response for {s}", .{call.method});
            if (metrics) |m| _ = m.tracked_send_failures_total.fetchAdd(1, .monotonic);
            pushTrackedErr(result_queues, tracking.worker_id, tracking.coro_id, "missing message_id in response", allocator);
            return;
        };
        pushTrackedSend(result_queues, tracking.worker_id, tracking.coro_id, message_id);
        log.debug("← tracked message_id={d} for {s}", .{ message_id, call.method });
    }
}

// ---------------------------------------------------------------------------
// Private: multipart body building
// ---------------------------------------------------------------------------

const MultipartBody = struct {
    bytes:    []u8,
    boundary: [34]u8, // "zB" + 32 hex chars
    blen:     usize,
};

/// Reject any string that would break a quoted Content-Disposition value.
fn validateHeaderValue(s: []const u8) !void {
    for (s) |c| {
        if (c == '\r' or c == '\n' or c == '"') return error.InvalidHeaderValue;
    }
}

fn buildMultipartBody(
    parts:     []types.MultipartPart,
    allocator: std.mem.Allocator,
) !MultipartBody {
    var mb: MultipartBody = undefined;
    var raw: [16]u8 = undefined;
    std.crypto.random.bytes(&raw);
    var hex: [32]u8 = undefined;
    for (raw, 0..) |byte, i| {
        const digits = "0123456789abcdef";
        hex[i * 2]     = digits[byte >> 4];
        hex[i * 2 + 1] = digits[byte & 0xf];
    }
    const b_slice = std.fmt.bufPrint(&mb.boundary, "zB{s}", .{hex}) catch unreachable;
    mb.blen = b_slice.len;
    const boundary = mb.boundary[0..mb.blen];

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);

    for (parts) |p| {
        try validateHeaderValue(p.name);
        try buf.appendSlice(allocator, "--");
        try buf.appendSlice(allocator, boundary);
        try buf.appendSlice(allocator, "\r\n");

        if (p.filename) |fname| {
            try validateHeaderValue(fname);
            const hdr = try std.fmt.allocPrint(allocator,
                "Content-Disposition: form-data; name=\"{s}\"; filename=\"{s}\"\r\nContent-Type: application/octet-stream\r\n",
                .{ p.name, fname },
            );
            defer allocator.free(hdr);
            try buf.appendSlice(allocator, hdr);
        } else {
            const hdr = try std.fmt.allocPrint(allocator,
                "Content-Disposition: form-data; name=\"{s}\"\r\n", .{p.name});
            defer allocator.free(hdr);
            try buf.appendSlice(allocator, hdr);
        }
        try buf.appendSlice(allocator, "\r\n");
        try buf.appendSlice(allocator, p.content);
        try buf.appendSlice(allocator, "\r\n");
    }

    try buf.appendSlice(allocator, "--");
    try buf.appendSlice(allocator, boundary);
    try buf.appendSlice(allocator, "--\r\n");

    mb.bytes = try buf.toOwnedSlice(allocator);
    return mb;
}

// ---------------------------------------------------------------------------
// Private: tracked-send helpers
// ---------------------------------------------------------------------------

fn parseMessageId(body: []const u8, allocator: std.mem.Allocator) !i64 {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();
    const root_obj = switch (parsed.value) {
        .object => |o| o,
        else    => return error.NotFound,
    };
    const result_val = root_obj.get("result") orelse return error.NotFound;
    const result_obj = switch (result_val) {
        .object => |o| o,
        else    => return error.NotFound,
    };
    const mid_val = result_obj.get("message_id") orelse return error.NotFound;
    return switch (mid_val) {
        .integer => |n| n,
        else     => error.NotFound,
    };
}

/// Parse Telegram's `parameters.retry_after` (seconds) from a 429 body into
/// milliseconds. Returns `default_ms` when absent/invalid/≤0; clamps to `max_ms`.
/// Treats the body as untrusted: any parse failure falls back to `default_ms`.
fn parseRetryAfter(body: []const u8, default_ms: u64, max_ms: u64, allocator: std.mem.Allocator) u64 {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, body, .{}) catch return default_ms;
    defer parsed.deinit();
    const obj = switch (parsed.value) { .object => |o| o, else => return default_ms };
    const params = obj.get("parameters") orelse return default_ms;
    const pobj = switch (params) { .object => |o| o, else => return default_ms };
    const ra = pobj.get("retry_after") orelse return default_ms;
    const secs: i64 = switch (ra) {
        .integer => |n| n,
        .float   => |f| @intFromFloat(f),
        else     => return default_ms,
    };
    if (secs <= 0) return default_ms;
    const ms: u64 = @as(u64, @intCast(secs)) *| 1000;
    return @min(ms, max_ms);
}

fn pushTrackedSend(
    queues:     ?[]*queue_mod.Queue(io_pool.IoResult),
    worker_id:  u8,
    coro_id:    u32,
    message_id: i64,
) void {
    const qs = queues orelse {
        log.warn("tracked send: io_result_queues is null", .{});
        return;
    };
    if (worker_id >= qs.len) {
        log.warn("tracked send: worker_id {d} out of range (len={d})", .{ worker_id, qs.len });
        return;
    }
    qs[worker_id].push(.{
        .coro_id = coro_id,
        .outcome = .{ .send = .{ .message_id = message_id } },
    }) catch {
        log.warn("tracked send: result queue full for worker {d}", .{worker_id});
    };
}

fn pushTrackedErr(
    queues:    ?[]*queue_mod.Queue(io_pool.IoResult),
    worker_id: u8,
    coro_id:   u32,
    msg:       []const u8,
    allocator: std.mem.Allocator,
) void {
    const qs = queues orelse return;
    if (worker_id >= qs.len) return;
    const duped = allocator.dupe(u8, msg) catch return;
    qs[worker_id].push(.{
        .coro_id = coro_id,
        .outcome = .{ .err = duped },
    }) catch {
        allocator.free(duped);
        log.warn("tracked send: result queue full for worker {d}", .{worker_id});
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

// The dispatcher does not build bodies or map method names — body
// construction lives in lua_engine.  Wire-level coverage is the MockServer
// integration tests below.

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
    /// Number of upcoming responses to answer with 429 instead of 200.
    force_429: std.atomic.Value(u32),
    /// retry_after seconds advertised in the 429 body.
    retry_after_secs: std.atomic.Value(u32),
    /// Number of upcoming 200 responses to answer with an oversized body
    /// (larger than the dispatcher's response ceiling).
    force_big: std.atomic.Value(u32),
    allocator: std.mem.Allocator,

    const MockRequest = struct {
        path:         []const u8,
        body:         []const u8,
        content_type: []const u8,
        allocator:    std.mem.Allocator,

        fn deinit(self: *MockRequest) void {
            self.allocator.free(self.path);
            self.allocator.free(self.body);
            self.allocator.free(self.content_type);
        }
    };

    // Heap-allocated so that `&self` passed to the spawned thread remains valid
    // after init() returns.  A by-value return would copy the struct and
    // invalidate the pointer held by mockLoop.
    pub fn init(allocator: std.mem.Allocator) !*MockServer {
        const self = try allocator.create(MockServer);
        errdefer allocator.destroy(self);
        const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
        self.server = try addr.listen(.{ .reuse_address = true });
        errdefer self.server.deinit();
        self.allocator = allocator;
        self.call_cnt = std.atomic.Value(u32).init(0);
        self.force_429 = std.atomic.Value(u32).init(0);
        self.retry_after_secs = std.atomic.Value(u32).init(1);
        self.force_big = std.atomic.Value(u32).init(0);
        self.stop = std.atomic.Value(bool).init(false);
        self.received = try queue_mod.Queue(MockRequest).init(allocator, 512);
        errdefer self.received.deinit(allocator);
        self.thread = try std.Thread.spawn(.{}, mockLoop, .{self});
        return self;
    }

    pub fn deinit(self: *MockServer) void {
        self.stop.store(true, .release);
        // mockLoop polls with a 10ms timeout and re-checks stop on each
        // iteration, so it exits within ~10ms of seeing stop = true.
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
        return std.fmt.bufPrint(buf, "http://127.0.0.2:{d}", .{
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
        // Poll with a short timeout to re-check `stop` regularly without
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

        // Reply with an oversized 200 body while force_big is armed: a body
        // larger than the dispatcher's response ceiling, to exercise rejection.
        if (srv.force_big.load(.acquire) > 0) {
            _ = srv.force_big.fetchSub(1, .release);
            const big_len: usize = 70_000; // > 64 KB ceiling
            var hdr_buf: [128]u8 = undefined;
            const hdr = std.fmt.bufPrint(&hdr_buf,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" ++
                "Content-Length: {d}\r\nConnection: keep-alive\r\n\r\n",
                .{big_len}) catch unreachable;
            conn.stream.writeAll(hdr) catch return;
            const chunk = [_]u8{'a'} ** 4096;
            var written: usize = 0;
            while (written < big_len) {
                const n = @min(chunk.len, big_len - written);
                conn.stream.writeAll(chunk[0..n]) catch return;
                written += n;
            }
            _ = srv.call_cnt.fetchAdd(1, .release);
            continue;
        }

        // Reply 429 while force_429 is armed, else a minimal Telegram 200.
        if (srv.force_429.load(.acquire) > 0) {
            _ = srv.force_429.fetchSub(1, .release);
            const secs = srv.retry_after_secs.load(.acquire);
            var body_buf: [96]u8 = undefined;
            const body = std.fmt.bufPrint(&body_buf,
                "{{\"ok\":false,\"error_code\":429,\"parameters\":{{\"retry_after\":{d}}}}}",
                .{secs}) catch unreachable;
            var resp_buf: [256]u8 = undefined;
            const resp = std.fmt.bufPrint(&resp_buf,
                "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\n" ++
                "Content-Length: {d}\r\nConnection: keep-alive\r\n\r\n{s}",
                .{ body.len, body }) catch unreachable;
            conn.stream.writeAll(resp) catch return;
        } else {
            const response =
                "HTTP/1.1 200 OK\r\n" ++
                "Content-Type: application/json\r\n" ++
                "Content-Length: 15\r\n" ++
                "Connection: keep-alive\r\n" ++
                "\r\n" ++
                "{\"ok\":true}\r\n\r\n";
            conn.stream.writeAll(response) catch return;
        }
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

    // Parse Content-Length and Content-Type.
    var content_length: usize = 0;
    var content_type_raw: []const u8 = "";
    var lines = std.mem.splitSequence(u8, header_section[rl_end + 2 ..], "\r\n");
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = line[0..colon];
        const value = std.mem.trim(u8, line[colon + 1 ..], " ");
        if (std.ascii.eqlIgnoreCase(name, "content-length")) {
            content_length = std.fmt.parseInt(usize, value, 10) catch 0;
        } else if (std.ascii.eqlIgnoreCase(name, "content-type")) {
            content_type_raw = value;
        }
    }
    // Dupe content_type before the body-reading loop may reallocate buf.
    const content_type = try allocator.dupe(u8, content_type_raw);
    errdefer allocator.free(content_type);

    // Ensure the full body is present.
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
        .path         = path,
        .body         = body,
        .content_type = content_type,
        .allocator    = allocator,
    };
}

// Helper: build a stop flag + spawn one dispatcher thread pointing at a mock.
//
// Heap-allocated so that &self.queue and &self.stop remain valid after init()
// returns.  A by-value return would copy the struct to the caller's stack and
// invalidate the pointers passed to the spawned thread.
const TestDispatcher = struct {
    stop: std.atomic.Value(bool),
    queue: queue_mod.Queue(types.ApiCall),
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
        self.queue = try queue_mod.Queue(types.ApiCall).init(allocator, 256);
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

// ── Mock server integration tests ──────────────────────────────────────────

/// Push a JSON ApiCall with heap-duplicated method+body.  The dispatcher takes
/// ownership and frees both after sending (ApiCall ownership contract).
fn pushCall(q: *queue_mod.Queue(types.ApiCall), method: []const u8, body: []const u8) !void {
    try q.push(.{
        .method  = try testing.allocator.dupe(u8, method),
        .payload = .{ .json = try testing.allocator.dupe(u8, body) },
    });
}

test "ApiCall POSTs to /bot{token}/{method} with verbatim JSON body" {
    const mock = try MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const d = try TestDispatcher.init(testing.allocator, "TESTTOKEN", mock.baseUrl(&url_buf));
    defer d.deinit();

    try pushCall(&d.queue, "editMessageText", "{\"chat_id\":1,\"message_id\":2,\"text\":\"edited\"}");

    try testing.expect(mock.waitForN(1, 2000));
    var req = mock.received.pop();
    defer req.deinit();

    try testing.expectEqualStrings("/botTESTTOKEN/editMessageText", req.path);
    try testing.expectEqualStrings(
        "{\"chat_id\":1,\"message_id\":2,\"text\":\"edited\"}",
        req.body,
    );
}

test "a method the dispatcher never names (sendDice) round-trips end-to-end" {
    // Proof of API-agnosticism: no code path special-cases "sendDice" — it
    // reaches the wire purely because ApiCall.method is opaque.
    const mock = try MockServer.init(testing.allocator);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const d = try TestDispatcher.init(testing.allocator, "TOK", mock.baseUrl(&url_buf));
    defer d.deinit();

    try pushCall(&d.queue, "sendDice", "{\"chat_id\":7}");

    try testing.expect(mock.waitForN(1, 2000));
    var req = mock.received.pop();
    defer req.deinit();

    try testing.expectEqualStrings("/botTOK/sendDice", req.path);
    try testing.expectEqualStrings("{\"chat_id\":7}", req.body);
}

test "dispatcher retries once after server closes connection; exactly 2 attempts" {
    // First connection: accept but immediately close without a response.
    // Second connection: answer normally.
    // The dispatcher must have retried exactly once.

    const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();

    const srv_port = server.listen_address.in.sa.port;
    var url_buf: [64]u8 = undefined;
    const api_base = std.fmt.bufPrint(&url_buf, "http://127.0.0.2:{d}", .{
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

    try pushCall(&d.queue, "sendMessage", "{\"chat_id\":1,\"text\":\"retry test\"}");

    srv_thread.join();

    // Both attempts must have been made — no more, no less.
    try testing.expectEqual(@as(u32, 2), attempt_count.load(.acquire));
}

test "both attempts fail — call discarded, no third attempt, no crash" {
    const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
    var server = try addr.listen(.{ .reuse_address = true });

    const srv_port = server.listen_address.in.sa.port;
    var url_buf: [64]u8 = undefined;
    const api_base = std.fmt.bufPrint(&url_buf, "http://127.0.0.2:{d}", .{
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
    // (Wait for both drops first, then close.)

    const d = try TestDispatcher.init(testing.allocator, "TOK", api_base);
    defer d.deinit();

    try pushCall(&d.queue, "sendMessage", "{\"chat_id\":2,\"text\":\"both-fail\"}");

    srv_thread.join();
    server.deinit(); // safe to call after thread exits

    // Exactly 2 attempts — no third attempt was made.
    try testing.expectEqual(@as(u32, 2), attempt_count.load(.acquire));
}

test "parseMessageId reads an id or reports NotFound" {
    // Valid result with an integer message_id.
    try testing.expectEqual(@as(i64, 42), try parseMessageId(
        "{\"ok\":true,\"result\":{\"message_id\":42,\"chat\":{\"id\":1}}}", testing.allocator));
    // message_id absent.
    try testing.expectError(error.NotFound, parseMessageId(
        "{\"ok\":true,\"result\":{\"chat\":{\"id\":1}}}", testing.allocator));
    // result absent.
    try testing.expectError(error.NotFound, parseMessageId(
        "{\"ok\":true}", testing.allocator));
    // message_id present but not an integer.
    try testing.expectError(error.NotFound, parseMessageId(
        "{\"ok\":true,\"result\":{\"message_id\":\"notanint\"}}", testing.allocator));
}

test "parseRetryAfter reads, defaults, and clamps" {
    // parameters.retry_after seconds → ms.
    try testing.expectEqual(@as(u64, 5000), parseRetryAfter(
        "{\"ok\":false,\"error_code\":429,\"parameters\":{\"retry_after\":5}}", 1000, 60000, testing.allocator));
    // Absent, unparseable, or zero → the supplied default.
    try testing.expectEqual(@as(u64, 1000), parseRetryAfter("{\"ok\":false}", 1000, 60000, testing.allocator));
    try testing.expectEqual(@as(u64, 1000), parseRetryAfter("not json", 1000, 60000, testing.allocator));
    try testing.expectEqual(@as(u64, 1000), parseRetryAfter("{\"parameters\":{\"retry_after\":0}}", 1000, 60000, testing.allocator));
    // Above max → clamped to max.
    try testing.expectEqual(@as(u64, 60000), parseRetryAfter("{\"parameters\":{\"retry_after\":99999}}", 1000, 60000, testing.allocator));
}

// Queue-level concurrency and FIFO ordering are covered by queue.zig;
// wire-level dispatch is covered by the mock-server tests above.

// ── Multipart boundary splitter (test-only helper) ───────────────────────────

/// Minimal boundary-split: given multipart body bytes and a boundary string,
/// populate `parts` with slices of each part's raw content (headers + body).
/// Returns the number of parts found (at most parts.len).
fn splitMultipart(body: []const u8, boundary: []const u8, parts: [][]const u8) usize {
    const delim = std.fmt.allocPrint(testing.allocator, "--{s}", .{boundary}) catch return 0;
    defer testing.allocator.free(delim);

    var found: usize = 0;
    var pos: usize = 0;
    while (pos < body.len and found < parts.len) {
        const start = std.mem.indexOf(u8, body[pos..], delim) orelse break;
        const abs_start = pos + start;
        const after_delim = abs_start + delim.len;
        if (after_delim >= body.len) break;
        if (body[after_delim] == '-') break; // closing --boundary--

        // Skip the \r\n immediately after the delimiter line
        const content_start = after_delim + 2;
        const next = std.mem.indexOf(u8, body[content_start..], delim) orelse {
            pos = after_delim;
            continue;
        };
        // Trim trailing \r\n before the next delimiter
        const part_end = content_start + next -| 2;
        parts[found] = body[content_start..part_end];
        found += 1;
        pos = content_start + next;
    }
    return found;
}

test "multipart and json bodies are encoded correctly on the wire" {
    const alloc = testing.allocator;

    // A file part is sent as multipart/form-data with the right path and bytes.
    {
        const mock = try MockServer.init(alloc);
        defer mock.deinit();
        var url_buf: [64]u8 = undefined;
        const d = try TestDispatcher.init(alloc, "test-tok", mock.baseUrl(&url_buf));
        defer d.deinit();

        var parts = try alloc.alloc(types.MultipartPart, 1);
        parts[0] = .{
            .name     = try alloc.dupe(u8, "photo"),
            .content  = try alloc.dupe(u8, "\xff\xd8\xff"),
            .filename = try alloc.dupe(u8, "img.jpg"),
        };
        try d.queue.push(.{
            .method  = try alloc.dupe(u8, "sendPhoto"),
            .payload = .{ .multipart = parts },
        });

        try testing.expect(mock.waitForN(1, 2000));
        var req = mock.received.pop();
        defer req.deinit();

        try testing.expectEqualStrings("/bottest-tok/sendPhoto", req.path);
        try testing.expect(std.mem.startsWith(u8, req.content_type, "multipart/form-data; boundary="));

        const boundary = req.content_type["multipart/form-data; boundary=".len..];
        var raw_parts: [8][]const u8 = undefined;
        const n = splitMultipart(req.body, boundary, &raw_parts);
        try testing.expect(n >= 1);
        var found_file = false;
        for (raw_parts[0..n]) |part| {
            if (std.mem.indexOf(u8, part, "img.jpg") != null) {
                try testing.expect(std.mem.indexOf(u8, part, "\xff\xd8\xff") != null);
                found_file = true;
            }
        }
        try testing.expect(found_file);
    }
    // String-only params stay application/json (no multipart regression).
    {
        const mock = try MockServer.init(alloc);
        defer mock.deinit();
        var url_buf: [64]u8 = undefined;
        const d = try TestDispatcher.init(alloc, "test-tok", mock.baseUrl(&url_buf));
        defer d.deinit();

        try d.queue.push(.{
            .method  = try alloc.dupe(u8, "sendMessage"),
            .payload = .{ .json = try alloc.dupe(u8, "{\"chat_id\":1,\"text\":\"hi\"}") },
        });

        try testing.expect(mock.waitForN(1, 2000));
        var req = mock.received.pop();
        defer req.deinit();
        try testing.expectEqualStrings("application/json", req.content_type);
        try testing.expectEqualStrings("{\"chat_id\":1,\"text\":\"hi\"}", req.body);
    }
    // A mixed scalar + file call carries both parts on the wire.
    {
        const mock = try MockServer.init(alloc);
        defer mock.deinit();
        var url_buf: [64]u8 = undefined;
        const d = try TestDispatcher.init(alloc, "test-tok", mock.baseUrl(&url_buf));
        defer d.deinit();

        var parts = try alloc.alloc(types.MultipartPart, 2);
        parts[0] = .{ .name = try alloc.dupe(u8, "caption"), .content = try alloc.dupe(u8, "My caption"), .filename = null };
        parts[1] = .{ .name = try alloc.dupe(u8, "photo"), .content = try alloc.dupe(u8, "\xff\xd8\xff"), .filename = try alloc.dupe(u8, "pic.jpg") };
        try d.queue.push(.{
            .method  = try alloc.dupe(u8, "sendPhoto"),
            .payload = .{ .multipart = parts },
        });

        try testing.expect(mock.waitForN(1, 2000));
        var req = mock.received.pop();
        defer req.deinit();

        const boundary = req.content_type["multipart/form-data; boundary=".len..];
        var raw_parts: [8][]const u8 = undefined;
        const n = splitMultipart(req.body, boundary, &raw_parts);
        try testing.expect(n >= 2);
        var found_caption = false;
        var found_file = false;
        for (raw_parts[0..n]) |part| {
            if (std.mem.indexOf(u8, part, "caption") != null and
                std.mem.indexOf(u8, part, "My caption") != null) found_caption = true;
            if (std.mem.indexOf(u8, part, "pic.jpg") != null) found_file = true;
        }
        try testing.expect(found_caption);
        try testing.expect(found_file);
    }
}

// header injection: CRLF in a field name or filename → call discarded, never sent.
// One call per test: the dispatcher retries an invalid call once (~200 ms) and
// frees it on the second failure, so a single call clears before deinit; two
// serial calls would not, and the queue does not free undispatched calls.
test "buildMultipartBody rejects CRLF in field name" {
    const alloc = testing.allocator;
    const mock = try MockServer.init(alloc);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const d = try TestDispatcher.init(alloc, "test-tok", mock.baseUrl(&url_buf));
    defer d.deinit();

    try d.queue.push(.{
        .method  = try alloc.dupe(u8, "sendPhoto"),
        .payload = .{ .multipart = blk: {
            var ps = try alloc.alloc(types.MultipartPart, 1);
            ps[0] = .{
                .name     = try alloc.dupe(u8, "bad\r\nfield"),
                .content  = try alloc.dupe(u8, "x"),
                .filename = null,
            };
            break :blk ps;
        }},
    });

    std.Thread.sleep(300 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 0), mock.received.len());
}

test "buildMultipartBody rejects CRLF in filename" {
    const alloc = testing.allocator;
    const mock = try MockServer.init(alloc);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const d = try TestDispatcher.init(alloc, "test-tok", mock.baseUrl(&url_buf));
    defer d.deinit();

    try d.queue.push(.{
        .method  = try alloc.dupe(u8, "sendPhoto"),
        .payload = .{ .multipart = blk: {
            var ps = try alloc.alloc(types.MultipartPart, 1);
            ps[0] = .{
                .name     = try alloc.dupe(u8, "photo"),
                .content  = try alloc.dupe(u8, "\xff\xd8\xff"),
                .filename = try alloc.dupe(u8, "img.jpg\r\nContent-Type: text/html"),
            };
            break :blk ps;
        }},
    });

    std.Thread.sleep(300 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 0), mock.received.len());
}

// ── Live integration test ─────────────────────────────────────────────────────

test "send real Telegram message via dispatcher" {
    const token = std.posix.getenv("TELEGRAM_BOT_TOKEN") orelse return error.SkipZigTest;
    const chat_id_str = std.posix.getenv("TELEGRAM_CHAT_ID") orelse return error.SkipZigTest;
    const chat_id = try std.fmt.parseInt(i64, chat_id_str, 10);

    const d = try TestDispatcher.init(testing.allocator, token, "https://api.telegram.org");
    defer d.deinit();

    const body = try std.fmt.allocPrint(
        testing.allocator,
        "{{\"chat_id\":{d},\"text\":\"zora dispatcher live test.\"}}",
        .{chat_id},
    );
    defer testing.allocator.free(body);
    try pushCall(&d.queue, "sendMessage", body);

    // Give the dispatcher enough time to send and confirm.
    std.Thread.sleep(4 * std.time.ns_per_s);
}

// ── tracked_send_failures_total ──────────────────────────────────────────────

test "tracked_send_failures_total increments when message_id absent in response" {
    const alloc = testing.allocator;

    // Inline stub: accepts one connection, returns 200 with a JSON body
    // that has no message_id — triggers the parseMessageId catch path.
    const addr = try std.net.Address.parseIp4("127.0.0.2", 0);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();

    const srv_port = server.listen_address.in.sa.port;
    var url_buf: [64]u8 = undefined;
    const api_base = std.fmt.bufPrint(&url_buf, "http://127.0.0.2:{d}", .{
        std.mem.bigToNative(u16, srv_port),
    }) catch unreachable;

    const srv_thread = try std.Thread.spawn(.{}, struct {
        fn run(srv: *std.net.Server) void {
            const conn = srv.accept() catch return;
            defer conn.stream.close();
            var buf: [4096]u8 = undefined;
            _ = conn.stream.read(&buf) catch {};
            conn.stream.writeAll(
                "HTTP/1.1 200 OK\r\n" ++
                "Content-Type: application/json\r\n" ++
                "Content-Length: 11\r\n" ++
                "\r\n" ++
                "{\"ok\":true}",
            ) catch {};
        }
    }.run, .{&server});

    var m = metrics_mod.Metrics{};
    var rq = try queue_mod.Queue(io_pool.IoResult).init(alloc, 8);
    defer rq.deinit(alloc);
    var rq_slice = [1]*queue_mod.Queue(io_pool.IoResult){&rq};

    var stop = std.atomic.Value(bool).init(false);
    var dq = try queue_mod.Queue(types.ApiCall).init(alloc, 16);
    defer dq.deinit(alloc);

    const disp_thread = try std.Thread.spawn(.{}, dispatcherThread, .{DispatcherArgs{
        .id               = 0,
        .queue            = &dq,
        .bot_token        = "TOK",
        .api_base         = api_base,
        .allocator        = alloc,
        .stop             = &stop,
        .io_result_queues = &rq_slice,
        .metrics          = &m,
    }});

    // Push a tracked sendMessage. Dispatcher owns method+payload after push.
    try dq.push(.{
        .method   = try alloc.dupe(u8, "sendMessage"),
        .payload  = .{ .json = try alloc.dupe(u8, "{\"chat_id\":1,\"text\":\"hi\"}") },
        .tracking = .{ .worker_id = 0, .coro_id = 7 },
    });

    // Wait for the error IoResult pushed by pushTrackedErr (up to 2 s).
    const maybe_result = rq.popTimeout(2_000 * std.time.ns_per_ms);
    try testing.expect(maybe_result != null);
    io_pool.freeIoResult(maybe_result.?, alloc);

    // failure counter must be exactly 1.
    try testing.expectEqual(@as(u64, 1), m.tracked_send_failures_total.load(.monotonic));

    stop.store(true, .release);
    disp_thread.join();
    srv_thread.join();
}

test "an oversize response is rejected and counted" {
    // The oversized reply must be refused rather than buffered and accepted, so
    // the dispatcher retries on a fresh connection — the mock sees the request
    // twice and the oversize counter rises. Without the ceiling the body would
    // be accepted on the first try and the mock would see it only once.
    const mock = try MockServer.init(testing.allocator);
    defer mock.deinit();
    mock.force_big.store(1, .release); // first 200 carries an oversized body

    var m = metrics_mod.Metrics{};
    var stop = std.atomic.Value(bool).init(false);
    var dq = try queue_mod.Queue(types.ApiCall).init(testing.allocator, 16);
    defer dq.deinit(testing.allocator);

    var url_buf: [64]u8 = undefined;
    const disp = try std.Thread.spawn(.{}, dispatcherThread, .{DispatcherArgs{
        .id = 0, .queue = &dq, .bot_token = "TOK", .api_base = mock.baseUrl(&url_buf),
        .allocator = testing.allocator, .stop = &stop, .metrics = &m,
    }});

    try dq.push(.{
        .method  = try testing.allocator.dupe(u8, "sendMessage"),
        .payload = .{ .json = try testing.allocator.dupe(u8, "{\"chat_id\":1,\"text\":\"x\"}") },
    });

    // The request reaches the mock twice (reject → retry).
    try testing.expect(mock.waitForN(2, 4000));
    // And the oversize counter is incremented.
    var waited: u64 = 0;
    while (m.response_oversize_total.load(.monotonic) == 0 and waited < 4000) {
        std.Thread.sleep(20 * std.time.ns_per_ms);
        waited += 20;
    }
    stop.store(true, .release);
    disp.join();
    while (dq.popTimeout(0)) |c| types.freeApiCall(c, testing.allocator);

    try testing.expect(m.response_oversize_total.load(.monotonic) >= 1);
}

// Spins up: mock server + 1 dispatcher (with delay_q + blocked_until) + 1
// requeue thread feeding the dispatcher's own queue. Heap-allocated so pointers
// passed to threads stay valid.
const ThrottleHarness = struct {
    stop:      std.atomic.Value(bool),
    queue:     queue_mod.Queue(types.ApiCall),
    delay_q:   delay.DelayQueue,
    blocked:   delay.BlockedMap,
    disp_t:    std.Thread,
    requeue_t: std.Thread,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, api_base: []const u8) !*ThrottleHarness {
        const self = try allocator.create(ThrottleHarness);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.stop = std.atomic.Value(bool).init(false);
        self.queue = try queue_mod.Queue(types.ApiCall).init(allocator, 64);
        self.delay_q = delay.DelayQueue.init(allocator, 64);
        self.blocked = delay.BlockedMap.init(allocator);
        self.disp_t = try std.Thread.spawn(.{}, dispatcherThread, .{DispatcherArgs{
            .id = 0, .queue = &self.queue, .bot_token = "TOK", .api_base = api_base,
            .allocator = allocator, .stop = &self.stop,
            .delay_q = &self.delay_q, .blocked_until = &self.blocked,
            .retry_after_default_ms = 1000, .retry_after_max_ms = 60000,
        }});
        self.requeue_t = try std.Thread.spawn(.{}, delay.requeueThread, .{delay.RequeueArgs{
            .delay_q = &self.delay_q, .disp_q = &self.queue, .stop = &self.stop,
            .metrics = null, .allocator = allocator,
        }});
        return self;
    }

    fn deinit(self: *ThrottleHarness) void {
        self.stop.store(true, .release);
        self.disp_t.join();
        self.requeue_t.join();
        while (self.queue.popTimeout(0)) |c| types.freeApiCall(c, self.allocator);
        self.delay_q.deinitDrain(self.allocator);
        self.blocked.deinit();
        self.queue.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    fn pushMsg(self: *ThrottleHarness, chat_id: i64) !void {
        var body_buf: [64]u8 = undefined;
        const body = try std.fmt.bufPrint(&body_buf, "{{\"chat_id\":{d},\"text\":\"x\"}}", .{chat_id});
        try self.queue.push(.{
            .method  = try self.allocator.dupe(u8, "sendMessage"),
            .payload = .{ .json = try self.allocator.dupe(u8, body) },
            .route   = .{ .chat_id = chat_id },
        });
    }
};

test "429 parks the call and a blocked chat waits for the window" {
    // A 429 with retry_after parks the call; it is re-sent after the wait, so
    // the mock sees the request twice (once 429'd, once 200).
    {
        const mock = try MockServer.init(testing.allocator);
        defer mock.deinit();
        mock.force_429.store(1, .release);          // first response = 429
        mock.retry_after_secs.store(1, .release);    // wait 1s

        var url_buf: [64]u8 = undefined;
        const h = try ThrottleHarness.init(testing.allocator, mock.baseUrl(&url_buf));
        defer h.deinit();

        try h.pushMsg(1);
        try testing.expect(mock.waitForN(2, 4000));
    }
    // While a chat is blocked, further messages to it are held back until the
    // window elapses, then all are delivered.
    {
        const mock = try MockServer.init(testing.allocator);
        defer mock.deinit();
        mock.force_429.store(1, .release);          // only the first send 429s
        mock.retry_after_secs.store(1, .release);

        var url_buf: [64]u8 = undefined;
        const h = try ThrottleHarness.init(testing.allocator, mock.baseUrl(&url_buf));
        defer h.deinit();

        try h.pushMsg(1);                            // → 429 → parks, blocks chat 1
        // Give the dispatcher a moment to process the 429 and set blocked_until.
        std.Thread.sleep(150 * std.time.ns_per_ms);
        try h.pushMsg(1);                            // diverted (no wire)
        try h.pushMsg(1);                            // diverted (no wire)

        // During the ~1s window, only the first (429'd) request reached the mock.
        std.Thread.sleep(500 * std.time.ns_per_ms);
        try testing.expectEqual(@as(usize, 1), mock.received.len());

        // After the window all three are delivered: 1 (429) + 3 (success) = 4 total.
        try testing.expect(mock.waitForN(4, 4000));
    }
}
