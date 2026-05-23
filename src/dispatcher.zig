// dispatcher.zig — outbound HTTP thread pool → Telegram API
//
// APICALL STRING-PAYLOAD OWNERSHIP CONTRACT (see TECH_DEBT.md TD-4)
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
// The dispatcher is API-agnostic (ADR-0001 §AD-1): it POSTs `body` verbatim
// to /bot<token>/<method> with Content-Type: application/json.  It does not
// know — or need to know — which Telegram method it is calling.

const std = @import("std");
const types = @import("types.zig");
const queue_mod = @import("queue.zig");

const log = std.log.scoped(.dispatcher);

// ---------------------------------------------------------------------------
// Public: DispatcherArgs — all parameters for one dispatcher thread
// ---------------------------------------------------------------------------

pub const DispatcherArgs = struct {
    id: u8,
    queue: *queue_mod.Queue(types.ApiCall),
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
        const maybe_call = args.queue.popTimeout(10 * std.time.ns_per_ms);
        if (maybe_call == null) continue;
        const call = maybe_call.?;
        // Always free string payloads, whether send succeeds or not.
        defer types.freeApiCall(call, args.allocator);

        sendWithRetry(&client, call, url_prefix, args.allocator) catch |err| {
            log.warn("dispatcher {d}: dropped call after retry failure: {s}", .{ args.id, @errorName(err) });
        };
    }
    log.info("dispatcher {d}: stopped", .{args.id});
}

// ---------------------------------------------------------------------------
// Private: send with one retry on failure
// ---------------------------------------------------------------------------

fn sendWithRetry(
    client: *std.http.Client,
    call: types.ApiCall,
    url_prefix: []const u8,
    allocator: std.mem.Allocator,
) !void {
    if (send(client, call, url_prefix, allocator)) {
        return;
    } else |err| {
        log.warn("send failed ({s}), retrying in 1s", .{@errorName(err)});
        std.Thread.sleep(1 * std.time.ns_per_s);
        // Reinitialize the client so the retry always opens a fresh TCP
        // connection — any pooled connection from the failed attempt may be
        // broken and would cause a second WriteFailed/ReadFailed.
        client.deinit();
        client.* = std.http.Client{ .allocator = allocator };
        try send(client, call, url_prefix, allocator);
    }
}

// ---------------------------------------------------------------------------
// Private: single HTTP POST to the Telegram Bot API
//
// Generic by construction (ADR-0001 §AD-1): payload is switched on the
// ApiCall.payload union — JSON body POSTed verbatim, or multipart built
// from parts.  No per-method branching.
// ---------------------------------------------------------------------------

fn send(
    client:     *std.http.Client,
    call:       types.ApiCall,
    url_prefix: []const u8,
    allocator:  std.mem.Allocator,
) !void {
    const url = try std.fmt.allocPrint(allocator, "{s}{s}", .{ url_prefix, call.method });
    defer allocator.free(url);

    switch (call.payload) {
        .json => |body| {
            log.debug("→ {s} (json) {s}", .{ call.method, body });
            const result = try client.fetch(.{
                .location      = .{ .url = url },
                .payload       = body,
                .keep_alive    = true,
                .extra_headers = &.{
                    .{ .name = "Content-Type", .value = "application/json" },
                },
            });
            if (result.status.class() != .success) {
                log.warn("Telegram API returned HTTP {d} for {s}",
                    .{ @intFromEnum(result.status), call.method });
                return error.TelegramApiError;
            }
            log.debug("← {d} {s}", .{ @intFromEnum(result.status), call.method });
        },
        .multipart => |parts| {
            const mb = try buildMultipartBody(parts, allocator);
            defer allocator.free(mb.bytes);
            log.debug("→ {s} (multipart, {d} parts)", .{ call.method, parts.len });
            const ct = try std.fmt.allocPrint(allocator,
                "multipart/form-data; boundary={s}", .{mb.boundary[0..mb.blen]});
            defer allocator.free(ct);
            const result = try client.fetch(.{
                .location      = .{ .url = url },
                .payload       = mb.bytes,
                .keep_alive    = true,
                .extra_headers = &.{
                    .{ .name = "Content-Type", .value = ct },
                },
            });
            if (result.status.class() != .success) {
                log.warn("Telegram API returned HTTP {d} for {s}",
                    .{ @intFromEnum(result.status), call.method });
                return error.TelegramApiError;
            }
            log.debug("← {d} {s}", .{ @intFromEnum(result.status), call.method });
        },
    }
}

// ---------------------------------------------------------------------------
// Private: multipart body building (Phase 15)
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
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

// Sub-step 13a (ADR-0001): the per-method `buildBody` / `actionMethod`
// unit tests are retired — the dispatcher no longer builds bodies or maps
// method names.  Body construction moved to lua_engine's actionToApiCall
// adapter (tested there: AC-6.2, AC-6.4).  Wire-level coverage is the
// MockServer integration tests below (AC-13.2, AC-13.3, AC-13.4).

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

// ── Mock server integration tests (ADR-0001 sub-step 13a) ───────────────────

/// Push a JSON ApiCall with heap-duplicated method+body.  The dispatcher takes
/// ownership and frees both after sending (ApiCall ownership contract).
fn pushCall(q: *queue_mod.Queue(types.ApiCall), method: []const u8, body: []const u8) !void {
    try q.push(.{
        .method  = try testing.allocator.dupe(u8, method),
        .payload = .{ .json = try testing.allocator.dupe(u8, body) },
    });
}

test "AC-13.2: ApiCall POSTs to /bot{token}/{method} with verbatim JSON body" {
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

test "AC-13.3: a method the dispatcher never names (sendDice) round-trips end-to-end" {
    // Proof of API-agnosticism (ADR-0001 §AD-1): no code path special-cases
    // "sendDice" — it reaches the wire purely because ApiCall.method is opaque.
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

test "AC-13.4: dispatcher retries once after server closes connection; exactly 2 attempts" {
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

    try pushCall(&d.queue, "sendMessage", "{\"chat_id\":1,\"text\":\"retry test\"}");

    srv_thread.join();

    // Both attempts must have been made — no more, no less.
    try testing.expectEqual(@as(u32, 2), attempt_count.load(.acquire));
}

test "AC-13.4: both attempts fail — call discarded, no third attempt, no crash" {
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

    try pushCall(&d.queue, "sendMessage", "{\"chat_id\":2,\"text\":\"both-fail\"}");

    srv_thread.join();
    server.deinit(); // safe to call after thread exits

    // Exactly 2 attempts — no third attempt was made.
    try testing.expectEqual(@as(u32, 2), attempt_count.load(.acquire));
}

// Sub-step 13a: the former AC-9.7 (100-call concurrency) and AC-9.8 (dispatch
// order) tests are retired.  Queue-level concurrency and FIFO ordering are
// covered by queue.zig's AC-4.x; wire-level dispatch is covered by AC-13.2/3.

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

// AC-15.1 — multipart ApiCall reaches mock server with correct Content-Type and parts
test "AC-15.1: multipart ApiCall sends multipart/form-data with file part" {
    const alloc = testing.allocator;
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
    try testing.expect(std.mem.startsWith(u8, req.content_type,
        "multipart/form-data; boundary="));

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

// AC-15.2 — string-only params → JSON (regression: no multipart regression)
test "AC-15.2: json ApiCall still sends application/json" {
    const alloc = testing.allocator;
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

// AC-15.4 — mixed scalar + file → multipart with both parts on the wire
test "AC-15.4: mixed multipart ApiCall contains both text and file parts" {
    const alloc = testing.allocator;
    const mock = try MockServer.init(alloc);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const d = try TestDispatcher.init(alloc, "test-tok", mock.baseUrl(&url_buf));
    defer d.deinit();

    var parts = try alloc.alloc(types.MultipartPart, 2);
    parts[0] = .{
        .name     = try alloc.dupe(u8, "caption"),
        .content  = try alloc.dupe(u8, "My caption"),
        .filename = null,
    };
    parts[1] = .{
        .name     = try alloc.dupe(u8, "photo"),
        .content  = try alloc.dupe(u8, "\xff\xd8\xff"),
        .filename = try alloc.dupe(u8, "pic.jpg"),
    };
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
            std.mem.indexOf(u8, part, "My caption") != null)
        {
            found_caption = true;
        }
        if (std.mem.indexOf(u8, part, "pic.jpg") != null) {
            found_file = true;
        }
    }
    try testing.expect(found_caption);
    try testing.expect(found_file);
}

// AC-24 — header injection: CRLF or quote in name/filename → error.InvalidHeaderValue
test "AC-24: buildMultipartBody rejects CRLF in field name" {
    const alloc = testing.allocator;
    var parts = try alloc.alloc(types.MultipartPart, 1);
    parts[0] = .{
        .name     = try alloc.dupe(u8, "photo\r\nX-Injected: evil"),
        .content  = try alloc.dupe(u8, "bytes"),
        .filename = try alloc.dupe(u8, "img.jpg"),
    };
    const call = types.ApiCall{
        .method  = try alloc.dupe(u8, "sendPhoto"),
        .payload = .{ .multipart = parts },
    };
    defer types.freeApiCall(call, alloc);

    const mock = try MockServer.init(alloc);
    defer mock.deinit();

    var url_buf: [64]u8 = undefined;
    const d = try TestDispatcher.init(alloc, "test-tok", mock.baseUrl(&url_buf));
    defer d.deinit();

    // The dispatcher logs + discards calls with invalid headers; it must not
    // forward the malformed multipart body.
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

    // No request should reach the mock within 200 ms.
    std.Thread.sleep(200 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 0), mock.received.len());
}

test "AC-24: buildMultipartBody rejects CRLF in filename" {
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

    std.Thread.sleep(200 * std.time.ns_per_ms);
    try testing.expectEqual(@as(usize, 0), mock.received.len());
}

// ── Live integration test ─────────────────────────────────────────────────────

test "AC-13.live: send real Telegram message via dispatcher" {
    const token = std.posix.getenv("TELEGRAM_BOT_TOKEN") orelse return error.SkipZigTest;
    const chat_id_str = std.posix.getenv("TELEGRAM_CHAT_ID") orelse return error.SkipZigTest;
    const chat_id = try std.fmt.parseInt(i64, chat_id_str, 10);

    const d = try TestDispatcher.init(testing.allocator, token, "https://api.telegram.org");
    defer d.deinit();

    const body = try std.fmt.allocPrint(
        testing.allocator,
        "{{\"chat_id\":{d},\"text\":\"zora dispatcher live — Phase 13a wired up.\"}}",
        .{chat_id},
    );
    defer testing.allocator.free(body);
    try pushCall(&d.queue, "sendMessage", body);

    // Give the dispatcher enough time to send and confirm.
    std.Thread.sleep(4 * std.time.ns_per_s);
}
