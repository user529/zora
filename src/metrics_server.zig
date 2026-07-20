//! metrics_server.zig — Prometheus scrape endpoint.
//!
//! One thread, one listener: GET /metrics renders the shared Metrics set in
//! the text exposition format (v0.0.4); every other request gets 404.
//! Enabled only when METRICS_ADDR is configured. The port carries no auth —
//! deployment keeps it private (bind 127.0.0.1 or firewall the port).
//!
//! A scrape failure never affects the bot: connection errors are logged at
//! warn and the loop continues. One thread comfortably serves the usual
//! 15-second scrape interval.

const std = @import("std");
const metrics_mod = @import("metrics.zig");
const types = @import("types.zig");

const log = std.log.scoped(.metrics_server);

/// Render/response scratch. 32 KiB covers the full exposition including
/// per-worker depth lines at the 255-worker maximum (~60 bytes per line).
const RENDER_BUF_BYTES = 32 * 1024;

pub const MetricsServerArgs = struct {
    listen_addr: std.Io.net.IpAddress,
    metrics: *const metrics_mod.Metrics,
    /// Queue pointers and build identity sampled at render time.
    sources: metrics_mod.RenderSources,
    io: std.Io,
    allocator: std.mem.Allocator,
};

pub const MetricsServer = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    metrics: *const metrics_mod.Metrics,
    sources: metrics_mod.RenderSources,
    listener: std.Io.net.Server,
    stop: std.atomic.Value(bool),
    thread: std.Thread,

    /// Bind and start the serve thread. Returns a heap-allocated server;
    /// deinit() stops the loop, closes the socket, and frees it.
    pub fn init(args: MetricsServerArgs) !*MetricsServer {
        const self = try args.allocator.create(MetricsServer);
        errdefer args.allocator.destroy(self);
        self.allocator = args.allocator;
        self.io = args.io;
        self.metrics = args.metrics;
        self.sources = args.sources;
        self.listener = try args.listen_addr.listen(args.io, .{ .reuse_address = true });
        errdefer self.listener.deinit(args.io);
        self.stop = std.atomic.Value(bool).init(false);
        self.thread = try std.Thread.spawn(.{ .stack_size = types.THREAD_STACK_SIZE }, serveLoop, .{self});
        return self;
    }

    pub fn deinit(self: *MetricsServer) void {
        self.stop.store(true, .release);
        self.thread.join();
        self.listener.deinit(self.io);
        self.allocator.destroy(self);
    }

    /// The bound address (for tests that listen on port 0).
    /// 0.16's net.Server drops listen_address, so query the socket directly.
    pub fn listenAddress(self: *const MetricsServer) std.Io.net.IpAddress {
        var sa: std.posix.sockaddr.in = undefined;
        var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
        _ = std.posix.system.getsockname(self.listener.socket.handle, @ptrCast(&sa), &len);
        return .{ .ip4 = .{
            .bytes = @bitCast(sa.addr),
            .port = std.mem.bigToNative(u16, sa.port),
        } };
    }
};

fn serveLoop(srv: *MetricsServer) void {
    const fd = srv.listener.socket.handle;
    while (!srv.stop.load(.acquire)) {
        var pfd = std.posix.pollfd{ .fd = fd, .events = std.posix.POLL.IN, .revents = 0 };
        const n = std.posix.poll(@as(*[1]std.posix.pollfd, &pfd)[0..1], 100) catch return;
        if (n == 0) continue;
        if (pfd.revents & std.posix.POLL.IN == 0) continue;
        const stream = srv.listener.accept(srv.io) catch |err| {
            log.warn("accept: {s}", .{@errorName(err)});
            continue;
        };
        serveConnection(srv, stream);
    }
}

fn serveConnection(srv: *MetricsServer, stream: std.Io.net.Stream) void {
    defer stream.close(srv.io);
    handleRequest(srv, stream) catch |err| {
        log.warn("scrape failed: {s}", .{@errorName(err)});
    };
}

fn handleRequest(srv: *MetricsServer, stream: std.Io.net.Stream) !void {
    var read_buf: [2048]u8 = undefined;
    var net_rdr = stream.reader(srv.io, &read_buf);
    const rdr = &net_rdr.interface;

    const req_raw = (try rdr.takeDelimiter('\n')) orelse return error.BadRequest;
    const req = std.mem.trimEnd(u8, req_raw, "\r");
    var parts = std.mem.splitScalar(u8, req, ' ');
    const method = parts.next() orelse "";
    const path = parts.next() orelse "";
    const found = std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/metrics");

    // Drain the remaining headers so the peer's request is fully consumed
    // before the response closes the connection — closing with unread input
    // can turn into a RST that discards the reply.
    while (true) {
        const maybe = rdr.takeDelimiter('\n') catch break;
        const h = maybe orelse break;
        if (std.mem.trimEnd(u8, h, "\r").len == 0) break;
    }

    if (!found) {
        var wbuf: [128]u8 = undefined;
        var sw = stream.writer(srv.io, &wbuf);
        try sw.interface.writeAll("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        try sw.interface.flush();
        return;
    }

    var body_buf: [RENDER_BUF_BYTES]u8 = undefined;
    var bw = std.Io.Writer.fixed(&body_buf);
    try metrics_mod.renderPrometheus(srv.metrics, srv.sources, &bw);
    const body = bw.buffered();

    var wbuf: [512]u8 = undefined;
    var sw = stream.writer(srv.io, &wbuf);
    try sw.interface.print(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{body.len},
    );
    try sw.interface.writeAll(body);
    try sw.interface.flush();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Send one request and return the full raw response (status line + headers +
/// body). The server closes the connection, so read-to-EOF terminates.
fn rawRequest(address: std.Io.net.IpAddress, request: []const u8, out: []u8) ![]const u8 {
    const io = testing.io;
    var stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var wbuf: [256]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    try sw.interface.writeAll(request);
    try sw.interface.flush();
    var rbuf: [4096]u8 = undefined;
    var net_rdr = stream.reader(io, &rbuf);
    const n = try net_rdr.interface.readSliceShort(out);
    return out[0..n];
}

test "GET /metrics serves the exposition; other requests get 404" {
    var m = metrics_mod.Metrics{};
    _ = m.updates_received_total.fetchAdd(5, .monotonic);

    // 127.0.0.2 matches the convention used by server.zig/dispatcher.zig/io_pool.zig
    // tests in this tree: some sandboxes used to run this suite block outbound
    // connect() to 127.0.0.1 specifically (loopback secondary address is unaffected).
    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.2", 0);
    const srv = try MetricsServer.init(.{
        .listen_addr = addr,
        .metrics = &m,
        .sources = .{ .release = 9, .branch = "test" },
        .io = testing.io,
        .allocator = testing.allocator,
    });
    defer srv.deinit();
    const bound = srv.listenAddress();

    var buf: [65536]u8 = undefined;

    const ok = try rawRequest(bound, "GET /metrics HTTP/1.1\r\nHost: t\r\nAccept: */*\r\nConnection: close\r\n\r\n", &buf);
    try testing.expect(std.mem.startsWith(u8, ok, "HTTP/1.1 200 OK"));
    try testing.expect(std.mem.indexOf(u8, ok, "text/plain; version=0.0.4") != null);
    try testing.expect(std.mem.indexOf(u8, ok, "zora_updates_received_total 5\n") != null);
    try testing.expect(std.mem.indexOf(u8, ok, "zora_build_info{release=\"9\",branch=\"test\"} 1\n") != null);

    const wrong_path = try rawRequest(bound, "GET /other HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n", &buf);
    try testing.expect(std.mem.startsWith(u8, wrong_path, "HTTP/1.1 404 Not Found"));

    const wrong_method = try rawRequest(bound, "POST /metrics HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", &buf);
    try testing.expect(std.mem.startsWith(u8, wrong_method, "HTTP/1.1 404 Not Found"));
}
