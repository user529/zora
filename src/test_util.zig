//! test_util.zig — helpers shared by the unit tests. The bot does not use it.

const std = @import("std");
const rt = @import("rt.zig");

/// The address a listening socket is bound to. A port-0 listen gets an
/// ephemeral port from the OS, and `net.Server` does not keep the bound
/// address, so the helper asks the socket with getsockname(). IPv4 only: every
/// test listens on an IPv4 loopback address.
pub fn boundAddress(server: *const std.Io.net.Server) std.Io.net.IpAddress {
    var sa: std.posix.sockaddr.in = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    _ = std.posix.system.getsockname(server.socket.handle, @ptrCast(&sa), &len);
    return .{ .ip4 = .{
        .bytes = @bitCast(sa.addr),
        .port = std.mem.bigToNative(u16, sa.port),
    } };
}

/// The port of boundAddress(), in native byte order.
pub fn boundPort(server: *const std.Io.net.Server) u16 {
    return boundAddress(server).ip4.port;
}

/// The absolute path of `name` inside `tmp`, NUL-terminated in `buf`, for a
/// SQLite database the test opens by path. `buf` must hold the directory path
/// plus `name`; `std.fs.max_path_bytes + 16` is enough for the short names the
/// tests use. The result points into `buf`. Returns `error.NoSpaceLeft` when
/// `buf` is too short; errors from `realPath` pass through.
pub fn tmpDbPath(tmp: *const std.testing.TmpDir, name: []const u8, buf: []u8) ![:0]const u8 {
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_len = try tmp.dir.realPath(std.testing.io, &dir_buf);
    return std.mem.printSentinel(buf, "{s}/{s}", .{ dir_buf[0..dir_len], name }, 0);
}

/// Poll `count(ctx)` every 5 ms until it reaches `n` or `timeout_ms` passes.
/// Returns true once the count is `>= n`, false when the timeout comes first.
/// `count` reads the counter under test, for example `atomicCount` or a
/// queue's `len`. The timeout runs on the wall clock (rt.nowMs()), so a clock
/// step makes a wait longer or shorter; the signed comparison keeps a
/// backward step from failing the cast it would need as unsigned.
pub fn waitAtLeast(ctx: anytype, comptime count: fn (@TypeOf(ctx)) usize, n: usize, timeout_ms: i64) bool {
    const start = rt.nowMs(std.testing.io);
    while (count(ctx) < n) {
        if (rt.nowMs(std.testing.io) - start >= timeout_ms) return false;
        rt.sleepNs(std.testing.io, 5 * std.time.ns_per_ms);
    }
    return true;
}

/// The value of an atomic counter as a `usize`, loaded with acquire ordering.
/// It is the `count` argument of `waitAtLeast` for counters the test shares
/// with a thread.
pub fn atomicCount(c: *std.atomic.Value(u64)) usize {
    return @intCast(c.load(.acquire));
}
