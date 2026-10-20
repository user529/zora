//! test_util.zig — helpers shared by the unit tests. The bot does not use it.

const std = @import("std");

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
