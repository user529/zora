//! latch.zig — one-shot stop signal that poll() can wait on.
//!
//! A `Latch` is a pipe. set() writes one byte into it, and nothing ever reads
//! the byte back, so the read end stays readable from then on. Any number of
//! threads can wait for the latch, alone or together with another fd in one
//! poll() call with no timeout, and set() wakes all of them.
//!
//! Why: a thread that waits in poll() for a socket cannot wait on a futex at
//! the same time. With a stop flag such a thread must poll with a short timeout
//! and recheck the flag, and every timeout is a wakeup with nothing to do. With
//! the latch the idle thread sleeps in the kernel until there is work or a stop
//! request.
//!
//! set() calls only write(2), which is async-signal-safe, so a signal handler
//! may call it.
//!
//! Caveats: a latch cannot be reset. Each latch holds two fds.

const std = @import("std");
const posix = std.posix;

pub const Latch = struct {
    /// `fds[0]` is the read end that waiters poll; `fds[1]` is the write end
    /// set() uses.
    fds: [2]posix.fd_t,

    /// Open the pipe. Both ends are close-on-exec, so children started by
    /// `bot.exec`/`bot.shell` do not inherit them. Both ends are non-blocking, so
    /// set() never blocks, however often it runs.
    pub fn init() error{ ProcessFdQuotaExceeded, SystemFdQuotaExceeded, Unexpected }!Latch {
        var fds: [2]posix.fd_t = undefined;
        return switch (posix.errno(std.c.pipe2(&fds, .{ .CLOEXEC = true, .NONBLOCK = true }))) {
            .SUCCESS => .{ .fds = fds },
            .MFILE => error.ProcessFdQuotaExceeded,
            .NFILE => error.SystemFdQuotaExceeded,
            else => |e| posix.unexpectedErrno(e),
        };
    }

    /// Close both ends. No thread may wait on the latch after this.
    pub fn deinit(self: *Latch) void {
        _ = std.c.close(self.fds[0]);
        _ = std.c.close(self.fds[1]);
    }

    /// Set the latch and wake every waiter. A second set() has no effect.
    /// set() is async-signal-safe.
    pub fn set(self: *const Latch) void {
        const byte = [1]u8{1};
        // The result is ignored: a full pipe (EAGAIN) already holds a byte, so
        // the latch is set either way.
        _ = std.c.write(self.fds[1], &byte, byte.len);
    }

    /// Block until the latch is set.
    pub fn wait(self: *const Latch) posix.PollError!void {
        var pfd = [1]posix.pollfd{.{ .fd = self.fds[0], .events = posix.POLL.IN, .revents = 0 }};
        _ = try posix.poll(&pfd, -1);
    }

    /// Block until `fd` is readable or the latch is set. Returns true when `fd`
    /// is ready and the latch is clear, false once the latch is set. A set
    /// latch wins over a ready `fd`, so a stopping thread does not start new
    /// work. "Ready" includes `POLLERR` and `POLLHUP`; the caller's next read or
    /// accept reports the error.
    pub fn waitReadable(self: *const Latch, fd: posix.fd_t) posix.PollError!bool {
        var pfds = [2]posix.pollfd{
            .{ .fd = self.fds[0], .events = posix.POLL.IN, .revents = 0 },
            .{ .fd = fd, .events = posix.POLL.IN, .revents = 0 },
        };
        _ = try posix.poll(&pfds, -1);
        return pfds[0].revents == 0;
    }
};

const testing = std.testing;

test "set wakes a thread blocked in wait" {
    var latch = try Latch.init();
    defer latch.deinit();

    const Waiter = struct {
        fn run(l: *const Latch, done: *std.atomic.Value(bool)) void {
            l.wait() catch return;
            done.store(true, .release);
        }
    };
    var done = std.atomic.Value(bool).init(false);
    const t = try std.Thread.spawn(.{}, Waiter.run, .{ &latch, &done });
    latch.set();
    t.join();
    try testing.expect(done.load(.acquire));
}

test "waitReadable reports a ready fd while the latch is clear" {
    var latch = try Latch.init();
    defer latch.deinit();
    // A second, set latch serves as an always-readable fd.
    var other = try Latch.init();
    defer other.deinit();
    other.set();

    try testing.expect(try latch.waitReadable(other.fds[0]));
}

test "waitReadable returns false once the latch is set, even with a ready fd" {
    var latch = try Latch.init();
    defer latch.deinit();
    var other = try Latch.init();
    defer other.deinit();
    other.set();
    latch.set();

    try testing.expect(!try latch.waitReadable(other.fds[0]));
}

test "set never blocks, even past the pipe capacity" {
    var latch = try Latch.init();
    defer latch.deinit();
    // 128 KiB of single-byte writes overfills any default pipe buffer; a
    // blocking write end would hang here.
    for (0..128 * 1024) |_| latch.set();
    try latch.wait();
}
