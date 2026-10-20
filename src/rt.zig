//! rt.zig — process I/O runtime and small time helpers.
//!
//! Zig 0.16 routes blocking primitives (mutex, condition, sleep, futex) through
//! an `Io` instance instead of free functions. This module holds the one runtime
//! the process uses and hands out its `Io`.
//!
//! The runtime is `std.Io.Threaded.init(std.heap.c_allocator, .{})`. zora
//! schedules work on its own `std.Thread` pool, so the runtime's async executor
//! is never used — only the mutex, futex, sleep, net and process paths, which
//! map straight to OS calls and are safe to call from any thread. One shared
//! instance is therefore correct for the whole process.
//!
//! Why not the allocation-free `init_single_threaded`: its allocator is
//! `Allocator.failing`, and `std.process.spawn` builds the child's argv and
//! environment blocks with the runtime's allocator before the fork, so every
//! `bot.exec`/`bot.shell` would fail with OutOfMemory. `init` also installs no-op
//! handlers for SIGIO and SIGPIPE; zora does not cancel I/O, so the SIGIO
//! interruption it enables goes unused.
//!
//! The runtime is built on the first io() call, because `init` runs at run
//! time (it reads the CPU count and installs the signal handlers) and tests
//! reach io() without `main`. It lives for the whole process and is never
//! deinitialized.
//!
//! `main` calls io() once and passes the result explicitly to the components
//! that need it; tests call io() to obtain the same runtime. Callers should
//! treat the returned `Io` as a value to thread through, not reach back here.

const std = @import("std");

/// Backing runtime, valid once `state` reads `.ready`. Address-stable for the
/// life of the process.
var runtime: std.Io.Threaded = undefined;
var state: std.atomic.Value(State) = .init(.unset);

const State = enum(u8) { unset, building, ready };

/// Returns the process `Io`, building the runtime on the first call. Cheap to
/// copy; the backing runtime outlives it. Safe to call from any thread.
pub fn io() std.Io {
    if (state.load(.acquire) != .ready) build();
    return runtime.io();
}

/// Build the runtime exactly once. A thread that loses the race spins until
/// the winner publishes it; this happens at most once per process, during
/// startup.
fn build() void {
    if (state.cmpxchgStrong(.unset, .building, .acquire, .acquire) == null) {
        runtime = .init(std.heap.c_allocator, .{});
        state.store(.ready, .release);
        return;
    }
    while (state.load(.acquire) != .ready) std.atomic.spinLoopHint();
}

/// Sleeps for `ns` nanoseconds on the monotonic clock. A best-effort backoff:
/// cancelation is not used in this codebase, so the (impossible) cancel error is
/// dropped. Centralizes the `Duration`/`Clock` choice for the many sleep sites.
pub fn sleepNs(target_io: std.Io, ns: u64) void {
    target_io.sleep(.fromNanoseconds(@intCast(ns)), .awake) catch {};
}

/// Wall-clock time in nanoseconds since the Unix epoch. Replaces the removed
/// `std.time.nanoTimestamp`; uses the real (settable) clock so values match the
/// previous behavior.
pub fn nowNs(target_io: std.Io) i128 {
    return std.Io.Timestamp.now(target_io, .real).nanoseconds;
}

/// Wall-clock time in milliseconds since the Unix epoch. Replaces the removed
/// `std.time.milliTimestamp`.
pub fn nowMs(target_io: std.Io) i64 {
    return @intCast(@divTrunc(nowNs(target_io), std.time.ns_per_ms));
}

test "nowMs and nowNs advance across a sleep" {
    const t = io();
    // Both helpers read the same clock; a real sleep between two reads must move
    // the returned value forward, guarding against a stuck or zeroed timestamp.
    const ns_before = nowNs(t);
    const ms_before = nowMs(t);
    sleepNs(t, 2 * std.time.ns_per_ms);
    try std.testing.expect(nowNs(t) > ns_before);
    try std.testing.expect(nowMs(t) >= ms_before);
}
