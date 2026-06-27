//! rt.zig — process I/O runtime and small time helpers.
//!
//! Zig 0.16 routes blocking primitives (mutex, condition, sleep, futex) through
//! an `Io` instance instead of free functions. This module holds the one runtime
//! the process uses and hands out its `Io`.
//!
//! The runtime is `std.Io.Threaded.init_single_threaded`: a stateless,
//! allocation-free executor. zora schedules work on its own `std.Thread` pool,
//! so the runtime's async executor is never used — only the mutex, futex, and
//! sleep paths, which map straight to OS calls and are safe to call from any
//! thread. One shared instance is therefore correct for the whole process.
//!
//! `main` calls `io()` once and passes the result explicitly to the components
//! that need it; tests call `io()` to obtain the same runtime. Callers should
//! treat the returned `Io` as a value to thread through, not reach back here.

const std = @import("std");

/// Backing runtime. Stateless and OS-backed, so a single shared instance serves
/// every thread. Address-stable for the life of the process.
var runtime: std.Io.Threaded = .init_single_threaded;

/// Returns the process `Io`. Cheap to copy; the backing runtime outlives it.
pub fn io() std.Io {
    return runtime.io();
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

test "io returns a usable runtime: sleep and futex round-trip" {
    const t = io();
    // A zero-length sleep must return promptly without error handling at the
    // call site (sleepNs swallows the unused cancel error).
    sleepNs(t, 0);

    // The mutex path must lock and unlock cleanly from the calling thread.
    var m: std.Io.Mutex = .init;
    m.lockUncancelable(t);
    m.unlock(t);
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
