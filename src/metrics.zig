// metrics.zig — single shared counter set for all subsystems.
// main.zig owns one instance on its stack and passes *Metrics everywhere.
// Null *Metrics means "don't count" (unit tests that skip metrics pass null).
const std = @import("std");

pub const Metrics = struct {
    // IoPool
    io_jobs_total:               std.atomic.Value(u64) = .init(0),
    io_errors_total:             std.atomic.Value(u64) = .init(0),
    io_timeouts_total:           std.atomic.Value(u64) = .init(0),
    io_jobs_inflight:            std.atomic.Value(i64) = .init(0), // gauge

    // Worker (all workers share the same instance)
    coroutines_inflight:         std.atomic.Value(i64) = .init(0), // gauge
    coroutines_reaped_total:     std.atomic.Value(u64) = .init(0),

    // Dispatcher
    tracked_send_failures_total: std.atomic.Value(u64) = .init(0),
};
