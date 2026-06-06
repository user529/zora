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
    response_oversize_total:     std.atomic.Value(u64) = .init(0), // replies over the response ceiling, dropped

    // Throttle / reactive rate limiting
    throttle_429_total:     std.atomic.Value(u64) = .init(0), // 429s observed
    throttle_delayed_total: std.atomic.Value(u64) = .init(0), // calls parked in delay_q
    throttle_shed_total:    std.atomic.Value(u64) = .init(0), // delay_q overflow drops
    throttle_delay_depth:   std.atomic.Value(i64) = .init(0), // gauge: delay_q size
};

const testing = std.testing;

test "throttle counters default to zero and increment" {
    var m = Metrics{};
    try testing.expectEqual(@as(u64, 0), m.throttle_429_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), m.throttle_delayed_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), m.throttle_shed_total.load(.monotonic));
    _ = m.throttle_429_total.fetchAdd(1, .monotonic);
    _ = m.throttle_delayed_total.fetchAdd(2, .monotonic);
    _ = m.throttle_shed_total.fetchAdd(3, .monotonic);
    try testing.expectEqual(@as(u64, 1), m.throttle_429_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 2), m.throttle_delayed_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 3), m.throttle_shed_total.load(.monotonic));
}
