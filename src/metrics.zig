// metrics.zig — single shared counter set for all subsystems.
// main.zig owns one instance on its stack and passes *Metrics everywhere.
// Null *Metrics means "don't count" (unit tests that skip metrics pass null).
// Exposure: renderPrometheus (served by metrics_server.zig) is the
// authoritative surface. The METRICS_LOG snapshot line (main.zig) is
// deprecated and frozen at the pre-Prometheus counter set.
const std = @import("std");
const queue_mod = @import("queue.zig");
const types = @import("types.zig");

// Label enums for the workflow counters. Each labeled counter is a fixed
// array of atomics indexed by @intFromEnum — one slot per label value, no
// allocation, no locking.
pub const RejectReason = enum { forbidden, oversize, malformed };
pub const ProcessOutcome = enum { ok, lua_error };
pub const CallOutcome = enum { ok, failed };
pub const ReloadOutcome = enum { ok, failed };

pub const Metrics = struct {
    // IoPool
    io_jobs_total: std.atomic.Value(u64) = .init(0),
    io_errors_total: std.atomic.Value(u64) = .init(0),
    io_timeouts_total: std.atomic.Value(u64) = .init(0),
    io_jobs_inflight: std.atomic.Value(i64) = .init(0), // gauge

    // Worker (all workers share the same instance)
    coroutines_inflight: std.atomic.Value(i64) = .init(0), // gauge
    coroutines_reaped_total: std.atomic.Value(u64) = .init(0),

    // Dispatcher
    tracked_send_failures_total: std.atomic.Value(u64) = .init(0),
    response_oversize_total: std.atomic.Value(u64) = .init(0), // replies over the response ceiling, dropped
    dispatch_timeouts_total: std.atomic.Value(u64) = .init(0), // sends abandoned at the poll-gate deadline

    // Server routing (webhook → worker-queue placement). Both counters record a
    // relaxation of the hash(user_id)%N affinity: overflow places a user's update
    // on a non-primary worker, drop sheds it when every queue is full. The
    // affinity is best-effort, not a hard single-writer invariant.
    route_overflow_total: std.atomic.Value(u64) = .init(0), // placed on a non-primary worker (primary queue full)
    route_drop_total: std.atomic.Value(u64) = .init(0), // dropped — all worker queues full

    // Throttle / reactive rate limiting
    throttle_429_total: std.atomic.Value(u64) = .init(0), // 429s observed
    throttle_delayed_total: std.atomic.Value(u64) = .init(0), // calls parked in delay_q
    throttle_shed_total: std.atomic.Value(u64) = .init(0), // delay_q overflow drops
    throttle_delay_depth: std.atomic.Value(i64) = .init(0), // gauge: delay_q size

    // Normal workflow (webhook → worker → dispatcher pipeline)
    updates_received_total: std.atomic.Value(u64) = .init(0), // webhook accepted + enqueued (200)
    updates_rejected: [3]std.atomic.Value(u64) = .{ .init(0), .init(0), .init(0) }, // by RejectReason
    updates_processed: [2]std.atomic.Value(u64) = .{ .init(0), .init(0) }, // by ProcessOutcome
    api_calls: [2]std.atomic.Value(u64) = .{ .init(0), .init(0) }, // by CallOutcome
    api_call_retries_total: std.atomic.Value(u64) = .init(0),
    rules_reloads: [2]std.atomic.Value(u64) = .{ .init(0), .init(0) }, // by ReloadOutcome
    scheduler_jobs_fired_total: std.atomic.Value(u64) = .init(0),

    pub fn incReject(m: *Metrics, reason: RejectReason) void {
        _ = m.updates_rejected[@intFromEnum(reason)].fetchAdd(1, .monotonic);
    }

    pub fn incProcessed(m: *Metrics, outcome: ProcessOutcome) void {
        _ = m.updates_processed[@intFromEnum(outcome)].fetchAdd(1, .monotonic);
    }

    pub fn incApiCall(m: *Metrics, outcome: CallOutcome) void {
        _ = m.api_calls[@intFromEnum(outcome)].fetchAdd(1, .monotonic);
    }

    pub fn incReload(m: *Metrics, outcome: ReloadOutcome) void {
        _ = m.rules_reloads[@intFromEnum(outcome)].fetchAdd(1, .monotonic);
    }
};

/// Live inputs sampled at render time, plus build identity. Queue depths are
/// read here — one mutex acquisition per queue per scrape — so the hot path
/// never maintains them.
pub const RenderSources = struct {
    worker_queues: []const *queue_mod.Queue(types.WorkItem) = &.{},
    dispatcher_queue: ?*queue_mod.Queue(types.ApiCall) = null,
    release: u32,
    branch: []const u8,
};

fn counterLine(w: *std.Io.Writer, comptime name: []const u8, comptime help: []const u8, value: u64) std.Io.Writer.Error!void {
    try w.print("# HELP " ++ name ++ " " ++ help ++ "\n# TYPE " ++ name ++ " counter\n" ++ name ++ " {d}\n", .{value});
}

fn gaugeLine(w: *std.Io.Writer, comptime name: []const u8, comptime help: []const u8, value: i64) std.Io.Writer.Error!void {
    try w.print("# HELP " ++ name ++ " " ++ help ++ "\n# TYPE " ++ name ++ " gauge\n" ++ name ++ " {d}\n", .{value});
}

/// One line per label value: `name{label="tag"} count`, indexed by @intFromEnum
/// like the counter arrays themselves.
fn labeledCounterLines(
    w: *std.Io.Writer,
    comptime name: []const u8,
    comptime help: []const u8,
    comptime label: []const u8,
    comptime E: type,
    counters: []const std.atomic.Value(u64),
) std.Io.Writer.Error!void {
    try w.writeAll("# HELP " ++ name ++ " " ++ help ++ "\n# TYPE " ++ name ++ " counter\n");
    for (std.enums.values(E), 0..) |tag, i| {
        try w.print(name ++ "{{" ++ label ++ "=\"{s}\"}} {d}\n", .{ @tagName(tag), counters[i].load(.monotonic) });
    }
}

/// Write the whole metric set in Prometheus text exposition format (v0.0.4).
/// Counter loads use .monotonic, matching fmtMetrics.
pub fn renderPrometheus(m: *const Metrics, src: RenderSources, w: *std.Io.Writer) std.Io.Writer.Error!void {
    // io_pool
    try counterLine(w, "zora_io_jobs_total", "I/O jobs executed by the io_pool.", m.io_jobs_total.load(.monotonic));
    try counterLine(w, "zora_io_errors_total", "I/O jobs that ended in an error.", m.io_errors_total.load(.monotonic));
    try counterLine(w, "zora_io_timeouts_total", "I/O jobs killed at IO_JOB_TIMEOUT_MS.", m.io_timeouts_total.load(.monotonic));
    try gaugeLine(w, "zora_io_jobs_inflight", "I/O jobs currently executing.", m.io_jobs_inflight.load(.monotonic));

    // worker
    try gaugeLine(w, "zora_coroutines_inflight", "Lua coroutines parked on I/O across all workers.", m.coroutines_inflight.load(.monotonic));
    try counterLine(w, "zora_coroutines_reaped_total", "Coroutines dropped at WORKFLOW_DEADLINE_MS.", m.coroutines_reaped_total.load(.monotonic));

    // dispatcher
    try counterLine(w, "zora_tracked_send_failures_total", "Tracked sends that failed or lacked a message_id.", m.tracked_send_failures_total.load(.monotonic));
    try counterLine(w, "zora_response_oversize_total", "API replies dropped for exceeding the response ceiling.", m.response_oversize_total.load(.monotonic));
    try counterLine(w, "zora_dispatch_timeouts_total", "Dispatches abandoned at the send poll-gate deadline.", m.dispatch_timeouts_total.load(.monotonic));

    // routing
    try counterLine(w, "zora_route_overflow_total", "Updates placed on a non-primary worker (primary queue full).", m.route_overflow_total.load(.monotonic));
    try counterLine(w, "zora_route_drop_total", "Updates dropped with every worker queue full.", m.route_drop_total.load(.monotonic));

    // throttle
    try counterLine(w, "zora_throttle_429_total", "HTTP 429 responses observed from the Telegram API.", m.throttle_429_total.load(.monotonic));
    try counterLine(w, "zora_throttle_delayed_total", "Calls parked in the delay queue by rate limiting.", m.throttle_delayed_total.load(.monotonic));
    try counterLine(w, "zora_throttle_shed_total", "Calls dropped on delay-queue overflow.", m.throttle_shed_total.load(.monotonic));
    try gaugeLine(w, "zora_throttle_delay_depth", "Calls currently parked in the delay queue.", m.throttle_delay_depth.load(.monotonic));

    // workflow
    try counterLine(w, "zora_updates_received_total", "Webhook updates accepted and enqueued.", m.updates_received_total.load(.monotonic));

    try labeledCounterLines(w, "zora_updates_rejected_total", "Webhook requests rejected before enqueue.", "reason", RejectReason, &m.updates_rejected);
    try labeledCounterLines(w, "zora_updates_processed_total", "Handler runs completed, by final outcome.", "outcome", ProcessOutcome, &m.updates_processed);
    try labeledCounterLines(w, "zora_api_calls_total", "Outbound Telegram API calls, by final outcome.", "outcome", CallOutcome, &m.api_calls);

    try counterLine(w, "zora_api_call_retries_total", "Retry attempts after a failed API send.", m.api_call_retries_total.load(.monotonic));

    try labeledCounterLines(w, "zora_rules_reloads_total", "Hot reloads of the rules file, per worker execution.", "outcome", ReloadOutcome, &m.rules_reloads);

    try counterLine(w, "zora_scheduler_jobs_fired_total", "Scheduled jobs claimed and dispatched to workers.", m.scheduler_jobs_fired_total.load(.monotonic));

    // queue depths — sampled now, not maintained
    try w.writeAll("# HELP zora_worker_queue_depth Updates waiting in each worker queue.\n# TYPE zora_worker_queue_depth gauge\n");
    for (src.worker_queues, 0..) |q, i| {
        try w.print("zora_worker_queue_depth{{worker=\"{d}\"}} {d}\n", .{ i, q.len() });
    }
    if (src.dispatcher_queue) |q| {
        try gaugeLine(w, "zora_dispatcher_queue_depth", "API calls waiting in the dispatcher queue.", @intCast(q.len()));
    }

    // build identity
    try w.print("# HELP zora_build_info Build identity (value is constant 1).\n# TYPE zora_build_info gauge\nzora_build_info{{release=\"{d}\",branch=\"{s}\"}} 1\n", .{ src.release, src.branch });
}

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

test "workflow counters default to zero and increment through the helpers" {
    var m = Metrics{};
    try testing.expectEqual(@as(u64, 0), m.updates_received_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), m.scheduler_jobs_fired_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), m.api_call_retries_total.load(.monotonic));

    m.incReject(.forbidden);
    m.incReject(.forbidden);
    m.incReject(.malformed);
    m.incProcessed(.ok);
    m.incProcessed(.lua_error);
    m.incApiCall(.failed);
    m.incReload(.ok);

    try testing.expectEqual(@as(u64, 2), m.updates_rejected[@intFromEnum(RejectReason.forbidden)].load(.monotonic));
    try testing.expectEqual(@as(u64, 0), m.updates_rejected[@intFromEnum(RejectReason.oversize)].load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.updates_rejected[@intFromEnum(RejectReason.malformed)].load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.updates_processed[@intFromEnum(ProcessOutcome.ok)].load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.updates_processed[@intFromEnum(ProcessOutcome.lua_error)].load(.monotonic));
    try testing.expectEqual(@as(u64, 0), m.api_calls[@intFromEnum(CallOutcome.ok)].load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.api_calls[@intFromEnum(CallOutcome.failed)].load(.monotonic));
    try testing.expectEqual(@as(u64, 1), m.rules_reloads[@intFromEnum(ReloadOutcome.ok)].load(.monotonic));
}

test "renderPrometheus emits every metric with HELP/TYPE and current values" {
    var m = Metrics{};
    _ = m.io_jobs_total.fetchAdd(3, .monotonic);
    _ = m.updates_received_total.fetchAdd(7, .monotonic);
    m.incReject(.malformed);
    m.incProcessed(.lua_error);
    m.incApiCall(.ok);
    _ = m.throttle_delay_depth.fetchAdd(2, .monotonic);

    // One worker queue holding one item → depth 1 in the output.
    var wq = try queue_mod.Queue(types.WorkItem).init(testing.allocator, testing.io, 4);
    defer {
        while (wq.popTimeout(0)) |_| {}
        wq.deinit(testing.allocator);
    }
    try wq.push(.{ .body = "", .user_id = null });
    var wq_ptrs = [_]*queue_mod.Queue(types.WorkItem){&wq};

    var buf: [32768]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try renderPrometheus(&m, .{
        .worker_queues = &wq_ptrs,
        .dispatcher_queue = null,
        .release = 42,
        .branch = "testbranch",
    }, &w);
    const out = w.buffered();

    // Every exposition name appears, each with a TYPE line.
    const names = [_][]const u8{
        "zora_io_jobs_total",               "zora_io_errors_total",
        "zora_io_timeouts_total",           "zora_io_jobs_inflight",
        "zora_coroutines_inflight",         "zora_coroutines_reaped_total",
        "zora_tracked_send_failures_total", "zora_response_oversize_total",
        "zora_dispatch_timeouts_total",     "zora_route_overflow_total",
        "zora_route_drop_total",
        "zora_throttle_429_total",          "zora_throttle_delayed_total",
        "zora_throttle_shed_total",         "zora_throttle_delay_depth",
        "zora_updates_received_total",      "zora_updates_rejected_total",
        "zora_updates_processed_total",     "zora_api_calls_total",
        "zora_api_call_retries_total",      "zora_rules_reloads_total",
        "zora_scheduler_jobs_fired_total",  "zora_worker_queue_depth",
        "zora_build_info",
    };
    for (names) |n| {
        try testing.expect(std.mem.indexOf(u8, out, n) != null);
        var type_buf: [96]u8 = undefined;
        const type_line = try std.fmt.bufPrint(&type_buf, "# TYPE {s} ", .{n});
        try testing.expect(std.mem.indexOf(u8, out, type_line) != null);
    }

    // Values and labels round-trip.
    try testing.expect(std.mem.indexOf(u8, out, "zora_io_jobs_total 3\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "zora_updates_received_total 7\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "zora_updates_rejected_total{reason=\"malformed\"} 1\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "zora_updates_rejected_total{reason=\"forbidden\"} 0\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "zora_updates_processed_total{outcome=\"lua_error\"} 1\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "zora_api_calls_total{outcome=\"ok\"} 1\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "zora_throttle_delay_depth 2\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "zora_worker_queue_depth{worker=\"0\"} 1\n") != null);
    try testing.expect(std.mem.indexOf(u8, out, "zora_build_info{release=\"42\",branch=\"testbranch\"} 1\n") != null);
    // dispatcher_queue == null → its gauge is omitted entirely.
    try testing.expect(std.mem.indexOf(u8, out, "zora_dispatcher_queue_depth") == null);
}
