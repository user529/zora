//! delay.zig — reactive rate-limit holding area.
//!
//! DelayQueue: a bounded min-heap of ApiCalls keyed by (ready_at_ms, seq).
//! Callers push a call with a future ready_at_ms; a consumer drains due
//! entries via popReady / waitNext.
//!
//! BlockedMap: per-chat "do not send before" timestamps.
//! requeueThread: releases due calls from DelayQueue back into disp_q.
//!
//! Imports std + project leaf modules only; never imports dispatcher.zig.
const std = @import("std");
const types = @import("types.zig");
const queue_mod = @import("queue.zig");
const metrics_mod = @import("metrics.zig");
const rt = @import("rt.zig");

const log = std.log.scoped(.delay);

pub const DelayedCall = struct {
    ready_at_ms: i64,
    seq:         u64,
    call:        types.ApiCall,
};

fn earlier(_: void, a: DelayedCall, b: DelayedCall) std.math.Order {
    if (a.ready_at_ms != b.ready_at_ms)
        return std.math.order(a.ready_at_ms, b.ready_at_ms);
    return std.math.order(a.seq, b.seq);
}

pub const DelayQueue = struct {
    const PQ = std.PriorityQueue(DelayedCall, void, earlier);

    pq:        PQ,
    allocator: std.mem.Allocator, // 0.16 PriorityQueue is unmanaged
    mutex:     std.Io.Mutex = .init,
    // Bumped on every push; waiters block on it with a timeout. Replaces the
    // 0.16-removed Condition.timedWait — same futex primitive, with a deadline.
    wake:      std.atomic.Value(u32) = .init(0),
    io:        std.Io,
    capacity:  usize,
    seq:       u64 = 0,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, capacity: usize) DelayQueue {
        return .{ .pq = .empty, .allocator = allocator, .io = io, .capacity = capacity };
    }

    /// Free any held ApiCalls, then release the heap. Call on shutdown only.
    pub fn deinitDrain(self: *DelayQueue, allocator: std.mem.Allocator) void {
        while (self.pq.pop()) |dc| types.freeApiCall(dc.call, allocator);
        self.pq.deinit(allocator);
    }

    /// Park `call` until `ready_at_ms`. error.Full when at capacity (caller
    /// then logs + frees). Assigns a monotonic seq for stable FIFO.
    pub fn push(self: *DelayQueue, ready_at_ms: i64, call: types.ApiCall) error{Full}!void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.pq.count() >= self.capacity) return error.Full;
        self.pq.push(self.allocator, .{ .ready_at_ms = ready_at_ms, .seq = self.seq, .call = call }) catch
            return error.Full; // OOM ⇒ treat as full
        self.seq += 1;
        // Wake the consumer; bump first so an about-to-wait consumer skips it.
        _ = self.wake.fetchAdd(1, .release);
        self.io.futexWake(u32, &self.wake.raw, 1);
    }

    /// Pop the earliest call if it is due at `now_ms`, else null.
    pub fn popReady(self: *DelayQueue, now_ms: i64) ?types.ApiCall {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const top = self.pq.peek() orelse return null;
        if (top.ready_at_ms > now_ms) return null;
        return self.pq.pop().?.call;
    }

    /// Block until the next call is due, a new earlier call is pushed, or
    /// `max_ns` elapses (so the caller can re-check a stop flag).
    pub fn waitNext(self: *DelayQueue, now_ms: i64, max_ns: u64) void {
        self.mutex.lockUncancelable(self.io);
        var wait_ns: u64 = max_ns;
        if (self.pq.peek()) |top| {
            if (top.ready_at_ms <= now_ms) {
                self.mutex.unlock(self.io);
                return; // due now — don't sleep
            }
            // Guarded above (top is strictly in the future); both are epoch-ms, so the
            // difference is positive and the cast is safe.
            const diff_ms: u64 = @intCast(top.ready_at_ms - now_ms);
            wait_ns = @min(max_ns, diff_ms *| std.time.ns_per_ms);
        }
        // Snapshot the wake counter under the lock, release, then wait for a
        // push (counter change) or the deadline — whichever comes first.
        const s = self.wake.load(.acquire);
        self.mutex.unlock(self.io);
        self.io.futexWaitTimeout(u32, &self.wake.raw, s, .{
            .duration = .{ .raw = .fromNanoseconds(@intCast(wait_ns)), .clock = .awake },
        }) catch {};
    }
};

/// chat_id → unblock-at ms. Lookups prune expired entries to bound memory.
pub const BlockedMap = struct {
    map:   std.AutoHashMap(i64, i64),
    mutex: std.Io.Mutex = .init,
    io:    std.Io,

    pub fn init(allocator: std.mem.Allocator, io: std.Io) BlockedMap {
        return .{ .map = std.AutoHashMap(i64, i64).init(allocator), .io = io };
    }

    pub fn deinit(self: *BlockedMap) void {
        self.map.deinit();
    }

    /// Mark `chat_id` blocked until `until_ms`. Best-effort: an OOM on insert
    /// leaves the chat unblocked rather than failing the caller.
    pub fn block(self: *BlockedMap, chat_id: i64, until_ms: i64) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.map.put(chat_id, until_ms) catch {};
    }

    /// Return the unblock time if `chat_id` is still blocked at `now_ms`, else
    /// null — removing the entry when its window has passed.
    pub fn blockedUntil(self: *BlockedMap, chat_id: i64, now_ms: i64) ?i64 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const until = self.map.get(chat_id) orelse return null;
        if (until > now_ms) return until;
        _ = self.map.remove(chat_id);
        return null;
    }
};

pub const RequeueArgs = struct {
    delay_q:   *DelayQueue,
    disp_q:    *queue_mod.Queue(types.ApiCall),
    stop:      *std.atomic.Value(bool),
    metrics:   ?*metrics_mod.Metrics,
    allocator: std.mem.Allocator,
};

/// Single thread: release due calls from delay_q back into disp_q. Sleeps on
/// the heap timer (woken early by pushes); never busy-waits. The 50ms cap keeps
/// `stop` responsive.
pub fn requeueThread(args: RequeueArgs) void {
    while (!args.stop.load(.acquire)) {
        const now = rt.nowMs(args.delay_q.io);
        while (args.delay_q.popReady(now)) |call| {
            args.disp_q.push(call) catch {
                // disp_q full — drop (rare). Count as shed.
                if (args.metrics) |m| _ = m.throttle_shed_total.fetchAdd(1, .monotonic);
                log.warn("requeue: disp_q full, dropped a delayed call", .{});
                types.freeApiCall(call, args.allocator);
            };
            if (args.metrics) |m| _ = m.throttle_delay_depth.fetchSub(1, .monotonic);
        }
        args.delay_q.waitNext(rt.nowMs(args.delay_q.io), 50 * std.time.ns_per_ms);
    }
    log.info("requeue thread stopped", .{});
}

const testing = std.testing;

fn dummyCall(allocator: std.mem.Allocator, chat_id: i64) !types.ApiCall {
    return .{
        .method  = try allocator.dupe(u8, "sendMessage"),
        .payload = .{ .json = try allocator.dupe(u8, "{}") },
        .route   = .{ .chat_id = chat_id },
    };
}

test "DelayQueue popReady orders by ready_at then seq and respects ready time" {
    var dq = DelayQueue.init(testing.allocator, rt.io(), 16);
    defer dq.deinitDrain(testing.allocator);

    // Same ready_at for chat 1 then chat 2 (push order), earlier ready_at for chat 3.
    try dq.push(100, try dummyCall(testing.allocator, 1));
    try dq.push(100, try dummyCall(testing.allocator, 2));
    try dq.push(50,  try dummyCall(testing.allocator, 3));

    // Nothing is due before the earliest ready_at.
    try testing.expectEqual(@as(?types.ApiCall, null), dq.popReady(49));

    // A call becomes due exactly at its ready_at: chat 3 (ready 50) pops first.
    const a = dq.popReady(50).?; defer types.freeApiCall(a, testing.allocator);
    try testing.expectEqual(@as(i64, 3), a.route.?.chat_id);

    // now=200 ⇒ the rest are due in seq order: chat 1 then chat 2.
    const b = dq.popReady(200).?; defer types.freeApiCall(b, testing.allocator);
    const c = dq.popReady(200).?; defer types.freeApiCall(c, testing.allocator);
    try testing.expectEqual(@as(i64, 1), b.route.?.chat_id);
    try testing.expectEqual(@as(i64, 2), c.route.?.chat_id);
    try testing.expectEqual(@as(?types.ApiCall, null), dq.popReady(200));
}

test "DelayQueue push past capacity returns error.Full" {
    var dq = DelayQueue.init(testing.allocator, rt.io(), 2);
    defer dq.deinitDrain(testing.allocator);
    try dq.push(10, try dummyCall(testing.allocator, 1));
    try dq.push(10, try dummyCall(testing.allocator, 2));
    const overflow = try dummyCall(testing.allocator, 3);
    try testing.expectError(error.Full, dq.push(10, overflow));
    types.freeApiCall(overflow, testing.allocator); // caller frees the rejected call
}

test "DelayQueue deinitDrain frees held calls (no leak)" {
    var dq = DelayQueue.init(testing.allocator, rt.io(), 16);
    try dq.push(10, try dummyCall(testing.allocator, 1));
    try dq.push(20, try dummyCall(testing.allocator, 2));
    dq.deinitDrain(testing.allocator); // must free both — leak check via testing allocator
}

test "BlockedMap returns window while blocked, prunes when expired" {
    var bm = BlockedMap.init(testing.allocator, rt.io());
    defer bm.deinit();

    bm.block(42, 1000);
    try testing.expectEqual(@as(?i64, 1000), bm.blockedUntil(42, 999));  // still blocked
    try testing.expectEqual(@as(?i64, null), bm.blockedUntil(42, 1000)); // expired ⇒ pruned
    try testing.expectEqual(@as(?i64, null), bm.blockedUntil(42, 1001)); // gone
    try testing.expectEqual(@as(?i64, null), bm.blockedUntil(7, 0));     // never blocked
}

test "requeueThread moves a due call to disp_q after its delay" {
    var disp = try queue_mod.Queue(types.ApiCall).init(testing.allocator, rt.io(), 16);
    defer disp.deinit(testing.allocator);
    var dq = DelayQueue.init(testing.allocator, rt.io(), 16);
    defer dq.deinitDrain(testing.allocator);
    var stop = std.atomic.Value(bool).init(false);

    const t = try std.Thread.spawn(.{}, requeueThread, .{RequeueArgs{
        .delay_q = &dq, .disp_q = &disp, .stop = &stop,
        .metrics = null, .allocator = testing.allocator,
    }});

    // Push a call due 100 ms out; record the push instant to bound the gap.
    const pushed_at = rt.nowMs(rt.io());
    try dq.push(pushed_at + 100, try dummyCall(testing.allocator, 1));

    // Not delivered before its ready time.
    try testing.expectEqual(@as(?types.ApiCall, null), disp.popTimeout(20 * std.time.ns_per_ms));

    // Delivered shortly after its ready time.
    const got = disp.popTimeout(2 * std.time.ns_per_s);
    try testing.expect(got != null);
    const delivered_at = rt.nowMs(rt.io());
    types.freeApiCall(got.?, testing.allocator);

    // The delay actually elapsed: a zero-delay requeue could not pass this.
    // 80 ms is a structural lower bound, comfortably below the 100 ms nominal
    // delay so it stays reliable under load.
    try testing.expect(delivered_at - pushed_at >= 80);

    stop.store(true, .release);
    t.join();
}

test "requeueThread sheds a due call when disp_q is full" {
    // disp_q capacity 1, pre-filled so the next push returns error.QueueFull.
    var disp = try queue_mod.Queue(types.ApiCall).init(testing.allocator, rt.io(), 1);
    defer disp.deinit(testing.allocator);
    const filler = try dummyCall(testing.allocator, 99);
    try disp.push(filler);

    var dq = DelayQueue.init(testing.allocator, rt.io(), 16);
    defer dq.deinitDrain(testing.allocator);
    var metrics = metrics_mod.Metrics{};
    var stop = std.atomic.Value(bool).init(false);

    // A due item: requeueThread pops it, fails to push (disp_q full), sheds it.
    try dq.push(rt.nowMs(rt.io()) - 1, try dummyCall(testing.allocator, 1));

    const t = try std.Thread.spawn(.{}, requeueThread, .{RequeueArgs{
        .delay_q = &dq, .disp_q = &disp, .stop = &stop,
        .metrics = &metrics, .allocator = testing.allocator,
    }});

    // Poll the shed counter with a bounded timeout instead of a fixed sleep.
    const deadline = rt.nowMs(rt.io()) + 2000;
    while (metrics.throttle_shed_total.load(.monotonic) == 0 and rt.nowMs(rt.io()) < deadline) {
        rt.sleepNs(rt.io(), std.time.ns_per_ms);
    }

    stop.store(true, .release);
    t.join();

    // Exactly one shed; the full disp_q was not force-enqueued.
    try testing.expectEqual(@as(u64, 1), metrics.throttle_shed_total.load(.monotonic));
    try testing.expectEqual(@as(usize, 1), disp.len());

    // Drain the pre-filled item so the testing allocator sees no leak.
    const left = disp.popTimeout(0).?;
    types.freeApiCall(left, testing.allocator);
}

test "DelayQueue waitNext is woken early by a push" {
    var dq = DelayQueue.init(testing.allocator, rt.io(), 16);
    defer dq.deinitDrain(testing.allocator);

    const Blocker = struct {
        // Block in waitNext with a 2 s ceiling and record the elapsed time.
        fn run(q: *DelayQueue, elapsed_ms: *i64, started: *std.atomic.Value(bool)) void {
            const before = rt.nowMs(q.io);
            started.store(true, .release);
            // No entry yet ⇒ waitNext parks on the wake futex for up to 2 s.
            q.waitNext(rt.nowMs(q.io), 2 * std.time.ns_per_s);
            elapsed_ms.* = rt.nowMs(q.io) - before;
        }
    };

    var elapsed_ms: i64 = 0;
    var started = std.atomic.Value(bool).init(false);
    const t = try std.Thread.spawn(.{}, Blocker.run, .{ &dq, &elapsed_ms, &started });

    // Ensure the waiter is parked, then push a due item to wake it early.
    while (!started.load(.acquire)) {}
    rt.sleepNs(rt.io(), 20 * std.time.ns_per_ms);
    try dq.push(rt.nowMs(rt.io()), try dummyCall(testing.allocator, 1));

    t.join();

    // The push woke waitNext well before the 2 s ceiling.
    try testing.expect(elapsed_ms < 500);
}
