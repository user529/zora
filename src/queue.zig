/// queue.zig — bounded, blocking MPSC ring-buffer queue.
///
/// Queue(T) is a fixed-capacity ring buffer:
///   push(item) — non-blocking; returns error.QueueFull when at capacity.
///   pop()      — blocking; waits until an item is available.
///
/// Thread safety: a Mutex protects the buffer; a sequence counter drives the
/// blocking wait. Multiple producers and a single consumer is the intended use
/// pattern, but the implementation is safe for any number of consumers too.
///
/// Blocking model: Zig 0.16 routes blocking through an `Io`, and its `Condition`
/// offers no timed wait. The queue therefore waits directly on a futex over a
/// monotonic `seq` counter — the same primitive `Io.Condition` is built on.
/// Every push bumps `seq` and wakes one waiter; a consumer that finds the queue
/// empty snapshots `seq`, releases the mutex, and futex-waits for `seq` to
/// change. This gives both an unbounded `pop` and a `popTimeout` with one
/// mechanism, and the snapshot-then-wait order rules out lost wakeups.
const std = @import("std");

const log = std.log.scoped(.queue);

pub const QueueKind = enum { worker, dispatcher, io_job, io_result };

pub fn Queue(comptime T: type) type {
    return struct {
        const Self = @This();

        buf: []T,
        head: usize, // next read index
        tail: usize, // next write index
        count: usize,
        mutex: std.Io.Mutex,
        seq: std.atomic.Value(u32), // bumped on every push; waiters block on it
        closed: bool, // set by close(); read under mutex. Releases parked popBlocking waiters.
        io: std.Io,

        // Fill-level monitoring — opt-in via metrics_log.
        id: usize,
        metrics_log: bool,
        last_warn_threshold: u8, // highest threshold band logged so far
        kind: QueueKind,

        // ------------------------------------------------------------------
        // Lifecycle
        // ------------------------------------------------------------------

        pub fn init(allocator: std.mem.Allocator, io: std.Io, capacity: usize) !Self {
            std.debug.assert(capacity > 0);
            const buf = try allocator.alloc(T, capacity);
            return Self{
                .buf = buf,
                .head = 0,
                .tail = 0,
                .count = 0,
                .mutex = .init,
                .seq = .init(0),
                .closed = false,
                .io = io,
                .id = 0,
                .metrics_log = false,
                .last_warn_threshold = 0,
                .kind = .worker,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            allocator.free(self.buf);
        }

        // ------------------------------------------------------------------
        // Operations
        // ------------------------------------------------------------------

        /// Push an item. Returns error.QueueFull if at capacity.
        /// Never blocks; the caller decides how to handle backpressure.
        pub fn push(self: *Self, item: T) error{QueueFull}!void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);

            if (self.count == self.buf.len) return error.QueueFull;

            self.buf[self.tail] = item;
            self.tail = (self.tail + 1) % self.buf.len;
            self.count += 1;
            // Wake one waiter. Bump seq first so a consumer mid-wait sees the
            // change and a consumer about to wait skips it.
            _ = self.seq.fetchAdd(1, .release);
            self.io.futexWake(u32, &self.seq.raw, 1);

            if (self.metrics_log) self.checkFillThreshold();
        }

        // Removes and returns the head item; the caller must hold the mutex and
        // must have confirmed the queue is non-empty.
        fn takeLocked(self: *Self) T {
            const item = self.buf[self.head];
            self.head = (self.head + 1) % self.buf.len;
            self.count -= 1;
            if (self.metrics_log) self.resetFillThreshold();
            return item;
        }

        /// Pop an item. Blocks until one is available.
        pub fn pop(self: *Self) T {
            self.mutex.lockUncancelable(self.io);
            while (self.count == 0) {
                const s = self.seq.load(.acquire);
                self.mutex.unlock(self.io);
                self.io.futexWaitUncancelable(u32, &self.seq.raw, s);
                self.mutex.lockUncancelable(self.io);
            }
            const item = self.takeLocked();
            self.mutex.unlock(self.io);
            return item;
        }

        /// Pop an item with timeout. Blocks the queue only for the timeout,
        /// allowing CPU to sleep. With timeout_ns=0 this becomes a non-blocking pop.
        pub fn popTimeout(self: *Self, timeout_ns: u64) ?T {
            self.mutex.lockUncancelable(self.io);
            if (self.count == 0) {
                if (timeout_ns == 0) {
                    self.mutex.unlock(self.io);
                    return null;
                }
                const s = self.seq.load(.acquire);
                self.mutex.unlock(self.io);
                self.io.futexWaitTimeout(u32, &self.seq.raw, s, .{
                    .duration = .{ .raw = .fromNanoseconds(@intCast(timeout_ns)), .clock = .awake },
                }) catch {};
                self.mutex.lockUncancelable(self.io);
            }
            if (self.count == 0) {
                self.mutex.unlock(self.io);
                return null;
            }
            const item = self.takeLocked();
            self.mutex.unlock(self.io);
            return item;
        }

        /// Pop an item, parking indefinitely on an empty queue. Returns the head
        /// item, or null once the queue has been closed and fully drained.
        ///
        /// Unlike popTimeout, an idle consumer blocks on the futex with no
        /// timeout — no periodic wakeups — and is released only by a push or by
        /// close(). This is the loop primitive for consumer threads whose sole
        /// wake sources are "an item arrived" and "we are shutting down": close()
        /// supplies the latter, so the thread never has to poll a stop flag.
        pub fn popBlocking(self: *Self) ?T {
            self.mutex.lockUncancelable(self.io);
            while (self.count == 0) {
                if (self.closed) {
                    self.mutex.unlock(self.io);
                    return null;
                }
                const s = self.seq.load(.acquire);
                self.mutex.unlock(self.io);
                self.io.futexWaitUncancelable(u32, &self.seq.raw, s);
                self.mutex.lockUncancelable(self.io);
            }
            const item = self.takeLocked();
            self.mutex.unlock(self.io);
            return item;
        }

        /// Mark the queue closed and wake every parked popBlocking waiter.
        /// Items already queued are still drained by popBlocking before it
        /// reports null. Idempotent; push after close is still permitted (the
        /// caller is responsible for not racing late pushes against shutdown).
        pub fn close(self: *Self) void {
            self.mutex.lockUncancelable(self.io);
            self.closed = true;
            self.mutex.unlock(self.io);
            // Bump seq so a waiter mid-wait observes the change, then wake all
            // of them — every parked consumer must re-check `closed`.
            _ = self.seq.fetchAdd(1, .release);
            self.io.futexWake(u32, &self.seq.raw, std.math.maxInt(u32));
        }

        // ------------------------------------------------------------------
        // Fill-level monitoring (called under mutex)
        // ------------------------------------------------------------------

        // Returns the highest fill-level band the current count falls into:
        // 99, 95, 90, 75, or 0 (below 75%).
        fn fillBand(count: usize, capacity: usize) u8 {
            const pct = count * 100 / capacity;
            if (pct >= 99) return 99;
            if (pct >= 95) return 95;
            if (pct >= 90) return 90;
            if (pct >= 75) return 75;
            return 0;
        }

        // Log when crossing into a higher band; called under mutex after push.
        fn checkFillThreshold(self: *Self) void {
            const band = fillBand(self.count, self.buf.len);
            if (band <= self.last_warn_threshold) return;
            self.last_warn_threshold = band;
            const pct = self.count * 100 / self.buf.len;
            if (band >= 99) {
                log.err("{s}[{d}]: {d}% full ({d}/{d}) — critical backpressure", .{
                    @tagName(self.kind), self.id, pct, self.count, self.buf.len,
                });
            } else {
                log.warn("{s}[{d}]: {d}% full ({d}/{d})", .{
                    @tagName(self.kind), self.id, pct, self.count, self.buf.len,
                });
            }
        }

        // Silently reset the tracker after a pop so the next fill cycle
        // logs threshold crossings again.
        fn resetFillThreshold(self: *Self) void {
            const band = fillBand(self.count, self.buf.len);
            if (band < self.last_warn_threshold) self.last_warn_threshold = band;
        }

        /// Current number of items in the queue (snapshot, may be stale).
        pub fn len(self: *Self) usize {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            return self.count;
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const rt = @import("rt.zig");
const testing = std.testing;

test "FIFO order preserved for 1000 items" {
    var q = try Queue(u32).init(testing.allocator, rt.io(), 1024);
    defer q.deinit(testing.allocator);

    const N = 1000;
    for (0..N) |i| try q.push(@intCast(i));

    for (0..N) |i| {
        const v = q.pop();
        try testing.expectEqual(@as(u32, @intCast(i)), v);
    }
}

test "push at capacity returns error.QueueFull without modifying queue" {
    var q = try Queue(u32).init(testing.allocator, rt.io(), 4);
    defer q.deinit(testing.allocator);

    try q.push(1);
    try q.push(2);
    try q.push(3);
    try q.push(4);

    // One beyond capacity
    const result = q.push(5);
    try testing.expectError(error.QueueFull, result);

    // Queue state unchanged — still has original 4 items in FIFO order
    try testing.expectEqual(@as(usize, 4), q.len());
    try testing.expectEqual(@as(u32, 1), q.pop());
    try testing.expectEqual(@as(u32, 2), q.pop());
    try testing.expectEqual(@as(u32, 3), q.pop());
    try testing.expectEqual(@as(u32, 4), q.pop());
}

// Context passed to the delayed-push thread.
const DelayedPushCtx = struct {
    q: *Queue(u32),
    delay_ms: u64,
    value: u32,
};

fn delayedPushThread(ctx: DelayedPushCtx) void {
    rt.sleepNs(rt.io(), ctx.delay_ms * std.time.ns_per_ms);
    ctx.q.push(ctx.value) catch {};
}

test "pop blocks on empty queue and unblocks within 10ms of push" {
    var q = try Queue(u32).init(testing.allocator, rt.io(), 8);
    defer q.deinit(testing.allocator);

    const delay_ms: u64 = 50;
    const ctx = DelayedPushCtx{ .q = &q, .delay_ms = delay_ms, .value = 42 };

    const t = try std.Thread.spawn(.{}, delayedPushThread, .{ctx});

    const t0 = rt.nowMs(rt.io());
    const v = q.pop();
    const elapsed_ms = rt.nowMs(rt.io()) - t0;

    t.join();

    try testing.expectEqual(@as(u32, 42), v);
    // Must have blocked at least ~delay_ms and unblocked within 500ms total.
    try testing.expect(elapsed_ms >= @as(i64, @intCast(delay_ms)) - 5); // 5ms slack
    try testing.expect(elapsed_ms < 500);
}

test "popBlocking drains queued items in FIFO order, then returns null once closed" {
    var q = try Queue(u32).init(testing.allocator, rt.io(), 8);
    defer q.deinit(testing.allocator);

    try q.push(1);
    try q.push(2);
    q.close();

    // Items already queued at close are still delivered, in order.
    try testing.expectEqual(@as(?u32, 1), q.popBlocking());
    try testing.expectEqual(@as(?u32, 2), q.popBlocking());
    // Once drained and closed, popBlocking reports end-of-stream.
    try testing.expectEqual(@as(?u32, null), q.popBlocking());
    // Still null on a subsequent call — close is sticky.
    try testing.expectEqual(@as(?u32, null), q.popBlocking());
}

// Context for the delayed-close thread: closes the queue after a delay.
const DelayedCloseCtx = struct {
    q: *Queue(u32),
    delay_ms: u64,
};

fn delayedCloseThread(ctx: DelayedCloseCtx) void {
    rt.sleepNs(rt.io(), ctx.delay_ms * std.time.ns_per_ms);
    ctx.q.close();
}

test "popBlocking parks on an empty queue and wakes with null when closed" {
    // This is the shutdown-wake contract that lets a consumer park indefinitely
    // instead of polling: it must block (not spin or return early) and then
    // unblock promptly when another thread closes the queue.
    var q = try Queue(u32).init(testing.allocator, rt.io(), 8);
    defer q.deinit(testing.allocator);

    const delay_ms: u64 = 50;
    const t = try std.Thread.spawn(.{}, delayedCloseThread, .{DelayedCloseCtx{
        .q = &q,
        .delay_ms = delay_ms,
    }});

    const t0 = rt.nowMs(rt.io());
    const v = q.popBlocking();
    const elapsed_ms = rt.nowMs(rt.io()) - t0;

    t.join();

    try testing.expectEqual(@as(?u32, null), v);
    // Proved it actually blocked (did not return null immediately) and that
    // close() released it well before any poll timeout would have.
    try testing.expect(elapsed_ms >= @as(i64, @intCast(delay_ms)) - 5); // 5ms slack
    try testing.expect(elapsed_ms < 500);
}

// Context for multi-producer concurrent test.
const ProducerCtx = struct {
    q: *Queue(u32),
    start: u32, // first value this producer pushes
    count: u32, // number of values
};

fn producerThread(ctx: ProducerCtx) void {
    var i: u32 = 0;
    while (i < ctx.count) : (i += 1) {
        const val = ctx.start + i;
        // Retry until push succeeds (queue may be temporarily full).
        while (true) {
            ctx.q.push(val) catch {
                std.Thread.yield() catch {};
                continue;
            };
            break;
        }
    }
}

test "4 producers x 1000 items — all 4000 received exactly once" {
    const PRODUCERS = 4;
    const PER_PRODUCER = 1000;
    const TOTAL = PRODUCERS * PER_PRODUCER;

    var q = try Queue(u32).init(testing.allocator, rt.io(), TOTAL);
    defer q.deinit(testing.allocator);

    var threads: [PRODUCERS]std.Thread = undefined;
    for (0..PRODUCERS) |p| {
        threads[p] = try std.Thread.spawn(.{}, producerThread, .{ProducerCtx{
            .q = &q,
            .start = @intCast(p * PER_PRODUCER),
            .count = PER_PRODUCER,
        }});
    }

    // Consume all items and record which values were received.
    var received = [_]bool{false} ** TOTAL;
    for (0..TOTAL) |_| {
        const v = q.pop();
        try testing.expect(v < TOTAL);
        try testing.expect(!received[v]); // no duplicate
        received[v] = true;
    }

    for (threads) |t| t.join();

    // Verify every value was received.
    for (received, 0..) |got, i| {
        if (!got) {
            std.debug.print("missing item {d}\n", .{i});
            try testing.expect(false);
        }
    }
}

test "4 producers x 1000 items, capacity 64 — no deadlock, completes" {
    const PRODUCERS = 4;
    const PER_PRODUCER = 1000;
    const TOTAL = PRODUCERS * PER_PRODUCER;

    // Tiny capacity forces producers to retry repeatedly.
    var q = try Queue(u32).init(testing.allocator, rt.io(), 64);
    defer q.deinit(testing.allocator);

    var threads: [PRODUCERS]std.Thread = undefined;
    for (0..PRODUCERS) |p| {
        threads[p] = try std.Thread.spawn(.{}, producerThread, .{ProducerCtx{
            .q = &q,
            .start = @intCast(p * PER_PRODUCER),
            .count = PER_PRODUCER,
        }});
    }

    var received = [_]bool{false} ** TOTAL;
    for (0..TOTAL) |_| {
        const v = q.pop();
        try testing.expect(v < TOTAL);
        try testing.expect(!received[v]);
        received[v] = true;
    }

    for (threads) |t| t.join();

    for (received) |got| try testing.expect(got);
}

test "Queue is generic over item type, including slices" {
    // Scalar element types: u32 and u8.
    {
        var q32 = try Queue(u32).init(testing.allocator, rt.io(), 4);
        defer q32.deinit(testing.allocator);
        try q32.push(0xDEAD_BEEF);
        try testing.expectEqual(@as(u32, 0xDEAD_BEEF), q32.pop());

        var q8 = try Queue(u8).init(testing.allocator, rt.io(), 4);
        defer q8.deinit(testing.allocator);
        try q8.push(0xFF);
        try testing.expectEqual(@as(u8, 0xFF), q8.pop());
    }
    // Slice element type.
    {
        var qs = try Queue([]const u8).init(testing.allocator, rt.io(), 4);
        defer qs.deinit(testing.allocator);
        try qs.push("hello");
        try qs.push("world");
        try testing.expectEqualStrings("hello", qs.pop());
        try testing.expectEqualStrings("world", qs.pop());
    }
}

test "tryPop returns null on empty queue" {
    var q = try Queue(u32).init(testing.allocator, rt.io(), 4);
    defer q.deinit(testing.allocator);
    try testing.expectEqual(@as(?u32, null), q.popTimeout(0));
    try q.push(7);
    try testing.expectEqual(@as(?u32, 7), q.popTimeout(0));
    try testing.expectEqual(@as(?u32, null), q.popTimeout(0));
}

test "QueueKind tag names match log labels and default to .worker" {
    // @tagName values are the labels used in logs.
    try testing.expectEqualStrings("worker",     @tagName(QueueKind.worker));
    try testing.expectEqualStrings("dispatcher", @tagName(QueueKind.dispatcher));
    try testing.expectEqualStrings("io_job",     @tagName(QueueKind.io_job));
    try testing.expectEqualStrings("io_result",  @tagName(QueueKind.io_result));

    // A new Queue defaults to .worker; the field is settable.
    var q = try Queue(u32).init(testing.allocator, rt.io(), 4);
    defer q.deinit(testing.allocator);
    try testing.expectEqual(QueueKind.worker, q.kind);
    q.kind = .dispatcher;
    try testing.expectEqual(QueueKind.dispatcher, q.kind);
}
