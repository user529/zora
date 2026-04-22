/// queue.zig — bounded, blocking MPSC ring-buffer queue.
///
/// Queue(T) is a fixed-capacity ring buffer:
///   push(item) — non-blocking; returns error.QueueFull when at capacity.
///   pop()      — blocking; waits on a Condition until an item is available.
///
/// Thread safety: all operations are protected by a Mutex.
/// Multiple producers and a single consumer is the intended use pattern,
/// but the implementation is safe for any number of consumers too.

const std = @import("std");

pub fn Queue(comptime T: type) type {
    return struct {
        const Self = @This();

        buf: []T,
        head: usize, // next read index
        tail: usize, // next write index
        count: usize,
        mutex: std.Thread.Mutex,
        not_empty: std.Thread.Condition,

        // ------------------------------------------------------------------
        // Lifecycle
        // ------------------------------------------------------------------

        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            std.debug.assert(capacity > 0);
            const buf = try allocator.alloc(T, capacity);
            return Self{
                .buf = buf,
                .head = 0,
                .tail = 0,
                .count = 0,
                .mutex = .{},
                .not_empty = .{},
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
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.count == self.buf.len) return error.QueueFull;

            self.buf[self.tail] = item;
            self.tail = (self.tail + 1) % self.buf.len;
            self.count += 1;
            self.not_empty.signal();
        }

        /// Pop an item. Blocks until one is available.
        pub fn pop(self: *Self) T {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.count == 0) {
                self.not_empty.wait(&self.mutex);
            }

            const item = self.buf[self.head];
            self.head = (self.head + 1) % self.buf.len;
            self.count -= 1;
            return item;
        }

        /// Non-blocking pop. Returns null if the queue is empty.
        pub fn tryPop(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.count == 0) return null;

            const item = self.buf[self.head];
            self.head = (self.head + 1) % self.buf.len;
            self.count -= 1;
            return item;
        }

        /// Current number of items in the queue (snapshot, may be stale).
        pub fn len(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.count;
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "AC-4.1: FIFO order preserved for 1000 items" {
    var q = try Queue(u32).init(testing.allocator, 1024);
    defer q.deinit(testing.allocator);

    const N = 1000;
    for (0..N) |i| try q.push(@intCast(i));

    for (0..N) |i| {
        const v = q.pop();
        try testing.expectEqual(@as(u32, @intCast(i)), v);
    }
}

test "AC-4.2: push at capacity returns error.QueueFull without modifying queue" {
    var q = try Queue(u32).init(testing.allocator, 4);
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

// Context passed to the delayed-push thread for AC-4.3.
const DelayedPushCtx = struct {
    q: *Queue(u32),
    delay_ms: u64,
    value: u32,
};

fn delayedPushThread(ctx: DelayedPushCtx) void {
    std.Thread.sleep(ctx.delay_ms * std.time.ns_per_ms);
    ctx.q.push(ctx.value) catch {};
}

test "AC-4.3: pop blocks on empty queue and unblocks within 10ms of push" {
    var q = try Queue(u32).init(testing.allocator, 8);
    defer q.deinit(testing.allocator);

    const delay_ms: u64 = 50;
    const ctx = DelayedPushCtx{ .q = &q, .delay_ms = delay_ms, .value = 42 };

    const t = try std.Thread.spawn(.{}, delayedPushThread, .{ctx});

    const t0 = std.time.milliTimestamp();
    const v = q.pop();
    const elapsed_ms = std.time.milliTimestamp() - t0;

    t.join();

    try testing.expectEqual(@as(u32, 42), v);
    // Must have blocked at least ~delay_ms and unblocked within 500ms total.
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

test "AC-4.4: 4 producers x 1000 items — all 4000 received exactly once" {
    const PRODUCERS = 4;
    const PER_PRODUCER = 1000;
    const TOTAL = PRODUCERS * PER_PRODUCER;

    var q = try Queue(u32).init(testing.allocator, TOTAL);
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

test "AC-4.5: 4 producers x 1000 items, capacity 64 — no deadlock, completes" {
    const PRODUCERS = 4;
    const PER_PRODUCER = 1000;
    const TOTAL = PRODUCERS * PER_PRODUCER;

    // Tiny capacity forces producers to retry repeatedly.
    var q = try Queue(u32).init(testing.allocator, 64);
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

test "AC-4.6: Queue is generic — instantiate with u32 and u8" {
    // u32 queue
    var q32 = try Queue(u32).init(testing.allocator, 4);
    defer q32.deinit(testing.allocator);
    try q32.push(0xDEAD_BEEF);
    try testing.expectEqual(@as(u32, 0xDEAD_BEEF), q32.pop());

    // u8 queue
    var q8 = try Queue(u8).init(testing.allocator, 4);
    defer q8.deinit(testing.allocator);
    try q8.push(0xFF);
    try testing.expectEqual(@as(u8, 0xFF), q8.pop());
}

test "AC-4.6: Queue([]const u8) handles slice items" {
    var qs = try Queue([]const u8).init(testing.allocator, 4);
    defer qs.deinit(testing.allocator);
    try qs.push("hello");
    try qs.push("world");
    try testing.expectEqualStrings("hello", qs.pop());
    try testing.expectEqualStrings("world", qs.pop());
}

test "AC-4.6: tryPop returns null on empty queue" {
    var q = try Queue(u32).init(testing.allocator, 4);
    defer q.deinit(testing.allocator);
    try testing.expectEqual(@as(?u32, null), q.tryPop());
    try q.push(7);
    try testing.expectEqual(@as(?u32, 7), q.tryPop());
    try testing.expectEqual(@as(?u32, null), q.tryPop());
}
