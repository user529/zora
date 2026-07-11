//! scheduler.zig — durable timer thread for future Lua-handler jobs.
//!
//! The persistent cousin of delay.zig's requeueThread: each tick it claims due
//! rows from the SQLite `schedule` table under a lease, round-robins them into
//! the worker queues as WorkItem{ kind = .schedule }, and re-arms from
//! SELECT MIN(fire_at_ms). Owns its own StateStore connection (WAL allows the
//! concurrent worker writers that create/cancel jobs).
//!
//! Delivery is at-least-once: a claimed row is deleted by the worker only on
//! terminal success, inside the handler's own transaction. A crash leaves the
//! lease to expire, and the row is re-claimed and re-fired.
const std = @import("std");
const types = @import("types.zig");
const queue_mod = @import("queue.zig");
const state_store = @import("state_store.zig");
const rt = @import("rt.zig");
const metrics_mod = @import("metrics.zig");

const log = std.log.scoped(.scheduler);

/// Lease window: a row claimed longer ago than this is presumed abandoned by a
/// dead worker and re-claimed. MUST exceed workflow_deadline_ms so a
/// legitimately-slow async job is never double-fired (the longest a single
/// workflow can run is workflow_deadline_ms). 10 min comfortably exceeds the
/// 60 s default.
pub const DEFAULT_LEASE_MS: i64 = 10 * 60 * 1000;
/// Rows claimed per tick — caps a post-downtime catch-up burst.
pub const DEFAULT_MAX_BATCH: u32 = 256;
/// Upper bound on a single wait, so `stop` stays responsive, stale leases are
/// reclaimed promptly, and a job scheduled inside a worker transaction (whose
/// row becomes visible only after that worker commits, after the wake fired) is
/// still picked up within this bound.
pub const DEFAULT_WAIT_CAP_NS: u64 = 1 * std.time.ns_per_s;

pub const Scheduler = struct {
    /// Bumped on every wakeUp; the thread waits on it with a timeout.
    wake: std.atomic.Value(u32) = .init(0),
    io: std.Io,
    lease_ms: i64 = DEFAULT_LEASE_MS,
    max_batch: u32 = DEFAULT_MAX_BATCH,
    wait_cap_ns: u64 = DEFAULT_WAIT_CAP_NS,

    /// Wake the timer thread (called by bot.schedule_* after inserting a job).
    pub fn wakeUp(self: *Scheduler) void {
        _ = self.wake.fetchAdd(1, .release);
        self.io.futexWake(u32, &self.wake.raw, 1);
    }
};

pub const SchedulerArgs = struct {
    db: *state_store.StateStore,
    worker_qs: []*queue_mod.Queue(types.WorkItem),
    sched: *Scheduler,
    stop: *std.atomic.Value(bool),
    io: std.Io,
    allocator: std.mem.Allocator,
    /// Optional metrics sink. Null means "don't count" (tests default to null).
    metrics: ?*metrics_mod.Metrics = null,
};

pub fn schedulerThread(args: SchedulerArgs) void {
    var rr: usize = 0;
    while (!args.stop.load(.acquire)) {
        const now = rt.nowMs(args.io);
        const cutoff = now - args.sched.lease_ms;

        const jobs = args.db.scheduleClaimDue(now, cutoff, args.sched.max_batch, args.allocator) catch |err| {
            log.err("claim failed: {s}", .{@errorName(err)});
            sleepCap(args, now);
            continue;
        };
        if (jobs.len > 0) {
            if (args.metrics) |m| _ = m.scheduler_jobs_fired_total.fetchAdd(jobs.len, .monotonic);
            log.info("fired {d} job(s)", .{jobs.len});
        }

        for (jobs) |job| {
            const q = args.worker_qs[rr % args.worker_qs.len];
            rr += 1;
            q.push(.{
                .body = job.payload, // ownership moves to the worker
                .user_id = null,
                .kind = .schedule,
                .schedule_id = job.id,
            }) catch {
                // Queue full: free the body; the row stays leased and is
                // reclaimed after lease_ms (fires late, not lost).
                log.warn("worker queue full; job {d} deferred to lease expiry", .{job.id});
                args.allocator.free(job.payload);
            };
        }
        args.allocator.free(jobs);

        sleepCap(args, rt.nowMs(args.io));
    }
    log.info("scheduler thread stopped", .{});
}

/// Wait until the next claimable job is due, a wakeUp arrives, or wait_cap_ns
/// elapses — whichever is first. Never busy-waits.
fn sleepCap(args: SchedulerArgs, now: i64) void {
    var wait_ns: u64 = args.sched.wait_cap_ns;
    const cutoff = now - args.sched.lease_ms;
    if (args.db.scheduleMinFire(cutoff) catch null) |next| {
        if (next <= now) return; // already due — loop again immediately
        const diff_ms: u64 = @intCast(next - now);
        wait_ns = @min(args.sched.wait_cap_ns, diff_ms *| std.time.ns_per_ms);
    }
    const s = args.sched.wake.load(.acquire);
    args.sched.io.futexWaitTimeout(u32, &args.sched.wake.raw, s, .{
        .duration = .{ .raw = .fromNanoseconds(@intCast(wait_ns)), .clock = .awake },
    }) catch {};
}

const testing = std.testing;

test "schedulerThread claims a due job and enqueues it round-robin" {
    var store = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer store.close();

    var q = try queue_mod.Queue(types.WorkItem).init(testing.allocator, rt.io(), 16);
    defer {
        while (q.popTimeout(0)) |item| testing.allocator.free(item.body);
        q.deinit(testing.allocator);
    }
    var qs = [_]*queue_mod.Queue(types.WorkItem){&q};

    // A job already due (fire_at_ms in the past).
    _ = try store.scheduleInsert(rt.nowMs(rt.io()) - 1000, "{\"hi\":1}");

    var sched = Scheduler{ .io = rt.io(), .wait_cap_ns = 50 * std.time.ns_per_ms };
    var stop = std.atomic.Value(bool).init(false);
    const t = try std.Thread.spawn(.{}, schedulerThread, .{SchedulerArgs{
        .db = &store, .worker_qs = &qs, .sched = &sched,
        .stop = &stop, .io = rt.io(), .allocator = testing.allocator,
    }});

    const item = q.popTimeout(2 * std.time.ns_per_s);
    try testing.expect(item != null);
    defer testing.allocator.free(item.?.body);
    try testing.expectEqual(types.WorkKind.schedule, item.?.kind);
    try testing.expect(item.?.schedule_id != null);
    try testing.expect(std.mem.indexOf(u8, item.?.body, "\"hi\":1") != null);

    stop.store(true, .release);
    sched.wakeUp();
    t.join();
}

test "schedulerThread does not fire a not-yet-due job" {
    var store = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer store.close();
    var q = try queue_mod.Queue(types.WorkItem).init(testing.allocator, rt.io(), 16);
    defer {
        while (q.popTimeout(0)) |item| testing.allocator.free(item.body);
        q.deinit(testing.allocator);
    }
    var qs = [_]*queue_mod.Queue(types.WorkItem){&q};

    _ = try store.scheduleInsert(rt.nowMs(rt.io()) + 60_000, "{}"); // due in 60s

    var sched = Scheduler{ .io = rt.io(), .wait_cap_ns = 50 * std.time.ns_per_ms };
    var stop = std.atomic.Value(bool).init(false);
    const t = try std.Thread.spawn(.{}, schedulerThread, .{SchedulerArgs{
        .db = &store, .worker_qs = &qs, .sched = &sched,
        .stop = &stop, .io = rt.io(), .allocator = testing.allocator,
    }});

    try testing.expectEqual(@as(?types.WorkItem, null), q.popTimeout(150 * std.time.ns_per_ms));

    stop.store(true, .release);
    sched.wakeUp();
    t.join();
}

test "schedulerThread re-claims a job whose lease has expired (at-least-once)" {
    // The core at-least-once guarantee: a worker that claims a row but crashes
    // before deleting it leaves the lease (claimed_at_ms) to expire. The next
    // scheduler tick — which claims rows whose claimed_at_ms < now - lease_ms —
    // must re-claim and re-fire the abandoned job.
    var store = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer store.close();

    var q = try queue_mod.Queue(types.WorkItem).init(testing.allocator, rt.io(), 16);
    defer {
        while (q.popTimeout(0)) |item| testing.allocator.free(item.body);
        q.deinit(testing.allocator);
    }
    var qs = [_]*queue_mod.Queue(types.WorkItem){&q};

    const lease_ms: i64 = DEFAULT_LEASE_MS;
    const now = rt.nowMs(rt.io());

    // Insert a row due far enough in the past that an earlier (expired) claim
    // could have been stamped on it, then simulate that abandoned claim: claim
    // it with a now_ms of (now - lease_ms - 1), so its claimed_at_ms predates
    // the lease cutoff the live thread computes (now - lease_ms). This mirrors a
    // worker that claimed the row long ago and never completed.
    const stale_claim_at = now - lease_ms - 1;
    _ = try store.scheduleInsert(stale_claim_at - 1000, "{\"job\":7}");
    const stale = try store.scheduleClaimDue(stale_claim_at, 0, 16, testing.allocator);
    defer {
        for (stale) |j| testing.allocator.free(j.payload);
        testing.allocator.free(stale);
    }
    try testing.expectEqual(@as(usize, 1), stale.len); // the abandoned claim happened

    var sched = Scheduler{ .io = rt.io(), .lease_ms = lease_ms, .wait_cap_ns = 50 * std.time.ns_per_ms };
    var stop = std.atomic.Value(bool).init(false);
    const t = try std.Thread.spawn(.{}, schedulerThread, .{SchedulerArgs{
        .db = &store, .worker_qs = &qs, .sched = &sched,
        .stop = &stop, .io = rt.io(), .allocator = testing.allocator,
    }});

    // The live thread's cutoff (now - lease_ms) exceeds the stale claim stamp,
    // so the row is reclaimed and re-enqueued despite already being "claimed".
    const item = q.popTimeout(2 * std.time.ns_per_s);
    try testing.expect(item != null);
    defer testing.allocator.free(item.?.body);
    try testing.expectEqual(types.WorkKind.schedule, item.?.kind);
    try testing.expect(std.mem.indexOf(u8, item.?.body, "\"job\":7") != null);

    stop.store(true, .release);
    sched.wakeUp();
    t.join();
}

test "schedulerThread round-robins due jobs across worker queues" {
    // The timer thread claims due jobs into worker queues round-robin
    // (rr increments per job). Two due jobs across two queues must land one each,
    // not both on a single queue.
    var store = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer store.close();

    var q0 = try queue_mod.Queue(types.WorkItem).init(testing.allocator, rt.io(), 16);
    defer {
        while (q0.popTimeout(0)) |item| testing.allocator.free(item.body);
        q0.deinit(testing.allocator);
    }
    var q1 = try queue_mod.Queue(types.WorkItem).init(testing.allocator, rt.io(), 16);
    defer {
        while (q1.popTimeout(0)) |item| testing.allocator.free(item.body);
        q1.deinit(testing.allocator);
    }
    var qs = [_]*queue_mod.Queue(types.WorkItem){ &q0, &q1 };

    const due = rt.nowMs(rt.io()) - 1000;
    _ = try store.scheduleInsert(due, "{\"a\":1}");
    _ = try store.scheduleInsert(due, "{\"b\":2}");

    var m = metrics_mod.Metrics{};
    var sched = Scheduler{ .io = rt.io(), .wait_cap_ns = 50 * std.time.ns_per_ms };
    var stop = std.atomic.Value(bool).init(false);
    const t = try std.Thread.spawn(.{}, schedulerThread, .{SchedulerArgs{
        .db = &store, .worker_qs = &qs, .sched = &sched,
        .stop = &stop, .io = rt.io(), .allocator = testing.allocator,
        .metrics = &m,
    }});

    // Both jobs are due at once; round-robin places the first on q0 and the
    // second on q1 (rr % 2). Each queue receives exactly one item.
    const item0 = q0.popTimeout(2 * std.time.ns_per_s);
    const item1 = q1.popTimeout(2 * std.time.ns_per_s);
    try testing.expect(item0 != null);
    try testing.expect(item1 != null);
    defer testing.allocator.free(item0.?.body);
    defer testing.allocator.free(item1.?.body);

    stop.store(true, .release);
    sched.wakeUp();
    t.join();

    // No third item leaked into either queue.
    try testing.expectEqual(@as(?types.WorkItem, null), q0.popTimeout(0));
    try testing.expectEqual(@as(?types.WorkItem, null), q1.popTimeout(0));

    try testing.expectEqual(@as(u64, 2), m.scheduler_jobs_fired_total.load(.monotonic));
}

test "schedulerThread caps a single tick at max_batch" {
    // DEFAULT_MAX_BATCH caps a post-downtime catch-up burst. With more due rows
    // than max_batch, no single tick may claim more than max_batch. A worker
    // queue sized to max_batch fills on the first tick and stays full: a tick
    // that claimed more would overflow it (and free the surplus), so the queue
    // can never hold more than max_batch — and every row remains in the DB,
    // since the scheduler never deletes a claimed row (only the worker does, on
    // terminal success).
    var store = try state_store.StateStore.open(testing.allocator, ":memory:");
    defer store.close();

    const max_batch: u32 = 3;
    const total: u32 = max_batch + 5;

    var q = try queue_mod.Queue(types.WorkItem).init(testing.allocator, rt.io(), max_batch);
    defer {
        while (q.popTimeout(0)) |item| testing.allocator.free(item.body);
        q.deinit(testing.allocator);
    }
    var qs = [_]*queue_mod.Queue(types.WorkItem){&q};

    const due = rt.nowMs(rt.io()) - 1000;
    var i: u32 = 0;
    while (i < total) : (i += 1) _ = try store.scheduleInsert(due, "{}");

    var sched = Scheduler{ .io = rt.io(), .max_batch = max_batch, .wait_cap_ns = 50 * std.time.ns_per_ms };
    var stop = std.atomic.Value(bool).init(false);
    const t = try std.Thread.spawn(.{}, schedulerThread, .{SchedulerArgs{
        .db = &store, .worker_qs = &qs, .sched = &sched,
        .stop = &stop, .io = rt.io(), .allocator = testing.allocator,
    }});

    // The queue is never drained while the thread runs, so it fills on the first
    // tick and stays full. Poll up to ~2 s for it to reach max_batch; it must
    // never exceed it — proof a single tick claimed no more than max_batch and
    // later ticks could not push surplus into the full queue.
    var spins: u32 = 0;
    while (q.len() < max_batch and spins < 200) : (spins += 1) {
        rt.sleepNs(rt.io(), 10 * std.time.ns_per_ms);
    }
    try testing.expectEqual(@as(usize, max_batch), q.len());

    stop.store(true, .release);
    sched.wakeUp();
    t.join();

    // The queue still holds exactly max_batch — no tick ever overfilled it.
    try testing.expectEqual(@as(usize, max_batch), q.len());

    // Every row is still in the DB — the scheduler never deletes a claimed row.
    // A far-future reclaim cutoff makes every still-present row claimable again.
    const future_cutoff = due + DEFAULT_LEASE_MS + 1_000_000;
    const remaining = try store.scheduleClaimDue(due + 1, future_cutoff, total + 10, testing.allocator);
    defer {
        for (remaining) |j| testing.allocator.free(j.payload);
        testing.allocator.free(remaining);
    }
    try testing.expectEqual(@as(usize, total), remaining.len);
}
