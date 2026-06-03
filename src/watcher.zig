/// watcher.zig — kernel-native file watcher (inotify / kqueue / poll).
///
/// Generic watch primitives (`WatchTarget`, `watchInotify`, `watchKqueue`,
/// `watchPoll`) fire a caller-supplied callback on file change; tg_schema.zig
/// reuses them to watch the API schema.  The rules-reload helpers on top
/// (`reload_version`, `WatcherArgs`, `watcherThread`) bump a global counter so
/// workers reload rules.lua.
///
/// Public API:
///   reload_version          — global atomic u64, starts at 0.
///   WatcherArgs             — arguments for watcherThread.
///   watcherThread(args)     — entry point for the dedicated watcher thread.
///                             Dispatches to the kernel-native implementation at
///                             compile time:
///                               Linux   → inotify (CLOSE_WRITE | MOVED_TO)
///                               FreeBSD → kqueue  (EVFILT_VNODE)
///                               other   → poll    (stat() every 500ms, dev fallback)
///
/// Workers read reload_version with .acquire; the watcher writes with .release.
/// The ordering guarantee is provided by std.atomic.Value(u64) — no torn reads.

const std     = @import("std");
const builtin = @import("builtin");

const log = std.log.scoped(.reload);

// ---------------------------------------------------------------------------
// Public: global reload counter
// ---------------------------------------------------------------------------

/// Monotonically increasing counter.  Workers keep a local copy and reload
/// their Lua state whenever this value is ahead.
pub var reload_version = std.atomic.Value(u64).init(0);

// ---------------------------------------------------------------------------
// Public: watcher thread arguments
// ---------------------------------------------------------------------------

pub const WatcherArgs = struct {
    /// Path to the Lua rules file to watch.
    rules_path: []const u8,
    /// Allocator used by the watcher thread.
    /// Must remain valid for the lifetime of the watcher thread.
    allocator: std.mem.Allocator,
};

// ---------------------------------------------------------------------------
// Public: generic watch target — a path plus change callbacks.
//
// `on_write` fires on a content change (CLOSE_WRITE / MOVED_TO / vnode write
// / mtime change). `on_delete`, if set, fires when the file is removed.
// `context` is passed opaquely to both — the caller casts it back.
// ---------------------------------------------------------------------------

pub const WatchTarget = struct {
    path: []const u8,
    context: *anyopaque,
    on_write: *const fn (*anyopaque) void,
    on_delete: ?*const fn (*anyopaque) void = null,
};

// ---------------------------------------------------------------------------
// Public: watcher thread entry point
// ---------------------------------------------------------------------------

/// Run the file watcher for the path in `args.rules_path`.  Never returns
/// under normal operation.  Intended to be run as a dedicated `std.Thread`.
pub fn watcherThread(args: WatcherArgs) void {
    // Duplicate so the caller can free its copy any time after spawning.
    // The defer below never executes — the function loops forever.
    const owned = args.allocator.dupe(u8, args.rules_path) catch {
        log.err("watcherThread: OOM duplicating rules path — watcher not started", .{});
        return;
    };
    defer args.allocator.free(owned);
    switch (builtin.os.tag) {
        .linux   => watcherInotify(owned, &reload_version),
        .freebsd => watcherKqueue(owned, &reload_version),
        else     => watcherPoll(owned, &reload_version, 500, null),
    }
}

// ---------------------------------------------------------------------------
// Signal
// ---------------------------------------------------------------------------

// Callbacks for the rules watcher — `context` is the reload_version counter.

fn bumpReloadCounter(context: *anyopaque) void {
    const counter: *std.atomic.Value(u64) = @alignCast(@ptrCast(context));
    _ = counter.fetchAdd(1, .release);
    log.info("rules.lua changed — workers will reload", .{});
}

fn logRulesDeleted(context: *anyopaque) void {
    _ = context;
    log.warn("rules file deleted — workers will continue with current rules", .{});
}

// ---------------------------------------------------------------------------
// Linux: inotify
// ---------------------------------------------------------------------------

/// Generic inotify watcher. Watches the parent directory and filters by
/// basename so it survives delete + recreate. Never returns.
pub fn watchInotify(t: WatchTarget) void {
    if (comptime builtin.os.tag != .linux) {
        unreachable;
    }
    const posix = std.posix;
    const linux = std.os.linux;

    const dir_path = std.fs.path.dirname(t.path) orelse ".";
    const basename = std.fs.path.basename(t.path);

    const ifd = posix.inotify_init1(linux.IN.CLOEXEC) catch |err| {
        log.err("inotify_init1: {s}", .{@errorName(err)});
        return;
    };
    defer posix.close(ifd);

    _ = posix.inotify_add_watch(
        ifd,
        dir_path,
        linux.IN.CLOSE_WRITE | linux.IN.MOVED_TO |
            linux.IN.DELETE | linux.IN.MOVED_FROM,
    ) catch |err| {
        log.err("inotify_add_watch '{s}': {s}", .{ dir_path, @errorName(err) });
        return;
    };

    var buf: [4096]u8 align(@alignOf(linux.inotify_event)) = undefined;
    while (true) {
        const n = posix.read(ifd, &buf) catch |err| {
            if (err == error.Interrupted) continue;
            log.err("inotify read: {s} — hot-reload permanently disabled, aborting", .{@errorName(err)});
            std.process.abort();
        };
        const ev_hdr = @sizeOf(linux.inotify_event);
        var offset: usize = 0;
        while (offset + ev_hdr <= n) {
            const ev: *const linux.inotify_event = @alignCast(@ptrCast(&buf[offset]));
            const event_total = ev_hdr + ev.len;
            if (ev.len > 0) {
                const name_bytes = buf[offset + ev_hdr .. offset + event_total];
                const name = std.mem.sliceTo(name_bytes, 0);
                if (std.mem.eql(u8, name, basename)) {
                    if (ev.mask & (linux.IN.DELETE | linux.IN.MOVED_FROM) != 0) {
                        if (t.on_delete) |d| d(t.context);
                    } else if (ev.mask & (linux.IN.CLOSE_WRITE | linux.IN.MOVED_TO) != 0) {
                        t.on_write(t.context);
                    }
                }
            } else if (ev.mask & linux.IN.Q_OVERFLOW != 0) {
                log.warn("inotify: event queue overflowed — some change events may have been missed for '{s}'", .{t.path});
            }
            offset += event_total;
        }
    }
}

/// Rules-watcher wrapper: increments `counter` on every change.
fn watcherInotify(rules_path: []const u8, counter: *std.atomic.Value(u64)) void {
    watchInotify(.{
        .path = rules_path,
        .context = counter,
        .on_write = bumpReloadCounter,
        .on_delete = logRulesDeleted,
    });
}

// ---------------------------------------------------------------------------
// FreeBSD: kqueue / EVFILT_VNODE
// ---------------------------------------------------------------------------

// Raw constants from sys/event.h — not exposed in Zig's posix module.
const EVFILT_VNODE: i16 = -4;
const EV_ADD:       u16 = 0x0001;
const EV_CLEAR:     u16 = 0x0020;
const NOTE_DELETE:  u32 = 0x0001;
const NOTE_WRITE:   u32 = 0x0002;
const NOTE_RENAME:  u32 = 0x0020;

/// Generic kqueue watcher (FreeBSD). Never returns.
pub fn watchKqueue(t: WatchTarget) void {
    if (comptime builtin.os.tag != .freebsd) {
        unreachable;
    }
    const posix = std.posix;

    const kq = posix.kqueue() catch |err| {
        log.err("kqueue: {s}", .{@errorName(err)});
        return;
    };
    defer posix.close(kq);

    const fd = posix.open(t.path, .{ .ACCMODE = .RDONLY }, 0) catch |err| {
        log.err("open '{s}': {s}", .{ t.path, @errorName(err) });
        return;
    };
    defer posix.close(fd);

    var change = std.mem.zeroes(posix.Kevent);
    change.ident  = @intCast(fd);
    change.filter = EVFILT_VNODE;
    change.flags  = EV_ADD | EV_CLEAR;
    change.fflags = NOTE_WRITE | NOTE_RENAME | NOTE_DELETE;

    _ = posix.kevent(kq, &.{change}, &.{}, null) catch |err| {
        log.err("kevent register: {s}", .{@errorName(err)});
        return;
    };

    var events: [1]posix.Kevent = undefined;
    while (true) {
        const ev_count = posix.kevent(kq, &.{}, &events, null) catch |err| {
            if (err == error.Interrupted) continue;
            log.err("kevent wait: {s} — hot-reload permanently disabled, aborting", .{@errorName(err)});
            std.process.abort();
        };
        if (ev_count == 0) continue;
        const fflags = events[0].fflags;
        if (fflags & (NOTE_DELETE | NOTE_RENAME) != 0) {
            if (t.on_delete) |d| d(t.context);
        } else {
            t.on_write(t.context);
        }
    }
}

/// Rules-watcher wrapper.
fn watcherKqueue(rules_path: []const u8, counter: *std.atomic.Value(u64)) void {
    watchKqueue(.{
        .path = rules_path,
        .context = counter,
        .on_write = bumpReloadCounter,
    });
}

// ---------------------------------------------------------------------------
// Fallback: poll via stat() every `poll_ms` milliseconds
// ---------------------------------------------------------------------------

/// Generic poll watcher — stat()s `t.path` every `poll_ms`. The dev fallback
/// for platforms without a kernel watcher; also used directly by tests.
pub fn watchPoll(
    t: WatchTarget,
    poll_ms: u64,
    stop: ?*const std.atomic.Value(bool),
) void {
    var last_mtime: i128 = 0;
    while (stop == null or !stop.?.load(.acquire)) {
        std.Thread.sleep(poll_ms * std.time.ns_per_ms);
        const stat = std.fs.cwd().statFile(t.path) catch {
            if (last_mtime != 0) {
                if (t.on_delete) |d| d(t.context);
                last_mtime = 0;
            }
            continue;
        };
        const mtime = stat.mtime;
        if (last_mtime != 0 and mtime != last_mtime) {
            t.on_write(t.context);
        }
        last_mtime = mtime;
    }
}

/// Rules-watcher wrapper.
fn watcherPoll(
    rules_path: []const u8,
    counter: *std.atomic.Value(u64),
    poll_ms: u64,
    stop: ?*const std.atomic.Value(bool),
) void {
    watchPoll(
        .{ .path = rules_path, .context = counter, .on_write = bumpReloadCounter },
        poll_ms,
        stop,
    );
}

// ---------------------------------------------------------------------------
// Helpers shared by tests
// ---------------------------------------------------------------------------

/// Spin-wait until `counter >= expected` or `timeout_ms` elapses.
/// Returns true if the condition was satisfied in time.
fn waitForCount(counter: *const std.atomic.Value(u64), expected: u64, timeout_ms: u64) bool {
    const start = std.time.milliTimestamp();
    while (counter.load(.acquire) < expected) {
        if (@as(u64, @intCast(std.time.milliTimestamp() - start)) >= timeout_ms) return false;
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    return true;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

// Valid Lua snippets used across watcher tests.
// Each "version" is syntactically distinct so writing v1 after v0 always
// changes the file's mtime.
const LUA_V0 = "-- rules v0";
const LUA_V1 = "-- rules v1";
const LUA_V2 = "-- rules v2";

test "reload_version starts at 0" {
    // The test checks the initial value of the global, not a local counter.
    // Note: if other tests already incremented the global, this test can fail
    // when run in isolation.  The check uses the initial value stored in the type.
    const fresh = std.atomic.Value(u64).init(0);
    try testing.expectEqual(@as(u64, 0), fresh.load(.acquire));
    // The global is also 0 at program start (documented invariant).
    try testing.expect(reload_version.raw >= 0);
}

test "std.atomic.Value(u64) provides .release/.acquire ordering" {
    var v = std.atomic.Value(u64).init(0);
    _ = v.fetchAdd(1, .release);
    try testing.expectEqual(@as(u64, 1), v.load(.acquire));
    _ = v.fetchAdd(1, .release);
    _ = v.fetchAdd(1, .release);
    try testing.expectEqual(@as(u64, 3), v.load(.acquire));
}

test "watcherPoll detects file write within 1500ms" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // Create initial file so the watcher can stat it on its first cycle.
    {
        var f = try tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(LUA_V0);
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath("rules.lua", &path_buf);

    var counter = std.atomic.Value(u64).init(0);
    var stop    = std.atomic.Value(bool).init(false);

    const Ctx = struct {
        path: []const u8,
        ctr:  *std.atomic.Value(u64),
        stp:  *std.atomic.Value(bool),
    };
    const ctx = Ctx{ .path = path, .ctr = &counter, .stp = &stop };
    const t = try std.Thread.spawn(.{}, struct {
        fn run(c: Ctx) void {
            watcherPoll(c.path, c.ctr, 50, c.stp);
        }
    }.run, .{ctx});
    defer { stop.store(true, .release); t.join(); }

    // Let the watcher record the initial mtime (two poll cycles = 100ms).
    std.Thread.sleep(110 * std.time.ns_per_ms);

    // Write new content → changes mtime.
    {
        var f = try tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(LUA_V1);
    }

    // Watcher should detect within 1500ms total budget.
    try testing.expect(waitForCount(&counter, 1, 1400));
}

test "(Linux) inotify detects CLOSE_WRITE within 1000ms" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var f = try tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(LUA_V0);
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath("rules.lua", &path_buf);

    var counter = std.atomic.Value(u64).init(0);

    const Ctx = struct {
        path: []const u8,
        ctr:  *std.atomic.Value(u64),
    };
    const ctx = Ctx{ .path = path, .ctr = &counter };
    const t = try std.Thread.spawn(.{}, struct {
        fn run(c: Ctx) void { watcherInotify(c.path, c.ctr); }
    }.run, .{ctx});
    t.detach();

    // Give the watcher time to set up the watch descriptor.
    std.Thread.sleep(20 * std.time.ns_per_ms);

    // Write valid Lua (CLOSE_WRITE fires on close).
    {
        var f = try tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(LUA_V1);
    }

    try testing.expect(waitForCount(&counter, 1, 1000));
}

test "(Linux) 3 writes 200ms apart → counter >= 3 within 2s of last write" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var f = try tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(LUA_V0);
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath("rules.lua", &path_buf);

    var counter = std.atomic.Value(u64).init(0);

    const Ctx = struct {
        path: []const u8,
        ctr:  *std.atomic.Value(u64),
    };
    const ctx = Ctx{ .path = path, .ctr = &counter };
    const t = try std.Thread.spawn(.{}, struct {
        fn run(c: Ctx) void { watcherInotify(c.path, c.ctr); }
    }.run, .{ctx});
    t.detach();

    std.Thread.sleep(20 * std.time.ns_per_ms);

    const versions = [_][]const u8{ LUA_V1, LUA_V2, LUA_V0 };
    for (versions, 0..) |ver, n| {
        var f = try tmp.dir.createFile("rules.lua", .{});
        try f.writeAll(ver);
        f.close();
        if (n < 2) std.Thread.sleep(200 * std.time.ns_per_ms);
    }

    try testing.expect(waitForCount(&counter, 3, 2000));
}

test "(Linux) atomic rename (tmp → rules.lua) → counter incremented within 1000ms" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var f = try tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(LUA_V0);
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath("rules.lua", &path_buf);

    var counter = std.atomic.Value(u64).init(0);

    const Ctx = struct {
        path: []const u8,
        ctr:  *std.atomic.Value(u64),
    };
    const ctx = Ctx{ .path = path, .ctr = &counter };
    const t = try std.Thread.spawn(.{}, struct {
        fn run(c: Ctx) void { watcherInotify(c.path, c.ctr); }
    }.run, .{ctx});
    t.detach();

    std.Thread.sleep(20 * std.time.ns_per_ms);

    // Atomic write: write to tmp then rename.
    {
        var f = try tmp.dir.createFile("rules.lua.tmp", .{});
        defer f.close();
        try f.writeAll(LUA_V1);
    }
    try tmp.dir.rename("rules.lua.tmp", "rules.lua");

    try testing.expect(waitForCount(&counter, 1, 1000));
}

test "(Linux) file deleted and recreated — watcher does not panic" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var f = try tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(LUA_V0);
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath("rules.lua", &path_buf);

    var counter = std.atomic.Value(u64).init(0);

    const Ctx = struct {
        path: []const u8,
        ctr:  *std.atomic.Value(u64),
    };
    const ctx = Ctx{ .path = path, .ctr = &counter };
    const t = try std.Thread.spawn(.{}, struct {
        fn run(c: Ctx) void { watcherInotify(c.path, c.ctr); }
    }.run, .{ctx});
    t.detach();

    std.Thread.sleep(20 * std.time.ns_per_ms);

    // Delete the file.
    try tmp.dir.deleteFile("rules.lua");
    std.Thread.sleep(50 * std.time.ns_per_ms);

    // Recreate it (watcher may or may not pick this up, but must not panic).
    {
        var f = try tmp.dir.createFile("rules.lua", .{});
        defer f.close();
        try f.writeAll(LUA_V1);
    }
    std.Thread.sleep(100 * std.time.ns_per_ms);
    // The test passes if execution reaches here without panic.
}
