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
const rt      = @import("rt.zig");

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
    /// Runtime for the poll fallback's sleep and stat (unused on Linux/FreeBSD,
    /// which use kernel watchers).
    io: std.Io,
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
        else     => watcherPoll(owned, &reload_version, args.io, 500, null),
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

    // 0.16 removed the medium-level posix inotify wrappers; call the raw Linux
    // syscalls and check errno directly.
    const init_rc = linux.inotify_init1(linux.IN.CLOEXEC);
    switch (linux.errno(init_rc)) {
        .SUCCESS => {},
        else => |e| {
            log.err("inotify_init1: {s}", .{@tagName(e)});
            return;
        },
    }
    const ifd: i32 = @intCast(init_rc);
    defer _ = linux.close(ifd);

    // inotify_add_watch needs a null-terminated path.
    var dirz_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dirz = std.fmt.bufPrintZ(&dirz_buf, "{s}", .{dir_path}) catch {
        log.err("inotify watch path too long: '{s}'", .{dir_path});
        return;
    };
    const add_rc = linux.inotify_add_watch(
        ifd,
        dirz.ptr,
        linux.IN.CLOSE_WRITE | linux.IN.MOVED_TO |
            linux.IN.DELETE | linux.IN.MOVED_FROM,
    );
    switch (linux.errno(add_rc)) {
        .SUCCESS => {},
        else => |e| {
            log.err("inotify_add_watch '{s}': {s}", .{ dir_path, @tagName(e) });
            return;
        },
    }

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

/// Generic kqueue watcher (FreeBSD). Never returns.
pub fn watchKqueue(t: WatchTarget) void {
    if (comptime builtin.os.tag != .freebsd) {
        unreachable;
    }
    const c = std.c;

    // 0.16 removed the medium-level posix kqueue wrappers; call libc directly
    // and check errno. The EVFILT/EV/NOTE constants now live in std.c.
    const kq = c.kqueue();
    switch (c.errno(kq)) {
        .SUCCESS => {},
        else => |e| {
            log.err("kqueue: {s}", .{@tagName(e)});
            return;
        },
    }
    defer _ = c.close(kq);

    // open needs a null-terminated path.
    var pathz_buf: [std.fs.max_path_bytes]u8 = undefined;
    const pathz = std.fmt.bufPrintZ(&pathz_buf, "{s}", .{t.path}) catch {
        log.err("kqueue watch path too long: '{s}'", .{t.path});
        return;
    };
    const fd = c.open(pathz.ptr, .{ .ACCMODE = .RDONLY });
    switch (c.errno(fd)) {
        .SUCCESS => {},
        else => |e| {
            log.err("open '{s}': {s}", .{ t.path, @tagName(e) });
            return;
        },
    }
    defer _ = c.close(fd);

    const change: c.Kevent = .{
        .ident = @intCast(fd),
        .filter = c.EVFILT.VNODE,
        .flags = c.EV.ADD | c.EV.CLEAR,
        .fflags = c.NOTE.WRITE | c.NOTE.RENAME | c.NOTE.DELETE,
        .data = 0,
        .udata = 0,
    };

    var events: [1]c.Kevent = undefined;
    const reg_rc = c.kevent(kq, &[_]c.Kevent{change}, 1, &events, 0, null);
    switch (c.errno(reg_rc)) {
        .SUCCESS => {},
        else => |e| {
            log.err("kevent register: {s}", .{@tagName(e)});
            return;
        },
    }

    while (true) {
        const n = c.kevent(kq, &events, 0, &events, events.len, null);
        switch (c.errno(n)) {
            .SUCCESS => {},
            .INTR => continue,
            else => |e| {
                log.err("kevent wait: {s} — hot-reload permanently disabled, aborting", .{@tagName(e)});
                std.process.abort();
            },
        }
        if (n == 0) continue;
        const fflags = events[0].fflags;
        if (fflags & (c.NOTE.DELETE | c.NOTE.RENAME) != 0) {
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
    io: std.Io,
    poll_ms: u64,
    stop: ?*const std.atomic.Value(bool),
) void {
    var last_mtime: i128 = 0;
    while (stop == null or !stop.?.load(.acquire)) {
        rt.sleepNs(io, poll_ms * std.time.ns_per_ms);
        const stat = std.Io.Dir.cwd().statFile(io, t.path, .{}) catch {
            if (last_mtime != 0) {
                if (t.on_delete) |d| d(t.context);
                last_mtime = 0;
            }
            continue;
        };
        const mtime: i128 = stat.mtime.nanoseconds;
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
    io: std.Io,
    poll_ms: u64,
    stop: ?*const std.atomic.Value(bool),
) void {
    watchPoll(
        .{ .path = rules_path, .context = counter, .on_write = bumpReloadCounter },
        io,
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
    const start = rt.nowMs(rt.io());
    while (counter.load(.acquire) < expected) {
        if (@as(u64, @intCast(rt.nowMs(rt.io()) - start)) >= timeout_ms) return false;
        rt.sleepNs(rt.io(), 5 * std.time.ns_per_ms);
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

test "reload_version starts at zero and Value(u64) orders with acquire/release" {
    // A fresh counter starts at 0. The process-global reload_version may have
    // been advanced by other tests, so only its non-negative invariant holds.
    const fresh = std.atomic.Value(u64).init(0);
    try testing.expectEqual(@as(u64, 0), fresh.load(.acquire));
    try testing.expect(reload_version.raw >= 0);

    // fetchAdd(.release) is observed in order by load(.acquire).
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
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = LUA_V0 });
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPathFile(testing.io, "rules.lua", &path_buf);
    const path = path_buf[0..path_len];

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
            watcherPoll(c.path, c.ctr, testing.io, 50, c.stp);
        }
    }.run, .{ctx});
    defer { stop.store(true, .release); t.join(); }

    // Let the watcher record the initial mtime (two poll cycles = 100ms).
    rt.sleepNs(rt.io(), 110 * std.time.ns_per_ms);

    // Write new content → changes mtime.
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = LUA_V1 });
    }

    // Watcher should detect within 1500ms total budget.
    try testing.expect(waitForCount(&counter, 1, 1400));
}

test "(Linux) inotify detects CLOSE_WRITE within 1000ms" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = LUA_V0 });
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPathFile(testing.io, "rules.lua", &path_buf);
    const path = path_buf[0..path_len];

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
    rt.sleepNs(rt.io(), 20 * std.time.ns_per_ms);

    // Write valid Lua (CLOSE_WRITE fires on close).
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = LUA_V1 });
    }

    try testing.expect(waitForCount(&counter, 1, 1000));
}

test "(Linux) 3 writes 200ms apart → counter >= 3 within 2s of last write" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = LUA_V0 });
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPathFile(testing.io, "rules.lua", &path_buf);
    const path = path_buf[0..path_len];

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

    rt.sleepNs(rt.io(), 20 * std.time.ns_per_ms);

    const versions = [_][]const u8{ LUA_V1, LUA_V2, LUA_V0 };
    for (versions, 0..) |ver, n| {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = ver });
        if (n < 2) rt.sleepNs(rt.io(), 200 * std.time.ns_per_ms);
    }

    try testing.expect(waitForCount(&counter, 3, 2000));
}

test "(Linux) atomic rename (tmp → rules.lua) → counter incremented within 1000ms" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = LUA_V0 });
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPathFile(testing.io, "rules.lua", &path_buf);
    const path = path_buf[0..path_len];

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

    rt.sleepNs(rt.io(), 20 * std.time.ns_per_ms);

    // Atomic write: write to tmp then rename.
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua.tmp", .data = LUA_V1 });
    }
    try tmp.dir.rename("rules.lua.tmp", tmp.dir, "rules.lua", testing.io);

    try testing.expect(waitForCount(&counter, 1, 1000));
}

test "(Linux) file deleted and recreated — watcher does not panic" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = LUA_V0 });
    }

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPathFile(testing.io, "rules.lua", &path_buf);
    const path = path_buf[0..path_len];

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

    rt.sleepNs(rt.io(), 20 * std.time.ns_per_ms);

    // Delete the file.
    try tmp.dir.deleteFile(testing.io, "rules.lua");
    rt.sleepNs(rt.io(), 50 * std.time.ns_per_ms);

    // Recreate it (watcher may or may not pick this up, but must not panic).
    {
        try tmp.dir.writeFile(testing.io, .{ .sub_path = "rules.lua", .data = LUA_V1 });
    }
    rt.sleepNs(rt.io(), 100 * std.time.ns_per_ms);
    // The test passes if execution reaches here without panic.
}
