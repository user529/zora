const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // --- release: read from RELEASE file, auto-increment on each build ---
    const release_number: u32 = blk: {
        const content = b.build_root.handle.readFileAlloc(b.allocator, "RELEASE", 16) catch break :blk 0;
        defer b.allocator.free(content);
        const n = std.fmt.parseInt(u32, std.mem.trim(u8, content, " \n\r\t"), 10) catch break :blk 0;
        const f = b.build_root.handle.createFile("RELEASE", .{}) catch break :blk n;
        defer f.close();
        var nbuf: [16]u8 = undefined;
        const next_str = std.fmt.bufPrint(&nbuf, "{d}\n", .{n + 1}) catch break :blk n;
        f.writeAll(next_str) catch {};
        break :blk n;
    };

    // --- git_branch: branch name at build time, falls back to "unknown" ---
    const git_branch: []const u8 = blk: {
        const result = std.process.Child.run(.{
            .allocator = b.allocator,
            .argv = &.{ "git", "rev-parse", "--abbrev-ref", "HEAD" },
            .cwd = b.build_root.path orelse ".",
        }) catch break :blk "unknown";
        const trimmed = std.mem.trim(u8, result.stdout, " \n\r\t");
        if (trimmed.len == 0) break :blk "unknown";
        break :blk trimmed;
    };

    const options = b.addOptions();
    options.addOption(u32, "release", release_number);
    options.addOption([]const u8, "git_branch", git_branch);

    // --- ThreadSanitizer option ---
    // Enable with: zig build -Dsanitize-thread
    // Requires the LLVM backend: Zig's self-hosted backend silently accepts
    // -fsanitize-thread but emits no instrumentation.  When this option is
    // set, use_llvm is forced true on the exe step so Debug builds also work.
    // SQLite C code is excluded via -fno-sanitize=thread to avoid false
    // positives from SQLITE_THREADSAFE=2 global-init (safe but unguarded).
    const sanitize_thread: ?bool = b.option(bool, "sanitize-thread", "Enable ThreadSanitizer (TSan); forces LLVM backend");

    // --- ziglua dependency ---
    const ziglua_dep = b.dependency("ziglua", .{
        .target = target,
        .optimize = optimize,
        .lang = .lua54,
    });
    const ziglua_mod = ziglua_dep.module("zlua");

    // --- SQLite compile flags ---
    // -fno-sanitize=thread: exclude SQLite C code from TSan instrumentation.
    // Each worker owns its own connection (SQLITE_THREADSAFE=2 guarantee), so
    // SQLite's global-init races are structural false positives, not real bugs.
    // The flag is a no-op when TSan is not enabled.
    const sqlite_flags: []const []const u8 = &.{
        "-DSQLITE_THREADSAFE=1",
        "-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1",
        "-fno-sanitize=undefined",
        "-fno-sanitize=thread",
        "-fno-omit-frame-pointer",
    };

    // --- root module for executable ---
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
    });
    exe_mod.addOptions("build_options", options);
    exe_mod.addImport("ziglua", ziglua_mod);

    const exe = b.addExecutable(.{
        .name = "zora",
        .root_module = exe_mod,
    });
    exe.addCSourceFile(.{
        .file = b.path("vendor/sqlite3.c"),
        .flags = sqlite_flags,
    });
    exe.addIncludePath(b.path("vendor"));
    exe.linkLibC();

    // TSan requires the LLVM backend; self-hosted backend silently drops
    // instrumentation.  Forcing use_llvm here makes Debug+TSan work too.
    if (sanitize_thread != null) exe.use_llvm = true;

    b.installArtifact(exe);

    // --- run step ---
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run zora");
    run_step.dependOn(&run_cmd.step);

    // --- test step ---
    const test_step = b.step("test", "Run all tests");

    const src_files = [_][]const u8{
        "src/types.zig",
        "src/config.zig",
        "src/queue.zig",
        "src/serializer.zig",
        "src/state_store.zig",
        "src/lua_engine.zig",
        "src/lua_api.zig",
        "src/reload.zig",
        "src/worker.zig",
        "src/dispatcher.zig",
        "src/server.zig",
        "src/main.zig",
    };

    for (src_files) |src| {
        const test_mod = b.createModule(.{
            .root_source_file = b.path(src),
            .target = target,
            .optimize = optimize,
        });
        test_mod.addOptions("build_options", options);
        test_mod.addImport("ziglua", ziglua_mod);

        const unit_tests = b.addTest(.{
            .root_module = test_mod,
        });
        unit_tests.addCSourceFile(.{
            .file = b.path("vendor/sqlite3.c"),
            .flags = sqlite_flags,
        });
        unit_tests.addIncludePath(b.path("vendor"));
        unit_tests.linkLibC();

        const run_tests = b.addRunArtifact(unit_tests);
        test_step.dependOn(&run_tests.step);
    }
}
