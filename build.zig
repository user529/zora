const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // --- build_number: count git commits, fall back to 0 ---
    const build_number: u32 = blk: {
        const result = std.process.Child.run(.{
            .allocator = b.allocator,
            .argv = &.{ "git", "rev-list", "--count", "HEAD" },
            .cwd = b.build_root.path orelse ".",
        }) catch break :blk 0;
        break :blk std.fmt.parseInt(u32, std.mem.trim(u8, result.stdout, " \n\r\t"), 10) catch 0;
    };

    const options = b.addOptions();
    options.addOption(u32, "build_number", build_number);

    // --- ziglua dependency ---
    const ziglua_dep = b.dependency("ziglua", .{
        .target = target,
        .optimize = optimize,
        .lang = .lua54,
    });
    const ziglua_mod = ziglua_dep.module("zlua");

    // --- SQLite compile flags ---
    const sqlite_flags: []const []const u8 = &.{
        "-DSQLITE_THREADSAFE=2",
        "-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1",
        "-fno-sanitize=undefined",
    };

    // --- helper: configure a compile step with shared deps ---
    // (inline helper — avoids repetition between exe and test steps)

    // --- root module for executable ---
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
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
