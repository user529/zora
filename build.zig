const std = @import("std");
const Translator = @import("translate_c").Translator;

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // --- release: read from RELEASE file ---
    const release_number: u32 = blk: {
        const io = b.graph.io;
        const content = b.root.root_dir.handle.readFileAlloc(io, "RELEASE", b.allocator, .limited(16)) catch break :blk 0;
        defer b.allocator.free(content);
        const n = std.fmt.parseInt(u32, std.mem.trim(u8, content, " \n\r\t"), 10) catch break :blk 0;
        break :blk n;
    };

    // --- git_branch: branch name at build time, falls back to "unknown" ---
    const git_branch: []const u8 = blk: {
        const result = std.process.run(b.allocator, b.graph.io, .{
            .argv = &.{ "git", "rev-parse", "--abbrev-ref", "HEAD" },
            .cwd = .{ .path = b.root.root_dir.path orelse "." },
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

    // Shared C-header translation base (aro-based translate-c package).
    // Replaces the removed @cImport builtin; the same package translates
    // ziglua's Lua headers. sqlite3.h -> a "c" module imported by the
    // SQLite-using Zig modules; sqlite3.c is still compiled/linked separately.
    const translate_c_dep = b.dependency("translate_c", .{});
    const sqlite_translated: Translator = .init(translate_c_dep, .{
        .c_source_file = b.path("vendor/sqlite3.h"),
        .target = target,
        .optimize = optimize,
        .link_system_libs = &.{},
    });
    const sqlite_mod = sqlite_translated.mod;

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
        .link_libc = true,
    });
    exe_mod.addOptions("build_options", options);
    exe_mod.addImport("ziglua", ziglua_mod);
    exe_mod.addImport("c", sqlite_mod);
    exe_mod.addCSourceFile(.{
        .file = b.path("vendor/sqlite3.c"),
        .flags = sqlite_flags,
    });
    exe_mod.addIncludePath(b.path("vendor"));

    const exe = b.addExecutable(.{
        .name = "zora",
        .root_module = exe_mod,
    });

    // TSan requires the LLVM backend; self-hosted backend silently drops
    // instrumentation.  Forcing use_llvm here makes Debug+TSan work too.
    if (sanitize_thread != null) exe.use_llvm = true;

    b.installArtifact(exe);

    // --- run step ---
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    run_cmd.addPassthruArgs();
    const run_step = b.step("run", "Run zora");
    run_step.dependOn(&run_cmd.step);

    // --- zora-migrate executable ---
    // Standalone v1 -> v2 database migration tool. Links the same SQLite C
    // source as the server and imports state_store; it needs neither ziglua nor
    // build_options (no version banner).
    const migrate_mod = b.createModule(.{
        .root_source_file = b.path("src/migrate.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    migrate_mod.addImport("c", sqlite_mod);
    migrate_mod.addCSourceFile(.{
        .file = b.path("vendor/sqlite3.c"),
        .flags = sqlite_flags,
    });
    migrate_mod.addIncludePath(b.path("vendor"));

    const migrate_exe = b.addExecutable(.{
        .name = "zora-migrate",
        .root_module = migrate_mod,
    });
    b.installArtifact(migrate_exe);

    const migrate_run = b.addRunArtifact(migrate_exe);
    migrate_run.step.dependOn(b.getInstallStep());
    migrate_run.addPassthruArgs();
    const migrate_step = b.step("migrate", "Run the v1 -> v2 state database migration tool");
    migrate_step.dependOn(&migrate_run.step);

    // --- test step ---
    const test_step = b.step("test", "Run all tests");

    const src_files = [_][]const u8{
        "src/rt.zig",
        "src/types.zig",
        "src/metrics.zig",
        "src/config.zig",
        "src/queue.zig",
        "src/io_pool.zig",
        "src/serializer.zig",
        "src/state_crypto.zig",
        "src/state_store.zig",
        "src/tg_schema.zig",
        "src/lua_engine.zig",
        "src/lua_api.zig",
        "src/watcher.zig",
        "src/worker.zig",
        "src/delay.zig",
        "src/scheduler.zig",
        "src/dispatcher.zig",
        "src/server.zig",
        "src/metrics_server.zig",
        "src/main.zig",
        "src/migrate.zig",
    };

    for (src_files) |src| {
        const test_mod = b.createModule(.{
            .root_source_file = b.path(src),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        test_mod.addOptions("build_options", options);
        test_mod.addImport("ziglua", ziglua_mod);
        test_mod.addImport("c", sqlite_mod);
        test_mod.addCSourceFile(.{
            .file = b.path("vendor/sqlite3.c"),
            .flags = sqlite_flags,
        });
        test_mod.addIncludePath(b.path("vendor"));

        const unit_tests = b.addTest(.{
            .root_module = test_mod,
        });

        const run_tests = b.addRunArtifact(unit_tests);
        test_step.dependOn(&run_tests.step);

        // Per-module step: "test-<basename>" (e.g. src/types.zig -> test-types).
        // Lets the migration validate one module at a time.
        const base = std.fs.path.stem(src);
        const step_name = b.fmt("test-{s}", .{base});
        const mod_step = b.step(step_name, b.fmt("Run {s} tests", .{src}));
        mod_step.dependOn(&run_tests.step);
    }
}
