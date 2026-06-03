/// types.zig — shared structs: ApiCall, WorkItem, Config
///
/// This file imports only the standard library and the ziglua external
/// dependency (not project files), keeping it free of circular imports.
const std = @import("std");
const ziglua = @import("ziglua");

// ---------------------------------------------------------------------------
// ApiCall — generic outgoing Telegram API call.
//
// The payload is a tagged union: `.json` for plain API calls (verbatim JSON
// body), `.multipart` for calls that upload files (slice of MultipartPart).
// Both `method` and all payload fields are heap-allocated; `freeApiCall`
// releases them.  The dispatcher takes ownership at push time and frees after
// the HTTP call completes.
// ---------------------------------------------------------------------------

pub const MultipartPart = struct {
    name:     []const u8,  // form field name — owned
    content:  []const u8,  // bytes           — owned
    filename: ?[]const u8, // non-null for file parts — owned
};

pub const ApiCall = struct {
    method:  []const u8,
    payload: union(enum) {
        json:      []const u8,      // verbatim JSON body — owned
        multipart: []MultipartPart, // slice of parts     — owned
    },
    /// Non-null for tracked sends only. Value-typed — freeApiCall unchanged.
    tracking: ?struct { worker_id: u8, coro_id: u32 } = null,
};

pub fn freeApiCall(call: ApiCall, allocator: std.mem.Allocator) void {
    allocator.free(call.method);
    switch (call.payload) {
        .json      => |b| allocator.free(b),
        .multipart => |parts| {
            for (parts) |p| {
                allocator.free(p.name);
                allocator.free(p.content);
                if (p.filename) |f| allocator.free(f);
            }
            allocator.free(parts);
        },
    }
}

pub fn freeApiCalls(calls: []ApiCall, allocator: std.mem.Allocator) void {
    for (calls) |c| freeApiCall(c, allocator);
    allocator.free(calls);
}

// ---------------------------------------------------------------------------
// WorkItem — one inbound webhook update queued for a worker.
//
// The server forwards the raw JSON `body` verbatim plus a `user_id` it
// point-extracts for queue routing.  `body` is heap-allocated; the worker
// frees it after `callOnMessage` returns.
// ---------------------------------------------------------------------------

pub const WorkItem = struct {
    body: []const u8, // owned: raw webhook JSON, freed by the worker
    user_id: ?i64,    // routing key; null when no sender could be extracted
};

// ---------------------------------------------------------------------------
// Configuration (populated by config.zig from env vars)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ValidationMode — outgoing-call schema validation policy.
//
//   off    — no validation
//   warn   — validate; log a warning on failure, send the call anyway
//   strict — validate; drop the call on failure (it is never dispatched)
// ---------------------------------------------------------------------------

pub const ValidationMode = enum { off, warn, strict };

pub const Config = struct {
    bot_token:          []const u8,
    webhook_secret:     []const u8,
    listen_addr:        std.net.Address,
    rules_file:         [:0]const u8,
    db_path:            [:0]const u8,
    worker_count:       u8,
    queue_capacity:     u16,
    dispatcher_threads: u8,
    conn_pool_threads:  u8,
    schema_file:        [:0]const u8,
    api_validation:     ValidationMode,
    api_base:           []const u8,
    multipart_max_file: usize,
    json_max_bytes:     usize,
    io_pool_threads:    u8,
    io_queue_capacity:  u16,
    io_job_timeout_ms:  u64,
    proc_max_output:         usize,
    max_inflight_per_worker: u16,
    workflow_deadline_ms:    u64,
    metrics_log:             bool,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// ApiCall lifecycle — no leaks under testing.allocator
// ---------------------------------------------------------------------------

test "freeApiCall releases method and body" {
    const call = ApiCall{
        .method  = try std.testing.allocator.dupe(u8, "sendDice"),
        .payload = .{ .json = try std.testing.allocator.dupe(u8, "{\"chat_id\":1}") },
    };
    freeApiCall(call, std.testing.allocator);
}

test "freeApiCalls releases a slice of ApiCalls" {
    var calls = try std.testing.allocator.alloc(ApiCall, 3);
    calls[0] = .{
        .method  = try std.testing.allocator.dupe(u8, "sendMessage"),
        .payload = .{ .json = try std.testing.allocator.dupe(u8, "{}") },
    };
    calls[1] = .{
        .method  = try std.testing.allocator.dupe(u8, "editMessageText"),
        .payload = .{ .json = try std.testing.allocator.dupe(u8, "{\"chat_id\":1,\"message_id\":2,\"text\":\"x\"}") },
    };
    calls[2] = .{
        .method  = try std.testing.allocator.dupe(u8, "deleteMessage"),
        .payload = .{ .json = try std.testing.allocator.dupe(u8, "{\"chat_id\":1,\"message_id\":2}") },
    };
    freeApiCalls(calls, std.testing.allocator);
}

// ---------------------------------------------------------------------------
// freeApiCall handles multipart payload — no leaks
// ---------------------------------------------------------------------------

test "freeApiCall on multipart ApiCall releases all parts" {
    const alloc = std.testing.allocator;
    var parts = try alloc.alloc(MultipartPart, 2);
    parts[0] = .{
        .name     = try alloc.dupe(u8, "caption"),
        .content  = try alloc.dupe(u8, "hello"),
        .filename = null,
    };
    parts[1] = .{
        .name     = try alloc.dupe(u8, "photo"),
        .content  = try alloc.dupe(u8, "\xff\xd8\xff"),
        .filename = try alloc.dupe(u8, "pic.jpg"),
    };
    const call = ApiCall{
        .method  = try alloc.dupe(u8, "sendPhoto"),
        .payload = .{ .multipart = parts },
    };
    freeApiCall(call, alloc);
}

test "freeApiCall on json ApiCall releases body" {
    const alloc = std.testing.allocator;
    const call = ApiCall{
        .method  = try alloc.dupe(u8, "sendMessage"),
        .payload = .{ .json = try alloc.dupe(u8, "{\"chat_id\":1}") },
    };
    freeApiCall(call, alloc);
}

test "ApiCall tracking field defaults null, can be set, freeApiCall unchanged" {
    const alloc = std.testing.allocator;
    // tracked call
    const tracked = ApiCall{
        .method   = try alloc.dupe(u8, "sendMessage"),
        .payload  = .{ .json = try alloc.dupe(u8, "{\"chat_id\":1}") },
        .tracking = .{ .worker_id = 2, .coro_id = 99 },
    };
    defer freeApiCall(tracked, alloc);
    try std.testing.expect(tracked.tracking != null);
    try std.testing.expectEqual(@as(u8, 2),  tracked.tracking.?.worker_id);
    try std.testing.expectEqual(@as(u32, 99), tracked.tracking.?.coro_id);

    // untracked call — default null
    const plain = ApiCall{
        .method  = try alloc.dupe(u8, "sendMessage"),
        .payload = .{ .json = try alloc.dupe(u8, "{}") },
    };
    defer freeApiCall(plain, alloc);
    try std.testing.expect(plain.tracking == null);
}
