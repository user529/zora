//! types.zig — shared structs: ApiCall, WorkItem, Config
//!
//! This file imports only the standard library (not project files), keeping
//! it free of circular imports.
const std = @import("std");

/// Stack size for every thread zora spawns. 1 MiB vs. Zig's 16 MiB default —
/// the deepest stack (depth-8 serializer recursion, TLS handshake) stays well
/// under this. Cuts ~15 MiB of VSZ reservation per thread (~40+ threads).
pub const THREAD_STACK_SIZE: usize = 1 << 20;

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
    /// Non-null for tracked sends only. A plain value: freeApiCall() has
    /// nothing to free here.
    tracking: ?struct { worker_id: u8, coro_id: u32 } = null,
    /// Outbound chat id for the throttle (blocked_until divert + 429 park).
    /// Null ⇒ no chat target. A plain value, like `tracking`.
    route: ?struct { chat_id: i64 } = null,
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
// frees it once the handler has started (the Lua table is a decoded copy).
// ---------------------------------------------------------------------------

/// Which ingress produced a WorkItem — selects the Lua handler in worker.zig.
/// The worker dispatches via an exhaustive switch with no `else`, so a new
/// variant cannot compile without choosing a handler.
pub const WorkKind = enum { message, schedule };

pub const WorkItem = struct {
    body: []const u8, // owned: raw webhook JSON (message) or job payload JSON (schedule)
    user_id: ?i64,    // routing key for messages; null for round-robined schedule jobs
    kind: WorkKind = .message,
    /// Set only for `.schedule` items: the schedule row id, so the worker can
    /// delete the row on terminal success and pass it to on_schedule.
    schedule_id: ?i64 = null,
};

// ---------------------------------------------------------------------------
// ValidationMode — outgoing-call schema validation policy.
//
//   off    — no validation
//   warn   — validate; log a warning on failure, send the call anyway
//   strict — validate; drop the call on failure (it is never dispatched)
// ---------------------------------------------------------------------------

pub const ValidationMode = enum { off, warn, strict };

// ---------------------------------------------------------------------------
// Configuration (populated by config.zig from env vars)
// ---------------------------------------------------------------------------

pub const Config = struct {
    bot_token:          []const u8,
    webhook_secret:     []const u8,
    listen_addr:        std.Io.net.IpAddress,
    rules_file:         [:0]const u8,
    db_path:            [:0]const u8,
    /// Passphrase enabling encrypted state-at-rest; null = plaintext mode.
    state_encryption_key: ?[]const u8 = null,
    worker_threads:       u8,
    worker_queue_capacity:     u16,
    dispatcher_threads: u8,
    webhook_pool_threads:  u8,
    schema_file:        [:0]const u8,
    api_validation:     ValidationMode,
    bot_api_base:           []const u8,
    json_max_bytes:     usize,
    io_pool_threads:    u8,
    io_queue_capacity:  u16,
    io_job_timeout_ms:  u64,
    proc_max_output_bytes:         usize,
    worker_max_inflight: u16,
    workflow_deadline_ms:    u64,
    metrics_log:             bool,
    delay_queue_capacity:    u16,
    retry_after_max_ms:      u64,
    retry_after_default_ms:  u64,
    /// Scrape-endpoint bind address; null disables the endpoint entirely.
    metrics_addr:            ?std.Io.net.IpAddress,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "freeApiCall releases json and multipart payloads" {
    const alloc = std.testing.allocator;

    // json payload: method and body are freed.
    {
        const call = ApiCall{
            .method  = try alloc.dupe(u8, "sendDice"),
            .payload = .{ .json = try alloc.dupe(u8, "{\"chat_id\":1}") },
        };
        freeApiCall(call, alloc);
    }
    // multipart payload: every part (name, content, optional filename) is freed.
    {
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
}
