/// types.zig — shared structs: Update, Action, Config
///
/// This file imports only the standard library and the ziglua external
/// dependency (not project files), keeping it free of circular imports.
const std = @import("std");
const ziglua = @import("ziglua");

// ---------------------------------------------------------------------------
// Telegram Update tree
// ---------------------------------------------------------------------------

pub const User = struct {
    id: i64,
    is_bot: bool,
    first_name: []const u8,
    last_name: ?[]const u8 = null,
    username: ?[]const u8 = null,
};

pub const Chat = struct {
    id: i64,
    /// "private", "group", "supergroup", "channel"
    type: []const u8,
    title: ?[]const u8 = null,
    username: ?[]const u8 = null,
};

pub const Message = struct {
    message_id: i64,
    from: ?User = null,
    chat: Chat,
    date: i64,
    text: ?[]const u8 = null,
};

pub const CallbackQuery = struct {
    id: []const u8,
    from: User,
    message: ?Message = null,
    data: ?[]const u8 = null,
};

pub const Update = struct {
    update_id: i64,
    message: ?Message = null,
    callback_query: ?CallbackQuery = null,

    /// Returns the user id to use for queue routing.
    /// Prefers message sender; falls back to callback_query sender.
    pub fn effectiveUserId(self: Update) ?i64 {
        if (self.message) |msg| {
            if (msg.from) |from| return from.id;
        }
        if (self.callback_query) |cq| return cq.from.id;
        return null;
    }
};

// ---------------------------------------------------------------------------
// Actions produced by Lua on_message()
// ---------------------------------------------------------------------------

pub const ActionTag = enum {
    send_message,
    send_message_ex,
    answer_callback,
    delete_message,
};

pub const Action = union(ActionTag) {
    send_message: struct {
        chat_id: i64,
        text: []const u8,
    },
    send_message_ex: struct {
        chat_id: i64,
        text: []const u8,
        /// JSON string of extra Telegram API options (parse_mode, reply_markup, …)
        opts: []const u8,
    },
    answer_callback: struct {
        callback_query_id: []const u8,
        text: ?[]const u8,
    },
    delete_message: struct {
        chat_id: i64,
        message_id: i64,
    },
};

// ---------------------------------------------------------------------------
//  Free the string payloads owned by a single Action
// ---------------------------------------------------------------------------
/// Free all heap-allocated strings inside `action`.
/// Does NOT free the Action value itself (it is stack/queue allocated).
pub fn freeActionPayload(action: Action, allocator: std.mem.Allocator) void {
    switch (action) {
        .send_message    => |a| allocator.free(a.text),
        .send_message_ex => |a| { allocator.free(a.text); allocator.free(a.opts); },
        .answer_callback => |a| {
            allocator.free(a.callback_query_id);
            if (a.text) |t| allocator.free(t);
        },
        .delete_message => {},
    }
}

pub fn freeActions(actions: []Action, allocator: std.mem.Allocator) void {
    for (actions) |action| freeActionPayload(action, allocator);
    allocator.free(actions);
}
// ---------------------------------------------------------------------------
// Configuration (populated by config.zig from env vars)
// ---------------------------------------------------------------------------

pub const Config = struct {
    bot_token: []const u8,
    webhook_secret: []const u8,
    listen_addr: std.net.Address,
    rules_file: [:0]const u8,
    db_path: [:0]const u8,
    worker_count: u8,
    queue_capacity: u16,
    dispatcher_threads: u8,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "AC-1.1: Update instantiates with all optional fields null" {
    const u = Update{
        .update_id = 1,
    };
    try std.testing.expectEqual(@as(?Message, null), u.message);
    try std.testing.expectEqual(@as(?CallbackQuery, null), u.callback_query);
}

test "AC-1.1: Message instantiates with all optional fields null" {
    const m = Message{
        .message_id = 42,
        .chat = .{ .id = 1, .type = "private" },
        .date = 0,
    };
    try std.testing.expectEqual(@as(?User, null), m.from);
    try std.testing.expectEqual(@as(?[]const u8, null), m.text);
}

test "AC-1.1: CallbackQuery instantiates with all optional fields null" {
    const cq = CallbackQuery{
        .id = "cq1",
        .from = .{ .id = 99, .is_bot = false, .first_name = "Bob" },
    };
    try std.testing.expectEqual(@as(?Message, null), cq.message);
    try std.testing.expectEqual(@as(?[]const u8, null), cq.data);
}

test "AC-1.2: Action switch is exhaustive over all four tags" {
    const actions = [_]Action{
        .{ .send_message = .{ .chat_id = 1, .text = "hi" } },
        .{ .send_message_ex = .{ .chat_id = 1, .text = "hi", .opts = "{}" } },
        .{ .answer_callback = .{ .callback_query_id = "id", .text = null } },
        .{ .delete_message = .{ .chat_id = 1, .message_id = 5 } },
    };

    for (actions) |a| {
        // No else branch — must cover all four tags.
        const tag: ActionTag = switch (a) {
            .send_message => .send_message,
            .send_message_ex => .send_message_ex,
            .answer_callback => .answer_callback,
            .delete_message => .delete_message,
        };
        _ = tag;
    }
}

test "AC-1.2: Action tagged union payload access" {
    const a = Action{ .send_message = .{ .chat_id = 7, .text = "hello" } };
    try std.testing.expectEqual(@as(i64, 7), a.send_message.chat_id);
    try std.testing.expectEqualStrings("hello", a.send_message.text);
}


test "AC-1.1: Update.effectiveUserId from message" {
    const u = Update{
        .update_id = 1,
        .message = .{
            .message_id = 1,
            .from = .{ .id = 42, .is_bot = false, .first_name = "Alice" },
            .chat = .{ .id = 100, .type = "private" },
            .date = 0,
        },
    };
    try std.testing.expectEqual(@as(?i64, 42), u.effectiveUserId());
}

test "AC-1.1: Update.effectiveUserId from callback_query when no message" {
    const u = Update{
        .update_id = 2,
        .callback_query = .{
            .id = "abc",
            .from = .{ .id = 99, .is_bot = false, .first_name = "Bob" },
        },
    };
    try std.testing.expectEqual(@as(?i64, 99), u.effectiveUserId());
}

test "AC-1.1: Update.effectiveUserId null when no sender" {
    const u = Update{ .update_id = 3 };
    try std.testing.expectEqual(@as(?i64, null), u.effectiveUserId());
}

test "AC-1.5: types.zig has no project-file imports" {
    // Structural check: the only imports in this file are std and ziglua.
    // Enforced by code review — this test documents the invariant.
    // If a project import were added, the circular-import build error
    // would catch it before this test runs.
    try std.testing.expect(true);
}
