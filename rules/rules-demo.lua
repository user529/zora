-- rules-demo.lua — zora capability tour
-- RULES_API_VERSION: 1
--
-- A self-contained demonstration of how zora rules are written. Every live
-- action is a real Telegram Bot API call or a local state operation; this file
-- never contacts any external service. Capabilities that inherently reach
-- outside Telegram (HTTP, subprocess, file upload) are shown as commented
-- ILLUSTRATIONS near the bottom — they are documentation, not executed code.
--
-- Outgoing calls use the tg.<method>{...} shorthand, which is identical to
--   bot.emit{ method = "<method>", params = {...} }
-- (fire-and-forget, dispatched after on_message returns). Handlers are therefore
-- imperative and on_message always returns {}. The one exception is
-- bot.send_message in /ping — a tracked send that returns a message_id.
--
-- on_message(update) is a thin router:
--   1. inline-button taps arrive as update.callback_query (no message text)
--   2. ordinary messages bump counters, then dispatch on the first word
--   3. unknown text is echoed back
--
-- Commands: /start /help /stats /menu /ping   (see the `commands` table)

-- ── command handlers ────────────────────────────────────────────────────────
-- Each handler takes a `ctx` table { user_id, chat_id, text } and emits calls
-- via tg.*; it returns nothing. Handlers are plain locals so they can be
-- referenced from the `commands` dispatch table below.

local function cmd_start(ctx)
    -- Records an onboarding marker in this user's state.
    local us = bot.get_user_state(ctx.user_id)
    us.step = "started"
    bot.set_user_state(ctx.user_id, us)

    tg.sendMessage{
        chat_id = ctx.chat_id,
        text    = "Welcome to the zora demo bot! Send /help to see what it can do.",
    }
end

local function cmd_help(ctx)
    tg.sendMessage{
        chat_id = ctx.chat_id,
        text = table.concat({
            "zora demo commands:",
            "/start — initialise your session",
            "/help  — show this message",
            "/stats — your / chat / global message counts",
            "/menu  — inline keyboard demo",
            "/ping  — async tracked-send + edit demo",
            "anything else is echoed back",
        }, "\n"),
    }
end

local function cmd_stats(ctx)
    local us    = bot.get_user_state(ctx.user_id)
    local cs    = bot.get_chat_state(ctx.chat_id)
    local total = tonumber(bot.get_global("total")) or 0
    tg.sendMessage{
        chat_id = ctx.chat_id,
        text = string.format(
            "Your messages: %d\nMessages in this chat: %d\nTotal across all chats: %d",
            us.count or 0, cs.count or 0, total),
    }
end

local function cmd_menu(ctx)
    -- reply_markup.inline_keyboard is an array of button rows. Each button
    -- carries a callback_data string; Telegram returns it as a callback query
    -- when a user taps the button (see handle_callback).
    tg.sendMessage{
        chat_id = ctx.chat_id,
        text    = "Pick an option:",
        reply_markup = {
            inline_keyboard = {
                { { text = "Show my stats", callback_data = "menu:stats" } },
                { { text = "Close",         callback_data = "menu:close" } },
            },
        },
    }
end

local function cmd_ping(ctx)
    -- bot.send_message is a tracked send: it parks the coroutine and resumes
    -- with the new message's id once Telegram acknowledges the send. The worker
    -- thread processes other updates while the coroutine is parked. This is the
    -- one call that does not use tg.sendMessage, because only the tracked send
    -- returns the message_id required to edit the message.
    --
    -- This models the pattern "post a placeholder, do slow work, then edit the
    -- placeholder with the result". Editing by message_id is idempotent.
    local mid = bot.send_message{ chat_id = ctx.chat_id, text = "⏳ working…" }
    if type(mid) ~= "number" then
        return  -- the send failed (resumed with an error); nothing to edit
    end
    tg.editMessageText{ chat_id = ctx.chat_id, message_id = mid, text = "✅ pong" }
end

-- ── inline-button callback handler ──────────────────────────────────────────
-- Telegram delivers a button tap as update.callback_query. The handler always
-- calls answerCallbackQuery (otherwise the client keeps showing a spinner),
-- then edits the originating message in place. It guards every field, because
-- the update is untrusted input.
function handle_callback(cq)
    -- answerCallbackQuery stops the client's loading spinner.
    tg.answerCallbackQuery{ callback_query_id = cq.id }

    local m = cq.message
    if not m or not m.chat then
        return
    end
    local chat_id    = m.chat.id
    local message_id = m.message_id
    local data       = cq.data or ""

    if data == "menu:stats" then
        local uid   = cq.from and cq.from.id
        local count = uid and (bot.get_user_state(uid).count or 0) or 0
        tg.editMessageText{
            chat_id    = chat_id,
            message_id = message_id,
            text       = "You have sent " .. count .. " message(s).",
        }
    elseif data == "menu:close" then
        tg.editMessageText{ chat_id = chat_id, message_id = message_id, text = "Menu closed." }
    end
end

-- ── dispatch table ──────────────────────────────────────────────────────────
-- Adding a command is one line here plus one handler function above.

local commands = {
    ["/start"] = cmd_start,
    ["/help"]  = cmd_help,
    ["/stats"] = cmd_stats,
    ["/menu"]  = cmd_menu,
    ["/ping"]  = cmd_ping,
}

-- ── router ──────────────────────────────────────────────────────────────────

---@param update table  Decoded Telegram Update object
---@return table        Always {} — outgoing calls are emitted via tg.*
function on_message(update)
    -- 1) Inline-button taps carry no message text; the router handles them first.
    if update.callback_query then
        handle_callback(update.callback_query)
        return {}
    end

    -- 2) Ordinary messages. Every field is untrusted input.
    local msg = update.message
    if not msg or not msg.from or not msg.chat then
        return {}
    end

    local ctx = {
        user_id = msg.from.id,
        chat_id = msg.chat.id,
        text    = msg.text or "",
    }

    -- 3) Every message bumps three counters: per-user, per-chat, and a global total.
    local us = bot.get_user_state(ctx.user_id)
    us.count = (us.count or 0) + 1
    bot.set_user_state(ctx.user_id, us)

    local cs = bot.get_chat_state(ctx.chat_id)
    cs.count = (cs.count or 0) + 1
    bot.set_chat_state(ctx.chat_id, cs)

    local total = (tonumber(bot.get_global("total")) or 0) + 1
    bot.set_global("total", tostring(total))

    -- 4) The first whitespace-delimited word selects a handler. A trailing
    --    "@botname" suffix (added by Telegram in group chats) is stripped first.
    local word = ctx.text:match("^(%S+)") or ""
    word = word:gsub("@[%w_]+$", "")
    local handler = commands[word]
    if handler then
        handler(ctx)
    elseif ctx.text ~= "" then
        -- 5) Echo fallback. Empty text (e.g. a photo with no caption) is ignored.
        tg.sendMessage{ chat_id = ctx.chat_id, text = "Echo: " .. ctx.text }
    end

    return {}
end

-- ── ILLUSTRATIONS (documentation only — never executed) ──────────────────────
-- The engine also provides external I/O and file upload. A self-contained demo
-- cannot run these safely, so they appear here for reference only. In a real
-- rule they would sit inside on_message or a handler.
--
-- HTTP request (parks the coroutine; returns { status = <int>, body = <string> }):
--   local resp = bot.http_request{ method = "GET", url = "https://example.invalid",
--                                  headers = "", body = "" }
--   bot.log("info", "status " .. resp.status)
--
-- Run a subprocess by argv — no shell, safe for untrusted args
-- (returns { exit_code = <int>, stdout = <string> }):
--   local r = bot.exec{ argv = { "echo", "hello" } }
--
-- Run a shell command — always wrap untrusted input with bot.shell_quote:
--   local user_input = "name; rm -rf /"          -- never trust this
--   local r = bot.shell{ command = "echo " .. bot.shell_quote(user_input) }
--
-- Upload a file from disk (multipart) via a real Telegram method:
--   tg.sendDocument{ chat_id = chat_id, document = { __file = "/abs/path/report.pdf" } }
--
-- Upload bytes built in-memory (no disk read) — `filename` is required:
--   local bytes = "col1,col2\n1,2\n"
--   tg.sendPhoto{ chat_id = chat_id,
--                 photo = { __file_bytes = bytes, filename = "data.csv" } }
--
-- Helpers available without `require`:
--   local t   = json.decode('{"a":1}')   -- string  → table
--   local s   = json.encode({ a = 1 })    -- table   → string
--   local enc = bot.url_encode("a b&c")   -- RFC 3986 percent-encoding
--
-- Outgoing-call forms — all equivalent. This demo uses tg.* everywhere. The
-- same call can go through bot.emit, or be returned in an array of action tables:
--   tg.sendMessage{ chat_id = chat_id, text = "hi" }
--   bot.emit{ method = "sendMessage", params = { chat_id = chat_id, text = "hi" } }
--   return { { method = "sendMessage", params = { chat_id = chat_id, text = "hi" } } }
