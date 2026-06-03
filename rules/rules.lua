-- rules.lua — zora default rules
-- RULES_API_VERSION: 1
--
-- on_message(update) receives a decoded Telegram Update and drives the bot.
-- Outgoing calls use the tg.<method>{...} shorthand, which is identical to
--   bot.emit{ method = "<method>", params = {...} }
-- Calls are fire-and-forget: they are queued during on_message and dispatched
-- after it returns, so on_message just returns {}.
--
-- Per-user and global state persist in SQLite across restarts via the
-- bot.get_*/set_* helpers.
--
-- Commands: /start /stats   (anything else is echoed back)

---@param update table  Decoded Telegram Update object
---@return table        Always {} — outgoing calls are emitted via tg.*
function on_message(update)
    -- Every field of the update is untrusted input; guard before use.
    local msg = update.message
    if not msg or not msg.from or not msg.chat then
        return {}
    end

    local user_id = msg.from.id
    local chat_id = msg.chat.id
    local text    = msg.text or ""

    -- Count this message: per-user state plus a global running total.
    local state = bot.get_user_state(user_id)
    state.count = (state.count or 0) + 1
    bot.set_user_state(user_id, state)

    local total = (tonumber(bot.get_global("total")) or 0) + 1
    bot.set_global("total", tostring(total))

    if text == "/start" then
        tg.sendMessage{
            chat_id = chat_id,
            text    = "Welcome! You are message #" .. total .. ".",
        }

    elseif text == "/stats" then
        tg.sendMessage{
            chat_id = chat_id,
            text    = string.format("Your messages: %d | Total: %d", state.count, total),
        }

    elseif text ~= "" then
        -- Echo fallback. Empty text (e.g. a photo with no caption) is ignored.
        tg.sendMessage{ chat_id = chat_id, text = "Echo: " .. text }
    end

    return {}
end
