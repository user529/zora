-- Zora example rules file
-- RULES_API_VERSION: 1
--
-- Actions are generic Telegram API calls: { method = "<apiMethod>", params = {...} }
-- (ADR-0001 §AD-1).  `params` is serialized to the JSON request body verbatim.

function on_message(update)
    local msg = update.message
    if not msg then return {} end

    local user_id = msg.from.id
    local chat_id = msg.chat.id
    local text    = msg.text or ""

    -- Load state
    local state = bot.get_user_state(user_id)
    state.count = (state.count or 0) + 1

    -- Global counter
    local total = tonumber(bot.get_global("total") or "0") + 1
    bot.set_global("total", tostring(total))

    -- Rules
    local actions = {}

    if text == "/start" then
        state.step = "started"
        table.insert(actions, {
            method = "sendMessage",
            params = {
                chat_id = chat_id,
                text    = "Welcome! You are message #" .. total,
            },
        })

    elseif text == "/stats" then
        table.insert(actions, {
            method = "sendMessage",
            params = {
                chat_id = chat_id,
                text    = string.format("Your messages: %d | Total: %d",
                              state.count, total),
            },
        })

    else
        table.insert(actions, {
            method = "sendMessage",
            params = {
                chat_id = chat_id,
                text    = "Echo: " .. text,
            },
        })
    end

    -- Save state
    bot.set_user_state(user_id, state)

    return actions
end
