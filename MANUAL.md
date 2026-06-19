# Zora — User Manual

Zora is a Telegram bot server that processes incoming updates using rules
written in Lua 5.4. Rules are hot-reloaded without restarting the process.

Zora is in beta. The features in this manual work as described. Some
interfaces may still change; the `schema` and `rules_api` version numbers in
the startup log mark the current contracts.

---

## Contents

1. [Prerequisites](#prerequisites)
2. [Build](#build)
3. [Configuration](#configuration)
4. [Registering the Webhook](#registering-the-webhook)
5. [Running](#running)
6. [Writing Rules](#writing-rules)
7. [Async Rules (RULES_API v1)](#async-rules-rules_api-v1)
8. [Hot-Reload](#hot-reload)
9. [State Management](#state-management)
10. [Database](#database)
11. [Deployment Notes](#deployment-notes)

---

## Prerequisites

| Requirement | Version |
|---|---|
| Zig | 0.16.0 |
| Linux | x86_64, kernel ≥ 5.x (inotify) |
| FreeBSD | x86_64, 14+ (kqueue) |

No other runtime dependencies. SQLite and Lua 5.4 are compiled in.

> **Note — jemalloc (optional, recommended).** Zora runs on the system
> allocator with no extra package. On Linux with glibc, the allocator can hold
> a large resident set under sustained load. Preloading jemalloc keeps the RSS
> flat. The `zora-run.sh` wrapper finds and preloads jemalloc through
> `LD_PRELOAD` when the library is present, so the only step is to install it
> (for example `apt install libjemalloc2`). If you set `LD_PRELOAD` yourself,
> the wrapper leaves it alone. See [Deployment Notes](#memory-and-the-zora-runsh-wrapper)
> for details.

---

## Build

```bash
# Debug build (with safety checks and GPA leak detection)
zig build

# Production build
zig build -Doptimize=ReleaseFast

# Cross-compile for FreeBSD from Linux
zig build -Dtarget=x86_64-freebsd -Doptimize=ReleaseFast

# Run unit tests
zig build test
```

The resulting binary is `zig-out/bin/zora`.

---

## Configuration

All configuration is read from environment variables. There is no config file
parser in this release — use a shell wrapper, systemd `EnvironmentFile=`, or
any secret manager that exports env vars.

The variables are grouped by subsystem in the order an update flows through the
process: `[bot]`, `[server]`, `[worker]`, `[io]`, `[dispatcher]`.

### `[bot]` — token, rules, and storage

| Variable | Default | Required | Description |
|---|---|---|---|
| `BOT_TOKEN` | — | **yes** | Telegram Bot API token from @BotFather |
| `WEBHOOK_SECRET` | — | **yes** | Arbitrary secret string; Telegram sends it in `X-Telegram-Bot-Api-Secret-Token` on every update |
| `BOT_API_BASE` | `https://api.telegram.org` | no | Override the Telegram API base URL (useful for testing with a local mock) |
| `RULES_FILE` | `rules/rules.lua` | no | Path to the Lua rules file |
| `DB_PATH` | `state.db` | no | Path to the SQLite database file |
| `SCHEMA_FILE` | `schema/botapi.json` | no | Path to the vendored Telegram Bot API schema used for outgoing-call validation |
| `API_VALIDATION` | `warn` | no | Outgoing-call validation mode: `off` (disabled), `warn` (log but send), `strict` (drop invalid calls) |
| `METRICS_LOG` | `true` | no | Emit per-dispatcher stats (sent / discarded / queue depth) every 60 seconds; set to `false` or `0` to suppress |

**`BOT_TOKEN` and `WEBHOOK_SECRET` are mandatory.** The process exits with a
clear error message if either is absent.

### `[server]` — inbound webhook

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `0.0.0.0:8443` | `host:port` the HTTP server binds to |
| `WEBHOOK_POOL_THREADS` | `cpu_count` | Number of inbound webhook connection-handler threads (minimum 2). One accept thread hands accepted connections to this pool |

### `[worker]` — Lua workers

| Variable | Default | Description |
|---|---|---|
| `WORKER_THREADS` | `cpu_count` | Number of Lua worker threads (minimum 2) |
| `WORKER_QUEUE_CAPACITY` | `1024` | Bounded queue depth per worker; excess updates are dropped with a warning |
| `WORKER_MAX_INFLIGHT` | `64` | Maximum coroutines parked simultaneously per worker thread |
| `WORKFLOW_DEADLINE_MS` | `60000` | Maximum wall-clock lifetime of a single coroutine workflow in milliseconds |
| `JSON_MAX_BYTES` | `1048576` | Maximum input size in bytes accepted by `json.decode`; larger inputs raise a Lua error |

### `[io]` — blocking I/O pool

| Variable | Default | Description |
|---|---|---|
| `IO_POOL_THREADS` | `8` | Number of blocking I/O pool threads (HTTP, exec, shell) |
| `IO_QUEUE_CAPACITY` | `256` | I/O job queue depth shared across all pool threads |
| `IO_JOB_TIMEOUT_MS` | `30000` | Per-job wall-clock timeout in milliseconds; jobs exceeding this are killed |
| `PROC_MAX_OUTPUT_BYTES` | `65536` | Maximum bytes of stdout captured from child processes; excess is truncated |

### `[dispatcher]` — outbound Telegram calls

| Variable | Default | Description |
|---|---|---|
| `DISPATCHER_THREADS` | `2 * cpu_count` | Outbound HTTP threads sending to the Telegram API (minimum 2). Each sends sequentially, so this multiplies outbound throughput; raise it for high send rates (e.g. premium) |
| `DELAY_QUEUE_CAPACITY` | `4096` | Capacity of the retry-after delay queue. Calls held back by a Telegram rate limit (HTTP 429) wait here; an overflow drops the call with a warning |
| `RETRY_AFTER_MAX_MS` | `60000` | Upper bound, in milliseconds, on how long a 429-throttled call waits before retry. Longer `retry_after` values from Telegram are capped to this |
| `RETRY_AFTER_DEFAULT_MS` | `1000` | Retry-after wait, in milliseconds, used when a Telegram 429 response omits the duration |

### Example

```bash
export BOT_TOKEN="123456:ABCDEFghijklmnopqrstuvwxyz"
export WEBHOOK_SECRET="my-random-secret-string"
export LISTEN_ADDR="0.0.0.0:8443"
export RULES_FILE="/etc/zora/rules.lua"
export DB_PATH="/var/lib/zora/state.db"
export METRICS_LOG="true"
./zora-run.sh
```

---

## Registering the Webhook

Telegram delivers updates by POSTing to an HTTPS URL you provide. Zora
handles plain HTTP internally; use nginx or Caddy for TLS termination in
front of it.

```bash
BOT_TOKEN="123456:ABCDEFghijklmnopqrstuvwxyz"
WEBHOOK_URL="https://yourdomain.com/webhook"
WEBHOOK_SECRET="my-random-secret-string"

curl -X POST "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \
     -H "Content-Type: application/json" \
     -d "{
       \"url\": \"${WEBHOOK_URL}\",
       \"secret_token\": \"${WEBHOOK_SECRET}\",
       \"allowed_updates\": [\"message\", \"callback_query\"]
     }"
```

Verify the webhook is active:

```bash
curl "https://api.telegram.org/bot${BOT_TOKEN}/getWebhookInfo"
```

### Minimal nginx config (TLS termination)

```nginx
server {
    listen 443 ssl;
    server_name yourdomain.com;

    ssl_certificate     /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;

    location /webhook {
        proxy_pass http://127.0.0.1:8443;
        proxy_set_header Host $host;
    }
}
```

---

## Running

```bash
# Foreground (logs to stderr)
BOT_TOKEN="..." WEBHOOK_SECRET="..." ./zora-run.sh

# Background with nohup
BOT_TOKEN="..." WEBHOOK_SECRET="..." nohup ./zora-run.sh &

# Graceful shutdown
kill -TERM $PID
```

On `SIGTERM` or `SIGINT`, zora drains in-flight work and exits cleanly.
The GPA leak detector (Debug builds only) reports any unreleased allocations
after shutdown.

### Startup log

```
info(main): zora starting (branch=dev release=2 schema=1 rules_api=1 api_validation=warn)
```

The banner prints five identifiers:

| Field | Meaning |
|---|---|
| `branch` | Git branch at build time |
| `release` | Monotonic build counter |
| `schema` | SQLite schema contract version |
| `rules_api` | Lua `bot.*` API contract version |
| `api_validation` | Active outgoing-call validation mode (`off`, `warn`, or `strict`) |

### Effective configuration dump

After the banner, zora prints every effective setting, one line per variable,
under the `config` scope. The lines are grouped by subsystem — `[bot]`,
`[server]`, `[worker]`, `[io]`, `[dispatcher]` — in the order an update flows
through the process. Each value is the one in force after defaults are applied,
so the dump is the quickest way to confirm what the process actually loaded.

```
info(config): [bot] BOT_TOKEN=123****wxYZ
info(config): [bot] WEBHOOK_SECRET=sup****alue
info(config): [bot] BOT_API_BASE=https://api.telegram.org
info(config): [server] LISTEN_ADDR=0.0.0.0:8443
info(config): [worker] WORKER_THREADS=8
info(config): [io] IO_POOL_THREADS=8
info(config): [dispatcher] DISPATCHER_THREADS=16
```

`BOT_TOKEN` and `WEBHOOK_SECRET` are masked: zora prints a short prefix and
suffix and replaces the rest with `****`. A value of three characters or fewer
is hidden in full. The mask reveals at most the first three and last four
characters, never more than half the value, and never its length — so the dump
is safe to leave in a log. Every other value is printed in full.

---

## Writing Rules

Create (or edit) the file pointed to by `RULES_FILE`. It must define a global
function `on_message`.

### Function signature

```lua
---@param update table  Decoded Telegram Update object
---@return table        Array of Action tables (empty array is valid)
function on_message(update)
    -- ...
    return { ... }
end
```

`update` mirrors the [Telegram Update object](https://core.telegram.org/bots/api#update)
decoded directly from JSON into a Lua table. Fields not present in the
incoming JSON are `nil`.

### Outgoing calls

Return an array of API-call tables. Each call must have a `method` field naming
a Telegram Bot API method, and an optional `params` table:

```lua
-- Send a plain text message
{ method = "sendMessage", params = { chat_id = 123456789, text = "Hello!" } }

-- Send a message with parse mode
{ method = "sendMessage", params = { chat_id = 123456789, text = "<b>Bold</b>",
                                     parse_mode = "HTML" } }

-- Answer an inline keyboard callback query
{ method = "answerCallbackQuery",
  params = { callback_query_id = update.callback_query.id, text = "Done" } }

-- Delete a message
{ method = "deleteMessage", params = { chat_id = 123456789, message_id = 42 } }
```

You can also fire calls mid-handler using `bot.emit{...}` (collected before the
return list) or the `tg.*` shorthand:

```lua
-- These are equivalent:
bot.emit{ method = "sendMessage", params = { chat_id = 1, text = "hi" } }
tg.sendMessage{ chat_id = 1, text = "hi" }
```

Calls are dispatched asynchronously after `on_message` returns. The order
within one invocation (emit calls first, then return-list) is preserved.

### `bot.*` API

These functions are available inside `on_message`:

```lua
-- Per-user state (persisted in SQLite, keyed by Telegram user_id)
local state = bot.get_user_state(user_id)   -- returns table (empty on first call)
bot.set_user_state(user_id, state)          -- persists the table

-- Per-chat state (keyed by Telegram chat_id)
local state = bot.get_chat_state(chat_id)
bot.set_chat_state(chat_id, state)

-- Global key-value store (string → string)
local val = bot.get_global("key")           -- returns string or nil
bot.set_global("key", "value")

-- Logging (level: "info" | "warn" | "error")
bot.log("info", "processing user " .. user_id)

-- Fire-and-forget outgoing call (collected ahead of the return list)
bot.emit{ method = "sendMessage", params = { chat_id = ..., text = "..." } }
-- Shorthand: tg.<method>{params} ≡ bot.emit{ method="<method>", params=params }
tg.sendMessage{ chat_id = ..., text = "..." }
```

**API version guard** (optional):
```lua
assert(bot.rules_api_version == 1,
       "rules.lua requires rules_api_version 1, got " .. tostring(bot.rules_api_version))
```

### Available stdlib

The following Lua standard libraries are available: `base`, `math`, `string`,
`table`, `utf8`.

The following are **disabled** for security: `io`, `os`, `package`, `debug`,
`coroutine`, `require`, `dofile`, `loadfile`.

### Execution limits

Each `on_message` call has a hard instruction-count limit to prevent runaway
loops or infinite recursion. Exceeding it logs a warning and returns an empty
action slice — the worker continues processing the next update.

State blobs are limited to **64 KB** serialised and **8 levels** of table
nesting. Writes that exceed these limits are rejected with a logged error;
the previous state value is preserved.

### Example rules file

```lua
function on_message(update)
    local msg = update.message
    if not msg then return {} end

    local user_id = msg.from.id
    local chat_id = msg.chat.id
    local text    = msg.text or ""

    local state = bot.get_user_state(user_id)
    state.count = (state.count or 0) + 1

    local total = tonumber(bot.get_global("total") or "0") + 1
    bot.set_global("total", tostring(total))

    if text == "/start" then
        state.step = "started"
        tg.sendMessage{ chat_id = chat_id,
                        text = "Welcome! You are message #" .. total }

    elseif text == "/stats" then
        tg.sendMessage{ chat_id = chat_id,
                        text = string.format("Your messages: %d | Total: %d",
                                  state.count, total) }

    else
        tg.sendMessage{ chat_id = chat_id, text = "Echo: " .. text }
    end

    bot.set_user_state(user_id, state)
    return {}
end
```

> **A fuller, annotated example** ships at `rules/rules-demo.lua`. It is a
> self-contained capability tour — a command-dispatch router with `/start`,
> `/help`, `/stats`, `/menu` (inline keyboard), inline-button callbacks
> (`answerCallbackQuery` + `editMessageText`), and an async `/ping` that uses a
> tracked `bot.send_message` and then edits the message. It uses the `tg.*`
> shorthand throughout and documents `http_request`, `exec`, `shell`, and file
> upload in comments. It contacts no external service, so it is safe to point
> `RULES_FILE` at while exploring.

---

## Async Rules (RULES_API v1)

Lua rules can perform blocking I/O without blocking any worker thread. When a
rule calls a yielding function the coroutine is parked and the worker
immediately picks up the next update. The coroutine resumes when the I/O
completes.

### Yielding functions (park the coroutine, return when I/O completes)

```lua
-- HTTP request; returns { status: integer, body: string, headers: table }
-- `headers` in the request is an optional map of name → value (both strings).
-- A transport error returns status = 0 with an empty headers table, so the
-- result is always safe to index.
local resp = bot.http_request{
    method  = "GET",
    url     = "https://example.com/api",
    headers = { Authorization = "Bearer " .. token, Accept = "application/json" },
    body    = "",
}
-- The response headers table keeps each name in the server's original casing.
-- Indexing is case-insensitive, so all three reads return the same value:
local ctype = resp.headers["content-type"]   -- or ["Content-Type"], ["CONTENT-TYPE"]
-- pairs() iterates the original casing.
for name, value in pairs(resp.headers) do bot.log("info", name .. ": " .. value) end

-- Run a subprocess by argv (no shell — safe for untrusted input)
-- `domain` comes from the user but is passed as a single argv element,
-- so there is no shell to inject into.
local r = bot.exec{ argv = { "dig", "+short", domain } }
-- r.exit_code: integer, r.stdout: string

-- Run a shell command (use bot.shell_quote for untrusted parts)
local r = bot.shell{ command = "echo hello" }

-- Send a message and get back the message_id (tracked send)
local mid = bot.send_message{ chat_id = chat_id, text = "🔍 searching…" }
```

### Fire-and-forget (does not park the coroutine)

```lua
bot.emit{ method = "sendMessage", params = { chat_id = id, text = "hi" } }
```

### Helpers

```lua
json.decode(str)      -- returns table; raises Lua error if input > JSON_MAX_BYTES or invalid JSON
json.encode(table)    -- returns string

bot.url_encode(str)   -- RFC 3986 percent-encoding; returns string
bot.shell_quote(str)  -- safe quoting for sh -c command strings; returns string
```

### Resource limits

Coroutines are bounded by two limits:

- `WORKER_MAX_INFLIGHT` — if this many coroutines are already parked, the
  worker stops accepting new updates until a slot frees.
- `WORKFLOW_DEADLINE_MS` — coroutines running longer than this are reaped (Lua
  state discarded, slot freed). Set to the maximum acceptable round-trip time
  for the slowest I/O operation a rule might perform.

---

## Hot-Reload

Zora watches `RULES_FILE` for changes using kernel APIs (inotify on Linux,
kqueue on FreeBSD). No polling, no restart required.

**How to update rules:**

```bash
# Atomic rename (recommended — avoids a window where the file is empty)
cp rules_new.lua rules.lua.tmp && mv rules.lua.tmp rules.lua

# Or edit in place with your editor — vim, nano, etc. all work
vim rules/rules.lua
```

**What happens on change:**

1. The watcher thread detects the change and increments an atomic reload
   counter. The watcher does not parse or validate the file.
2. Each worker compares the counter to its own copy before it handles the
   next update. When the counter is ahead, the worker reloads its own Lua
   state from the file and advances its copy.
3. If the new file fails to load (syntax or load error), the worker logs
   `worker N: reload failed` at `WARN` level and keeps the `on_message` it
   already had. The counter copy still advances, so a broken file is not
   retried until the file changes again.
4. If the rules file is deleted, the watcher logs a warning and the workers
   keep their current rules.

Each worker reloads on its own, so during a reload some workers may run the
new rules for a short window while others still run the old ones.

**Caveats:** A worker reloads into its existing Lua state. A syntax error in
the new file leaves the previously loaded `on_message` in place, because the
failed chunk never runs. A file that parses but raises an error part-way
through can leave the state partly updated. There is no `.bak` fallback. If
the rules file is missing or broken at startup, the worker starts without an
`on_message`; every update is then logged as `on_message not found` and
produces no actions. Check the file before you start or replace it — for
example with `luac -p rules.lua`.

---

## State Management

State is stored in SQLite and is accessible from Lua via `bot.*` functions.
Three namespaces exist:

| Namespace | Key type | Use case |
|---|---|---|
| user state | `user_id` (integer) | per-user conversation state, step tracking |
| chat state | `chat_id` (integer) | per-group or per-channel state |
| global state | arbitrary string | shared counters, feature flags |

State values are Lua tables serialised to JSON. All reads return an empty
table `{}` for unknown keys — never `nil`.

### Concurrency

Each worker thread holds its own SQLite connection in WAL mode. Reads are
concurrent. Writes are serialised by SQLite internally. The `hash(user_id) %
worker_count` routing guarantee means all updates from one user are always
processed by the same worker in order — no cross-worker state races for
user-scoped data.

---

## Database

The SQLite database is created automatically on first run. The schema version
is checked at startup; if it does not match the compiled-in `SCHEMA_VERSION`,
zora exits with an explicit error rather than silently migrating.

```
error(main): database 'state.db': SchemaMismatch
```

To inspect or back up the database at any time:

```bash
sqlite3 state.db ".dump"
sqlite3 state.db "SELECT * FROM user_state LIMIT 10;"
sqlite3 state.db "PRAGMA integrity_check;"
```

---

## Deployment Notes

- **TLS**: Zora speaks plain HTTP. Put nginx or Caddy in front for TLS
  termination. Telegram requires HTTPS for webhooks.
- **Firewall**: Bind `LISTEN_ADDR` to `127.0.0.1` and expose only the
  reverse-proxy port publicly.
- **Systemd**: Use `EnvironmentFile=/etc/zora/env` to supply secrets;
  set `Restart=on-failure`. For `ExecStart`, prefer the `zora-run.sh`
  wrapper over the bare binary (see below).
- **Database backups**: `state.db` is a standard SQLite WAL file. Copy it
  with `sqlite3 state.db ".backup backup.db"` while the bot is running.
- **Metrics**: Set `METRICS_LOG=true` to emit per-dispatcher stats every
  60 seconds to the log. Redirect to a file or pipe to a log aggregator.
- **Multiple instances**: Not supported in this beta. A single instance
  handles all traffic.

### Memory and the `zora-run.sh` wrapper

On Linux with glibc, the standard allocator can keep a large resident set
under sustained load. Preloading jemalloc keeps the RSS flat. The `zora-run.sh`
wrapper handles this with no rebuild:

1. If you set `LD_PRELOAD` yourself, the wrapper uses it and does not search.
2. On FreeBSD it does nothing — jemalloc is already the system `libc` malloc.
3. On Linux it finds jemalloc and preloads it through `LD_PRELOAD`.
4. If jemalloc is absent, it caps glibc arenas (`MALLOC_ARENA_MAX=2`); on musl
   it leaves the standard allocator in place.

Install jemalloc to get step 3 (for example `apt install libjemalloc2`); the
wrapper preloads it automatically on the next start.

The wrapper is POSIX `sh` and `exec`s the binary, so signals and `$MAINPID`
pass straight through. It works the same as a systemd `ExecStart`, an
rc.d / runit / dinit run script, or a container ENTRYPOINT.

```bash
# Bare binary — system allocator
BOT_TOKEN="..." WEBHOOK_SECRET="..." ./zig-out/bin/zora

# Through the wrapper — jemalloc preloaded when present
BOT_TOKEN="..." WEBHOOK_SECRET="..." ./zora-run.sh
```
