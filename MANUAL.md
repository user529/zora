# Zora — User Manual

Zora is a Telegram bot server that processes incoming updates using rules
written in Lua 5.4. It hot-reloads rules without restarting the process.

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
11. [Encrypting State at Rest](#encrypting-state-at-rest)
12. [Migrating an Existing Database to v2](#migrating-an-existing-database-to-v2)
13. [Metrics](#metrics)
14. [Deployment Notes](#deployment-notes)

---

## Prerequisites

| Requirement | Version |
|---|---|
| Zig | 0.16.0 |
| Linux | x86_64, kernel ≥ 5.x (inotify) |
| FreeBSD | x86_64, 14+ (kqueue) |

No other runtime dependencies. The build compiles SQLite and Lua 5.4 into
the binary.

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

### CPU target

By default the build targets the build host's own CPU (`native`), so the binary
may use instructions the run host lacks and fault with "invalid opcode" (SIGILL).
A larger or newer run host is not a safe bet — CPU vendors differ in which
instructions they support. When you build on one machine and run on another, pin
the CPU with `-Dcpu`:

```bash
# Build for a specific microarchitecture (here, AMD Zen 4)
zig build -Doptimize=ReleaseFast -Dcpu=znver4

# Or build for a portable baseline that runs on any modern x86_64 host
zig build -Doptimize=ReleaseFast -Dcpu=x86_64_v3
```

---

## Configuration

Zora reads all configuration from environment variables. This release has no
config file parser — use a shell wrapper, systemd `EnvironmentFile=`, or any
secret manager that exports env vars.

This manual groups the variables by subsystem in the order an update flows
through the process: `[bot]`, `[server]`, `[worker]`, `[io]`, `[dispatcher]`,
`[metrics]`.

### `[bot]` — token, rules, and storage

| Variable | Default | Required | Description |
|---|---|---|---|
| `BOT_TOKEN` | — | **yes** | Telegram Bot API token from @BotFather |
| `WEBHOOK_SECRET` | — | **yes** | Arbitrary secret string; Telegram sends it in `X-Telegram-Bot-Api-Secret-Token` on every update |
| `BOT_API_BASE` | `https://api.telegram.org` | no | Override the Telegram API base URL (useful for testing with a local mock) |
| `RULES_FILE` | `rules/rules.lua` | no | Path to the Lua rules file |
| `DB_PATH` | `state.db` | no | Path to the SQLite database file |
| `STATE_ENCRYPTION_KEY` | — | no | Passphrase for [state-at-rest encryption](#encrypting-state-at-rest). When set, zora encrypts every stored payload; when unset, the database stays plaintext |
| `SCHEMA_FILE` | `schema/botapi.json` | no | Path to the vendored Telegram Bot API schema used for outgoing-call validation |
| `API_VALIDATION` | `warn` | no | Outgoing-call validation mode: `off` (disabled), `warn` (log but send), `strict` (drop invalid calls) |
| `METRICS_LOG` | `false` | no | **Deprecated.** Set to `true` to emit a counter snapshot to the log every 60 seconds. The snapshot is frozen at an older counter set. Scrape [`METRICS_ADDR`](#metrics) instead |

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
| `WORKER_QUEUE_CAPACITY` | `1024` | Bounded queue depth per worker; the worker drops excess updates with a warning |
| `WORKER_MAX_INFLIGHT` | `64` | Maximum coroutines parked simultaneously per worker thread |
| `WORKFLOW_DEADLINE_MS` | `60000` | Maximum wall-clock lifetime of a single coroutine workflow in milliseconds |
| `JSON_MAX_BYTES` | `1048576` | Maximum input size in bytes accepted by `json.decode`; larger inputs raise a Lua error |

### `[io]` — blocking I/O pool

| Variable | Default | Description |
|---|---|---|
| `IO_POOL_THREADS` | `8` | Number of blocking I/O pool threads (HTTP, exec, shell) |
| `IO_QUEUE_CAPACITY` | `256` | I/O job queue depth shared across all pool threads |
| `IO_JOB_TIMEOUT_MS` | `30000` | Per-job wall-clock timeout in milliseconds; the pool kills jobs that exceed it |
| `PROC_MAX_OUTPUT_BYTES` | `65536` | Maximum bytes of stdout captured from child processes; zora truncates the excess |

### `[dispatcher]` — outbound Telegram calls

| Variable | Default | Description |
|---|---|---|
| `DISPATCHER_THREADS` | `2 * cpu_count` | Outbound HTTP threads sending to the Telegram API (minimum 2). Each sends sequentially, so this multiplies outbound throughput; raise it for high send rates (e.g. premium) |
| `DELAY_QUEUE_CAPACITY` | `4096` | Capacity of the retry-after delay queue. Calls held back by a Telegram rate limit (HTTP 429) wait here; an overflow drops the call with a warning |
| `RETRY_AFTER_MAX_MS` | `60000` | Upper bound, in milliseconds, on how long a 429-throttled call waits before retry. Zora caps longer `retry_after` values from Telegram to this |
| `RETRY_AFTER_DEFAULT_MS` | `1000` | Retry-after wait, in milliseconds, used when a Telegram 429 response omits the duration |

### `[metrics]` — Prometheus scrape endpoint

| Variable | Default | Description |
|---|---|---|
| `METRICS_ADDR` | — | `host:port` for the Prometheus scrape endpoint. Unset disables the endpoint. The port carries no authentication, so bind it to `127.0.0.1` or firewall it. See [Metrics](#metrics) |

### Example

```bash
export BOT_TOKEN="123456:ABCDEFghijklmnopqrstuvwxyz"
export WEBHOOK_SECRET="my-random-secret-string"
export LISTEN_ADDR="0.0.0.0:8443"
export RULES_FILE="/etc/zora/rules.lua"
export DB_PATH="/var/lib/zora/state.db"
export METRICS_ADDR="127.0.0.1:9100"
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
info(main): zora starting (branch=dev release=2 schema=2 rules_api=1 api_validation=warn enc=off)
```

The banner prints six identifiers:

| Field | Meaning |
|---|---|
| `branch` | Git branch at build time |
| `release` | Monotonic build counter |
| `schema` | SQLite schema contract version |
| `rules_api` | Lua `bot.*` API contract version |
| `api_validation` | Active outgoing-call validation mode (`off`, `warn`, or `strict`) |
| `enc` | Whether state-at-rest encryption is on (`on`) or off (`off`) |

### Effective configuration dump

After the banner, zora prints every effective setting, one line per variable,
under the `config` scope. It groups the lines by subsystem — `[bot]`,
`[server]`, `[worker]`, `[io]`, `[dispatcher]`, `[metrics]` — in the order an
update flows through the process. Each value is the one in force after zora applies its
defaults, so the dump is the quickest way to confirm what the process loaded.

```
info(config): [bot] BOT_TOKEN=123****wxYZ
info(config): [bot] WEBHOOK_SECRET=sup****alue
info(config): [bot] BOT_API_BASE=https://api.telegram.org
info(config): [server] LISTEN_ADDR=0.0.0.0:8443
info(config): [worker] WORKER_THREADS=8
info(config): [io] IO_POOL_THREADS=8
info(config): [dispatcher] DISPATCHER_THREADS=16
info(config): [metrics] METRICS_ADDR=disabled
```

Zora masks `BOT_TOKEN`, `WEBHOOK_SECRET`, and `STATE_ENCRYPTION_KEY`: it prints a
short prefix and suffix and replaces the rest with `****`. The mask hides a value of three
characters or fewer in full. It reveals at most the first three and last four
characters, never more than half the value, and never its length — so the dump
is safe to leave in a log. Zora prints every other value in full.

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

### Scheduler handler

The `on_schedule` handler is optional. If defined, it fires when a scheduled
task's delay expires:

```lua
---@param payload table  The payload object passed to bot.schedule_at or bot.schedule_after
---@param id      integer The schedule row id (for dedup/cancellation via bot.unschedule)
---@return table         Array of Action tables (empty array is valid)
function on_schedule(payload, id)
    -- ...
    return { ... }
end
```

**At-least-once delivery:** If the process crashes between firing this handler
and completing the task, the same `id` may fire again after the process
restarts. Keep all side effects idempotent — for example, use a unique key
(like `message_id` from Telegram) instead of a counter. Any external service
the handler contacts must also handle duplicate requests gracefully.

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

Zora dispatches the calls asynchronously after `on_message` returns. It
preserves the order within one invocation: emit calls first, then the
return list.

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

-- Scheduler: schedule a task to fire at a specific Unix timestamp (milliseconds)
-- Returns the schedule row id (an integer)
local id = bot.schedule_at{ at_ms = 1234567890123, payload = { ... } }

-- Scheduler: schedule a task to fire after N seconds from now (N >= 0;
-- a negative or non-finite N raises a Lua error)
-- Returns the schedule row id
local id = bot.schedule_after{ seconds = 60, payload = { ... } }

-- Scheduler: get the current time in milliseconds since Unix epoch
local now_ms = bot.now_ms()

-- Scheduler: cancel a scheduled task by row id (returns true if cancelled, false if not found)
local ok = bot.unschedule(id)
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
loops or infinite recursion. When a call exceeds it, the worker logs a warning,
returns an empty action slice, and continues with the next update.

Zora limits state blobs to **64 KB** serialised and **8 levels** of table
nesting. It rejects writes that exceed these limits with a logged error and
preserves the previous state value.

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

> **A fuller, annotated example** ships at `rules/rules-demo.lua`. It exercises
> the full rule API in one self-contained file — a command-dispatch router with `/start`,
> `/help`, `/stats`, `/menu` (inline keyboard), inline-button callbacks
> (`answerCallbackQuery` + `editMessageText`), and an async `/ping` that uses a
> tracked `bot.send_message` and then edits the message. It uses the `tg.*`
> shorthand throughout and documents `http_request`, `exec`, `shell`, and file
> upload in comments. It contacts no external service, so it is safe to point
> `RULES_FILE` at while exploring.

---

## Async Rules (RULES_API v1)

Lua rules can perform blocking I/O without blocking any worker thread. When a
rule calls a yielding function, the worker parks the coroutine and immediately
picks up the next update. The coroutine resumes when the I/O completes.

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
-- `domain` comes from the user, but `bot.exec` passes it as a single argv
-- element, so there is no shell to inject into.
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

Two limits bound coroutines:

- `WORKER_MAX_INFLIGHT` — if this many coroutines are already parked, the
  worker stops accepting new updates until a slot frees.
- `WORKFLOW_DEADLINE_MS` — the worker reaps coroutines running longer than
  this, discarding the Lua state and freeing the slot. Set it to the maximum
  acceptable round-trip time for the slowest I/O operation a rule might perform.

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
`on_message`; it then logs every update as `on_message not found` and
produces no actions. Check the file before you start or replace it — for
example with `luac -p rules.lua`.

---

## State Management

Zora stores state in SQLite and exposes it to Lua via `bot.*` functions.
Three namespaces exist:

| Namespace | Key type | Use case |
|---|---|---|
| user state | `user_id` (integer) | per-user conversation state, step tracking |
| chat state | `chat_id` (integer) | per-group or per-channel state |
| global state | arbitrary string | shared counters, feature flags |

State values are Lua tables that zora serialises to JSON. A read returns an
empty table `{}` for an unknown key — never `nil`.

### Concurrency

Each worker thread holds its own SQLite connection in WAL mode. WAL lets reads
run concurrently, and SQLite serialises writes internally.

The `hash(user_id) % worker_count` routing steers a user's updates to one
worker in the common case, but this affinity is best-effort, not a guarantee.
Under queue saturation an update overflows to another worker — a different
SQLite connection — and within one worker a coroutine that yields on I/O lets
the next update for the same user run before it resumes. Two updates for one
user can therefore interleave. Write rules that tolerate this: prefer
idempotent updates keyed on something stable (such as `message_id`), and treat
ordering as a hint, not a guarantee.

---

## Database

Zora creates the SQLite database automatically on first run. It checks the
schema version at startup; if the version does not match the compiled-in
`SCHEMA_VERSION`, zora exits with an explicit error rather than silently
migrating.

```
error(main): database 'state.db': SchemaMismatch
```

To inspect or back up the database at any time:

```bash
sqlite3 state.db ".dump"
sqlite3 state.db "SELECT * FROM user_state LIMIT 10;"
sqlite3 state.db "PRAGMA integrity_check;"
```

The current schema version is 2. A database created by an earlier zora (schema 1)
needs a one-time upgrade before this release will open it; see [Migrating an
Existing Database to v2](#migrating-an-existing-database-to-v2). When encryption
is on, the payload columns hold ciphertext, so `.dump` and `SELECT` return BLOBs
rather than readable JSON; see [Encrypting State at Rest](#encrypting-state-at-rest).

---

## Encrypting State at Rest

By default zora stores state as plaintext JSON. Set `STATE_ENCRYPTION_KEY` to a
passphrase and zora encrypts every stored payload instead. The feature is off
unless the variable is set, and plaintext databases are unchanged.

### What it protects

Encryption guards the database file at rest — a copy, a backup, or a stolen disk
— against someone who does not hold the passphrase. It does not protect state
while a rule runs: zora decrypts a value to process it, so the value lives in
process memory, and inside Lua, in the clear. It is also not a defence against a
strong attacker with a large offline budget against a weak passphrase. Pick a
long, high-entropy passphrase and store it in the same secret manager you use for
`BOT_TOKEN`.

### What is and is not encrypted

Zora encrypts the payload of every state row: user state, chat state, global
values, and scheduler payloads. It does not encrypt the keys (user id, chat id,
global key, schedule id, and fire time) or the table structure, so row counts and
timing stay visible. Each encrypted value carries a fixed 40-byte envelope (a
24-byte nonce and a 16-byte tag); there is no other size change.

### How it works

- **Cipher.** XChaCha20-Poly1305, an authenticated cipher. Zora draws a fresh
  24-byte nonce from the system CSPRNG for every write, so two encryptions of the
  same value differ.
- **Key.** Argon2id derives a 32-byte key from the passphrase and a random
  16-byte salt. Zora stores the salt and the Argon2id cost parameters in the
  database, so a later parameter change does not lock out an existing database.
- **Verifier.** On first run zora encrypts a known constant and stores it. On a
  later open it checks the passphrase against that verifier and fails fast on a
  mismatch, before it reads or writes any user data.

### Key hygiene

Zora holds the derived key in a locked memory page (`mlock`, best-effort) so it
does not reach swap, and wipes the key on shutdown. It strips
`STATE_ENCRYPTION_KEY` — along with `BOT_TOKEN` and `WEBHOOK_SECRET` — from the
environment of any child process a rule spawns through `bot.shell`, `bot.exec`,
or `bot.http_request`. The configuration dump masks the passphrase like the other
secrets, and the startup banner shows only `enc=on` or `enc=off`, never the value.

### Enabling it

- **New database.** Set `STATE_ENCRYPTION_KEY` before the first run. Zora creates
  the database encrypted and records the salt, parameters, and verifier.
- **Existing plaintext database.** The running server does not encrypt in place.
  Convert the database with `zora-migrate` (see [Migrating an Existing Database to
  v2](#migrating-an-existing-database-to-v2)).

### Operational behaviour

- A wrong passphrase on startup exits with `WrongEncryptionKey`, before any read
  or write.
- A mode mismatch — the key is set but the database is plaintext, or the key is
  unset but the database is encrypted — exits with `EncryptionModeMismatch` and
  points to the migration tool.
- **The passphrase is not recoverable.** If you lose it, the encrypted data is
  gone; there is no backdoor.
- An encrypted database inspected with `sqlite3` shows ciphertext BLOBs in the
  payload columns, not readable JSON.

---

## Migrating an Existing Database to v2

The state-at-rest encryption feature raised the schema version from 1 to 2 and
changed how payloads are stored. The server refuses to open a v1 database and
exits with `SchemaMismatch`; the `zora-migrate` tool upgrades it. The source file
is never modified — the tool writes a new v2 file, which you swap in once you are
satisfied.

```bash
# Plaintext v1 -> plaintext v2
zora-migrate --in state.db --out state.v2.db

# Plaintext v1 -> encrypted v2 (the same variable the server uses)
STATE_ENCRYPTION_KEY=… zora-migrate --in state.db --out state.v2.db

mv state.v2.db state.db   # once you are satisfied
```

The presence of `STATE_ENCRYPTION_KEY` selects the output: set, the new database
is encrypted (see [Encrypting State at Rest](#encrypting-state-at-rest)); unset,
it is plaintext v2. The tool copies every user, chat, global, and scheduler row —
preserving scheduler ids and pending leases — then reopens the result to confirm
it is valid before finishing. It refuses to overwrite an existing `--out` file and
refuses a source that is not schema v1. Like the server, it hardens its own
process — it locks memory and suppresses core dumps — so the passphrase and the
decrypted data do not reach swap or a core file.

The tool builds with the rest of zora; the binary is `zig-out/bin/zora-migrate`.

---

## Metrics

Zora exposes runtime counters over a Prometheus scrape endpoint. Set
`METRICS_ADDR` to a `host:port` and zora serves the metrics there; leave it
unset and the endpoint does not start. The endpoint runs on its own thread and
touches nothing on the update path, so a scrape never slows the bot. A scrape
that fails is logged at `warn` and dropped.

### The endpoint

```bash
export METRICS_ADDR="127.0.0.1:9100"
./zora-run.sh

# Scrape it:
curl http://127.0.0.1:9100/metrics
```

`GET /metrics` returns the counter set in the Prometheus text exposition
format (version 0.0.4). Every other path and method returns `404`.

The port carries **no authentication**. Anyone who reaches it reads the
counters. Bind it to `127.0.0.1`, or firewall the port so only the scraper
reaches it. A Prometheus scrape job needs only the target address:

```yaml
scrape_configs:
  - job_name: zora
    static_configs:
      - targets: ["127.0.0.1:9100"]
```

### Exposed metrics

Every metric name carries the `zora_` prefix. Counters only rise; gauges rise
and fall. Zora samples the queue depths at scrape time, so they cost nothing
between scrapes.

| Metric | Type | Meaning |
|---|---|---|
| `zora_updates_received_total` | counter | Webhook updates accepted and enqueued |
| `zora_updates_rejected_total{reason}` | counter | Webhook requests rejected before enqueue. `reason`: `forbidden`, `oversize`, `malformed` |
| `zora_updates_processed_total{outcome}` | counter | Handler runs completed. `outcome`: `ok`, `lua_error` |
| `zora_api_calls_total{outcome}` | counter | Outbound Telegram API calls. `outcome`: `ok`, `failed` |
| `zora_api_call_retries_total` | counter | Retry attempts after a failed API send |
| `zora_rules_reloads_total{outcome}` | counter | Hot reloads of the rules file. `outcome`: `ok`, `failed` |
| `zora_scheduler_jobs_fired_total` | counter | Scheduled jobs claimed and dispatched to workers |
| `zora_route_overflow_total` | counter | Updates placed on a non-primary worker because the primary queue was full |
| `zora_route_drop_total` | counter | Updates dropped because every worker queue was full |
| `zora_throttle_429_total` | counter | HTTP 429 responses observed from the Telegram API |
| `zora_throttle_delayed_total` | counter | Calls parked in the delay queue by rate limiting |
| `zora_throttle_shed_total` | counter | Calls dropped on delay-queue overflow |
| `zora_throttle_delay_depth` | gauge | Calls currently parked in the delay queue |
| `zora_io_jobs_total` | counter | I/O jobs executed by the io_pool |
| `zora_io_errors_total` | counter | I/O jobs that ended in an error |
| `zora_io_timeouts_total` | counter | I/O jobs killed at `IO_JOB_TIMEOUT_MS` |
| `zora_io_jobs_inflight` | gauge | I/O jobs currently executing |
| `zora_coroutines_inflight` | gauge | Lua coroutines parked on I/O across all workers |
| `zora_coroutines_reaped_total` | counter | Coroutines dropped at `WORKFLOW_DEADLINE_MS` |
| `zora_tracked_send_failures_total` | counter | Tracked sends that failed or lacked a `message_id` |
| `zora_response_oversize_total` | counter | API replies dropped for exceeding the response ceiling |
| `zora_worker_queue_depth{worker}` | gauge | Updates waiting in each worker queue, one series per worker |
| `zora_dispatcher_queue_depth` | gauge | API calls waiting in the dispatcher queue |
| `zora_build_info{release,branch}` | gauge | Build identity; the value is always `1` |

A ready-made Grafana dashboard covering every metric above ships at
`docs/grafana/zora-dashboard.json` (Grafana 11.x). Import it and pick your
Prometheus datasource when prompted. `docs/grafana/check-dashboard.sh` checks
the dashboard still covers every metric this build exposes.

The `route_overflow` and `route_drop` counters record where the
`hash(user_id) % worker_count` affinity relaxes under load — see
[State Management](#concurrency).

### The deprecated `METRICS_LOG` snapshot

`METRICS_LOG` predates the scrape endpoint and is off by default. When set to
`true`, zora writes a one-line counter snapshot to the log every 60 seconds and
logs a deprecation warning at startup. The snapshot is frozen at an older,
smaller counter set and does not grow with new metrics. Prefer the scrape
endpoint; `METRICS_LOG` stays only for setups without a Prometheus scraper.

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
  with `sqlite3 state.db ".backup backup.db"` while the bot is running. When
  encryption is on, the backup is ciphertext too, so it carries the same
  protection as the live file.
- **Metrics**: Set `METRICS_ADDR` to expose a Prometheus scrape endpoint;
  bind it to `127.0.0.1` or firewall the port, since it carries no auth. See
  [Metrics](#metrics). The older `METRICS_LOG` snapshot line is deprecated.
- **Multiple instances**: Not supported in this beta. A single instance
  handles all traffic.
- **CPU target**: A binary built with the default `native` target may use
  instructions the run host lacks and crash with SIGILL. When the build host and
  run host have different CPUs, pin `-Dcpu` (see [Build](#build)).

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
