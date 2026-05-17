# zora

_zig Telegram bot with Lua hot-reload rules_

## Status

**BETA**

The prototype is ready - see the `dev` branch.

## About

The vision is simple: a high-performance core that stays stable, while interpretable layer for message processing provides agility for rule updates without recompilation.

Lua 5.4 is choosen as interpretible engine. It provide the balance of simplicity and power, allowing creating sophisticated logic for rules with acceptable trade-offs.

Linux and FreeBSD are supported so far.

## Testing

Config:
* 12 workers 
* 8 dispatchers 
* state.db in :memory:
* 50,000 distinct user IDs 
* traffic mix 70% echo / 20% /start / 10% /stats

### Throughput & Latency

| Metric | Value |
| ------ | ----- |
| Total requests | 3,941,999 |
| Average throughput | 2,265.5 msg/sec (target: 2,500) |
| Error rate | 0.00000% |
| avg latency | 0.287 ms |
| p50 | 0.179 ms |
| p90 | 0.377 ms |
| p95 | 0.565 ms |
| p99 | 2.750 ms |
| p99.9 | 8.494 ms |

### CPU & Memory 

| Stage | CPU avg | CPU max | Mem avg | Mem max |
| --- | --- | --- | --- | --- |
|  warmup | 27.6% | 31.9% | 7.4 MB | 8.6 MB |
|  ramp | 59.6% | 87.0% | 8.9 MB | 9.7 MB |
|  steady-1 | 113.0% | 127.4% | 9.3 MB | 10.2 MB |
|  burst-ramp | 129.4% | 132.7% | 9.4 MB | 10.1 MB |
|  burst | 139.4% | 144.9% | 9.7 MB | 10.3 MB |
|  recovery | 154.6% | 157.2% | 10.1 MB | 11.4 MB |
|  steady-2 | 157.4% | 157.8% | 10.5 MB | 11.1 MB |
|  cooldown | 156.0% | 157.8% | 10.6 MB | 11.4 MB |

CPU% is per-core (100% = 1 core fully used). At 2,500 req/s the bot used ~1.1–1.3 cores; at 5,000 req/s ~1.4 cores, out of 12 available workers.

## Features

### Configuration

All configuration is read from environment variables at startup. 
No config file parsing. Use shell wrapper or docker/podman environment.

| Variable | Default | Description |
| --- | --- | --- |
| BOT_TOKEN | required | Telegram Bot API token |
| WEBHOOK_SECRET | required | Validates incoming webhook header |
| LISTEN_ADDR | 0.0.0.0:8443 | Bind address for the webhook endpoint |
| RULES_FILE | rules/rules.lua | Path to the Lua rules file |
| DB_PATH | state.db | SQLite database path (:memory: supported) |
| WORKER_COUNT | CPU count, min 2 | Worker thread pool size |
| QUEUE_CAPACITY | 255 | Bounded queue depth per worker |
| DISPATCHER_THREADS | 2 | Outbound HTTP thread count |

Missing required variables cause a clear error and immediate exit before the socket is bound. Invalid (non-numeric) optional values are rejected with InvalidConfig.

### Webhook HTTP server

* Binds TCP on LISTEN_ADDR, single accept thread, one thread per connection
* Validates X-Telegram-Bot-Api-Secret-Token with XOR
* Rejects wrong method, wrong path, or missing/wrong secret with 403
* Enforces Content-Length limit; rejects bodies > 1 MB with 413
* Runs a JSON depth scan (max 32 levels) before parsing — deeply-nested bombs return 400
* On valid request: parses body as types.Update, routes to queue[hash(user_id) % worker_count], responds 200 OK immediately
* Server returns 200 without waiting for Lua or dispatch — Telegram gets its fast ACK

### Lua Rules Engine

One lua_State per worker, not shared.
*Sandbox*: only standard libraries are loaded: base, math, string, table, utf8. The following are explicitly blocked after base opens:
* io.open, os.execute, require, package.loadlib, debug.getregistry, dofile, loadfile

*Execution limit* — lua_sethook with LUA_MASKCOUNT at 10,000,000 instructions per on_message call (~100 ms equivalent). Runaway loops, infinite recursion, and huge allocations all hit this limit; the worker logs the error and continues.

#### RULES_API_VERSION 1 

*API available inside Lua*

```lua
bot.get_user_state(user_id: integer) -> table
bot.set_user_state(user_id: integer, data: table)
bot.get_chat_state(chat_id: integer) -> table
bot.set_chat_state(chat_id: integer, data: table)
bot.get_global(key: string) -> string | nil
bot.set_global(key: string, value: string)
bot.log(level: string, message: string)  -- "info"|"warn"|"error"
bot.rules_api_version                    -- integer, read-only
```

#### Two-phase execution

`on_message` returns a Lua table of action tables; the engine converts them into []Action structs. Lua never calls Telegram directly.
Supported action types returned from `on_message`:

```lua
{ action = "send_message",    chat_id = i64, text = string }
{ action = "send_message_ex", chat_id = i64, text = string, opts = table }
{ action = "answer_callback", callback_query_id = string, text = string|nil }
{ action = "delete_message",  chat_id = i64, message_id = i64 }
```


### Hot-Reload Lua Rules file

* Dedicated watcher thread monitors RULES_FILE using kernel APIs — no polling
* Linux: inotify on the parent directory (handles CLOSE_WRITE, MOVED_TO, IN_DELETE)
* FreeBSD: kqueue with EVFILT_VNODE (NOTE_WRITE | NOTE_RENAME)
* Other platforms: stat() every 500 ms (development fallback)
* Before incrementing the reload counter, the watcher validates the new file by loading it in a throw-away Lua state. Invalid Lua syntax → counter unchanged, workers keep old rules, reload_failures_total incremented
* On success: writes a .bak copy, then atomically increments reload_version
* Workers pick up the new version lazily at the start of their next message
* Startup fallback: if RULES_FILE fails to load, the worker tries <rules_file>.bak before giving up

### State Store 

SQLite in WAL mode with synchronous=NORMAL and busy_timeout=5000ms. Each worker holds its own connection; concurrent reads are free, writes are serialized by SQLite internally.

### Lua - JSON Serializer 

* Bidirectional: Lua table → JSON string → SQLite TEXT and the reverse.
* Lua arrays → JSON arrays
* Lua tables with string/mixed keys → JSON objects
* Full primitive support: string, integer, float, boolean, nil
* Max nesting depth: 8 levels — deeper tables return error.MaxDepthExceeded
* Max serialized size: 64 KB — larger tables return error.MaxSizeExceeded
* Lua integers up to 2^53 and the full int64 range round-trip without floating-point loss

### Dispatcher

* Fixed thread pool consuming from a shared bounded Action queue (capacity 4096)
* Each thread maintains a persistent HTTP/1.1 connection to the Telegram API (keep-alive)
* Supports BOT_API_BASE env override for redirecting to a local stub during testing
* On send failure: retries once after 1 second (reinitializes the client to avoid stale connections), then discards
* Action string payloads are freed by the dispatcher after every send attempt, successful or not

### Versioning

Startup log line printed before the server accepts any connection:
>
> info: zora starting (branch=dev release=2 schema=1 rules_api=1)
>

### Graceful Shutdown

SIGTERM and SIGINT are handled. 
Shutdown order:

> stop accepting → join workers → drain worker queues → close DB connections → join dispatchers → drain dispatcher queue.

In-flight requests complete before queues are freed.
