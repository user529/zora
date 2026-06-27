# zora

High-performance Telegram bot with dynamic Lua rules.

## Quick start

```bash
export BOT_TOKEN=your_token
export WEBHOOK_SECRET=your_secret
zig build -Doptimize=ReleaseFast
./zora-run.sh          # selects jemalloc/glibc allocator, then runs zora
```

## Configuration

See `MANUAL.md` for the full configuration reference.

## Telegram API schema

`schema/botapi.json` is the machine-readable Telegram Bot API surface used to
validate outgoing calls. It is vendored from
[`PaulSonOfLars/telegram-bot-api-spec`](https://github.com/PaulSonOfLars/telegram-bot-api-spec)
(`api.json`), pinned to commit `9dca8b0ecbf37af83615fc4563db3523a98d182f`. That
repository tracks the official [Telegram Bot API
reference](https://core.telegram.org/bots/api) and regenerates `api.json` from
it; the file lists every method and type as a `{ "methods": ..., "types": ... }`
object.

To update the supported API surface, replace the file with a newer `api.json`
from that repository and update the pinned commit above. No rebuild is
required — the running process hot-reloads the file (see `SCHEMA_FILE` /
`API_VALIDATION` in the configuration table).
