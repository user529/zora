PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS user_state (
    user_id  INTEGER PRIMARY KEY,
    data     TEXT    NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE IF NOT EXISTS chat_state (
    chat_id  INTEGER PRIMARY KEY,
    data     TEXT    NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE IF NOT EXISTS global_state (
    key      TEXT PRIMARY KEY,
    value    TEXT NOT NULL
) STRICT;

INSERT OR IGNORE INTO meta VALUES ('schema_version', '1');
