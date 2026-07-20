
CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS user_state (
    user_id  INTEGER PRIMARY KEY,
    data     ANY     NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE IF NOT EXISTS chat_state (
    chat_id  INTEGER PRIMARY KEY,
    data     ANY     NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE IF NOT EXISTS global_state (
    key      TEXT PRIMARY KEY,
    value    ANY  NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS schedule (
    id            INTEGER PRIMARY KEY,           -- rowid; returned by bot.schedule_*
    fire_at_ms    INTEGER NOT NULL,              -- absolute epoch ms; when to fire
    payload       ANY     NOT NULL DEFAULT '{}', -- Lua table -> JSON (TEXT) or encrypted BLOB
    claimed_at_ms INTEGER                        -- NULL = unclaimed; else lease start (epoch ms)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_schedule_fire ON schedule(fire_at_ms);

INSERT INTO meta VALUES ('schema_version', '2');
