
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

INSERT INTO meta VALUES ('schema_version', '1');
