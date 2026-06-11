PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;
-- mmap disabled: avoids 256 MiB VSZ reservation per worker connection.
-- At ~30 msg/s write volume the read()/write() fallback is negligible.
PRAGMA mmap_size = 0;
