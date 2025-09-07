CREATE TABLE IF NOT EXISTS devices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    devnode TEXT NOT NULL,
    uuid TEXT,
    removed INTEGER NOT NULL DEFAULT 0,
    joined INTEGER NOT NULL DEFAULT 0,
    mount_success INTEGER NOT NULL DEFAULT 0,
    mount_path TEXT,
    last_seen BIGINT NOT NULL
);
-- Unique index on uuid (allows multiple NULLs automatically in SQLite)
CREATE UNIQUE INDEX IF NOT EXISTS devices_uuid_unique ON devices(uuid);
