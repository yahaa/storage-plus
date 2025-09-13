-- Create files metadata table
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    "key" TEXT NOT NULL UNIQUE,
    filename TEXT NOT NULL,
    content_type TEXT,
    size BIGINT NOT NULL,
    path TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_files_key ON files("key");
CREATE INDEX IF NOT EXISTS idx_files_deleted ON files(deleted);
