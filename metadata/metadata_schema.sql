-- Metadata table for ingestion
CREATE TABLE IF NOT EXISTS source_metadata (
    source_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_location TEXT NOT NULL,
    delimeter TEXT DEFAULT ',',
    has_header INTEGER DEFAULT 1,
    load_type TEXT DEFAULT 'FULL',
    destination_location TEXT NOT NULL,
    transform_rules TEXT,
    is_active INTEGER DEFAULT 1,
    created_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT NOT NULL,
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT NOT NULL
);