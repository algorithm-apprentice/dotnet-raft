user_version=1

table app_metadata
CREATE TABLE app_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    format TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    node_id BLOB NOT NULL CHECK (length(node_id) = 8),
    physical_applied BLOB NOT NULL CHECK (length(physical_applied) = 8),
    conf_state BLOB NOT NULL
) STRICT

table kv
CREATE TABLE kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID, STRICT

table requests
CREATE TABLE requests (
    request_id BLOB PRIMARY KEY CHECK (length(request_id) = 16),
    command_hash BLOB NOT NULL CHECK (length(command_hash) = 32),
    result_index BLOB NOT NULL CHECK (length(result_index) = 8)
) WITHOUT ROWID, STRICT
