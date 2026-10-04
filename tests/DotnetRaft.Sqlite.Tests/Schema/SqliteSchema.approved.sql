user_version=1

table raft_entries
CREATE TABLE raft_entries (
    entry_index BLOB PRIMARY KEY CHECK (length(entry_index) = 8),
    entry_term BLOB NOT NULL CHECK (length(entry_term) = 8),
    payload BLOB NOT NULL
) WITHOUT ROWID, STRICT

table raft_metadata
CREATE TABLE raft_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    format TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    hard_state BLOB NULL,
    snapshot BLOB NOT NULL,
    compacted_index BLOB NOT NULL CHECK (length(compacted_index) = 8),
    last_index BLOB NOT NULL CHECK (length(last_index) = 8),
    pending_snapshot_index BLOB NULL CHECK (
        pending_snapshot_index IS NULL
        OR length(pending_snapshot_index) = 8)
) STRICT

