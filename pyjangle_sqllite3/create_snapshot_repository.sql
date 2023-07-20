CREATE TABLE IF NOT EXISTS "snapshots" (
    "aggregate_id"          TEXT NOT NULL UNIQUE,
    "version"               INTEGER NOT NULL,
    "data"                  TEXT NOT NULL,
    PRIMARY KEY("aggregate_id")
)