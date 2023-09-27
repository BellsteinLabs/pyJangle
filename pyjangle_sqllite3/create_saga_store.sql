
CREATE TABLE IF NOT EXISTS "saga_metadata" (
    "saga_id"           TEXT NOT NULL UNIQUE,
    "saga_type"         TEXT NOT NULL,
    "retry_at"          DATETIME,
    "timeout_at"        DATETIME,
    "is_complete"       INTEGER NOT NULL,
	"is_timed_out"  	INTEGER NOT NULL,
    PRIMARY KEY("saga_id")
);
CREATE INDEX IF NOT EXISTS saga_needs_retry ON saga_metadata(retry_at, timeout_at, is_complete, is_timed_out) WHERE is_complete = FALSE AND is_timed_out = FALSE AND retry_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS "saga_events" (
    "saga_id"           TEXT NOT NULL,
	"event_id"		    TEXT NOT NULL UNIQUE,
	"data"	 		    TEXT NOT NULL,
	"created_at"	    DATETIME NOT NULL,
	"type"			    TEXT NOT NULL,
	PRIMARY KEY("event_id"),
	FOREIGN KEY(saga_id) REFERENCES saga_metadata(saga_id)
);
CREATE INDEX IF NOT EXISTS saga_events_by_saga_id ON saga_events(saga_id);

