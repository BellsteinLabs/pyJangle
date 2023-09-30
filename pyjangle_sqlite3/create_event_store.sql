CREATE TABLE IF NOT EXISTS "event_store" (
	
	"event_id"			TEXT NOT NULL UNIQUE,
	"aggregate_id"		TEXT NOT NULL,
	"aggregate_version"	INTEGER NOT NULL,
	"data"	 			TEXT NOT NULL,
	"created_at"		DATETIME NOT NULL,
	"type"				TEXT NOT NULL,
	PRIMARY KEY("aggregate_id","aggregate_version")
);

CREATE TABLE IF NOT EXISTS "pending_events" (
	"event_id"			TEXT NOT NULL UNIQUE,
	"published_at"		DATETIME NOT NULL default CURRENT_TIMESTAMP,
	FOREIGN KEY(event_id) REFERENCES event_store(event_id)
);