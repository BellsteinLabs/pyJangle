CREATE TABLE IF NOT EXISTS "event_store" (
	--"sequential_id" SQLLite uses ROWID, so clustered index not needed -- INTEGER NOT NULL UNIQUE, --CLUSTERED AUTOINCREMENT, -- TODO: in Postgres, this will be `CLUSTER`, not `CLUSTERED`, but not sure it's the same thing? https://www.postgresql.org/docs/current/sql-cluster.html
	"event_id"		TEXT NOT NULL UNIQUE,
	"aggregate_id"	TEXT NOT NULL,
	"aggregate_version"	INTEGER NOT NULL,
	"data"	 		TEXT NOT NULL,  -- TODO: JSONB when moving to postgres
	"created_at"	TEXT NOT NULL,
	"type"			TEXT NOT NULL,
	PRIMARY KEY("aggregate_id","aggregate_version")
);

CREATE TABLE IF NOT EXISTS "pending_events" (
	--"sequential_id" SQLLite uses ROWID, so clustered index not needed -- INTEGER NOT NULL UNIQUE, --CLUSTERED AUTOINCREMENT, -- TODO: in Postgres, this will be `CLUSTER`, not `CLUSTERED`, but not sure it's the same thing? https://www.postgresql.org/docs/current/sql-cluster.html
	"event_id"		TEXT NOT NULL UNIQUE,
	"published_at"	TEXT NOT NULL default CURRENT_TIMESTAMP,
	FOREIGN KEY(event_id) REFERENCES event_store(event_id)
);