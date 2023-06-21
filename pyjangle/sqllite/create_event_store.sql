CREATE TABLE IF NOT EXISTS "event_store" (
	"sequential_id"	INTEGER NOT NULL UNIQUE, --CLUSTERED AUTOINCREMENT, -- TODO: in Postgres, this will be `CLUSTER`, not `CLUSTERED`, but not sure it's the same thing? https://www.postgresql.org/docs/current/sql-cluster.html
	"aggregate_id"	TEXT NOT NULL,
	"version_id"	INTEGER NOT NULL,
	"data"	 		TEXT NOT NULL default current_timestamp,  -- TODO: JSONB when moving to postgres
	"created_at"	DATETIME NOT NULL DEFAULT 'datetime()',
	"event_name"	TEXT NOT NULL,
	PRIMARY KEY("aggregate_id","version_id")
);