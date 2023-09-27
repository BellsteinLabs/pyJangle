import os

DEFAULT_DB_PATH = "DELETE_ME_NO_DB_PATH_SET.db"
DB_EVENT_STORE_PATH = os.getenv("JANGLE_EVENT_STORE_PATH", DEFAULT_DB_PATH)
DB_SAGA_STORE_PATH = os.getenv("JANGLE_SAGA_STORE_PATH", DEFAULT_DB_PATH)
DB_SNAPSHOTS_PATH = os.getenv("JANGLE_SNAPSHOTS_PATH", DEFAULT_DB_PATH)


class TABLES:
    "Names of tables in DB schema."

    EVENT_STORE = "event_store"
    PENDING_EVENTS = "pending_events"
    SAGA_EVENTS = "saga_events"
    SAGA_METADATA = "saga_metadata"
    SNAPSHOTS = "snapshots"


class FIELDS:
    "Names of Fields in DB schema."

    class EVENT_STORE:
        "Names of event store fields."
        EVENT_ID = "event_id"
        AGGREGATE_ID = "aggregate_id"
        AGGREGATE_VERSION = "aggregate_version"
        DATA = "data"
        CREATED_AT = "created_at"
        TYPE = "type"

    class PENDING_EVENTS:
        "Names of pending events fields."
        EVENT_ID = "event_id"
        PUBLISHED_AT = "published_at"

    class SAGA_EVENTS:
        "Names of saga events fields."

        SAGA_ID = "saga_id"
        EVENT_ID = "event_id"
        DATA = "data"
        CREATED_AT = "created_at"
        TYPE = "type"

    class SAGA_METADATA:
        "Names of saga metadata fields."

        SAGA_ID = "saga_id"
        SAGA_TYPE = "saga_type"
        RETRY_AT = "retry_at"
        TIMEOUT_AT = "timeout_at"
        IS_COMPLETE = "is_complete"
        IS_TIMED_OUT = "is_timed_out"

    class SNAPSHOTS:
        "Names of snapshots fields."

        AGGREGATE_ID = "aggregate_id"
        VERSION = "version"
        DATA = "data"
