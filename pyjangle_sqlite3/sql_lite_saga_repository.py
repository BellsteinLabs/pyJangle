from sqlite3 import connect, PARSE_DECLTYPES, Row, IntegrityError
from pyjangle import (
    DuplicateKeyError,
    Saga,
    SagaRepository,
    get_event_type,
    get_saga_type,
    get_saga_name,
    get_event_name,
    get_serializer,
    get_deserializer,
)
from pyjangle_sqlite3 import (
    get_saga_store_path,
    FIELDS,
    TABLES,
    yield_results,
    raise_if_not_initialized,
)


class SqliteSagaRepository(SagaRepository):
    """Sqlite3 implementation of `SagaRepository`.

    Environment Variables:
        Use the environment variable `JANGLE_SAGA_STORE_PATH` to specify a location for
        the database.  Ensure there is a registered serializer capable of converting
        events to and from a string representation.

    Adapters & Converters:
        Register the appropriate adapter and converter for your desired datetime formate
        *before* instantiating this class using `register_adapter` and
        `register_converter` from the sqlite3 package.  See the `adapters` module for
        examples.
    """

    def __init__(self) -> None:
        raise_if_not_initialized()
        with open(
            "pyjangle_sqlite3/create_saga_store.sql", "r"
        ) as create_saga_store_sql_file:
            script = create_saga_store_sql_file.read()
        with connect(get_saga_store_path(), detect_types=PARSE_DECLTYPES) as conn:
            conn.executescript(script)
            conn.commit()
        conn.close()

    async def get_saga(self, saga_id: any) -> Saga:
        q_metadata = f"""
            SELECT 
                {FIELDS.SAGA_METADATA.SAGA_ID}, 
                {FIELDS.SAGA_METADATA.SAGA_TYPE}, 
                {FIELDS.SAGA_METADATA.RETRY_AT}, 
                {FIELDS.SAGA_METADATA.TIMEOUT_AT}, 
                {FIELDS.SAGA_METADATA.IS_COMPLETE},
                {FIELDS.SAGA_METADATA.IS_TIMED_OUT}
            FROM 
                {TABLES.SAGA_METADATA}
            WHERE 
                {FIELDS.SAGA_METADATA.SAGA_ID} = ?
        """
        q_events = f"""
            SELECT 
                {FIELDS.SAGA_EVENTS.DATA},
                {FIELDS.SAGA_EVENTS.TYPE}
            FROM 
                {TABLES.SAGA_EVENTS}
            WHERE
                {FIELDS.SAGA_EVENTS.SAGA_ID} = ?
        """
        p = (saga_id,)
        metadata_row = None
        event_rows = None
        try:
            with connect(get_saga_store_path(), detect_types=PARSE_DECLTYPES) as conn:
                conn.row_factory = Row
                cursor = conn.cursor()
                cursor.execute(q_metadata, p)
                metadata_row = cursor.fetchone()
                cursor.execute(q_events, p)
                event_rows = cursor.fetchall()
                conn.commit()
        finally:
            conn.close()

        if not metadata_row:
            return None

        events = [
            get_event_type(r[FIELDS.SAGA_EVENTS.TYPE])(
                **get_deserializer()(r[FIELDS.SAGA_EVENTS.DATA])
            )
            for r in event_rows
        ]

        saga_type = get_saga_type(metadata_row[FIELDS.SAGA_METADATA.SAGA_TYPE])
        return saga_type(
            saga_id=metadata_row[FIELDS.SAGA_METADATA.SAGA_ID],
            events=events,
            retry_at=metadata_row[FIELDS.SAGA_METADATA.RETRY_AT],
            timeout_at=metadata_row[FIELDS.SAGA_METADATA.TIMEOUT_AT],
            is_complete=bool(metadata_row[FIELDS.SAGA_METADATA.TIMEOUT_AT]),
            is_timed_out=bool(metadata_row[FIELDS.SAGA_METADATA.IS_TIMED_OUT]),
        )

    async def commit_saga(self, saga: Saga):
        q_upsert_metadata = f"""
            INSERT INTO {TABLES.SAGA_METADATA} 
            (
                {FIELDS.SAGA_METADATA.SAGA_ID}, 
                {FIELDS.SAGA_METADATA.SAGA_TYPE},
                {FIELDS.SAGA_METADATA.RETRY_AT},
                {FIELDS.SAGA_METADATA.TIMEOUT_AT},
                {FIELDS.SAGA_METADATA.IS_COMPLETE},
                {FIELDS.SAGA_METADATA.IS_TIMED_OUT}
            )
            VALUES
            (
                ?,?,?,?,?,?
            )
            ON CONFLICT DO UPDATE SET
                {FIELDS.SAGA_METADATA.RETRY_AT} = ?,
                {FIELDS.SAGA_METADATA.TIMEOUT_AT} = ?,
                {FIELDS.SAGA_METADATA.IS_COMPLETE} = ?,
                {FIELDS.SAGA_METADATA.IS_TIMED_OUT} = ?
        """
        q_upsert_events = f"""
            INSERT INTO 
                {TABLES.SAGA_EVENTS}
                (
                    {FIELDS.SAGA_EVENTS.SAGA_ID},
                    {FIELDS.SAGA_EVENTS.EVENT_ID},
                    {FIELDS.SAGA_EVENTS.DATA},
                    {FIELDS.SAGA_EVENTS.CREATED_AT},
                    {FIELDS.SAGA_EVENTS.TYPE}
                ) VALUES (?,?,?,?,?)
        """

        data_metadata = (
            saga.saga_id,
            get_saga_name(type(saga)),
            saga.retry_at,
            saga.timeout_at,
            saga.is_complete,
            saga.is_timed_out,
            saga.retry_at,
            saga.timeout_at,
            saga.is_complete,
            saga.is_timed_out,
        )
        data_events = [
            (
                saga.saga_id,
                event.id,
                get_serializer()(event),
                event.created_at,
                get_event_name(type(event)),
            )
            for event in saga.new_events
        ]

        try:
            with connect(get_saga_store_path(), detect_types=PARSE_DECLTYPES) as conn:
                conn.execute(q_upsert_metadata, data_metadata)
                conn.executemany(q_upsert_events, data_events)
                conn.commit()
        except IntegrityError as e:
            raise DuplicateKeyError(e)
        finally:
            conn.close()

    async def get_retry_saga_ids(self, batch_size: int) -> list[any]:
        # get not complete, not timed_out, retry not null
        q_metadata = f"""
            SELECT 
                {FIELDS.SAGA_METADATA.SAGA_ID}
            FROM 
                {TABLES.SAGA_METADATA}
            WHERE 
                {FIELDS.SAGA_METADATA.IS_COMPLETE} = 0 AND
                {FIELDS.SAGA_METADATA.IS_TIMED_OUT} = 0 AND
                (
                    {FIELDS.SAGA_METADATA.TIMEOUT_AT} IS NULL OR 
                    {FIELDS.SAGA_METADATA.TIMEOUT_AT} > CURRENT_TIMESTAMP
                ) AND
                {FIELDS.SAGA_METADATA.RETRY_AT} IS NOT NULL AND
                {FIELDS.SAGA_METADATA.RETRY_AT} < CURRENT_TIMESTAMP
        """

        return yield_results(
            get_saga_store_path(),
            batch_size=100,
            query=q_metadata,
            params=None,
            row_handler=lambda row: row[FIELDS.SAGA_METADATA.SAGA_ID],
        )
