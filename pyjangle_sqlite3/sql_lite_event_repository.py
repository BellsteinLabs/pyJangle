from datetime import timedelta
from sqlite3 import connect, PARSE_DECLTYPES, IntegrityError
from typing import Iterator

from pyjangle import (
    VersionedEvent,
    DuplicateKeyError,
    EventRepository,
    get_deserializer,
    get_serializer,
    get_event_name,
    get_event_type,
    get_batch_size,
)
from pyjangle_sqlite3 import (
    yield_results,
    get_event_store_path,
    FIELDS,
    TABLES,
    raise_if_not_initialized,
)


class _SerializableEvent:
    def __init__(self, event: VersionedEvent, aggregate_id: str):
        self.event_id = event.id
        self.aggregate_id = aggregate_id
        self.version = event.version
        self.event_name = get_event_name(type(event))
        self.created_at = event.created_at
        self.data = get_serializer()(event)


class SqliteEventRepository(EventRepository):
    """Sqlite3 implementation of `EventRepository`.

    Environment Variables:
        Use the environment variable `JANGLE_EVENT_STORE_PATH` to specify a location for
        the database.  Ensure there is a registered serializer capable of converting
        events to and from a string representation.


    Adapters & Converters:
        Register the appropriate adapter and converter for your desired datetime format
        *before* instantiating this class using `register_adapter` and
        `register_converter` from the sqlite3 package.  See the `adapters` module for
        examples.
    """

    def __init__(self):
        raise_if_not_initialized()
        with open(
            "pyjangle_sqlite3/create_event_store.sql", "r"
        ) as create_event_store_sql_file:
            create_event_store_sql_script = create_event_store_sql_file.read()
        with connect(get_event_store_path(), detect_types=PARSE_DECLTYPES) as conn:
            conn.executescript(create_event_store_sql_script)
            conn.commit()
        conn.close()

    async def get_events(
        self,
        aggregate_id: any,
        current_version=0,
        batch_size: int = get_batch_size(),
    ) -> Iterator[VersionedEvent]:
        q = f"""
            SELECT 
                {FIELDS.EVENT_STORE.DATA}, 
                {FIELDS.EVENT_STORE.TYPE}
            FROM {TABLES.EVENT_STORE}
            WHERE {FIELDS.EVENT_STORE.AGGREGATE_ID} = ?
            AND {FIELDS.EVENT_STORE.AGGREGATE_VERSION} > ?
        """

        def _handle_row(row):
            event_data = row[FIELDS.EVENT_STORE.DATA]
            event_name = row[FIELDS.EVENT_STORE.TYPE]
            event_type = get_event_type(event_name)
            return event_type(**get_deserializer()(event_data))

        params = (aggregate_id, current_version)
        return yield_results(
            db_path=get_event_store_path(),
            query=q,
            params=params,
            batch_size=batch_size,
            row_handler=_handle_row,
        )

    async def get_unhandled_events(
        self, batch_size: int, time_delta: timedelta
    ) -> Iterator[VersionedEvent]:
        q = f"""
            SELECT 
                {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.DATA}, 
                {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.TYPE}
            FROM {TABLES.PENDING_EVENTS}
            INNER JOIN 
                {TABLES.EVENT_STORE} 
            ON 
                {TABLES.PENDING_EVENTS}.{FIELDS.PENDING_EVENTS.EVENT_ID} = 
                {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.EVENT_ID}
            WHERE 
                {TABLES.PENDING_EVENTS}.{FIELDS.PENDING_EVENTS.PUBLISHED_AT} <= 
                datetime(CURRENT_TIMESTAMP, '+{time_delta.total_seconds()} seconds') 
        """

        def _row_handler(row):
            event_data = row[FIELDS.EVENT_STORE.DATA]
            event_name = row[FIELDS.EVENT_STORE.TYPE]
            event_type = get_event_type(event_name)
            return event_type(**get_deserializer()(event_data))

        for result in yield_results(
            db_path=get_event_store_path(),
            batch_size=batch_size,
            query=q,
            params=None,
            row_handler=_row_handler,
        ):
            yield result

    async def commit_events(
        self, aggregate_id_and_event_tuples: list[tuple[any, VersionedEvent]]
    ):
        q_insert_event_store = f"""
            INSERT INTO 
                {TABLES.EVENT_STORE} 
                (
                    {FIELDS.EVENT_STORE.EVENT_ID}, 
                    {FIELDS.EVENT_STORE.AGGREGATE_ID}, 
                    {FIELDS.EVENT_STORE.AGGREGATE_VERSION}, 
                    {FIELDS.EVENT_STORE.DATA}, 
                    {FIELDS.EVENT_STORE.CREATED_AT}, 
                    {FIELDS.EVENT_STORE.TYPE}
                ) 
            VALUES 
                (?,?,?,?,?,?)
        """
        q_insert_pending_events = f"""
            INSERT INTO 
                {TABLES.PENDING_EVENTS} 
                (
                    {FIELDS.PENDING_EVENTS.EVENT_ID}
                ) 
            VALUES (?)
        """

        serializable_events: list[_SerializableEvent] = [
            _SerializableEvent(tupe[1], tupe[0])
            for tupe in aggregate_id_and_event_tuples
        ]

        data_insert_event_store = [
            (
                se.event_id,
                se.aggregate_id,
                se.version,
                se.data,
                se.created_at,
                se.event_name,
            )
            for se in serializable_events
        ]

        data_insert_pending_events = [(se.event_id,) for se in serializable_events]

        try:
            with connect(get_event_store_path(), detect_types=PARSE_DECLTYPES) as conn:
                conn.executemany(q_insert_event_store, data_insert_event_store)
                conn.executemany(q_insert_pending_events, data_insert_pending_events)
                conn.commit()
        except IntegrityError as e:
            raise DuplicateKeyError(e)
        finally:
            conn.close()

    async def mark_event_handled(self, id: any):
        q = f"""
        DELETE FROM 
            {TABLES.PENDING_EVENTS} 
        WHERE 
            {FIELDS.EVENT_STORE.EVENT_ID} = ?
        """
        params = (id,)
        try:
            with connect(get_event_store_path(), detect_types=PARSE_DECLTYPES) as conn:
                conn.execute(q, params)
                conn.commit()
        finally:
            conn.close()
