from datetime import timedelta
import sqlite3
from typing import Iterator

from pyjangle import (
    VersionedEvent,
    DuplicateKeyError,
    EventRepository,
    get_event_deserializer,
    get_event_serializer,
)

# from pyjangle.logging.logging import log
# from pyjangle.serialization.event_serialization_registration import get_event_deserializer, get_event_serializer
from pyjangle_sqllite3.adapters import register_all
from pyjangle_sqllite3.symbols import DB_EVENT_STORE_PATH, FIELDS, TABLES
from pyjangle_sqllite3.yield_results import yield_results

register_all()


class SqlLiteEventRepository(EventRepository):
    def __init__(self):
        # Create event store table if it's not already there
        with open(
            "pyjangle_sqllite3/create_event_store.sql", "r"
        ) as create_event_store_sql_file:
            create_event_store_sql_script = create_event_store_sql_file.read()
        with sqlite3.connect(
            DB_EVENT_STORE_PATH, detect_types=sqlite3.PARSE_DECLTYPES
        ) as conn:
            conn.executescript(create_event_store_sql_script)
            conn.commit()
        conn.close()

    async def get_events(
        self, aggregate_id: any, batch_size: int = 100, current_version=0
    ) -> Iterator[VersionedEvent]:
        q = f"""
            SELECT {FIELDS.EVENT_STORE.EVENT_ID}, {FIELDS.EVENT_STORE.AGGREGATE_ID}, {FIELDS.EVENT_STORE.AGGREGATE_VERSION}, {FIELDS.EVENT_STORE.DATA}, {FIELDS.EVENT_STORE.CREATED_AT}, {FIELDS.EVENT_STORE.TYPE}
            FROM {TABLES.EVENT_STORE}
            WHERE {FIELDS.EVENT_STORE.AGGREGATE_ID} = ?
            AND {FIELDS.EVENT_STORE.AGGREGATE_VERSION} > ?
        """
        params = (aggregate_id, current_version)
        return yield_results(
            db_path=DB_EVENT_STORE_PATH,
            query=q,
            params=params,
            batch_size=batch_size,
            deserializer=get_event_deserializer(),
        )

    async def get_unhandled_events(
        self, batch_size: int, time_delta: timedelta
    ) -> Iterator[VersionedEvent]:
        q = f"""
            SELECT {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.EVENT_ID}, {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.AGGREGATE_ID}, {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.AGGREGATE_VERSION}, {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.DATA}, {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.CREATED_AT}, {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.TYPE}
            FROM {TABLES.PENDING_EVENTS}
            INNER JOIN {TABLES.EVENT_STORE} ON {TABLES.PENDING_EVENTS}.{FIELDS.PENDING_EVENTS.EVENT_ID} = {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.EVENT_ID}
            WHERE {TABLES.PENDING_EVENTS}.{FIELDS.PENDING_EVENTS.PUBLISHED_AT} <= datetime(CURRENT_TIMESTAMP, '+{time_delta.total_seconds()} seconds') 
        """
        for result in yield_results(
            db_path=DB_EVENT_STORE_PATH,
            batch_size=batch_size,
            query=q,
            params=None,
            deserializer=get_event_deserializer(),
        ):
            yield result

    async def commit_events(
        self, aggregate_id_and_event_tuples: list[tuple[any, VersionedEvent]]
    ):
        q_insert_event_store = f"""
            INSERT INTO {TABLES.EVENT_STORE} ({FIELDS.EVENT_STORE.EVENT_ID}, {FIELDS.EVENT_STORE.AGGREGATE_ID}, {FIELDS.EVENT_STORE.AGGREGATE_VERSION}, {FIELDS.EVENT_STORE.DATA}, {FIELDS.EVENT_STORE.CREATED_AT}, {FIELDS.EVENT_STORE.TYPE}) VALUES (?,?,?,?,?,?)
        """
        q_insert_pending_events = f"""
            INSERT INTO {TABLES.PENDING_EVENTS} ({FIELDS.PENDING_EVENTS.EVENT_ID}) VALUES (?)
        """
        data_insert_event_store = [
            (
                aggregate_id_and_serialized_event_tuple[1][FIELDS.EVENT_STORE.EVENT_ID],
                aggregate_id_and_serialized_event_tuple[0],
                aggregate_id_and_serialized_event_tuple[1][
                    FIELDS.EVENT_STORE.AGGREGATE_VERSION
                ],
                aggregate_id_and_serialized_event_tuple[1][FIELDS.EVENT_STORE.DATA],
                aggregate_id_and_serialized_event_tuple[1][
                    FIELDS.EVENT_STORE.CREATED_AT
                ],
                aggregate_id_and_serialized_event_tuple[1][FIELDS.EVENT_STORE.TYPE],
            )
            for aggregate_id_and_serialized_event_tuple in [
                (
                    aggregate_id_and_event_tuple[0],
                    get_event_serializer()(aggregate_id_and_event_tuple[1]),
                )
                for aggregate_id_and_event_tuple in aggregate_id_and_event_tuples
            ]
        ]
        data_insert_pending_events = [
            (row_tuple[0],) for row_tuple in data_insert_event_store
        ]
        try:
            with sqlite3.connect(
                DB_EVENT_STORE_PATH, detect_types=sqlite3.PARSE_DECLTYPES
            ) as conn:
                conn.executemany(q_insert_event_store, data_insert_event_store)
                conn.executemany(q_insert_pending_events, data_insert_pending_events)
                conn.commit()
        except sqlite3.IntegrityError as e:
            raise DuplicateKeyError(e)
        finally:
            conn.close()

    async def mark_event_handled(self, id: any):
        q = f"DELETE FROM {TABLES.PENDING_EVENTS} WHERE {FIELDS.EVENT_STORE.EVENT_ID} = ?"
        params = (id,)
        try:
            with sqlite3.connect(
                DB_EVENT_STORE_PATH, detect_types=sqlite3.PARSE_DECLTYPES
            ) as conn:
                conn.execute(q, params)
                conn.commit()
        finally:
            conn.close()
