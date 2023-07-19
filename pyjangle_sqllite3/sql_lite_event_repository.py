from datetime import timedelta
import json
import sqlite3
import os
from typing import Iterator, List
from pyjangle.event.event import Event

from pyjangle.event.event_repository import DuplicateKeyError, EventRepository
from pyjangle.logging.logging import log
from pyjangle.serialization.register import get_event_deserializer, get_event_serializer
from pyjangle_sqllite3.symbols import DB_EVENT_STORE_PATH, FIELDS, TABLES
from pyjangle_sqllite3.yield_results import yield_results

class SqlLiteEventRepository(EventRepository):
    def __init__(self):
        #Create event store table if it's not already there
        with open('pyjangle_sqllite3/create_event_store.sql', 'r') as create_event_store_sql_file:
            create_event_store_sql_script = create_event_store_sql_file.read()
        with sqlite3.connect(DB_EVENT_STORE_PATH) as conn:
            conn.executescript(create_event_store_sql_script)
        conn.close()

    async def get_events(self, aggregate_id: any, batch_size: int = 100, current_version = 0) -> Iterator[Event]:
        q = f"""
            SELECT {FIELDS.EVENT_STORE.EVENT_ID}, {FIELDS.EVENT_STORE.AGGREGATE_ID}, {FIELDS.EVENT_STORE.AGGREGATE_VERSION}, {FIELDS.EVENT_STORE.DATA}, {FIELDS.EVENT_STORE.CREATED_AT}, {FIELDS.EVENT_STORE.TYPE}
            FROM {TABLES.EVENT_STORE}
            WHERE {FIELDS.EVENT_STORE.AGGREGATE_ID} = ?
            AND {FIELDS.EVENT_STORE.AGGREGATE_VERSION} > ?
        """
        params = (aggregate_id, current_version)
        return yield_results(db_path=DB_EVENT_STORE_PATH, query=q, params=params, batch_size=batch_size, deserializer=get_event_deserializer())

    async def get_unhandled_events(self, batch_size: int, time_since_published: timedelta) -> Iterator[Event]:
        q = f"""
            SELECT {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.EVENT_ID}, {FIELDS.EVENT_STORE.AGGREGATE_ID}, {FIELDS.EVENT_STORE.AGGREGATE_VERSION}, {FIELDS.EVENT_STORE.DATA}, {FIELDS.EVENT_STORE.CREATED_AT}, {FIELDS.EVENT_STORE.TYPE}
            FROM {TABLES.PENDING_EVENTS}
            INNER JOIN {TABLES.EVENT_STORE} ON {TABLES.PENDING_EVENTS}.{FIELDS.PENDING_EVENTS.EVENT_ID} = {TABLES.EVENT_STORE}.{FIELDS.EVENT_STORE.EVENT_ID}
            WHERE {FIELDS.PENDING_EVENTS.PUBLISHED_AT} >= datetime(CURRENT_TIMESTAMP, '+{time_since_published.total_seconds()} seconds') 
        """
        return yield_results(db_path=DB_EVENT_STORE_PATH, batch_size=batch_size, query=q, params=None, deserializer=get_event_deserializer())

    async def commit_events(self, aggregate_id: any, events: List[Event]):
        q_insert_event_store = f"""
            INSERT INTO {TABLES.EVENT_STORE} ({FIELDS.EVENT_STORE.EVENT_ID}, {FIELDS.EVENT_STORE.AGGREGATE_ID}, {FIELDS.EVENT_STORE.AGGREGATE_VERSION}, {FIELDS.EVENT_STORE.DATA}, {FIELDS.EVENT_STORE.CREATED_AT}, {FIELDS.EVENT_STORE.TYPE}) VALUES (?,?,?,?,?,?);
        """
        q_insert_pending_events = f"""
            INSERT INTO {TABLES.PENDING_EVENTS} ({FIELDS.PENDING_EVENTS.EVENT_ID}) VALUES (?);
        """
        data_insert_event_store = [
            (serialized_event[FIELDS.EVENT_STORE.EVENT_ID], 
            aggregate_id, 
            serialized_event[FIELDS.EVENT_STORE.AGGREGATE_VERSION], 
            serialized_event[FIELDS.EVENT_STORE.DATA], 
            serialized_event[FIELDS.EVENT_STORE.CREATED_AT], 
            serialized_event[FIELDS.EVENT_STORE.TYPE])
            for serialized_event in 
                [get_event_serializer()(event) for event in events]]
        data_insert_pending_events = [(row_tuple[0],) for row_tuple in data_insert_event_store]
        try:
            with sqlite3.connect(DB_EVENT_STORE_PATH) as conn:
                conn.executemany(q_insert_event_store, data_insert_event_store)
                conn.executemany(q_insert_pending_events, data_insert_pending_events)
        except sqlite3.IntegrityError as e:
            raise DuplicateKeyError(e)
        finally: 
            conn.close()

    async def mark_event_handled(self, id: any):
        q=f"DELETE FROM {TABLES.EVENT_STORE} WHERE {FIELDS.EVENT_STORE.EVENT_ID} = ?"
        params = (id,)
        try:
            with sqlite3.connect(DB_EVENT_STORE_PATH) as conn:
                conn.execute(q, params)
        finally:
            conn.close()

