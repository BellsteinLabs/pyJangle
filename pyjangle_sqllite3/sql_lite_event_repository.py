from datetime import timedelta
import json
import sqlite3
import os
from typing import Iterator, List
from pyjangle.event.event import Event

from pyjangle.event.event_repository import DuplicateKeyError, EventRepository, RegisterEventRepository
from pyjangle.event.register import get_event_name, get_event_type
from pyjangle.logging.logging import log
from pyjangle.serialization.register import get_event_deserializer, get_event_serializer

DB_PATH = os.getenv("JANGLE_EVENT_STORE_PATH", "event_store.db")
EVENT_ID = "event_id"
AGGREGATE_ID = "aggregate_id"
AGGREGATE_VERSION = "aggregate_version"
DATA = "data"
CREATED_AT = "created_at"
TYPE = "type"
EVENT_STORE = "event_store"
PENDING_EVENTS = "pending_events"
PUBLISHED_AT = "published_at"

class SqlLiteEventRepository(EventRepository):
    def __init__(self):
        #Create event store table if it's not already there
        with open('pyjangle_sqllite3/create_event_store.sql', 'r') as create_event_store_sql_file:
            create_event_store_sql_script = create_event_store_sql_file.read()
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(create_event_store_sql_script)
        conn.close()

    async def get_events(self, aggregate_id: any, batch_size: int = 100, current_version = 0) -> Iterator[Event]:
        q = f"""
            SELECT {EVENT_ID}, {AGGREGATE_ID}, {AGGREGATE_VERSION}, {DATA}, {CREATED_AT}, {TYPE}
            FROM {EVENT_STORE}
            WHERE {AGGREGATE_ID} = ?
            AND {AGGREGATE_VERSION} > ?
        """
        params = (aggregate_id, current_version)
        return yield_events(db_path=DB_PATH, query=q, params=params, batch_size=batch_size)

    async def get_unhandled_events(self, batch_size: int, time_since_published: timedelta) -> Iterator[Event]:
        q = f"""
            SELECT {EVENT_STORE}.{EVENT_ID}, {AGGREGATE_ID}, {AGGREGATE_VERSION}, {DATA}, {CREATED_AT}, {TYPE}
            FROM {PENDING_EVENTS}
            INNER JOIN {EVENT_STORE} ON {PENDING_EVENTS}.{EVENT_ID} = {EVENT_STORE}.{EVENT_ID}
            WHERE {PUBLISHED_AT} >= datetime(CURRENT_TIMESTAMP, '+{time_since_published.total_seconds()} seconds') 
        """
        return yield_events(db_path=DB_PATH, batch_size=batch_size, query=q, params=None)

    async def commit_events(self, aggregate_id: any, events: List[Event]):
        q_insert_event_store = f"""
            INSERT INTO {EVENT_STORE} ({EVENT_ID}, {AGGREGATE_ID}, {AGGREGATE_VERSION}, {DATA}, {CREATED_AT}, {TYPE}) VALUES (?,?,?,?,?,?);
        """
        q_insert_pending_events = f"""
            INSERT INTO {PENDING_EVENTS} ({EVENT_ID}) VALUES (?);
        """
        data_insert_event_store = [
            (serialized_event[EVENT_ID], 
            aggregate_id, 
            serialized_event[AGGREGATE_VERSION], 
            serialized_event[DATA], 
            serialized_event[CREATED_AT], 
            serialized_event[TYPE])
            for serialized_event in 
                [get_event_serializer()(event) for event in events]]
        data_insert_pending_events = [(row_tuple[0],) for row_tuple in data_insert_event_store]
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.executemany(q_insert_event_store, data_insert_event_store)
                conn.executemany(q_insert_pending_events, data_insert_pending_events)
        except sqlite3.IntegrityError as e:
            raise DuplicateKeyError(e)
        finally: 
            conn.close()

    async def mark_event_handled(self, id: any):
        q=f"DELETE FROM {EVENT_STORE} WHERE {EVENT_ID} = ?"
        params = (id,)
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(q, params)
        finally:
            conn.close()

def yield_events(db_path: str, batch_size: int, query: str, params: tuple[any]) -> Iterator[Event]:
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.arraysize = batch_size
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            while True:
                rows = cursor.fetchmany(size=cursor.arraysize)
                if not len(rows):
                    break
                for row in rows:
                    yield get_event_deserializer()(row)
    finally:
        conn.close()