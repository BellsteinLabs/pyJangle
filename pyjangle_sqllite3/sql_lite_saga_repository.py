import sqlite3
from typing import Any, Coroutine, List
from pyjangle.event.event import Event
from pyjangle.event.event_repository import DuplicateKeyError
from pyjangle.event.register import get_event_name
from pyjangle.saga.register_saga import get_saga_name
from pyjangle.saga.saga_metadata import SagaMetadata
from pyjangle.saga.saga_repository import SagaRepository
from pyjangle.serialization.register import get_event_deserializer, get_event_serializer, get_saga_deserializer, get_saga_serializer
from pyjangle_sqllite3.symbols import DB_SAGA_STORE_PATH, FIELDS, TABLES
from pyjangle_sqllite3.yield_results import yield_results

class SqlLiteSagaRepository(SagaRepository):
    def __init__(self) -> None:
        with open('pyjangle_sqllite3/create_saga_store.sql', 'r') as create_saga_store_sql_file:
            script = create_saga_store_sql_file.read()
        with sqlite3.connect(DB_SAGA_STORE_PATH) as conn:
            conn.executescript(script)
        conn.close()

    async def get_saga(self, saga_id: any) -> tuple[SagaMetadata, List[Event]]:
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
                {FIELDS.SAGA_EVENTS.SAGA_ID},
                {FIELDS.SAGA_EVENTS.EVENT_ID},
                {FIELDS.SAGA_EVENTS.DATA},
                {FIELDS.SAGA_EVENTS.CREATED_AT},
                {FIELDS.SAGA_EVENTS.TYPE}
            FROM 
                {TABLES.SAGA_EVENTS}
            WHERE
                {FIELDS.SAGA_EVENTS.SAGA_ID} = ?
        """
        p = (saga_id,)

        saga_metadata = [metadata for metadata in yield_results(DB_SAGA_STORE_PATH, batch_size=100, query=q_metadata, params=p, deserializer=saga_metadata_from_row)][0]
        saga_events = yield_results(DB_SAGA_STORE_PATH, batch_size=100, query=q_events, params=p, deserializer=get_saga_deserializer())
        return (saga_metadata, saga_events)

    async def commit_saga(self, metadata: SagaMetadata, events: list[Event]):
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
        data_metadata = (metadata.id, metadata.type, metadata.retry_at, metadata.timeout_at, metadata.is_complete, metadata.is_timed_out, metadata.retry_at, metadata.timeout_at, metadata.is_complete, metadata.is_timed_out)
        data_events = [(metadata.id, event.id, get_saga_serializer()(event)[FIELDS.SAGA_EVENTS.DATA], event.created_at, get_event_name(type(event))) for event in events]
        try:
            with sqlite3.connect(DB_SAGA_STORE_PATH) as conn:
                conn.execute(q_upsert_metadata, data_metadata)
                conn.executemany(q_upsert_events, data_events)
        except sqlite3.IntegrityError as e:
            raise DuplicateKeyError(e)
        finally:
            conn.close()
    
    async def get_retry_saga_metadata(self, max_count: int) -> list[SagaMetadata]:
        # get not complete, not timed_out, retry not null
        q_metadata = f"""
            SELECT 
                {FIELDS.SAGA_METADATA.SAGA_ID}, 
                {FIELDS.SAGA_METADATA.SAGA_TYPE}, 
                {FIELDS.SAGA_METADATA.RETRY_AT}, 
                {FIELDS.SAGA_METADATA.TIMEOUT_AT}, 
                {FIELDS.SAGA_METADATA.IS_COMPLETE},
                {FIELDS.SAGA_METADATA.IS_TIMED_OUT},
            FROM 
                {TABLES.SAGA_METADATA}
            WHERE 
                {FIELDS.SAGA_METADATA.IS_COMPLETE} = 0 AND
                {FIELDS.SAGA_METADATA.IS_TIMED_OUT} = 0 AND
                ({FIELDS.SAGA_METADATA.TIMEOUT_AT} IS NULL OR {FIELDS.SAGA_METADATA.TIMEOUT_AT} > CURRENT_TIMESTAMP) AND
                {FIELDS.SAGA_METADATA.RETRY_AT} IS NOT NULL AND
                {FIELDS.SAGA_METADATA.RETRY_AT} < CURRENT_TIMESTAMP
        """
        return yield_results(DB_SAGA_STORE_PATH, batch_size=100, query=q_metadata, params=None, deserializer=saga_metadata_from_row)

def saga_metadata_from_row(row: dict) -> SagaMetadata:
    return SagaMetadata(id=row[FIELDS.SAGA_METADATA.SAGA_ID], type=row[FIELDS.SAGA_METADATA.SAGA_TYPE], retry_at=row[FIELDS.SAGA_METADATA.RETRY_AT], timeout_at=row[FIELDS.SAGA_METADATA.TIMEOUT_AT], is_complete=bool(row[FIELDS.SAGA_METADATA.IS_COMPLETE]), is_timed_out=bool(row[FIELDS.SAGA_METADATA.IS_TIMED_OUT]))