import sqlite3
from pyjangle.event.event_repository import DuplicateKeyError
from pyjangle.event.register_event import get_event_name
from pyjangle.saga.register_saga import get_saga_name
from pyjangle.saga.saga import Saga
from pyjangle.saga.saga_repository import SagaRepository
from pyjangle.serialization.saga_serialization_registration import get_saga_deserializer, get_saga_serializer
from pyjangle_sqllite3.symbols import DB_SAGA_STORE_PATH, FIELDS, TABLES
from pyjangle_sqllite3.yield_results import yield_results

class SqlLiteSagaRepository(SagaRepository):
    def __init__(self) -> None:
        with open('pyjangle_sqllite3/create_saga_store.sql', 'r') as create_saga_store_sql_file:
            script = create_saga_store_sql_file.read()
        with sqlite3.connect(DB_SAGA_STORE_PATH) as conn:
            conn.executescript(script)
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
        metadata_row = None
        event_rows = None
        try:
            with sqlite3.connect(DB_SAGA_STORE_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(q_metadata, p)
                metadata_row = cursor.fetchone()
                cursor.execute(q_events, p)
                event_rows = cursor.fetchall()
        finally:
            conn.close()

        if not metadata_row:
            return None
        serialized_saga = (metadata_row, event_rows)
        return get_saga_deserializer()(serialized_saga)


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
        metadata_dict, event_dict_list = get_saga_serializer()(saga)

        data_metadata = (
            metadata_dict[FIELDS.SAGA_METADATA.SAGA_ID], 
            metadata_dict[FIELDS.SAGA_METADATA.SAGA_TYPE],
            metadata_dict[FIELDS.SAGA_METADATA.RETRY_AT],
            metadata_dict[FIELDS.SAGA_METADATA.TIMEOUT_AT],
            metadata_dict[FIELDS.SAGA_METADATA.IS_COMPLETE],
            metadata_dict[FIELDS.SAGA_METADATA.IS_TIMED_OUT],
            metadata_dict[FIELDS.SAGA_METADATA.RETRY_AT],
            metadata_dict[FIELDS.SAGA_METADATA.TIMEOUT_AT],
            metadata_dict[FIELDS.SAGA_METADATA.IS_COMPLETE],
            metadata_dict[FIELDS.SAGA_METADATA.IS_TIMED_OUT])
        data_events = [(
            event_dict[FIELDS.SAGA_EVENTS.SAGA_ID],
            event_dict[FIELDS.SAGA_EVENTS.EVENT_ID],
            event_dict[FIELDS.SAGA_EVENTS.DATA],
            event_dict[FIELDS.SAGA_EVENTS.CREATED_AT],
            event_dict[FIELDS.SAGA_EVENTS.TYPE])for event_dict in event_dict_list]
        
        try:
            with sqlite3.connect(DB_SAGA_STORE_PATH) as conn:
                conn.execute(q_upsert_metadata, data_metadata)
                conn.executemany(q_upsert_events, data_events)
        except sqlite3.IntegrityError as e:
            raise DuplicateKeyError(e)
        finally:
            conn.close()
    
    async def get_retry_saga_metadata(self, max_count: int) -> list[any]:
        # get not complete, not timed_out, retry not null
        q_metadata = f"""
            SELECT 
                {FIELDS.SAGA_METADATA.SAGA_ID}
            FROM 
                {TABLES.SAGA_METADATA}
            WHERE 
                {FIELDS.SAGA_METADATA.IS_COMPLETE} = 0 AND
                {FIELDS.SAGA_METADATA.IS_TIMED_OUT} = 0 AND
                ({FIELDS.SAGA_METADATA.TIMEOUT_AT} IS NULL OR {FIELDS.SAGA_METADATA.TIMEOUT_AT} > CURRENT_TIMESTAMP) AND
                {FIELDS.SAGA_METADATA.RETRY_AT} IS NOT NULL AND
                {FIELDS.SAGA_METADATA.RETRY_AT} < CURRENT_TIMESTAMP
        """

        return yield_results(DB_SAGA_STORE_PATH, batch_size=100, query=q_metadata, params=None, deserializer=lambda x : x[FIELDS.SAGA_METADATA.SAGA_ID])