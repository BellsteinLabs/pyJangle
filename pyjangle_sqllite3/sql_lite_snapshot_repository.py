import sqlite3
from pyjangle.serialization.register import get_snapshot_deserializer, get_snapshot_serializer
from pyjangle.snapshot.snapshot_repository import SnapshotRepository
from pyjangle_sqllite3.symbols import DB_SNAPSHOTS_PATH, FIELDS, TABLES
from pyjangle_sqllite3.yield_results import yield_results


class SqliteSnapshotRepository(SnapshotRepository):

    def __init__(self) -> None:
        with open('pyjangle_sqllite3/create_snapshot_repository.sql', 'r') as script_file:
            script = script_file.read()
        with sqlite3.connect(DB_SNAPSHOTS_PATH) as conn:
            conn.executescript(script)
        conn.close()

    async def get_snapshot(self, aggregate_id: str) -> tuple[int, any] | None:
        q = f"""
            SELECT {FIELDS.SNAPSHOTS.VERSION}, {FIELDS.SNAPSHOTS.DATA}
            FROM {TABLES.SNAPSHOTS}
            WHERE {FIELDS.SNAPSHOTS.AGGREGATE_ID} = ?
        """
        p = (aggregate_id,)
        try:
            with sqlite3.connect(DB_SNAPSHOTS_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(q, p)
                result_set = cursor.fetchall()
                if not result_set:
                    return None
                row = result_set[0]
                return get_snapshot_deserializer()(row)
        finally:
            conn.close()
    
    async def store_snapshot(self, aggregate_id: any, version: int, snapshot: any):
        q=f"""
            INSERT INTO {TABLES.SNAPSHOTS} (
                {FIELDS.SNAPSHOTS.AGGREGATE_ID},
                {FIELDS.SNAPSHOTS.VERSION},
                {FIELDS.SNAPSHOTS.DATA}
            ) VALUES (?,?,?)
            ON CONFLICT DO UPDATE SET
                {FIELDS.SNAPSHOTS.AGGREGATE_ID} = CASE WHEN {FIELDS.SNAPSHOTS.VERSION} < ? THEN ? ELSE {FIELDS.SNAPSHOTS.AGGREGATE_ID} END,
                {FIELDS.SNAPSHOTS.VERSION} = CASE WHEN {FIELDS.SNAPSHOTS.VERSION} < ? THEN ? ELSE {FIELDS.SNAPSHOTS.VERSION} END,
                {FIELDS.SNAPSHOTS.DATA} = CASE WHEN {FIELDS.SNAPSHOTS.VERSION} < ? THEN ? ELSE {FIELDS.SNAPSHOTS.DATA} END
        """
        serialized_snapshot = get_snapshot_serializer()(snapshot)
        p=(aggregate_id, version, serialized_snapshot, version, aggregate_id, version, version, version, serialized_snapshot)
        try:
            with sqlite3.connect(DB_SNAPSHOTS_PATH) as conn:
                conn.execute(q, p)
        finally:
            conn.close()
    
    async def delete_snapshot(self, aggregate_id: str):
        q=f"DELETE FROM {TABLES.SNAPSHOTS} WHERE {FIELDS.SNAPSHOTS.AGGREGATE_ID} = ?"
        p=(aggregate_id,)
        try:
            with sqlite3.connect(DB_SNAPSHOTS_PATH) as conn:
                conn.execute(q, p)
        finally:
            conn.close()