from sqlite3 import connect, PARSE_DECLTYPES, Row
from pyjangle import SnapshotRepository, get_deserializer, get_serializer
from pyjangle_sqlite3 import (
    get_snapshot_store_path,
    FIELDS,
    TABLES,
    raise_if_not_initialized,
)


class SqliteSnapshotRepository(SnapshotRepository):
    """Sqlite3 implementation of `SnapshotRepository`.

    Environment Variables:
        Use the environment variable `JANGLE_SNAPSHOTS_PATH` to specify a location for
        the database.  Ensure there is a registered serializer capable of converting
        snapshots to and from a string representation.

    Adapters & Converters:
        Register the appropriate adapter and converter for your desired datetime formate
        *before* instantiating this class using `register_adapter` and
        `register_converter` from the sqlite3 package.  See the `adapters` module for
        examples.
    """

    def __init__(self) -> None:
        raise_if_not_initialized()
        with open(
            "pyjangle_sqlite3/create_snapshot_repository.sql", "r"
        ) as script_file:
            script = script_file.read()
        with connect(get_snapshot_store_path(), detect_types=PARSE_DECLTYPES) as conn:
            conn.executescript(script)
            conn.commit()
        conn.close()

    async def get_snapshot(self, aggregate_id: str) -> tuple[int, any] | None:
        q = f"""
            SELECT {FIELDS.SNAPSHOTS.VERSION}, {FIELDS.SNAPSHOTS.DATA}
            FROM {TABLES.SNAPSHOTS}
            WHERE {FIELDS.SNAPSHOTS.AGGREGATE_ID} = ?
        """
        p = (aggregate_id,)
        try:
            with connect(
                get_snapshot_store_path(), detect_types=PARSE_DECLTYPES
            ) as conn:
                conn.row_factory = Row
                cursor = conn.cursor()
                cursor.execute(q, p)
                result_set = cursor.fetchall()
                if not result_set:
                    return None
                row = result_set[0]
                conn.commit()
                return (
                    row[FIELDS.SNAPSHOTS.VERSION],
                    get_deserializer()(row[FIELDS.SNAPSHOTS.DATA]),
                )
        finally:
            conn.close()

    async def store_snapshot(self, aggregate_id: any, version: int, snapshot: any):
        q = f"""
            INSERT INTO {TABLES.SNAPSHOTS} (
                {FIELDS.SNAPSHOTS.AGGREGATE_ID},
                {FIELDS.SNAPSHOTS.VERSION},
                {FIELDS.SNAPSHOTS.DATA}
            ) VALUES (?,?,?)
            ON CONFLICT DO UPDATE SET
                {FIELDS.SNAPSHOTS.AGGREGATE_ID} = 
                    CASE WHEN {FIELDS.SNAPSHOTS.VERSION} < ? 
                    THEN ? 
                    ELSE {FIELDS.SNAPSHOTS.AGGREGATE_ID} 
                    END,
                {FIELDS.SNAPSHOTS.VERSION} = 
                    CASE WHEN {FIELDS.SNAPSHOTS.VERSION} < ? 
                    THEN ? ELSE {FIELDS.SNAPSHOTS.VERSION} 
                    END,
                {FIELDS.SNAPSHOTS.DATA} = 
                    CASE WHEN {FIELDS.SNAPSHOTS.VERSION} < ? 
                    THEN ? ELSE {FIELDS.SNAPSHOTS.DATA} 
                    END
        """
        serialized_snapshot = get_serializer()(snapshot)
        p = (
            aggregate_id,
            version,
            serialized_snapshot,
            version,
            aggregate_id,
            version,
            version,
            version,
            serialized_snapshot,
        )
        try:
            with connect(
                get_snapshot_store_path(), detect_types=PARSE_DECLTYPES
            ) as conn:
                conn.execute(q, p)
                conn.commit()
        finally:
            conn.close()

    async def delete_snapshot(self, aggregate_id: str):
        q = f"DELETE FROM {TABLES.SNAPSHOTS} WHERE {FIELDS.SNAPSHOTS.AGGREGATE_ID} = ?"
        p = (aggregate_id,)
        try:
            with connect(
                get_snapshot_store_path(), detect_types=PARSE_DECLTYPES
            ) as conn:
                conn.execute(q, p)
                conn.commit()
        finally:
            conn.close()
