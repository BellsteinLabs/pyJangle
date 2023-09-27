import sqlite3

from pyjangle import SnapshotRepository, get_event_deserializer, get_event_serializer

from pyjangle_sqllite3.symbols import DB_SNAPSHOTS_PATH, FIELDS, TABLES


class SqliteSnapshotRepository(SnapshotRepository):
    """Sqlite3 implementation of `SnapshotRepository`.

    Environment Variables:
        Use the environment variable `JANGLE_SNAPSHOTS_PATH` to specify a location for
        the database.

    Deserialization:
        The registered deserializer is expected receive a dictionary with keys
        corresponding to the fields in `symbols.FIELDS.SNAPSHOTS`.  It should return a
        tuple[int, any] where the first element is the version of the snapshot, and the
        second is the snapshot which is serialized as a string.

    Sesrialization:
        The registered serializer is expected convert the snapshot to a string.

    Adapters & Converters:
        Register the appropriate adapter and converter for your desired datetime formate
        *before* instantiating this class using `register_adapter` and
        `register_converter` from the sqlite3 package.  See the `adapters` module for
        examples.  As a convenience, use the function
        `register_datetime_and_decimal_adapters_and_converters` to register pre-made
        adapters and converters for decimal and datetime.
    """

    def __init__(self) -> None:
        with open(
            "pyjangle_sqllite3/create_snapshot_repository.sql", "r"
        ) as script_file:
            script = script_file.read()
        with sqlite3.connect(
            DB_SNAPSHOTS_PATH, detect_types=sqlite3.PARSE_DECLTYPES
        ) as conn:
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
            with sqlite3.connect(
                DB_SNAPSHOTS_PATH, detect_types=sqlite3.PARSE_DECLTYPES
            ) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(q, p)
                result_set = cursor.fetchall()
                if not result_set:
                    return None
                row = result_set[0]
                conn.commit()
                return (
                    row[FIELDS.SNAPSHOTS.VERSION],
                    get_event_deserializer()(row[FIELDS.SNAPSHOTS.DATA]),
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
        serialized_snapshot = get_event_serializer()(snapshot)
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
            with sqlite3.connect(
                DB_SNAPSHOTS_PATH, detect_types=sqlite3.PARSE_DECLTYPES
            ) as conn:
                conn.execute(q, p)
                conn.commit()
        finally:
            conn.close()

    async def delete_snapshot(self, aggregate_id: str):
        q = f"DELETE FROM {TABLES.SNAPSHOTS} WHERE {FIELDS.SNAPSHOTS.AGGREGATE_ID} = ?"
        p = (aggregate_id,)
        try:
            with sqlite3.connect(
                DB_SNAPSHOTS_PATH, detect_types=sqlite3.PARSE_DECLTYPES
            ) as conn:
                conn.execute(q, p)
                conn.commit()
        finally:
            conn.close()
