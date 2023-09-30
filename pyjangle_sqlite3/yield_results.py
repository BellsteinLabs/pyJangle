import sqlite3
from typing import Callable, Iterator

from pyjangle import Event


def yield_results(
    db_path: str,
    batch_size: int,
    query: str,
    params: tuple[any],
    row_handler: Callable[[any], Event],
) -> Iterator:
    """Convenience method for retrieving records from database.

    Args:
        db_path:
            Path to the database.
        batch_size:
            Max number of records to keep in memory
        query:
            Sql query that returns data.
        params:
            Parameters for `query`.
        deserializer:
            The deserializer that converts a rows to instance objects.
    """

    try:
        with sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.arraysize = batch_size
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            while True:
                rows = cursor.fetchmany()
                if not len(rows):
                    break
                for row in rows:
                    yield row_handler(row)
            conn.commit()
    finally:
        conn.close()
