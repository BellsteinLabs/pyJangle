import sqlite3
from typing import Callable, Iterator


def yield_results(db_path: str, batch_size: int, query: str, params: tuple[any], deserializer: Callable[[dict], any]) -> Iterator:
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
                rows = cursor.fetchmany()
                if not len(rows):
                    break
                for row in rows:
                    yield deserializer(row)
            conn.commit()
    finally:
        conn.close()
