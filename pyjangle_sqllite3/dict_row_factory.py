import sqlite3


def dict_row_factory(cursor: sqlite3.Cursor, row: tuple):
    d = dict()
    for i, col in enumerate(cursor.description):
        d[col[0]] = row[i]
    return d
