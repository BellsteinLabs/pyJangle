import functools
import sqlite3
from pyjangle.event.event import VersionedEvent
from pyjangle_example.data_access.db_schema import COLUMNS, TABLES

from pyjangle_example.data_access.db_settings import BATCH_SIZE, DB_JANGLE_BANKING_PATH
from pyjangle_sqllite3 import dict_row_factory
from pyjangle_sqllite3.event_handler_query_builder import SqlLite3QueryBuilder as q_bldr


def fetch_multiple_rows(wrapped):
    @functools.wraps
    async def wrapper(*args, **kwargs):
        q, p = wrapped(*args, **kwargs)
        result = list()
        try:
            with sqlite3.connect(DB_JANGLE_BANKING_PATH) as conn:
                conn.row_factory = dict_row_factory
                cursor = conn.cursor()
                cursor.arraysize = BATCH_SIZE
                if isinstance(q, str):
                    cursor.execute(q, p)
                if (isinstance(q, list)):
                    cursor.executescript(";".join(q))
                while True:
                    row_dicts = cursor.fetchmany()
                    if not len(row_dicts):
                        break
                    result += row_dicts
        finally:
            conn.close()
        return result
    return wrapper

def fetch_single_row(wrapped):
    @functools.wraps
    async def wrapper(*args, **kwargs):
        q, p = wrapped(*args, **kwargs)
        try:
            with sqlite3.connect(DB_JANGLE_BANKING_PATH) as conn:
                conn.row_factory = dict_row_factory
                cursor = conn.cursor()
                cursor.arraysize = BATCH_SIZE
                cursor.execute(q, p)
        finally:
            conn.close()
        return cursor.fetchone()
    return wrapper

def upsert_single_row(wrapped):
    @functools.wraps
    async def wrapper(*args, **kwargs):
        q, p = wrapped(*args, **kwargs)
        try:
            with sqlite3.connect(DB_JANGLE_BANKING_PATH) as conn:
                conn.row_factory = dict_row_factory
                cursor = conn.cursor()
                cursor.execute(q, p)
        finally:
            conn.close()
    return wrapper

def upsert_multiple_rows(wrapped):
    @functools.wraps
    async def wrapper(*args, **kwargs):
        qp_tuples = wrapped(*args, **kwargs)
        try:
            with sqlite3.connect(DB_JANGLE_BANKING_PATH) as conn:
                conn.row_factory = dict_row_factory
                cursor = conn.cursor()
                for q, p in qp_tuples:
                    cursor.execute(q, p)
        finally:
            conn.close()
    return wrapper

def make_update_balance_query(account_id: str, event: VersionedEvent):
    return q_bldr(TABLES.BANK_SUMMARY) \
        .at(COLUMNS.BANK_SUMMARY.ACCOUNT_ID, account_id) \
            .upsert(COLUMNS.BANK_SUMMARY.BALANCE, 0, COLUMNS.BANK_SUMMARY.BALANCE_VERSION, event.version) \
        .done()

def make_update_transactions_query(account_id: str, transaction_type: int, event: VersionedEvent):
    return q_bldr(TABLES.TRANSACTIONS) \
        .at(COLUMNS.TRANSACTIONS.TRANSACTION_ID, event.transaction_id,) \
            .upsert(COLUMNS.TRANSACTIONS.ACCOUNT_ID, account_id) \
            .upsert(COLUMNS.TRANSACTIONS.INITIATED_AT, event.created_at) \
            .upsert(COLUMNS.TRANSACTIONS.AMOUNT, event.amount) \
            .upsert(COLUMNS.TRANSACTIONS.DESCRIPTION, transaction_type) \
        .done()