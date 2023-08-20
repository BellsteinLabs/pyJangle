import functools
import sqlite3
from pyjangle.event.event import VersionedEvent
from pyjangle_example.data_access.db_schema import COLUMNS, TABLES

from pyjangle_example.data_access.db_settings import BATCH_SIZE, DB_JANGLE_BANKING_PATH
from pyjangle_sqllite3.dict_row_factory import dict_row_factory
from pyjangle_sqllite3.event_handler_query_builder import SqlLite3QueryBuilder as q_bldr


def fetch_multiple_rows(wrapped):
    async def wrapper(query):
        q, transform = await wrapped(query)
        result = list()
        try:
            with sqlite3.connect(DB_JANGLE_BANKING_PATH) as conn:
                conn.row_factory = dict_row_factory
                cursor = conn.cursor()
                cursor.arraysize = BATCH_SIZE
                if isinstance(q, str):
                    cursor.execute(q)
                    while True:
                        row_dicts = cursor.fetchmany()
                        if not len(row_dicts):
                            break
                        result += row_dicts
                elif (isinstance(q, list)):
                    for i in q:
                        cursor.execute(i)
                        while True:
                            row_dicts = cursor.fetchmany()
                            if not len(row_dicts):
                                break
                            result += row_dicts
        finally:
            conn.close()
        return transform(result)
    return wrapper


def fetch_single_row(wrapped):
    async def wrapper(query):
        q = await wrapped(query)
        try:
            with sqlite3.connect(DB_JANGLE_BANKING_PATH) as conn:
                conn.row_factory = dict_row_factory
                cursor = conn.cursor()
                cursor.arraysize = BATCH_SIZE
                cursor.execute(q)
        finally:
            conn.close()
        return cursor.fetchone()
    return wrapper


def upsert_single_row(wrapped):
    async def wrapper(query):
        q, p = await wrapped(query)
        try:
            with sqlite3.connect(DB_JANGLE_BANKING_PATH) as conn:
                conn.row_factory = dict_row_factory
                cursor = conn.cursor()
                cursor.execute(q, p)
        finally:
            conn.close()
    return wrapper


def upsert_multiple_rows(wrapped):
    async def wrapper(query):
        tupes = await wrapped(query)
        try:
            with sqlite3.connect(DB_JANGLE_BANKING_PATH) as conn:
                conn.row_factory = dict_row_factory
                cursor = conn.cursor()
                for tupe in tupes:
                    cursor.execute(tupe[0], tupe[1])
        finally:
            conn.close()
    return wrapper


def make_update_balance_query(account_id: str, event: VersionedEvent, balance_getter=None):
    return q_bldr(TABLES.BANK_SUMMARY) \
        .at(COLUMNS.BANK_SUMMARY.ACCOUNT_ID, account_id) \
        .upsert(COLUMNS.BANK_SUMMARY.BALANCE, balance_getter(event) if balance_getter else event.balance, COLUMNS.BANK_SUMMARY.BALANCE_VERSION, event.version) \
        .done()


def make_update_transactions_query(account_id: str, transaction_type: int, event: VersionedEvent):
    return q_bldr(TABLES.TRANSACTIONS) \
        .at(COLUMNS.TRANSACTIONS.EVENT_ID, event.id) \
        .upsert(COLUMNS.TRANSACTIONS.TRANSACTION_ID, event.transaction_id,) \
        .upsert(COLUMNS.TRANSACTIONS.ACCOUNT_ID, account_id) \
        .upsert(COLUMNS.TRANSACTIONS.INITIATED_AT, event.created_at) \
        .upsert(COLUMNS.TRANSACTIONS.AMOUNT, event.amount) \
        .upsert(COLUMNS.TRANSACTIONS.TRANSACTION_TYPE, transaction_type) \
        .done()
