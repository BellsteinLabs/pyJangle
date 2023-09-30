from datetime import datetime
from decimal import Decimal
from sqlite3 import register_adapter, register_converter
from typing import Callable
from pyjangle_sqlite3 import (
    PyjangleSqlite3Error,
    set_event_store_path,
    set_saga_store_path,
    set_snapshot_store_path,
    set_default_db_path,
    adapt_datetime,
    adapt_decimal,
    convert_datetime,
    convert_decimal,
)

_initialized = False


class NotInitialized(PyjangleSqlite3Error):
    pass


def raise_if_not_initialized():
    """Raises `NotInitialized` if `initialize` has yet to be called."""
    if not _initialized:
        raise NotInitialized("Missing call to pyjangle_sqlite3.initialize.")


def initialize_pyjangle_sqlite3(
    adapters: list[tuple[type, Callable]] = [
        (datetime, adapt_datetime),
        (Decimal, adapt_decimal),
    ],
    converters: list[tuple[str, Callable]] = [
        (datetime.__name__, convert_datetime),
        (Decimal.__name__, convert_decimal),
    ],
    default_db_path: str = None,
    event_store_path: str = None,
    saga_store_path: str = None,
    snapshot_store_path: str = None,
):
    """Registers necessary components.

    Args:
        adapters:
            List of tuples, each containing the arguments that would be passed to
            sqlite3.register_adapter (type, adapter, /).  If False, uses adapters in the
            `adapters_converters` module.
        converters:
            List of tuples, each containing the arguments that would be passed to
            sqlite3.register_converter (typename, converter, /).  If False, uses
            converters in `adapters_converters` module.
        default_db_path:
            Path used as a fallback whenever a db path name is not specified.
        event_store_path:
            File path to the event store database.  Defaults to environment variable
            `JANGLE_EVENT_STORE_PATH` then DEFAULT_DB_PATH.
        saga_store_path:
            File path to the saga store database.  Defaults to environment variable
            `JANGLE_SAGA_STORE_PATH` then DEFAULT_DB_PATH.
        snapshot_store_path:
            File path to the snapshot store database.  Defaults to environment variable
            `JANGLE_SNAPSHOTS_PATH` then DEFAULT_DB_PATH.
    """

    set_default_db_path(default_db_path)
    set_event_store_path(event_store_path)
    set_saga_store_path(saga_store_path)
    set_snapshot_store_path(snapshot_store_path)

    for a in adapters:
        register_adapter(*a)

    for c in converters:
        register_converter(*c)

    global _initialized
    _initialized = True
