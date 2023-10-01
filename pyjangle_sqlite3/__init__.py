"""Sqlite3 implementations of persistence-related pyjangle types.

    Use the `initialize` module, specifically, `initialize_pyjangle_sqlite3` to register 
    components needed by this package.  Requires that serializers are registered using 
    `register_serializer` and `register_deserializer` decorators in the `pyjangle` 
    package.  Additionally, the event and saga repositories require that an event ID 
    factory is registered with the `register_event_id_factory` decorator.

Classes:
    SqliteEventRepository
    SqliteSagaRepository
    SqliteSnapshotRepository
    Sqlite3QueryBuilder
    TABLES,
    FIELDS

Functions:
    set_event_store_path,
    get_event_store_path,
    set_saga_store_path,
    get_saga_store_path,
    set_snapshot_store_path,
    get_snapshot_store_path,
    adapt_datetime,
    convert_datetime,
    adapt_decimal,
    convert_decimal,
    initialize,
    yield_results

Configuration:
    Use the following environment variables to configure the file location of databases.
    Using the same value for all databases is allowed.  These values can also be set 
    using `initialize`.

        JANGLE_EVENT_STORE_PATH 
        JANGLE_SAGA_STORE_PATH
        JANGLE_SNAPSHOTS_PATH
"""

from .pyjangle_sqlite3_error import PyjangleSqlite3Error
from .symbols import (
    TABLES,
    FIELDS,
    set_default_db_path,
    get_default_db_path,
    set_event_store_path,
    get_event_store_path,
    set_saga_store_path,
    get_saga_store_path,
    set_snapshot_store_path,
    get_snapshot_store_path,
)
from .yield_results import yield_results
from .adapters_converters import (
    adapt_datetime,
    convert_datetime,
    adapt_decimal,
    convert_decimal,
)
from .event_handler_query_builder import Sqlite3QueryBuilder
from .initialize import initialize_pyjangle_sqlite3, raise_if_not_initialized
from .sql_lite_event_repository import SqliteEventRepository
from .sql_lite_saga_repository import SqliteSagaRepository
from .sql_lite_snapshot_repository import SqliteSnapshotRepository
