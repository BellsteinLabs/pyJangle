from asyncio import create_task, run, sleep
from dataclasses import asdict
from json import dumps, loads
import os
from pyjangle import (
    RegisterCommandDispatcher,
    handle_command,
    begin_retry_failed_events_loop,
    RegisterEventDispatcher,
    begin_processing_committed_events,
    default_event_dispatcher_with_blacklist,
    RegisterEventRepository,
    MESSAGE,
    begin_retry_sagas_loop,
    RegisterSagaRepository,
    register_event_deserializer,
    register_event_serializer,
    register_saga_deserializer,
    register_saga_serializer,
    register_snapshot_deserializer,
    register_snapshot_serializer,
    RegisterSnapshotRepository,
)

from pyjangle_example.custom_json_encoder import CustomJSONDecoder, CustomJSONEncoder
from pyjangle_example.events import AccountIdProvisioned
from pyjangle_example.terminal_context import RootContext
from pyjangle_example import event_handlers
from pyjangle_example.aggregates import account_aggregate, account_creation_aggregate
from pyjangle_example.data_access import sqlite3_bank_data_access_object
from pyjangle_json.logging import initialize_jangle_logging
from pyjangle_sqllite3.adapters import (
    register_datetime_and_decimal_adapters_and_converters,
)
from pyjangle_sqllite3.sql_lite_event_repository import SqlLiteEventRepository
from pyjangle_sqllite3.sql_lite_saga_repository import SqlLiteSagaRepository
from pyjangle_sqllite3.sql_lite_snapshot_repository import SqliteSnapshotRepository

os.environ["DB_JANGLE_BANKING_PATH"] = "jangle.db"
os.environ["JANGLE_SAGA_STORE_PATH"] = "jangle.db"
os.environ["JANGLE_SNAPSHOTS_PATH"] = "jangle.db"
os.environ["EVENTS_READY_FOR_DISPATCH_QUEUE_SIZE"] = 100
os.environ["SAGA_RETRY_INTERVAL"] = 30

initialize_jangle_logging(MESSAGE)
print("Jangle Banking terminal initializing...")
sqlite3_bank_data_access_object.Sqlite3BankDataAccessObject.initialize()
register_datetime_and_decimal_adapters_and_converters()
RegisterEventRepository(SqlLiteEventRepository)
RegisterEventDispatcher(default_event_dispatcher_with_blacklist(AccountIdProvisioned))
register_event_deserializer(lambda x: loads(x, cls=CustomJSONDecoder))
register_event_serializer(lambda event: dumps(asdict(event), cls=CustomJSONEncoder))
RegisterSagaRepository(SqlLiteSagaRepository)
RegisterSnapshotRepository(SqliteSnapshotRepository)
RegisterCommandDispatcher(handle_command)


async def main():
    begin_processing_committed_events()
    begin_retry_sagas_loop(120)
    begin_retry_failed_events_loop(frequency_in_seconds=10)

    context = RootContext()

    while True:
        context = await context.run()


run(main())
