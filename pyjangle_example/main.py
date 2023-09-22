from asyncio import create_task, run, sleep
import os
from pyjangle import tasks
from pyjangle.command.command_dispatcher import RegisterCommandDispatcher
from pyjangle.command.command_handler import handle_command
from pyjangle.event.event_daemon import begin_retry_failed_events_loop
from pyjangle.event.event_dispatcher import (
    RegisterEventDispatcher,
    begin_processing_committed_events,
)
from pyjangle.event.event_handler import dispatch_event, dispatch_event_with_blacklist
from pyjangle.event.event_repository import RegisterEventRepository
from pyjangle.logging.logging import MESSAGE
from pyjangle.saga.saga_daemon import begin_retry_sagas_loop
from pyjangle.saga.saga_repository import RegisterSagaRepository
from pyjangle.serialization.event_serialization_registration import (
    register_event_deserializer,
    register_event_serializer,
)
from pyjangle.serialization.saga_serialization_registration import (
    register_saga_deserializer,
    register_saga_serializer,
)
from pyjangle.serialization.snapshot_serialization_registration import (
    register_snapshot_deserializer,
    register_snapshot_serializer,
)
from pyjangle.snapshot.snapshot_repository import RegisterSnapshotRepository
from pyjangle.test.serialization import (
    deserialize_event,
    deserialize_saga,
    deserialize_snapshot,
    serialize_event,
    serialize_saga,
    serialize_snapshot,
)
from pyjangle_example.custom_json_encoder import CustomJSONDecoder, CustomJSONEncoder
from pyjangle_example.events import AccountIdProvisioned
from pyjangle_example.terminal_context import RootContext
from pyjangle_example import event_handlers
from pyjangle_example.aggregates import account_aggregate, account_creation_aggregate
from pyjangle_example.data_access import sqlite3_bank_data_access_object
from pyjangle_json.logging import initialize_jangle_logging
from pyjangle_sqllite3.sql_lite_event_repository import SqlLiteEventRepository
from pyjangle_sqllite3.sql_lite_saga_repository import SqlLiteSagaRepository
from pyjangle_sqllite3.sql_lite_snapshot_repository import SqliteSnapshotRepository


async def main():
    begin_processing_committed_events()
    tasks.background_tasks.append(create_task(begin_retry_sagas_loop(120)))
    tasks.background_tasks.append(
        create_task(begin_retry_failed_events_loop(frequency_in_seconds=10))
    )

    context = RootContext()

    while True:
        context = await context.run()


os.environ["DB_JANGLE_BANKING_PATH"] = "jangle.db"
os.environ["JANGLE_SAGA_STORE_PATH"] = "jangle.db"
os.environ["JANGLE_SNAPSHOTS_PATH"] = "jangle.db"

initialize_jangle_logging(MESSAGE)
print("Jangle Banking terminal initializing...")
sqlite3_bank_data_access_object.Sqlite3BankDataAccessObject.initialize()
RegisterEventRepository(SqlLiteEventRepository)
RegisterEventDispatcher(dispatch_event_with_blacklist(AccountIdProvisioned))
register_event_deserializer(deserialize_event)
register_event_serializer(
    lambda event: serialize_event(event, json_encoder=CustomJSONEncoder)
)
register_saga_serializer(lambda saga: serialize_saga(saga, CustomJSONDecoder))
register_saga_deserializer(deserialize_saga)
register_snapshot_serializer(serialize_snapshot)
register_snapshot_deserializer(deserialize_snapshot)
RegisterSagaRepository(SqlLiteSagaRepository)
RegisterSnapshotRepository(SqliteSnapshotRepository)
RegisterCommandDispatcher(handle_command)


run(main())
