from asyncio import run
from dataclasses import asdict
from json import dumps, loads
from logging import DEBUG
from uuid import uuid4

from pyjangle import (
    default_event_dispatcher_with_blacklist,
    MESSAGE,
    initialize_pyjangle,
    init_background_tasks,
)
from pyjangle_example.initialize import initialize_pyjangle_example
from pyjangle_example.json_encode_decode import CustomJSONDecoder, CustomJSONEncoder
from pyjangle_example.events import AccountIdProvisioned
from pyjangle_example.terminal_context import RootContext
from pyjangle_example import event_handlers
from pyjangle_example.aggregates import account_aggregate, account_creation_aggregate
from pyjangle_json_logging import initialize_logging
from pyjangle_sqlite3 import (
    SqliteEventRepository,
    SqliteSagaRepository,
    SqliteSnapshotRepository,
    initialize_pyjangle_sqlite3,
)

print("Jangle Banking terminal initializing...")

initialize_logging(MESSAGE, logging_level=DEBUG)
initialize_pyjangle_sqlite3(default_db_path="example.db")
initialize_pyjangle(
    event_repository_type=SqliteEventRepository,
    event_dispatcher_func=default_event_dispatcher_with_blacklist(AccountIdProvisioned),
    event_id_factory=lambda: str(uuid4()),
    deserializer=lambda x: loads(x, cls=CustomJSONDecoder),
    serializer=lambda event: dumps(asdict(event), cls=CustomJSONEncoder),
    saga_repository_type=SqliteSagaRepository,
    snapshot_repository_type=SqliteSnapshotRepository,
)
initialize_pyjangle_example(db_path="example.db")

print("Initialization complete.")


async def main():
    print("Initializing backgroun tasks...")
    init_background_tasks(failed_events_retry_interval_seconds=10)
    print("Background tasks initialized.")

    context = RootContext()
    while True:
        context = await context.run()


run(main())
