import datetime
import json
import logging
import sqlite3
from typing import Callable
import uuid
import example_account_aggregate 
import example_account_creation_aggregate
from example_commands import CreateAccount
from example_events import DebtForgiven
from pyjangle import log_tools
from pyjangle.command.command_handler import handle_command
from pyjangle.event.event import Event
from pyjangle.event.event_dispatcher import RegisterEventDispatcher
from pyjangle.log_tools.log_tools import MESSAGE, initialize_jangle_logging
from pyjangle.serialization.dumps_encoders import CustomEncoder
from pyjangle.sqllite.event_handler_query_builder import SqlLite3QueryBuilder
from pyjangle.sqllite.sql_lite_event_repository import SqlLiteEventRepository

from json import dumps

sqlite3.register_adapter(uuid.UUID, lambda uuid: uuid.bytes)
sqlite3.register_converter('GUID', lambda b: uuid.UUID(bytes=b))

@RegisterEventDispatcher
def dispatch_events_locally(event: Event, event_handled_callback: Callable[[Event], None]):
    pass

initialize_jangle_logging(MESSAGE)

command_1 = CreateAccount(account_id=uuid.uuid4(), name="HSPBC")
command_2 = CreateAccount(account_id=uuid.uuid4(), name="Natalie", initial_deposit=100)

#commands = [, , CreateAccount("Bob", initial_deposit=decimal(20)), CreateAccount("Hermione", initial_deposit=decimal(42))]

handle_command(command_1)

# tuple_query_and_params = SqlLite3QueryBuilder("accounts")\
#     .at(column_name="id", value="0001")\
#     .at(column_name="id2", value="352")\
#     .upsert(column_name="name", value="Tiffany", version_column_name="name_version",version_column_value=4)\
#     .upsert(column_name="amount", value=42, version_column_name="amount_version", version_column_value=5)\
#     .done()