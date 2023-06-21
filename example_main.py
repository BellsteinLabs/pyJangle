import uuid
from example_commands import CreateAccount
from example_events import DebtForgiven
from pyjangle.command.command_handler import handle_command
from pyjangle.sqllite.event_handler_query_builder import SqlLite3QueryBuilder
from pyjangle.sqllite.sql_lite_event_repository import SqlLiteEventRepository
from json import dumps

command_1 = CreateAccount(account_id=uuid.uuid4(), name="HSPBC")
command_2 = CreateAccount(account_id=uuid.uuid4(), name="Natalie", initial_deposit=100)

#commands = [, , CreateAccount("Bob", initial_deposit=decimal(20)), CreateAccount("Hermione", initial_deposit=decimal(42))]

handle_command(command_1)

tuple_query_and_params = SqlLite3QueryBuilder("accounts")\
    .at(column_name="id", value="0001")\
    .at(column_name="id2", value="352")\
    .upsert(column_name="name", value="Tiffany", version_column_name="name_version",version_column_value=4)\
    .upsert(column_name="amount", value=42, version_column_name="amount_version", version_column_value=5)\
    .done()


print(tuple_query_and_params[0] + '\n')
print(tuple_query_and_params[1])

print(dumps(DebtForgiven(id='foo', version=42, account_id='4200', transaction_id='922678').__dict__))