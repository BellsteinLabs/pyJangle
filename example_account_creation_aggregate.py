import uuid
from example_commands import CreateAccount
from example_events import AccountCreated, AccountIdProvisioned, FundsDeposited
from pyjangle.aggregate.aggregate import Aggregate, reconstitute_aggregate_state, validate_command
from pyjangle.command import command_response
from pyjangle.command.register import RegisterCommand

ACCOUNT_CREATION_AGGREGATE_ID = "ACCOUNT_CREATION_AGGREGATE"

@RegisterCommand(CreateAccount)
class AccountCreationAggregate(Aggregate):

    @validate_command(CreateAccount)
    # TODO: Rename handle methods to validate
    def handle_create_account_command(self, command: CreateAccount, next_version: int) -> command_response:
        next_account_id = "{:06d}".format(next_version)
        account_id_incremented_event = AccountIdProvisioned(version=next_version, id=ACCOUNT_CREATION_AGGREGATE_ID)
        self._post_new_event(account_id_incremented_event)
        account_created_event = AccountCreated(id=uuid.uuid4, version=1, account_id=next_account_id, name=command.name)
        self._post_new_event(account_created_event)  # TODO: Define the add_to_new_events method
        if command.initial_deposit:
            funds_deposited_event = FundsDeposited(version=2, account_id=next_account_id, initial_deposit=command.initial_deposit)
            self._post_new_event(funds_deposited_event)
        return command_response.CommandResponse(True, next_account_id)

    @reconstitute_aggregate_state(AccountIdProvisioned)
    def handle_account_created_event(self, event: AccountIdProvisioned):
        pass

