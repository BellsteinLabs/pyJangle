import uuid
from datetime import datetime

from example_commands import CreateAccount
from example_events import AccountCreated, AccountIdProvisioned, FundsDeposited

from pyjangle import (Aggregate, CommandResponse, RegisterCommand,
                      reconstitute_aggregate_state, validate_command)

ACCOUNT_CREATION_AGGREGATE_ID = uuid.uuid3(
    uuid.NAMESPACE_URL, "ACCOUNT_CREATION_AGGREGATE")


@RegisterCommand(CreateAccount)
class AccountCreationAggregate(Aggregate):

    @validate_command(CreateAccount)
    # TODO: Rename handle methods to validate
    def handle_create_account_command(self, command: CreateAccount, next_version: int) -> CommandResponse:
        next_account_id = "{:06d}".format(next_version)
        account_id_incremented_event = AccountIdProvisioned(
            version=next_version, id=ACCOUNT_CREATION_AGGREGATE_ID, created_at=datetime.now())
        self._post_new_event(account_id_incremented_event)
        account_created_event = AccountCreated(
            version=1, account_id=next_account_id, name=command.name, created_at=datetime.now())
        # TODO: Define the add_to_new_events method
        self._post_new_event(account_created_event)
        if command.initial_deposit:
            funds_deposited_event = FundsDeposited(
                version=2, account_id=next_account_id, initial_deposit=command.initial_deposit)
            self._post_new_event(funds_deposited_event)
        return CommandResponse(True, next_account_id)

    @reconstitute_aggregate_state(AccountIdProvisioned)
    def handle_account_created_event(self, event: AccountIdProvisioned):
        pass
