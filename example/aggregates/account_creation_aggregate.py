import uuid
from datetime import datetime

from ..commands import CreateAccount
from ..events import AccountCreated, AccountIdProvisioned, FundsDeposited

from pyjangle import (
    Aggregate,
    CommandResponse,
    RegisterAggregate,
    reconstitute_aggregate_state,
    validate_command,
)


@RegisterAggregate
class AccountCreationAggregate(Aggregate):
    @validate_command(CreateAccount)
    # TODO: Rename handle methods to validate
    def handle_create_account_command(
        self, command: CreateAccount, next_version: int
    ) -> CommandResponse:
        next_account_id = "{:06d}".format(next_version)
        account_id_incremented_event = AccountIdProvisioned(version=next_version)
        self.post_new_event(account_id_incremented_event)
        account_created_event = AccountCreated(
            version=1, account_id=next_account_id, name=command.name
        )
        # TODO: Define the add_to_new_events method
        self.post_new_event(account_created_event, aggregate_id=next_account_id)
        if command.initial_deposit:
            funds_deposited_event = FundsDeposited(
                version=2,
                account_id=next_account_id,
                balance=command.initial_deposit,
                amount=command.initial_deposit,
            )
            self.post_new_event(funds_deposited_event, next_account_id)
        return CommandResponse(True, next_account_id)

    @reconstitute_aggregate_state(AccountIdProvisioned)
    def handle_account_created_event(self, event: AccountIdProvisioned):
        pass
