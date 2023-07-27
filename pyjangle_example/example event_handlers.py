from pyjangle_example.example_commands import CreditSendFunds, RollbackSendFundsDebit, SendFunds
from pyjangle_example.example_events import SendFundsDebited

from pyjangle import (JangleError, command_dispatcher_instance,
                      register_event_handler)


class SendFundsRollbackError(JangleError):
    pass


@register_event_handler
def handle_send_funds_event(event: SendFundsDebited):
    command_dispatcher = command_dispatcher_instance()
    send_funds_command = CreditSendFunds(
        event.funding_account_id, funded_account_id=event.funded_account_id, transaction_id=event.transaction_id)
    command_response = command_dispatcher(send_funds_command)
    if not command_response.is_success:
        rollback_command = RollbackSendFundsDebit(
            funding_account_id=event.funding_account_id, transaction_id=event.transaction_id)
        if not command_dispatcher(rollback_command).is_success:
            raise SendFundsRollbackError()
