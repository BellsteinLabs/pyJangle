from example_commands import CreditSendFunds, RollbackSendFundsDebit, SendFunds
from example_events import SendFundsDebited
from pyjangle.command.command_dispatcher import command_dispatcher_instance
from pyjangle.error.error import SquirmError
from pyjangle.event.event_handler import register_event_handler

class SendFundsRollbackError(SquirmError):
    pass

@register_event_handler
def handle_send_funds_event(event: SendFundsDebited):
    command_dispatcher = command_dispatcher_instance()
    send_funds_command = CreditSendFunds(event.funding_account_id, funded_account_id=event.funded_account_id, transaction_id=event.transaction_id)
    command_response = command_dispatcher(send_funds_command)
    if not command_response.is_success:
        rollback_command = RollbackSendFundsDebit(funding_account_id=event.funding_account_id, transaction_id=event.transaction_id)
        if not command_dispatcher(rollback_command).is_success:
            raise SendFundsRollbackError()