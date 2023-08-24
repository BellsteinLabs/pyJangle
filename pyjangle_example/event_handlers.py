from pyjangle import register_event_handler
from pyjangle.command.command_dispatcher import command_dispatcher_instance
from pyjangle.saga.saga_handler import handle_saga_event
from pyjangle_example.commands import CreditSendFunds
from pyjangle_example.events import ReceiveFundsApproved, ReceiveFundsRejected, ReceiveFundsRequested, SendFundsDebited
from pyjangle_example.saga import RequestFundsFromAnotherAccount, SendFundsToAnotherAccountSaga


@register_event_handler(SendFundsDebited)
async def handle_send_funds_debited_event(event: SendFundsDebited):
    credit_command = CreditSendFunds(funding_account_id=event.funding_account_id,
                                     funded_account_id=event.funded_account_id, amount=event.amount, transaction_id=event.transaction_id)
    response = await command_dispatcher_instance()(credit_command)
    if not response.is_success:
        raise Exception("sending funds failed")


@register_event_handler(ReceiveFundsRequested)
async def handle_receive_funds_requested(event: ReceiveFundsRequested):
    await handle_saga_event(saga_id=event.transaction_id, saga_type=RequestFundsFromAnotherAccount, event=event)


@register_event_handler(ReceiveFundsApproved)
async def handle_receive_funds_approved(event: ReceiveFundsApproved):
    await handle_saga_event(saga_id=event.transaction_id, saga_type=RequestFundsFromAnotherAccount, event=event)


@register_event_handler(ReceiveFundsRejected)
async def handle_receive_funds_rejected(event: ReceiveFundsRejected):
    await handle_saga_event(saga_id=event.transaction_id, saga_type=RequestFundsFromAnotherAccount, event=event)
