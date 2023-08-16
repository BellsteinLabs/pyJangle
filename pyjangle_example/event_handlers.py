from pyjangle import register_event_handler
from pyjangle.saga.saga_handler import handle_saga_event
from pyjangle_example.events import ReceiveFundsApproved, ReceiveFundsRejected, ReceiveFundsRequested, SendFundsDebited
from pyjangle_example.saga import RequestFundsFromAnotherAccount, SendFundsToAnotherAccountSaga


@register_event_handler(SendFundsDebited)
async def handle_send_funds_debited_event(event: SendFundsDebited):
    await handle_saga_event(saga_id=event.transaction_id, event=event, saga_type=SendFundsToAnotherAccountSaga)


@register_event_handler(ReceiveFundsRequested)
async def handle_receive_funds_requested(event: ReceiveFundsRequested):
    await handle_saga_event(saga_id=event.transaction_id, saga_type=RequestFundsFromAnotherAccount, event=event)


@register_event_handler(ReceiveFundsApproved)
async def handle_receive_funds_approved(event: ReceiveFundsApproved):
    await handle_saga_event(saga_id=event.transaction_id, saga_type=RequestFundsFromAnotherAccount, event=event)


@register_event_handler(ReceiveFundsRejected)
async def handle_receive_funds_rejected(event: ReceiveFundsRejected):
    await handle_saga_event(saga_id=event.transaction_id, saga_type=RequestFundsFromAnotherAccount, event=event)
