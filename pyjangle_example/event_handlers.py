from pyjangle import register_event_handler
from pyjangle.command.command_dispatcher import command_dispatcher_instance
from pyjangle.saga.saga_handler import handle_saga_event
from pyjangle_example.commands import CreditTransfer
from pyjangle_example.events import (
    RequestApproved,
    RequestRejected,
    RequestCreated,
    TransferDebited,
)
from pyjangle_example.saga import RequestSaga


@register_event_handler(TransferDebited)
async def handle_send_funds_debited_event(event: TransferDebited):
    credit_command = CreditTransfer(
        funding_account_id=event.funding_account_id,
        funded_account_id=event.funded_account_id,
        amount=event.amount,
        transaction_id=event.transaction_id,
    )
    response = await command_dispatcher_instance()(credit_command)
    if not response.is_success:
        raise Exception("sending funds failed")


@register_event_handler(RequestCreated)
async def handle_receive_funds_requested(event: RequestCreated):
    await handle_saga_event(
        saga_id=event.transaction_id, saga_type=RequestSaga, event=event
    )


@register_event_handler(RequestApproved)
async def handle_receive_funds_approved(event: RequestApproved):
    await handle_saga_event(
        saga_id=event.transaction_id, saga_type=RequestSaga, event=event
    )


@register_event_handler(RequestRejected)
async def handle_receive_funds_rejected(event: RequestRejected):
    await handle_saga_event(
        saga_id=event.transaction_id, saga_type=RequestSaga, event=event
    )
