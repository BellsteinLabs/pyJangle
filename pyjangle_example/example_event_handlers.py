from pyjangle import register_event_handler
from pyjangle.saga.saga_handler import handle_saga_event
from pyjangle_example.example_events import SendFundsDebited
from pyjangle_example.example_saga import SendFundsToAnotherAccountSaga


@register_event_handler(SendFundsDebited)
async def handle_send_funds_debited_event(event: SendFundsDebited):
    await handle_saga_event(saga_id=event.transaction_id, event=event, saga_type=SendFundsToAnotherAccountSaga)
