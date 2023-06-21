from pyjangle.event.event import Event
from pyjangle.saga.saga import Saga
from pyjangle.saga.saga_metadata import SagaMetadata
from pyjangle.saga.saga_repository import saga_repository_instance

def handle_saga_event(saga_id: any, event: Event, saga_type: type[Saga]):
    """Connects events to their respective sagas.
    
    When an event handler for an event related
    to a saga is called, this method facilitates
    retrieving other related events from the saga
    repository, checking that the saga isn't 
    already completed, and evaluating the new event
    to progress the saga's state.  Once the saga 
    has processed the latest event, it is 
    recommited to the saga store."""
    saga_repository = saga_repository_instance()
    saga_metadata, saga_events = saga_repository.get_saga(saga_id=saga_id)
    if saga_metadata.is_complete:
        return
    saga = saga_type(saga_id, saga_events, saga_metadata.retry_at, saga_metadata.timeout_at, saga_metadata.is_complete)
    if event: 
        saga._apply_events(event)
        saga.evaluate()
    else:
        saga.evaluate()
    saga_repository.commit_saga(SagaMetadata(id=saga_id,type=saga_type, retry_at=saga.retry_at, timeout_at=saga.timeout_at, is_complete=saga.is_complete), saga.new_events)

