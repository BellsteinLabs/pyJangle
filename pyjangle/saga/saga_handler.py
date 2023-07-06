import logging
from pyjangle.error.error import JangleError
from pyjangle.event.event import Event
from pyjangle.event.event_repository import DuplicateKeyError
from pyjangle.log_tools.log_tools import LogToggles, log
from pyjangle.saga.saga import Saga
from pyjangle.saga.saga_metadata import SagaMetadata
from pyjangle.saga.saga_repository import saga_repository_instance

logger = logging.getLogger(__name__)

class SagaHandlerError(JangleError):
    pass

def handle_saga_event(saga_id: any, event: Event, saga_type: type[Saga]):
    """Connects events to their respective sagas.
    
    When an event handler for an event related
    to a saga is called, this method facilitates
    retrieving other related events from the saga
    repository, checking that the saga isn't 
    already completed, and evaluating the new event
    to progress the saga's state.  Once the saga 
    has processed the latest event, it is 
    recommited to the saga store.
    
    THROWS
    ------
    SagaHandlerError if event is empty and saga_id 
    has no corresponding events.
    """
    saga_repository = saga_repository_instance()
    saga_metadata, saga_events = saga_repository.get_saga(saga_id=saga_id)
    if not saga_events and not event:
        log(LogToggles.saga_empty, "Tried to restore non-existant saga and apply no events to it.", {"saga_id": saga_id, "metadata": saga_metadata.__dict__, "events": [x.__dict__ for x in saga_events], "event": event})
        return
    log(LogToggles.saga_retrieved, "Retrieved saga", {"saga_id": saga_id, "metadata": saga_metadata.__dict__, "events": [x.__dict__ for x in saga_events]})
    if saga_metadata.is_complete:
        return
    saga = saga_type(saga_id, saga_events, saga_metadata.retry_at, saga_metadata.timeout_at, saga_metadata.is_complete)
    if event: 
        saga.evaluate(event)
        log(LogToggles.apply_event_to_saga, "Applied event to saga", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__,"event": event.__dict__})
    else:
        saga.evaluate()
    if saga.is_dirty:
        try:
            saga_repository.commit_saga(SagaMetadata(id=saga_id,type=saga_type, retry_at=saga.retry_at, timeout_at=saga.timeout_at, is_complete=saga.is_complete), saga.new_events)
        except DuplicateKeyError as e:
            log(LogToggles.saga_duplicate_key, "Concurrent saga execution detected.  This is unlikely and could indicate an issue.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__,"event": event.__dict__})
            return
        log(LogToggles.saga_committed, "Committed saga to saga store.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__})
    else:
        log(LogToggles.saga_nothing_happened, "Saga state was not changed.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__})

