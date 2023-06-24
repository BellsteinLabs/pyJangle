import logging
from pyjangle.event.event import Event
from pyjangle.log_tools.log_tools import Toggles
from pyjangle.saga.saga import Saga
from pyjangle.saga.saga_metadata import SagaMetadata
from pyjangle.saga.saga_repository import saga_repository_instance

logger = logging.getLogger(__name__)

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
    if Toggles.Debug.log_saga_retrieved:
        logger.debug("Retrieved saga", {"id": saga_id, "metadata": saga_metadata.__dict__, "events": [x.__dict__ for x in saga_events]})
    if saga_metadata.is_complete:
        return
    saga = saga_type(saga_id, saga_events, saga_metadata.retry_at, saga_metadata.timeout_at, saga_metadata.is_complete)
    if event: 
        if Toggles.Debug.log_apply_event_to_saga:
            logger.debug("Applying event to saga", {"id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__,"event": event.__dict__})
        saga._apply_events([event])
        saga.evaluate()
    else:
        saga.evaluate()
    saga_repository.commit_saga(SagaMetadata(id=saga_id,type=saga_type, retry_at=saga.retry_at, timeout_at=saga.timeout_at, is_complete=saga.is_complete), saga.new_events)
    if Toggles.Debug.log_saga_committed:
        logger.debug("Committed saga to saga store.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__})

