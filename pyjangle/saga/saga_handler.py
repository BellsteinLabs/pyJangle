from pyjangle.error.error import JangleError
from pyjangle.event.event import Event
from pyjangle.event.event_repository import DuplicateKeyError
from pyjangle.logging.logging import LogToggles, log
from pyjangle.saga.saga import Saga
from pyjangle.saga.saga_repository import saga_repository_instance

class SagaHandlerError(JangleError):
    pass

async def handle_saga_event(saga_id: any, event: Event, saga_type: type[Saga] | None):
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
    saga = await saga_repository.get_saga(saga_id)
    # if _is_duplicate_event(event, saga_events=saga_events):
    #     log(LogToggles.saga_duplicate_event, "Duplicate event received for saga.", {"saga_id": saga_id, "metadata":  saga_metadata.__dict__, "event": event.__dict__})
    #     return 
    if not saga and not event:
        raise SagaHandlerError(f"Tried to restore non-existant saga with id '{saga_id}' and apply no events to it.")
    if (saga):
        log(LogToggles.saga_retrieved, "Retrieved saga", {"saga_id": saga_id, "saga":  saga.__dict__})
    else:
        log(LogToggles.saga_new, "Received first event in a new saga", {"saga_id": saga_id})
        saga = saga_type(saga_id=saga_id)
    if saga and (saga.is_complete or saga.is_timed_out):
        return
    await saga.evaluate(event)
    log(LogToggles.apply_event_to_saga, "Applied event to saga", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__,"event": event.__dict__})
    if saga.is_dirty:
        try:
            await saga_repository.commit_saga(saga)
        except DuplicateKeyError as e:
            log(LogToggles.saga_duplicate_key, "Concurrent saga execution detected.  This is unlikely and could indicate an issue.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__,"event": event.__dict__})
            return
        log(LogToggles.saga_committed, "Committed saga to saga store.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__})
    else:
        log(LogToggles.saga_nothing_happened, "Saga state was not changed.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__})

async def retry_saga(saga_id: any):
    saga_repository = saga_repository_instance()
    saga = await saga_repository.get_saga(saga_id)
    # if _is_duplicate_event(event, saga_events=saga_events):
    #     log(LogToggles.saga_duplicate_event, "Duplicate event received for saga.", {"saga_id": saga_id, "metadata":  saga_metadata.__dict__, "event": event.__dict__})
    #     return 
    if not saga:
        raise SagaHandlerError(f"Attempted to retry non-existent saga with id '{saga_id}'.")
    log(LogToggles.saga_retrieved, "Retrieved saga", {"saga_id": saga_id, "saga":  saga.__dict__})
    if saga.is_complete or saga.is_timed_out:
        return
    await saga.evaluate()
    if saga.is_dirty:
        try:
            await saga_repository.commit_saga(saga)
        except DuplicateKeyError as e:
            log(LogToggles.saga_duplicate_key, "Concurrent saga execution detected.  This is unlikely and could indicate an issue.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__})
            return
        log(LogToggles.saga_committed, "Committed saga to saga store.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__})
    else:
        log(LogToggles.saga_nothing_happened, "Saga state was not changed.", {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": saga.__dict__})