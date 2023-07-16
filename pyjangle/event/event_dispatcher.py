from asyncio import Queue
import asyncio
import functools
import inspect
import logging
import os
from typing import Callable, List

from pyjangle.error.error import JangleError
from pyjangle.event.event import Event
from pyjangle.event.event_repository import event_repository_instance
from pyjangle.logging.logging import LogToggles, log

COMMITTED_EVENT_QUEUE_SIZE = int(os.getenv("JANGLE_COMMITTED_EVENT_QUEUE_SIZE", "200"))

async def begin_processing_committed_events():
    event_dispatcher = event_dispatcher_instance()
    if not event_dispatcher:
        raise EventDispatcherError("Unable to process committed events--no event dispatcher registered")
    log(LogToggles.event_dispatcher_ready, "Event dispatcher ready to process events")
    while True:
        event = await _committed_event_queue.get()
        await _dispatch_event(event)

async def _dispatch_event(event: Event):
    try:
        event_repo = event_repository_instance()
        await _event_dispatcher(event)
        await event_repo.mark_event_handled(event.id)
    except Exception as e:
        log(LogToggles.event_dispatching_error, "Encountered an error while dispatching event", {"event": event.__dict__}, exc_info=e)

#holds the registered singleton event dispatcher.
#access this via event_dispatcher_instance()
_event_dispatcher = None
_committed_event_queue = Queue(maxsize=COMMITTED_EVENT_QUEUE_SIZE)

async def enqueue_committed_event_for_dispatch(event: Event):
    await _committed_event_queue.put(event)

class EventDispatcherError(JangleError):
    pass


def RegisterEventDispatcher(wrapped):
    """Register a single event dispatcher.
    
    Once events are persisted to durable storage,
    maybe you're not using a message bus that will 
    send the events to handlers in some other 
    process.  Sometimes, just handling the events
    in the same process is good enough if you 
    have the memory and CPU to spare.  In that case
    just register one of these components and 
    ensure your call to handle_command in the 
    command_handler module has the flag set to
    handle events locally.  
    
    The decorated function's parameters are an 
    event - event to be handled

    SIGNATURE
    ---------
    async def event_dispatcher(event: Event)

    THROWS
    ------
    EventDispatcherError when multiple event 
    dispatchers are registered.
    """
    if len(inspect.signature(wrapped).parameters) != 1:
            raise EventDispatcherError("@RegisterEventDispatcher must decorate a method with 1 parameters: event: Event")
    global _event_dispatcher
    if _event_dispatcher != None:
        raise EventDispatcherError(
            "Cannot register multiple event dispatchers: " + str(type(_event_dispatcher)) + ", " + str(wrapped))
    _event_dispatcher = wrapped
    log(LogToggles.event_dispatcher_registration, "Event dispatcher registered", {"event_dispatcher_type": str(type(wrapped))})
    return wrapped


def event_dispatcher_instance() -> Callable[[List[Event], Callable[[Event], None]], None]:
    """Returns the registered singleton event dispatcher."""
    return _event_dispatcher
