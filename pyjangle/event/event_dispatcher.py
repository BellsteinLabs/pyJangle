import functools
import inspect
import logging
from typing import Callable, List

from pyjangle.error.error import JangleError
from pyjangle.event.event import Event
from pyjangle.log_tools.log_tools import Toggles

logger = logging.getLogger(__name__)

#holds the registered singleton event dispatcher.
#access this via event_dispatcher_instance()
__event_dispatcher = None


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
    event_handled_callback - a callback that 
    marks the event as "handled" on durable
    storage.  Unhandled events (maybe) because
    of a temporary network outage will not be 
    marked and can be retried later.

    SIGNATURE
    ---------
    def event_dispatcher(event: Event, event_handled_callback: Callable[[Event], None])

    THROWS
    ------
    EventDispatcherError when multiple event 
    dispatchers are registered.
    """
    if len(inspect.signature(wrapped).parameters) != 2:
            raise EventDispatcherError("@RegisterEventDispatcher must decorate a method with 2 parameters: event: Event, event_handled_callback: Callable[[Event], None]")
    global __event_dispatcher
    if __event_dispatcher != None:
        raise EventDispatcherError(
            "Cannot register multiple event dispatchers: " + str(type(__event_dispatcher)) + ", " + str(wrapped))
    __event_dispatcher = wrapped
    if Toggles.Info.log_event_dispatcher_registration:
        logger.info("Event dispatcher registered", {"event_dispatcher_type": str(type(wrapped))})
    @functools.wraps(wrapped)
    def wrapper(*args, **kwargs):
        return wrapped(*args, **kwargs)
    return wrapper


def event_dispatcher_instance() -> Callable[[List[Event], Callable[[Event], None]], None]:
    """Returns the registered singleton event dispatcher."""
    return __event_dispatcher
