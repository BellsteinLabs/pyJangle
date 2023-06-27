import functools
import logging
from typing import Callable, List

from pyjangle.error.error import JangleError
from pyjangle.event.event import Event
from pyjangle.log_tools.log_tools import LogToggles, log

logger = logging.getLogger(__name__)

#contains the singleton that maps event types to 
#event handlers.  You shouldn't need to access 
#this directly.
__event_type_to_event_handler_handler_map: dict[type, List[Callable[[Event], None]]] = dict()

class EventHandlerError(JangleError):
    pass

def register_event_handler(event_type: any):
    """Registers a function that handles an event.
    
    This could mean a lot of things, but the decorated
    function should answer the question, "Here's an event...
    now what?  
    
    A common case is that the event handler
    update database tables to reflect the information 
    in the event.  These handlers should be idempotent
    because there is generally never a guarantee that 
    an event won't be received more that once (even
    if it's a rare occurrence) in a distributed system.
    
    Another common case is that the event is to be used
    to drive a state change in a saga.  If that's the 
    case, the handler can call handle_saga_event() in 
    the saga_handler module.

    Another case might involve dispatching a command
    and commiting an event once the command is 
    responded to. 

    The bottom-line is that this is a very extensible 
    point in the framework.

    If the handler is completed without an exception
    having been thrown, the event is automatically 
    marked as completed on the durable event storage.

    Seriously, don't forget to make these IDEMPOTENT.
    """

    def decorator(wrapped: Callable[[Event], None]):
        @functools.wraps(wrapped)
        def wrapper(*args, **kwargs):
            event = args[0]
            event_completion_marker = args[1]
            try:
                wrapped(args[0])
                #Mark the event as completed if an exception wasn't thrown.
                #If it's not marked as completed, it will be picked up
                #by the retry daemon that you set up.  See the 
                #event_daemon module for more info.
                event_completion_marker(event)
            except:
                #log that the event wasn't handled properly
                pass
        if not event_type in __event_type_to_event_handler_handler_map:
            __event_type_to_event_handler_handler_map[event_type] = []
        __event_type_to_event_handler_handler_map[event_type].append(wrapper)
        log(LogToggles.event_handler_registration, "Event handler registered", {"event_type": str(event_type), "event_handler_type": str(type(wrapped))})
        return wrapper
    return decorator

def handle_event(event: Event, event_handled_callback: Callable[[Event], None], raise_on_missing_event_handler: bool = True):
    """Finds the appropriate event handler for the specified event.
    
    Event handlers are decorated with @register_event_handler.
    The decorated function will execute event_handled_callback
    if it completes without exception.  If an exception is thrown
    the event will be retried if a retry daemon is set up.  See 
    the event_daemon module for more information.
    
    THROWS
    ------
    EventHandlerError when there is a event handler registered for 
    an event type."""

    event_type = type(event)
    if not event_type in __event_type_to_event_handler_handler_map:
        if raise_on_missing_event_handler: 
            raise EventHandlerError("No event handler registered for " + str(event_type))
    try:
        for handler in __event_type_to_event_handler_handler_map[event_type]:
            handler(event, event_handled_callback)
    except:
        #log that handling events failed
        pass
    return