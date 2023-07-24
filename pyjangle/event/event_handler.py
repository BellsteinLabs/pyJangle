import functools
import inspect
import logging
from typing import Callable, List

from pyjangle import JangleError
from pyjangle.event.event import Event
from pyjangle.logging.logging import LogToggles, log

# contains the singleton that maps event types to
# event handlers.  You shouldn't need to access
# this directly.
__event_type_to_event_handler_handler_map: dict[type, List[Callable[[
    Event], None]]] = dict()


class EventHandlerError(JangleError):
    pass


class EventHandlerRegistrationError(JangleError):
    pass


def register_event_handler(event_type: any):
    """Registers a function that handles an event.

    This could mean a lot of things, but the decorated
    function should answer the question, "Here's an event...
    now what?  

    The signature for the wrapped function is:

        def func_name(event: Event) -> None:

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
        if not callable(wrapped) or len(inspect.signature(wrapped).parameters) != 1 or not inspect.iscoroutinefunction(wrapped):
            raise EventHandlerRegistrationError(
                "@register_event_handler should decoratate a function with signature: async def func_name(event: Event) -> None")
        if not event_type in __event_type_to_event_handler_handler_map:
            __event_type_to_event_handler_handler_map[event_type] = []
        __event_type_to_event_handler_handler_map[event_type].append(wrapped)
        log(LogToggles.event_handler_registration, "Event handler registered", {
            "event_type": str(event_type), "event_handler_type": str(type(wrapped))})
        return wrapped
    return decorator


async def handle_event(event: Event):
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
        raise EventHandlerError(
            "No event handler registered for " + str(event_type))
    try:
        for handler in __event_type_to_event_handler_handler_map[event_type]:
            await handler(event)
    except:
        log(LogToggles.event_handler_failed, "Event handler failed", {"event_type": str(
            event_type), "event_handler_type": str(type(handler)), "event": event.__dict__})
    return
