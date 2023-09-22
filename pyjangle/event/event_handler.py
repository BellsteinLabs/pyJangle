import functools
import inspect
import logging
from typing import Awaitable, Callable, List, Type

from pyjangle import JangleError
from pyjangle import VersionedEvent
from pyjangle import LogToggles, log

# contains the singleton that maps event types to
# event handlers.  You shouldn't need to access
# this directly.
_event_type_to_event_handler_handler_map: dict[
    type, List[Callable[[VersionedEvent], None]]
] = dict()


class EventHandlerMissingError(JangleError):
    "Event handler not registered."
    pass


class EventHandlerError(JangleError):
    "An error occurred while handling an event."
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

    def decorator(wrapped: Callable[[VersionedEvent], None]):
        global _event_type_to_event_handler_handler_map
        if (
            not callable(wrapped)
            or len(inspect.signature(wrapped).parameters) != 1
            or not inspect.iscoroutinefunction(wrapped)
        ):
            raise EventHandlerRegistrationError(
                "@register_event_handler should decoratate a function with signature: async def func_name(event: Event) -> None"
            )
        if not event_type in _event_type_to_event_handler_handler_map:
            _event_type_to_event_handler_handler_map[event_type] = []
        _event_type_to_event_handler_handler_map[event_type].append(wrapped)
        log(
            LogToggles.event_handler_registration,
            "Event handler registered",
            {"event_type": str(event_type), "event_handler_type": str(type(wrapped))},
        )

        @functools.wraps
        async def wrapper(event: VersionedEvent):
            try:
                await wrapped(event)
            except Exception as e:
                log(
                    LogToggles.event_handler_failed,
                    "Event handler failed",
                    {
                        "event_type": str(event_type),
                        "event_handler_type": str(wrapped),
                        "event": vars(event),
                    },
                    exc_info=e,
                )
                raise EventHandlerError() from e

        return wrapper

    return decorator


def has_registered_event_handler(event_type: Type) -> bool:
    global _event_type_to_event_handler_handler_map
    return event_type in _event_type_to_event_handler_handler_map


def event_type_to_handler_instance():
    return _event_type_to_event_handler_handler_map
