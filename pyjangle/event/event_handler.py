import functools
import inspect
from typing import Callable, List, Type

from pyjangle import JangleError, VersionedEvent, LogToggles, log

# Registered event handlers singleton instance.
_event_type_to_event_handler_handler_map: dict[
    type, List[Callable[[VersionedEvent], None]]
] = dict()


class EventHandlerMissingError(JangleError):
    "Event handler not registered."
    pass


class EventHandlerError(JangleError):
    "An error occurred while handling an event."
    pass


class EventHandlerBadSignatureError(JangleError):
    "Event handler signature is invalid."
    pass


def register_event_handler(event_type: any):
    """Decorates a function that handles a type of event.

    The default event dispatcher, `default_event_dispatcher` in the `event_dispatcher`
    module, dispatches events to functions that are decorated with this decorator based
    on `event_type`.  If the implementation of the handler executes without error, the
    dispatcher will mark the event as completed.  If an exception is thrown, the event
    will not be marked completed.  Multiple event handlers *can* be registered to the
    same type.  Multiple logical event handlers could also be combined into a single
    event handler.

    The most common implementations of event handlers will:
    - Upsert data into a database based on the payload of the event.  That data is
      eventually retrieved by a query.
    - Instantiates a saga to facilitate a distributed transaction between aggregates.
    - Execute a very simple transaction where a proper saga is not needed.

    An event handler that is not idempotent is an error.  There are several reasons why
    an event handler might be executed multiple times or even concurrently on a single
    unique event:
    - The handler's execution is sufficiently long that the event appears to need to be
      retried based on the timeout passed to `begin_retry_failed_events_loop` in the
      `event_daemon` module.
    - A message queue generally does not have a guarantee that you won't receive a
      message more that once under certain conditions.  This is rare, but it will happen
      eventually.
    - A system update or outage necessitates an event replay, so the event handler
      might execute upsert code even though a record already exists.
    - Trigger some action such as an e-mail or SMS.

    Event handlers should also assume that it may not receive events in order.  This is
    expected in an asynchronous distributed environment.

    Args:
        event_type:
            The type of event the handler is mapped to.
    Signature:
        async def func_name(event: Event) -> None:
    """

    def decorator(wrapped: Callable[[VersionedEvent], None]):
        global _event_type_to_event_handler_handler_map
        if (
            not callable(wrapped)
            or len(inspect.signature(wrapped).parameters) != 1
            or not inspect.iscoroutinefunction(wrapped)
        ):
            raise EventHandlerBadSignatureError(
                """@register_event_handler should decoratate a function with signature: 
                async def func_name(event: Event) -> None
                """
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
    "Returns true if the type has a registered event handler.  False otherwise."
    global _event_type_to_event_handler_handler_map
    return event_type in _event_type_to_event_handler_handler_map


def event_type_to_handler_instance():
    "Returns the singleton instance of the mapping of event types to event handlers."
    return _event_type_to_event_handler_handler_map
