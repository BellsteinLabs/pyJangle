import functools
import inspect
import logging
from typing import Any
from pyjangle import JangleError
from pyjangle.event.event import Event, VersionedEvent
from pyjangle.logging.logging import LogToggles, log

__name_to_event_type_map = dict()
__event_type_to_name_map = dict()
# __local_event_dispatcher = None

# class LocalEventDispatcherError(JangleError):
#     pass

# def register_local_event_dispatcher(wrapped):
#     global __local_event_dispatcher
#     if not callable(wrapped) or not not len(inspect.signature(wrapped).parameters == 2):
#         raise LocalEventDispatcherError("Must wrap a function with signature: def func_name(event: Event, mark_event_handled: Callable[[any], None]) -> None")
#     __local_event_dispatcher = wrapped
#     return wrapped

# def get_local_event_dispatcher():
#     return __local_event_dispatcher


class EventRegistrationError(JangleError):
    pass


def RegisterEvent(name: str = None):
    """Registers an event and its name.

    When an event is deserialized, some sort of 
    metadata (its name) is required to deserialize it to the 
    appropriate type.  The (human-readable) name is also useful when
    examining the event_store or logs for troubleshooting
    purposes. 

    If no name is provided, the default implementation
    is: 

        type.__module__ + "." + type.__name__

    PARAMETERS
    ----------
    name - the name that should be registered
    to the event.

    EXAMPLES
    --------
    "com.example.events.WidgetCreated"
    "NameUpdated"

    THROWS
    ------
    EventRegistrationError if decorated member is
    not a subclass of Event, or if the name is 
    already registered to another event.
    """

    def decorator(cls):
        global __name_to_event_type_map
        global __event_type_to_name_map
        event_name = ".".join(
            [cls.__module__, cls.__name__]) if not name else name
        if not issubclass(cls, Event):
            raise EventRegistrationError("Decorated member is not an event")
        if event_name in __name_to_event_type_map and __name_to_event_type_map[event_name] != cls:
            raise EventRegistrationError("Name already registered", {"name": event_name, "current_registrant": str(
                __name_to_event_type_map[event_name]), "duplicate_registrant": str(cls)})
        __name_to_event_type_map[event_name] = cls
        __event_type_to_name_map[cls] = event_name
        log(LogToggles.event_registered, "Event registered", {
            "event_name": event_name, "event_type": str(cls)})
        return cls
    if inspect.isclass(name):  # Decorator was used without parenthesis
        cls = name
        name = None
        return decorator(cls)
    return decorator


def get_event_type(name: str) -> type:
    """Returns the type registered to an event name.

    Names are registered to types vie @RegisterEvent.
    This function returns the type for a given name.

    THROWS 
    ------
    KeyError when name has no matching event type.
    """
    try:
        return __name_to_event_type_map[name]
    except KeyError:
        raise KeyError(
            f"No event registered with name: {name}.  Ensure the event is decorated with @RegisterEvent.")


def get_event_name(event_type: type) -> str:
    """Returns the name registered to an event type.

    Names are registered to types vie @RegisterEvent.
    This function returns the name for a given event type.

    THROWS 
    ------
    KeyError when name has no matching event type.
    """
    try:
        return __event_type_to_name_map[event_type]
    except KeyError:
        raise KeyError(
            f"{str(event_type)} is not registered as an event.  Ensure the event is decorated with @RegisterEvent.")
