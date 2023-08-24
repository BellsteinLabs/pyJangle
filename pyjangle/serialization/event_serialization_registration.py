import inspect
from typing import Callable
from pyjangle import JangleError
from pyjangle.logging.logging import LogToggles, log

__event_serializer = None
__event_deserializer = None


class EventSerializerRegistrationError(JangleError):
    pass


class EventDeserializerRegistrationError(JangleError):
    pass


def register_event_serializer(wrapped: Callable[[any], None]):
    """Registers an event serializer.

    Wraps a function that can serialize events.  The 
    output should be in the format expected by the 
    registered persistence mechanisms (event store,
    saga store, etc.)

    SIGNATURE
    ---------
    def func_name(event: Event) -> any:

    THROWS
    ------
    EventSerializerRegistrationError when decorated 
    function is not a function, has the wrong signature,
    or if a serializer is already registered."""

    global __event_serializer
    if not inspect.isfunction(wrapped):
        raise EventSerializerRegistrationError(
            "Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise EventSerializerRegistrationError(
            "@register_event_serializer must decorate a method with 1 parameters: event: Event")
    if __event_serializer:
        raise EventSerializerRegistrationError(
            f"A serializer is already registered: {str(__event_serializer)}")
    __event_serializer = wrapped
    log(LogToggles.serializer_registered, "Serializer registered", {
        "serializer", wrapped.__module__ + "." + wrapped.__name__})
    return wrapped


def register_event_deserializer(wrapped: Callable[[any], None]):
    """Registers an event deserializer.

    Wraps a function that can deserialize events.  This 
    will typically be used by the event repository.  The 
    fields parameter will contain whatever is provided
    from your persistence mechanism (event store, 
    saga store, etc.)

    SIGNATURE
    ---------
    def func_name(serialized_event: any) -> Event:

    THROWS
    ------
    EventDeserializerRegistrationError when decorated 
    function is not a function, has the wrong signature,
    or if a deserializer is already registered"""

    global __event_deserializer
    if not inspect.isfunction(wrapped):
        raise EventDeserializerRegistrationError(
            "Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise EventDeserializerRegistrationError(
            "@register_event_deserializer must decorate a method with 1 parameters: serialized_event: any")
    if __event_deserializer:
        raise EventDeserializerRegistrationError(
            f"A deserializer is already registered: {str(type(__event_deserializer))}")
    __event_deserializer = wrapped
    log(LogToggles.deserializer_registered, "Deserializer registered",
        {"deserializer", wrapped.__module__ + "." + wrapped.__name__})
    return wrapped


def get_event_serializer():
    """Returns event serializer that was registered with @register_event_serializer

    THROWS
    ------
    EventSerializerRegistrationError if no serializer is registered."""

    if not __event_serializer:
        raise EventSerializerRegistrationError(
            "Event serializer has not been registered with @register_event_serializer")
    return __event_serializer


def get_event_deserializer():
    """Returns event deserializer that was registered with @register_event_deserializer

    THROWS
    ------
    EventDeserializerRegistrationError if no deserializer is registered."""

    if not __event_deserializer:
        raise EventDeserializerRegistrationError(
            "Event deserializer has not been registered with @register_event_deserializer")
    return __event_deserializer
