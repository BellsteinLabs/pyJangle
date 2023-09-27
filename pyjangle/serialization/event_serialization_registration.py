import inspect
from typing import Callable
from pyjangle import JangleError, LogToggles, log

_event_serializer = None
_event_deserializer = None


class EventSerializerBadSignatureError(JangleError):
    "Event serializer signature is invalid."
    pass


class EventSerializerMissingError(JangleError):
    "Event serializer not registered."
    pass


class EventDeserializerBadSignatureError(JangleError):
    "Event deserializer signature is invalid."
    pass


class EventDeserializerMissingError(JangleError):
    "Event deserializer not registered."
    pass


def register_event_serializer(wrapped: Callable[[any], None]):
    """Registers an event serializer.

    Wraps a function that can serialize events.  The wrapped function's output should be 
    in the format expected by the registered event repository.

    Signature:
        def func_name(event: Event) -> any:

    Raises:
        EventSerializerBadSignatureError:
            Event serializer signature is invalid.
    """

    global _event_serializer
    if not inspect.isfunction(wrapped):
        raise EventSerializerBadSignatureError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise EventSerializerBadSignatureError(
            """@register_event_serializer must decorate a method with 1 parameters: 
            event: Event"""
        )
    if _event_serializer:
        raise EventSerializerBadSignatureError(
            f"A serializer is already registered: {str(_event_serializer)}"
        )
    _event_serializer = wrapped
    log(
        LogToggles.serializer_registered,
        "Serializer registered",
        {"serializer", wrapped.__module__ + "." + wrapped.__name__},
    )
    return wrapped


def register_event_deserializer(wrapped: Callable[[any], None]):
    """Registers an event deserializer.

    Wraps a function that can deserialize events.  This will typically be used by the
    event repository.  The `serialized_event` parameter will contain whatever is
    provided from the event repository.  See the event repository documentation for 
    details.

    Signature:
        def func_name(serialized_event: any) -> Event:

    Raises:
        EventDeserializerBadSignatureError:
            Event deserializer signature is invalid.
    """

    global _event_deserializer
    if not inspect.isfunction(wrapped):
        raise EventDeserializerBadSignatureError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise EventDeserializerBadSignatureError(
            """@register_event_deserializer must decorate a method with 1 parameters: 
            serialized_event: any"""
        )
    if _event_deserializer:
        raise EventDeserializerBadSignatureError(
            f"A deserializer is already registered: {str(type(_event_deserializer))}"
        )
    _event_deserializer = wrapped
    log(
        LogToggles.deserializer_registered,
        "Deserializer registered",
        {"deserializer", wrapped.__module__ + "." + wrapped.__name__},
    )
    return wrapped


def get_event_serializer():
    """Returns event serializer that was registered with @register_event_serializer

    Raises:
        EventSerializerMissingError:
            Event serializer not registered.
    """

    if not _event_serializer:
        raise EventSerializerMissingError(
            "Event serializer has not been registered with @register_event_serializer"
        )
    return _event_serializer


def get_event_deserializer():
    """Returns event deserializer that was registered with @register_event_deserializer

    Raises:
        EventDeserializerMissingError:
            Event deserializer not registered.
    """

    if not _event_deserializer:
        raise EventDeserializerMissingError(
            """Event deserializer has not been registered with 
            @register_event_deserializer"""
        )
    return _event_deserializer
