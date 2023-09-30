import inspect
from typing import Callable
from pyjangle import JangleError, LogToggles, log

_serializer = None
_deserializer = None


class SerializerBadSignatureError(JangleError):
    "Serializer signature is invalid."
    pass


class SerializerMissingError(JangleError):
    "Serializer not registered."
    pass


class DeserializerBadSignatureError(JangleError):
    "Deserializer signature is invalid."
    pass


class DeserializerMissingError(JangleError):
    "Deserializer not registered."
    pass


def register_serializer(wrapped: Callable[[any], None]):
    """Registers a serializer.

    Wraps a function that can serialize data.  The wrapped function's output should be
    in the format expected by the registered event, snapshot, and saga repositories.

    Signature:
        def func_name(data: any) -> any:

    Raises:
        SerializerBadSignatureError:
            Serializer signature is invalid.
    """

    global _serializer
    if not inspect.isfunction(wrapped):
        raise SerializerBadSignatureError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise SerializerBadSignatureError(
            """@register_serializer must decorate a method with 1 parameters: 
            data: any"""
        )
    if _serializer:
        raise SerializerBadSignatureError(
            f"A serializer is already registered: {str(_serializer)}"
        )
    _serializer = wrapped
    log(
        LogToggles.serializer_registered,
        "Serializer registered",
        {"serializer", wrapped.__module__ + "." + wrapped.__name__},
    )
    return wrapped


def register_deserializer(wrapped: Callable[[any], None]):
    """Registers a deserializer.

    Wraps a function that can deserialize data.  This will typically be used by the
    event, snapshot, and saga repositories.  This function should 'undo' the action of
    the registered serializer.

    Signature:
        def func_name(serialized_data: any) -> any:

    Raises:
        DeserializerBadSignatureError:
            Deserializer signature is invalid.
    """

    global _deserializer
    if not inspect.isfunction(wrapped):
        raise DeserializerBadSignatureError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise DeserializerBadSignatureError(
            """@register_deserializer must decorate a method with 1 parameters: 
            serialized_data: any"""
        )
    if _deserializer:
        raise DeserializerBadSignatureError(
            f"A deserializer is already registered: {str(type(_deserializer))}"
        )
    _deserializer = wrapped
    log(
        LogToggles.deserializer_registered,
        "Deserializer registered",
        {"deserializer", wrapped.__module__ + "." + wrapped.__name__},
    )
    return wrapped


def get_serializer():
    """Returns serializer that was registered with @register_serializer

    Raises:
        EventSerializerMissingError:
            Serializer not registered.
    """

    if not _serializer:
        raise SerializerMissingError(
            "Serializer has not been registered with @register_serializer"
        )
    return _serializer


def get_deserializer():
    """Returns deserializer that was registered with @register_deserializer

    Raises:
        ventDeserializerMissingError:
            Deserializer not registered.
    """

    if not _deserializer:
        raise DeserializerMissingError(
            """Deserializer has not been registered with 
            @register_deserializer"""
        )
    return _deserializer
