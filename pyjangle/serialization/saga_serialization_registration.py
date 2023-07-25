import inspect
from typing import Callable
from pyjangle import JangleError
from pyjangle.logging.logging import LogToggles, log

__saga_serializer = None
__saga_deserializer = None


class SagaSerializerRegistrationError(JangleError):
    pass


class SagaDeserializerRegistrationError(JangleError):
    pass


def register_saga_serializer(wrapped: Callable[[any], None]):
    """Registers an saga serializer.

    Wraps a function that can serialize sagas.  The 
    output should be in the format expected by the 
    registered persistence mechanisms (saga store,
    saga store, etc.)

    SIGNATURE
    ---------
    def func_name(saga: Saga) -> any:

    THROWS
    ------
    SagaSerializerRegistrationError when decorated 
    function is not a function, has the wrong signature,
    or if a serializer is already registered."""

    global __saga_serializer
    if not inspect.isfunction(wrapped):
        raise SagaSerializerRegistrationError(
            "Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise SagaSerializerRegistrationError(
            "@register_saga_serializer must decorate a method with 1 parameters: saga: Saga")
    if __saga_serializer:
        raise SagaSerializerRegistrationError(
            f"A serializer is already registered: {str(type(__saga_serializer))}")
    __saga_serializer = wrapped
    log(LogToggles.serializer_registered, "Serializer registered", {
        "serializer", wrapped.__module__ + "." + wrapped.__name__})
    return wrapped


def register_saga_deserializer(wrapped: Callable[[any], None]):
    """Registers an saga deserializer.

    Wraps a function that can deserialize sagas.  This 
    will typically be used by the saga repository.  The 
    fields parameter will contain whatever is provided
    from your persistence mechanism (saga store, 
    saga store, etc.)

    SIGNATURE
    ---------
    def func_name(serialized_saga: any) -> Saga:

    THROWS
    ------
    SagaDeserializerRegistrationError when decorated 
    function is not a function, has the wrong signature,
    or if a deserializer is already registered"""

    global __saga_deserializer
    if not inspect.isfunction(wrapped):
        raise SagaDeserializerRegistrationError(
            "Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise SagaDeserializerRegistrationError(
            "@register_saga_deserializer must decorate a method with 1 parameters: serialized_saga: any")
    if __saga_deserializer:
        raise SagaDeserializerRegistrationError(
            f"A deserializer is already registered: {str(type(__saga_deserializer))}")
    __saga_deserializer = wrapped
    log(LogToggles.deserializer_registered, "Deserializer registered",
        {"deserializer", wrapped.__module__ + "." + wrapped.__name__})
    return wrapped


def get_saga_serializer():
    """Returns saga serializer that was registered with @register_saga_serializer

    THROWS
    ------
    SagaSerializerRegistrationError if no serializer is registered."""

    if not __saga_serializer:
        raise SagaSerializerRegistrationError(
            "Saga serializer has not been registered with @register_saga_serializer")
    return __saga_serializer


def get_saga_deserializer():
    """Returns saga deserializer that was registered with @register_saga_deserializer

    THROWS
    ------
    SagaDeserializerRegistrationError if no deserializer is registered."""

    if not __saga_deserializer:
        raise SagaDeserializerRegistrationError(
            "Saga deserializer has not been registered with @register_saga_deserializer")
    return __saga_deserializer
