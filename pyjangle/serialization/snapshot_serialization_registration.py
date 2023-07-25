import inspect
from typing import Callable
from pyjangle import JangleError
from pyjangle.logging.logging import LogToggles, log

__snapshot_serializer = None
__snapshot_deserializer = None


class SnapshotSerializerRegistrationError(JangleError):
    pass


class SnapshotDeserializerRegistrationError(JangleError):
    pass


def register_snapshot_serializer(wrapped: Callable[[any], None]):
    """Registers an snapshot serializer.

    Wraps a function that can serialize snapshots.  The 
    output should be in the format expected by the 
    registered persistence mechanisms (snapshot store,
    saga store, etc.)

    SIGNATURE
    ---------
    def func_name(snapshot: Snapshot) -> any:

    THROWS
    ------
    SnapshotSerializerRegistrationError when decorated 
    function is not a function, has the wrong signature,
    or if a serializer is already registered."""

    global __snapshot_serializer
    if not inspect.isfunction(wrapped):
        raise SnapshotSerializerRegistrationError(
            "Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise SnapshotSerializerRegistrationError(
            "@register_snapshot_serializer must decorate a method with 1 parameters: snapshot: Snapshot")
    if __snapshot_serializer:
        raise SnapshotSerializerRegistrationError(
            f"A serializer is already registered: {str(type(__snapshot_serializer))}")
    __snapshot_serializer = wrapped
    log(LogToggles.serializer_registered, "Serializer registered", {
        "serializer", wrapped.__module__ + "." + wrapped.__name__})
    return wrapped


def register_snapshot_deserializer(wrapped: Callable[[any], None]):
    """Registers an snapshot deserializer.

    Wraps a function that can deserialize snapshots.  This 
    will typically be used by the snapshot repository.  The 
    fields parameter will contain whatever is provided
    from your persistence mechanism (snapshot store, 
    saga store, etc.)

    SIGNATURE
    ---------
    def func_name(serialized_snapshot: any) -> Snapshot:

    THROWS
    ------
    SnapshotDeserializerRegistrationError when decorated 
    function is not a function, has the wrong signature,
    or if a deserializer is already registered"""

    global __snapshot_deserializer
    if not inspect.isfunction(wrapped):
        raise SnapshotDeserializerRegistrationError(
            "Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise SnapshotDeserializerRegistrationError(
            "@register_snapshot_deserializer must decorate a method with 1 parameters: serialized_snapshot: any")
    if __snapshot_deserializer:
        raise SnapshotDeserializerRegistrationError(
            f"A deserializer is already registered: {str(type(__snapshot_deserializer))}")
    __snapshot_deserializer = wrapped
    log(LogToggles.deserializer_registered, "Deserializer registered",
        {"deserializer", wrapped.__module__ + "." + wrapped.__name__})
    return wrapped


def get_snapshot_serializer():
    """Returns snapshot serializer that was registered with @register_snapshot_serializer

    THROWS
    ------
    SnapshotSerializerRegistrationError if no serializer is registered."""

    if not __snapshot_serializer:
        raise SnapshotSerializerRegistrationError(
            "Snapshot serializer has not been registered with @register_snapshot_serializer")
    return __snapshot_serializer


def get_snapshot_deserializer():
    """Returns snapshot deserializer that was registered with @register_snapshot_deserializer

    THROWS
    ------
    SnapshotDeserializerRegistrationError if no deserializer is registered."""

    if not __snapshot_deserializer:
        raise SnapshotDeserializerRegistrationError(
            "Snapshot deserializer has not been registered with @register_snapshot_deserializer")
    return __snapshot_deserializer
