import functools
import inspect
from typing import Callable
from pyjangle.error.error import JangleError
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
        raise SagaSerializerRegistrationError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise SagaSerializerRegistrationError("@register_saga_serializer must decorate a method with 1 parameters: saga: Saga")
    if __saga_serializer:
        raise SagaSerializerRegistrationError(f"A serializer is already registered: {str(type(__saga_serializer))}")
    __saga_serializer = wrapped
    log(LogToggles.serializer_registered, "Serializer registered", {"serializer", wrapped.__module__ + "." + wrapped.__name__})
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
        raise SagaDeserializerRegistrationError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise SagaDeserializerRegistrationError("@register_saga_deserializer must decorate a method with 1 parameters: serialized_saga: any")
    if __saga_deserializer:
        raise SagaDeserializerRegistrationError(f"A deserializer is already registered: {str(type(__saga_deserializer))}")
    __saga_deserializer = wrapped
    log(LogToggles.deserializer_registered, "Deserializer registered", {"deserializer", wrapped.__module__ + "." + wrapped.__name__})
    return wrapped

def get_saga_serializer():
    """Returns saga serializer that was registered with @register_saga_serializer
    
    THROWS
    ------
    SagaSerializerRegistrationError if no serializer is registered."""

    if not __saga_serializer:
        raise SagaSerializerRegistrationError("Saga serializer has not been registered with @register_saga_serializer")
    return __saga_serializer

def get_saga_deserializer():
    """Returns saga deserializer that was registered with @register_saga_deserializer
    
    THROWS
    ------
    SagaDeserializerRegistrationError if no deserializer is registered."""

    if not __saga_deserializer:
        raise SagaDeserializerRegistrationError("Saga deserializer has not been registered with @register_saga_deserializer")
    return __saga_deserializer

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
        raise EventSerializerRegistrationError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise EventSerializerRegistrationError("@register_event_serializer must decorate a method with 1 parameters: event: Event")
    if __event_serializer:
        raise EventSerializerRegistrationError(f"A serializer is already registered: {str(type(__event_serializer))}")
    __event_serializer = wrapped
    log(LogToggles.serializer_registered, "Serializer registered", {"serializer", wrapped.__module__ + "." + wrapped.__name__})
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
        raise EventDeserializerRegistrationError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise EventDeserializerRegistrationError("@register_event_deserializer must decorate a method with 1 parameters: serialized_event: any")
    if __event_deserializer:
        raise EventDeserializerRegistrationError(f"A deserializer is already registered: {str(type(__event_deserializer))}")
    __event_deserializer = wrapped
    log(LogToggles.deserializer_registered, "Deserializer registered", {"deserializer", wrapped.__module__ + "." + wrapped.__name__})
    return wrapped

def get_event_serializer():
    """Returns event serializer that was registered with @register_event_serializer
    
    THROWS
    ------
    EventSerializerRegistrationError if no serializer is registered."""

    if not __event_serializer:
        raise EventSerializerRegistrationError("Event serializer has not been registered with @register_event_serializer")
    return __event_serializer

def get_event_deserializer():
    """Returns event deserializer that was registered with @register_event_deserializer
    
    THROWS
    ------
    EventDeserializerRegistrationError if no deserializer is registered."""

    if not __event_deserializer:
        raise EventDeserializerRegistrationError("Event deserializer has not been registered with @register_event_deserializer")
    return __event_deserializer

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
        raise SnapshotSerializerRegistrationError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise SnapshotSerializerRegistrationError("@register_snapshot_serializer must decorate a method with 1 parameters: snapshot: Snapshot")
    if __snapshot_serializer:
        raise SnapshotSerializerRegistrationError(f"A serializer is already registered: {str(type(__snapshot_serializer))}")
    __snapshot_serializer = wrapped
    log(LogToggles.serializer_registered, "Serializer registered", {"serializer", wrapped.__module__ + "." + wrapped.__name__})
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
        raise SnapshotDeserializerRegistrationError("Decorated member is not a function")
    if len(inspect.signature(wrapped).parameters) != 1:
        raise SnapshotDeserializerRegistrationError("@register_snapshot_deserializer must decorate a method with 1 parameters: serialized_snapshot: any")
    if __snapshot_deserializer:
        raise SnapshotDeserializerRegistrationError(f"A deserializer is already registered: {str(type(__snapshot_deserializer))}")
    __snapshot_deserializer = wrapped
    log(LogToggles.deserializer_registered, "Deserializer registered", {"deserializer", wrapped.__module__ + "." + wrapped.__name__})
    return wrapped

def get_snapshot_serializer():
    """Returns snapshot serializer that was registered with @register_snapshot_serializer
    
    THROWS
    ------
    SnapshotSerializerRegistrationError if no serializer is registered."""

    if not __snapshot_serializer:
        raise SnapshotSerializerRegistrationError("Snapshot serializer has not been registered with @register_snapshot_serializer")
    return __snapshot_serializer

def get_snapshot_deserializer():
    """Returns snapshot deserializer that was registered with @register_snapshot_deserializer
    
    THROWS
    ------
    SnapshotDeserializerRegistrationError if no deserializer is registered."""

    if not __snapshot_deserializer:
        raise SnapshotDeserializerRegistrationError("Snapshot deserializer has not been registered with @register_snapshot_deserializer")
    return __snapshot_deserializer