import inspect
from uuid import uuid4

from pyjangle import JangleError


class DuplicateEventIdFactoryRegistrationError(JangleError):
    """Registering multiple event id factories is not allowed."""

    pass


class EventIdRegistrationFactoryBadSignatureError(JangleError):
    """Event id registration factory signature is invalid."""

    pass


def default_event_id_factory():
    "Default implementation of an event id registration factory.  Returns random UUID"
    return uuid4()


# Registered event id registration factory accessible via `event_id_factory_instance`.
_event_id_factory = default_event_id_factory


def register_event_id_factory(wrapped):
    """Decorates a function that is registered as the event id factory.

    An event id factory is called whenever a new event is created.  The return value of
    this function is used as the event's unique identifier.

    Raises:
        DuplicateEventIdFactoryRegistrationError:
            Registering multiple event id factories is not allowed.
        EventIdRegistrationFactoryBadSignatureError:
            Event id registration factory signature is invalid.
    """

    global _event_id_factory
    if _event_id_factory != default_event_id_factory:
        raise DuplicateEventIdFactoryRegistrationError(
            f"""Already registered: {str(_event_id_factory)}  Unable to register: 
            {str(wrapped)}"""
        )
    if not callable(wrapped) or len(inspect.signature(wrapped).parameters) != 0:
        raise EventIdRegistrationFactoryBadSignatureError(
            f"""@{register_event_id_factory.__name__} must decorate a callable with 
            signature: def func_name()"""
        )
    _event_id_factory = wrapped
    return wrapped


def event_id_factory_instance():
    "Returns the singleton instance of the registered event id factory."
    return _event_id_factory
