import functools
import inspect
import types
from uuid import uuid4


class EventIdRegistrationError(Exception):
    pass


def _default_event_id_factory():
    return uuid4()


__event_id_factory = _default_event_id_factory


def register_event_id_factory(wrapped):
    global __event_id_factory
    if __event_id_factory != _default_event_id_factory:
        raise EventIdRegistrationError(
            f"Already registered: {str(__event_id_factory)}  Unable to register: {str(wrapped)}")
    if not callable(wrapped) or len(inspect.signature(wrapped).parameters) != 0:
        raise EventIdRegistrationError(
            f"@{register_event_id_factory.__name__} must decorate a callable with signature: def func_name()")
    __event_id_factory = wrapped
    return wrapped


def event_id_factory_instance():
    return __event_id_factory
