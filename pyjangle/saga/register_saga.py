import inspect
from pyjangle import JangleError
from pyjangle.saga.saga import Saga
from pyjangle.logging.logging import LogToggles, log

__name_to_saga_type_map = dict()
__saga_type_to_name_map = dict()


class SagaRegistrationError(JangleError):
    pass


def RegisterSaga(name: str = None):
    """Registers an saga and its name.

    When an saga is deserialized, some sort of 
    metadata (its name) is required to deserialize it to the 
    appropriate type.  The (human-readable) name is also useful when
    examining the saga_store or logs for troubleshooting
    purposes. 

    If no name is provided, the default implementation
    is: 

        type.__module__ + "." + type.__name__

    PARAMETERS
    ----------
    name - the name that should be registered
    to the saga.

    EXAMPLES
    --------
    "com.example.sagas.SomeDistributedTransaction"
    "NameUpdated"

    THROWS
    ------
    SagaRegistrationError if decorated member is
    not a subclass of Saga, or if the name is 
    already registered to another saga.
    """

    def decorator(cls):
        global __name_to_saga_type_map
        global __saga_type_to_name_map
        saga_name = ".".join([cls.__module__, cls.__name__]
                             ) if not name else name
        if not issubclass(cls, Saga):
            raise SagaRegistrationError("Decorated member is not an saga")
        if saga_name in __name_to_saga_type_map:
            raise SagaRegistrationError("Name already registered", {"name": saga_name, "current_registrant": str(
                __name_to_saga_type_map[saga_name]), "duplicate_registrant": str(cls)})
        __name_to_saga_type_map[saga_name] = cls
        __saga_type_to_name_map[cls] = saga_name
        log(LogToggles.saga_registered, "Saga registered", {
            "saga_name": saga_name, "saga_type": str(cls)})
        return cls
    if inspect.isclass(name):  # Decorator was used without parenthesis
        cls = name
        name = None
        return decorator(cls)
    return decorator


def get_saga_type(name: str) -> type:
    """Returns the type registered to an saga name.

    Names are registered to types vie @RegisterSaga.
    This function returns the type for a given name.

    THROWS 
    ------
    KeyError when name has no matching saga type.
    """
    try:
        return __name_to_saga_type_map[name]
    except KeyError:
        raise KeyError(
            f"No saga registered with name: {name}.  Ensure the saga is decorated with @RegisterSaga.")


def get_saga_name(saga_type: type) -> str:
    """Returns the name registered to an saga type.

    Names are registered to types vie @RegisterSaga.
    This function returns the name for a given saga type.

    THROWS 
    ------
    KeyError when name has no matching saga type.
    """
    try:
        return __saga_type_to_name_map[saga_type]
    except KeyError:
        raise KeyError(
            f"{str(type)} is not registered as an saga.  Ensure the saga is decorated with @RegisterSaga.")
