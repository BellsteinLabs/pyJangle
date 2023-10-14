import inspect
from pyjangle import JangleError
from pyjangle.saga.saga import Saga
from pyjangle.logging.logging import LogToggles, log

__name_to_saga_type_map = dict()
__saga_type_to_name_map = dict()


class SagaRegistrationError(JangleError):
    "Decorated member is not a saga class."
    pass


class DuplicateSagaNameError(JangleError):
    "The registered name has already been used."
    pass


def RegisterSaga(name: str = None):
    """Registers an saga with an associated name.

    Registering a saga with a name serves two purposes.  First, because the serialized
    saga is stored with its name, the name suggests what type the saga should be
    deserialized into.  Additionally, the name of the saga can be useful when
    examining logs and records when troubleshooting.  If no name is provided,
    the default implementation is:

        type.__module__ + "." + type.__name__

    Here are some examples of other types of names you may find useful:

        "com.example.sagas.SomeDistributedTransaction"
        "DistributeFundsAcrossAccounts"

    Args:
        name - the name that the saga is registered with.

    Raises:
        SagaRegistrationError:
            Decorated member is not a saga class.
        DuplicateSagaNameError:
            The registered name has already been used.
    """

    def decorator(cls):
        global __name_to_saga_type_map
        global __saga_type_to_name_map
        saga_name = ".".join([cls.__module__, cls.__name__]) if not name else name
        if not issubclass(cls, Saga):
            raise SagaRegistrationError("Decorated member is not an saga")
        if saga_name in __name_to_saga_type_map:
            raise DuplicateSagaNameError(
                "Name already registered",
                {
                    "name": saga_name,
                    "current_registrant": str(__name_to_saga_type_map[saga_name]),
                    "duplicate_registrant": str(cls),
                },
            )
        __name_to_saga_type_map[saga_name] = cls
        __saga_type_to_name_map[cls] = saga_name
        log(
            LogToggles.saga_registered,
            "Saga registered",
            {"saga_name": saga_name, "saga_type": str(cls)},
        )
        return cls

    if inspect.isclass(name):  # Decorator was used without parenthesis
        cls = name
        name = None
        return decorator(cls)
    return decorator


def get_saga_type(name: str) -> type:
    """Returns the type registered to an saga name.

    Names are registered to types via `RegisterSaga`.  This function returns the type
    for a given name.

    Args:
        name:
            A name that has been associated to a saga type.

    Returns:
        Type of saga associated to `name`.

    Raises:
        KeyError:
            Name is not associated to a saga.
    """

    try:
        return __name_to_saga_type_map[name]
    except KeyError:
        raise KeyError(
            f"""No saga registered with name: {name}.  Ensure the saga is decorated with 
            @RegisterSaga."""
        )


def get_saga_name(saga_type: type) -> str:
    """Returns the name registered to a saga type.

    Names are registered to types vie `RegisterSaga`.  This function returns the name
    for a given saga type.

    Args:
        saga_type:
            A saga type that has been associated to a name.

    Returns:
        The name associated to `saga_type`.

    Raises:
        KeyError:
            `saga_type` is not associated to a name.
    """
    try:
        return __saga_type_to_name_map[saga_type]
    except KeyError:
        raise KeyError(
            f"""{str(type)} is not registered as a saga.  Ensure the saga is decorated 
            with `RegisterSaga`."""
        )
