import inspect

from pyjangle import (
    Aggregate,
    JangleError,
    LogToggles,
    log,
    COMMAND_TYPE_ATTRIBUTE_NAME,
)

# Maps commands to aggregates.  Access via `command_to_aggregate_map_instance`
_command_to_aggregate_map: dict[any, Aggregate] = dict()


class DuplicateCommandRegistrationError(JangleError):
    "Detected duplicate command registration."
    pass


class AggregateRegistrationError(JangleError):
    "Encountered non-Aggregate class."
    pass


def RegisterAggregate(cls: Aggregate):
    """Registers an Aggregate class.

    Associates an aggregate with the commands that it is responsible for validating.
    This is accomplished by searching for methods decorated with the `validate_command`
    decorator.  Registered aggregates and their commands can be retrieved via
    calling `command_to_aggregate_map_instance`.

    Raises:
        DuplicateCommandRegistrationError:
            Detected duplicate command registration.

        AggregateRegistrationError:
            Encountered non-Aggregate class.
    """

    global _command_to_aggregate_map
    # Make sure the decorated member is an aggregate.
    if not issubclass(cls, Aggregate):
        raise AggregateRegistrationError("Decorated member is not an Aggregate")

    command_types = []
    methods = [
        getattr(cls, method_name)
        for method_name in dir(cls)
        if not method_name.startswith("_")
        and inspect.isfunction(getattr(cls, method_name))
    ]
    for method in methods:
        command_type = getattr(method, COMMAND_TYPE_ATTRIBUTE_NAME, None)
        if command_type:
            if command_type in _command_to_aggregate_map:
                raise DuplicateCommandRegistrationError(
                    "Command type '" + str(command_types) + "' already registered"
                )
            command_types.append(command_type)

    _command_to_aggregate_map = _command_to_aggregate_map | dict.fromkeys(
        command_types, cls
    )

    log(
        LogToggles.command_registered_to_aggregate,
        "Commands registered to aggregate",
        {"aggregate_type": str(cls), "command_types": list(command_types)},
    )

    return cls


def command_to_aggregate_map_instance():
    "Returns singleton instance of the map that associates commands to aggregates."
    return _command_to_aggregate_map
