from __future__ import annotations
import inspect

from typing import TYPE_CHECKING


from pyjangle.aggregate.aggregate import COMMAND_VALIDATOR_TYPE, Aggregate

from pyjangle import JangleError
from pyjangle.logging.logging import LogToggles, log

# Maps command types to aggregate types.  Use
# command_to_aggregate_map_instance() to access
# this field.
_command_to_aggregate_map: dict[any, Aggregate] = dict()


class CommandRegistrationError(JangleError):
    pass


def RegisterAggregate(cls: Aggregate):
    """Decorates aggregates with the types of commands they validate.

    THROWS
    ------
    CommandRegistrationError when decorated member is not an aggregate.
    CommandRegistrationError when the command type is already registered."""

    global _command_to_aggregate_map
    # Make sure the decorated member is an aggregate.
    if not issubclass(cls, Aggregate):
        raise CommandRegistrationError(
            "Decorated member is not an Aggregate")

    command_types = []
    methods = [getattr(cls, method_name) for method_name in dir(
        cls) if not method_name.startswith("_") and inspect.isfunction(getattr(cls, method_name))]
    for method in methods:
        command_type = getattr(method, COMMAND_VALIDATOR_TYPE, None)
        if command_type:
            command_types.append(command_type)

    log(LogToggles.command_registered_to_aggregate, "Commands registered to aggregate", {
        "aggregate_type": str(cls), "command_types": list(command_types)})
    for current_command_type in command_types:
        if current_command_type in _command_to_aggregate_map:
            raise CommandRegistrationError(
                "Command type '" + str(command_types) + "' already registered")
        _command_to_aggregate_map[current_command_type] = cls
    return cls


def command_to_aggregate_map_instance():
    """Returns singleton map that associates command types to aggregates."""
    return _command_to_aggregate_map


__all__ = [CommandRegistrationError.__name__,
           RegisterAggregate.__name__, command_to_aggregate_map_instance.__name__]
