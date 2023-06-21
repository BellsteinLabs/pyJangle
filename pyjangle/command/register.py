import functools
from typing import Any
from pyjangle.aggregate.aggregate import Aggregate

from pyjangle.error.error import SquirmError

#Maps command types to aggregate types.  Use
#command_to_aggregate_map_instance() to access
#this field.
_command_to_aggregate_map: dict[any, Aggregate] = dict()

class CommandRegistrationError(SquirmError):
    pass

def RegisterCommand(*command_types: type):
    """Decorates aggregates with the types of commands they validate.
    
    THROWS
    ------
    CommandRegistrationError when decorated member is not an aggregate.
    CommandRegistrationError when the command type is already registered."""
    def decorator(cls: Aggregate):
        global _command_to_aggregate_map
        #Make sure the decorated member is an aggregate.
        if not issubclass(cls, Aggregate):
            raise CommandRegistrationError("Decorated member is not an Aggregate")
        for current_command_type in command_types:
            if current_command_type in _command_to_aggregate_map:
                raise CommandRegistrationError("Command type '" + str(command_types) + "' already registered")
            _command_to_aggregate_map[current_command_type] = cls
        @functools.wraps(cls)
        def wrapper(*args, **kwargs):
            return cls
        return wrapper
    return decorator

def command_to_aggregate_map_instance():
    """Returns singleton map that associates command types to aggregates."""
    return _command_to_aggregate_map

