
from __future__ import annotations

import pyjangle
import functools
import inspect
from typing import TYPE_CHECKING, Callable, List
from pyjangle import CommandResponse
from pyjangle import Command


from pyjangle.event.event import VersionedEvent
from pyjangle.logging.logging import LogToggles, log
from pyjangle.registration.utility import (find_decorated_method_names,
                                           register_methods)

# These constants represent attributes on each aggregate that are used
# to store method registration info.  Methods are registered using
# the @validate_command and @reconstitute_aggregate_state decorators.
EVENT_TO_STATE_RECONSTITUTOR_MAP = "__event_to_state_reconstitutor_map"
COMMAND_TYPE_TO_COMMAND_VALIDATOR_MAP = "__command_type_to_command_validator_map"
# These constants represent the names of the attributes that decorated
# methods are tagged with so that they can be discovered for
# registration.
STATE_RECONSTITUTOR_TYPE = "__state_reconstitutor_type"
COMMAND_VALIDATOR_TYPE = "__command_validator_type"


def _method_is_command_validator(method: Callable) -> bool:
    """Looks for decorated methods with an attribute named COMMAND_VALIDATOR_TYPE."""
    return hasattr(method, COMMAND_VALIDATOR_TYPE)


def _method_is_state_reconstitutor(method: Callable) -> bool:
    """Looks for decorated methods with an attribute named STATE_RECONSTITUTOR_TYPE."""
    return hasattr(method, STATE_RECONSTITUTOR_TYPE)


class AggregateError(Exception):
    pass


class Aggregate:
    """An aggregation of domain objects that are treated as a whole.

    Taken from fowler's Domain-Driven-Design, https://martinfowler.com/bliki/DDD_Aggregate.html.  
    In this context, we refine the definition and say that the aggregate IS its component events.  
    Without events, there is no aggregate.  It's purpose in this architecture is to validate 
    commands which results in the creation of new events.  Aggregates are transient and exist in 
    memory only long enough to validate that a command is valid before attempting to persist the 
    events resulting from the command to durable storage.

    When an aggregate is instantiated, it is a blank slate until it processes historical events
    and possibly snapshots to represent the current state.  Methods decorated with the  
    @reconstitute_aggregate_state decorator are used to process historical events.  The method
    signature should include a single parameter corresponding to an event type.  

    When an aggregate validates a command, it uses a method decorated with the @validate_command
    decorator and a matching event type argument.  The method signature should contain two 
    parameters: the command to be validated, and a second integer argument representing the 
    value of the next event in the sequence for this aggregate which should be used in the 
    creation of any new events.  If multiple new events are created from a single command, 
    increment the version for each of the new events in-turn.  These methods
    should call _post_new_event(event: Event) to propose any new events for the event store.
    Methods decorated with @validate_command should return a CommandResponse.  If the method 
    returns normally (without exception), a CommandResponse(True) is implicitly returned.
    In the case of a failed command, it is up to the method to explicitly return a 
    CommandResponse(False, reason: any).

    An important note is that command validators should NEVER update the aggregate's state.
    State changes only ever occur via events.  Otherwise, any state changes will be lost
    once the aggregate is no longer in memory.  If it's not in an event, IT DIDN'T HAPPEN!
    """

    # cache of method names corresponding to command validators for each type of aggregate
    # Looking for these each time an aggregate is instantiated would be slow,
    # so they're cached
    _aggregate_type_to_command_validator_method_names = dict()
    # cache of method names corresponding to state reconstitutors decorated with
    # reconstitute aggregate state for each type of aggregate.
    # Looking for these each time an aggregate is instantiated would be slow,
    # so they're cached
    _aggregate_type_to_state_reconstitutor_method_names = dict()

    def __init__(self, id: any):
        self.id = id
        self._new_events = []
        self._register_aggregate_validators_and_state_reconstitutors()

    def _register_aggregate_validators_and_state_reconstitutors(self):
        """Registers methods decorated with @validate_command and @reconstitute_aggregate_state."""
        aggregate_type = type(self)

        # Cache method names for CommandValidators and StateReconstitutors for each aggregate type on the first instantiation.
        if aggregate_type not in Aggregate._aggregate_type_to_command_validator_method_names:
            Aggregate._aggregate_type_to_command_validator_method_names[aggregate_type] = find_decorated_method_names(
                self, _method_is_command_validator)
            log(LogToggles.command_validator_method_name_caching, "Command Validator Method Names Cached", {"aggregate_type": str(
                aggregate_type), "method_names": Aggregate._aggregate_type_to_command_validator_method_names[aggregate_type]})
        if aggregate_type not in Aggregate._aggregate_type_to_state_reconstitutor_method_names:
            Aggregate._aggregate_type_to_state_reconstitutor_method_names[aggregate_type] = find_decorated_method_names(
                self, _method_is_state_reconstitutor)
            log(LogToggles.state_reconstitutor_method_name_caching, "State Reconstitutor Method Names Cached", {"aggregate_type": str(
                aggregate_type), "method_names": Aggregate._aggregate_type_to_state_reconstitutor_method_names[aggregate_type]})

        register_methods(self, COMMAND_TYPE_TO_COMMAND_VALIDATOR_MAP, COMMAND_VALIDATOR_TYPE,
                         Aggregate._aggregate_type_to_command_validator_method_names[aggregate_type])
        register_methods(self, EVENT_TO_STATE_RECONSTITUTOR_MAP, STATE_RECONSTITUTOR_TYPE,
                         Aggregate._aggregate_type_to_state_reconstitutor_method_names[aggregate_type])

    @property
    def new_events(self) -> List[tuple[any, VersionedEvent]]:
        """New events created from validating commands.

        When an aggregate is instantiated and all of its 
        historical events have been replayed to rebuild the current state,
        new_events will correspond to an empty list.  Once
        commands are validated, any events they create via
        _post_new_event will show up here."""
        return self._new_events

    def post_new_event(self, event: VersionedEvent, aggregate_id: any = None):
        aggregate_id = self.id if aggregate_id == None else aggregate_id
        """Advertises new events that should be committed to the event store."""
        self._new_events.append((aggregate_id, event))
        log(LogToggles.post_new_event, "Posted New Event", {"aggregate_type": str(type(self)),
            "aggregate_id": aggregate_id, "event_type": str(type(event)), "event": event.__dict__})

    @property
    def version(self):
        """The current 'sequence number' for this aggregate.

        Each new event typically corresponds to a new version."""
        return self._version if hasattr(self, "_version") else 0

    @version.setter
    def version(self, value: int):
        self._version = value

    def apply_events(self, events: List[VersionedEvent]):
        """Process events to rebuild aggregate state.

        THROWS
        ------
        AggregateError when missing a corresponding method decorated
        with @reconstitute_aggregate_state."""
        try:
            for event in sorted(events, key=lambda x: x.version):
                state_reconstitutor = getattr(
                    self, EVENT_TO_STATE_RECONSTITUTOR_MAP)[type(event)]
                state_reconstitutor(event)
        except KeyError as ke:
            raise AggregateError(
                "Missing state reconstitutor for " + str(type(event)) + "}", ke)

    def validate(self, command: Command) -> pyjangle.CommandResponse:
        """Validates a command for the purpose of creating new events.

        This method forwards to any methods decorated with @validate_command.

        THROWS
        ------
        AggregateError when command validator is not found."""
        try:
            command_validator = getattr(self, COMMAND_TYPE_TO_COMMAND_VALIDATOR_MAP)[
                type(command)]
            return command_validator(command)
        except KeyError as ke:
            raise AggregateError(
                "Missing command validator for " + str(type(command)) + "}", ke)


def reconstitute_aggregate_state(event_type: type):
    """Decorates methods in an aggregate that reconstitute state from historical events."""
    def decorator(wrapped):
        # mark methods with this attribute to be found by _method_is_state_reconstitutor()
        setattr(wrapped, STATE_RECONSTITUTOR_TYPE, event_type)

        @functools.wraps(wrapped)
        def wrapper(self: Aggregate, event: VersionedEvent, *args, **kwargs):
            # update the aggregate version if it is lower than the event version
            self.version = event.version if event.version > self.version else self.version
            log(LogToggles.event_applied_to_aggregate, "Reconstituting aggregate state", {"aggregate_type": str(type(self)),
                "aggregate_id": self.id, "event_type": str(type(event)), "event": event.__dict__})
            return wrapped(self, event, *args, **kwargs)
        return wrapper
    return decorator


def validate_command(command_type: type):
    """Decorates methods in an aggregate that validate commands to produce events."""
    def decorator(wrapped):
        # mark methods with this attribute to be found by _method_is_command_validator()
        setattr(wrapped, COMMAND_VALIDATOR_TYPE, command_type)
        if len(inspect.signature(wrapped).parameters) != 3:
            raise AggregateError(
                "@validate_command must decorate a method with 3 parameters: self, command: Command, next_version: int")

        @functools.wraps(wrapped)
        def wrapper(self: Aggregate, *args, **kwargs):
            # the command validator provides the next version number to implementors
            # to facilitate creating new events
            next_aggregate_version = self.version + 1
            command = args[0]
            retVal = wrapped(self, command, next_aggregate_version)
            # if the command validator returns nothing, assume success.  It's a convenience feature.
            response = CommandResponse(True) if retVal == None else retVal
            if not response.is_success:
                log(LogToggles.command_validation_failed, "Command validation failed", {"aggregate_type": str(type(self)),
                    "aggregate_id": self.id, "command_type": str(type(command)), "command": command.__dict__})
            if response.is_success:
                log(LogToggles.command_validation_succeeded, "Command validation succeeded", {"aggregate_type": str(type(self)),
                    "aggregate_id": self.id, "command_type": str(type(command)), "command": command.__dict__})
            return response
        return wrapper
    return decorator


__all__ = [AggregateError.__name__, Aggregate.__name__,
           reconstitute_aggregate_state.__name__, validate_command.__name__]
