import functools
import inspect

from pyjangle import (
    Command,
    CommandResponse,
    VersionedEvent,
    LogToggles,
    log,
    JangleError,
    find_decorated_method_names,
    register_instance_methods,
)

# References to methods decorated with @reconstitute_aggregate_state map are stored in
# an attribute on the aggregate with this name.  The attribute contains a map having a
# key corresponding to the event_type argument of @reconstitute_aggregate_state and a
# value corresponding to the decorated method.
EVENT_TO_STATE_RECONSTITUTOR_ATTRIBUTE_NAME = "_event_to_state_reconstitutor"

# References to methods decorated with @validate_command map are stored in an attribute
# on the aggregate with this name.  The attribute contains a map having a key
# corresponding to the command_type argument of @validate_command and a value
# corresponding to the decorated method.
COMMAND_TYPE_TO_COMMAND_VALIDATOR_ATTRIBUTE_NAME = "_command_type_to_command_validator"

# When a method is decorated with @reconstitute_aggregate_state, an attribute is added
# to the method with this name.  The attribute contains the type of the event passed
# in to the event_type parameter of @reconstitute_aggregate_state.
EVENT_TYPE_ATTRIBUTE_NAME = "_state_reconstitutor_event_type"

# When a method is decorated with @validate_command, an attribute is added to the method
# with this name.  The attribute contains the type of the command passed to the
# command_type parameter of @validate_command.
COMMAND_TYPE_ATTRIBUTE_NAME = "_command_validator_command_type"


class CommandValidatorBadSignatureError(JangleError):
    "Command validation method has a bad signature."
    pass


class ReconstituteStateBadSignatureError(JangleError):
    "Reconstitute state method has a bad signature."
    pass


class ReconstituteStateMethodMissingError(JangleError):
    "Couldn't find a method decorated with `reconstitute_aggregate_state`."
    pass


class ValidateCommandMethodMissingError(JangleError):
    "Couldn't find a method decorated with `validate_command`."
    pass


class ReconstituteStateError(JangleError):
    "Reconstituting an aggregate failed."
    pass


class CommandValidationError(JangleError):
    "Encountered an error while validating a command."
    pass


class Aggregate:
    """Validates commands and creates new events.

    ***Do not forget to register aggregates using the `RegisterAggregate` decorator.
    Failure to do so will prevent commands from being correctly mapped to the
    appropriate aggregate at runtime.

    See Fowler's Domain-Driven-Design,
    https://martinfowler.com/bliki/DDD_Aggregate.html.  For our purposes, it is best to
    think of an aggregate as a component that reconstitutes its state from persisted
    events, and uses that state to validate commands.  If the validation succeeds, one
    or more new events are created--these new events are not official until they are
    successfully persisted to an event store.

    Aggregates are ephemeral.  This short, but busy, life-cycle is managed by the
    `pyjangle.command.command_handler.handle_command` method.  For practical purposes,
    know that methods decorated with the `reconstitute_aggregate_state` decorator are
    used to reconstitute the aggregate's current state from the relevant events from the
    event store.  Commands are then validated against the rebuilt state via methods
    decorated with the `validate_command` decorator, and shortly thereafter, the
    aggregate is marked for garbage collection.

    It is crucial to know that the aggregate's state should NEVER be modified, only
    read, from methods decorated with `validate_command`.  The reason for this is that
    because aggregates are ephemeral, the only changes that will be 'remembered' on the
    next instantiation of the aggregate will be those changes that result from
    historical events read from the event store.  In other words, to modify the state of
    the aggregate, a `validate_command` method must publish new 'event candidates' via
    the `post_new_event` method.  These published events are not official until they are
    written to an event store, so it is an error to use them for any processing.
    """

    # Cache of the names of methods decorated with `command_validator` with a key
    # corresponding to the type of the aggregate, and a value corresponding to a list of
    # method names as strings.
    _aggregate_type_to_command_validator_method_names = dict()
    # Cache of the names of methods decorated with `reconstitute_aggregate_state` with a
    # key corresponding to the type of the aggregate, and a value corresponding to a
    # list of method names as strings.
    _aggregate_type_to_state_reconstitutor_method_names = dict()

    def __init__(self, id: any):
        self.id = id
        self._new_events = []
        self._register_aggregate_validators_and_state_reconstitutors()

    def _register_aggregate_validators_and_state_reconstitutors(self):
        """Registers methods decorated with @validate_command and @reconstitute_aggregate_state."""
        aggregate_type = type(self)

        # Cache method names for @command_validator and @reconstitute_aggregate_state
        # of the first instantiation of each aggregate type.
        if (
            aggregate_type
            not in Aggregate._aggregate_type_to_command_validator_method_names
        ):
            Aggregate._aggregate_type_to_command_validator_method_names[
                aggregate_type
            ] = find_decorated_method_names(
                self, lambda method: hasattr(method, COMMAND_TYPE_ATTRIBUTE_NAME)
            )
            log(
                LogToggles.command_validator_method_name_caching,
                "Command Validator Method Names Cached",
                {
                    "aggregate_type": str(aggregate_type),
                    "method_names": Aggregate._aggregate_type_to_command_validator_method_names[
                        aggregate_type
                    ],
                },
            )
        if (
            aggregate_type
            not in Aggregate._aggregate_type_to_state_reconstitutor_method_names
        ):
            Aggregate._aggregate_type_to_state_reconstitutor_method_names[
                aggregate_type
            ] = find_decorated_method_names(
                self, lambda method: hasattr(method, EVENT_TYPE_ATTRIBUTE_NAME)
            )
            log(
                LogToggles.state_reconstitutor_method_name_caching,
                "State Reconstitutor Method Names Cached",
                {
                    "aggregate_type": str(aggregate_type),
                    "method_names": Aggregate._aggregate_type_to_state_reconstitutor_method_names[
                        aggregate_type
                    ],
                },
            )

        register_instance_methods(
            self,
            COMMAND_TYPE_TO_COMMAND_VALIDATOR_ATTRIBUTE_NAME,
            COMMAND_TYPE_ATTRIBUTE_NAME,
            Aggregate._aggregate_type_to_command_validator_method_names[aggregate_type],
        )
        register_instance_methods(
            self,
            EVENT_TO_STATE_RECONSTITUTOR_ATTRIBUTE_NAME,
            EVENT_TYPE_ATTRIBUTE_NAME,
            Aggregate._aggregate_type_to_state_reconstitutor_method_names[
                aggregate_type
            ],
        )

    @property
    def new_events(self) -> list[tuple[any, VersionedEvent]]:
        """New events created from validating commands.

        Returns:
            A list of tuples.  Each tuple contains an aggregate id, and a
            corresponding event.  It is not always the case that every event will
            be owned by this aggregate such as when a new aggregate is created.
        """
        return self._new_events

    def post_new_event(self, event: VersionedEvent, aggregate_id: any = None):
        """Make an event available to be persisted.

        In the case that an event is created that corresponds to a new aggregate that
        does not currently exist, use the aggregate_id parameter to specify the new
        aggregate's id.

        Args:
            event: Unpersisted event.
            aggregate_id: The aggregate id of event.
        """

        aggregate_id = self.id if aggregate_id == None else aggregate_id
        self._new_events.append((aggregate_id, event))
        log(
            LogToggles.post_new_event,
            "Posted New Event",
            {
                "aggregate_type": str(type(self)),
                "aggregate_id": aggregate_id,
                "event_type": str(type(event)),
                "event": vars(event),
            },
        )

    @property
    def version(self):
        """The current 'sequence number' for this aggregate.

        Each new event typically corresponds to a new version."""
        return self._version if hasattr(self, "_version") else 0

    @version.setter
    def version(self, value: int):
        self._version = value

    def apply_events(self, events: list[VersionedEvent]):
        """Process events to rebuild aggregate's current state.

        Raises:
            ReconstituteStateMethodMissingError: Expected a method decorated with
              @reconstitute_aggregate_state.
            ReconstituteStateError: An error occurred while reconstituting aggregate
              state.
        """
        for event in sorted(events, key=lambda x: x.version):
            try:
                state_reconstitutor = getattr(
                    self, EVENT_TO_STATE_RECONSTITUTOR_ATTRIBUTE_NAME
                )[type(event)]
            except KeyError as ke:
                log(
                    LogToggles.aggregate_cant_find_state_reconstitutor,
                    "Missing state reconstitutor.",
                    {"aggregate_type": str(type(self)), "event_type": str(type(event))},
                )
                raise ReconstituteStateMethodMissingError(
                    f"Missing @reconstitute_aggregate_state method for {str(type(event))}"
                ) from ke
            try:
                state_reconstitutor(event)
                log(
                    LogToggles.aggregate_event_applied,
                    "Event reconstituted aggregate state.",
                    {
                        "aggregate_type": str(type(self)),
                        "event_type": str(type(event)),
                        "event": vars(event),
                    },
                )
            except Exception as e:
                log(
                    LogToggles.aggregate_event_application_failed,
                    "Error when applying event to aggregate",
                    {
                        "aggregate_type": str(type(self)),
                        "event_type": str(type(event)),
                        "event": vars(event),
                    },
                )
                raise ReconstituteStateError(
                    "An error occurred while reconstituting aggregate state."
                ) from e

    def validate(self, command: Command) -> CommandResponse:
        """Validates a command and creates new events if validation succeeds.

        This method forwards to any methods decorated with `validate_command`.

        Raises:
            ValidateCommandMethodMissingError: "Couldn't find a method decorated with
              `validate_command`."
            CommandValidationError: "Encountered an error while validating a command.
        """
        try:
            command_validator = getattr(
                self, COMMAND_TYPE_TO_COMMAND_VALIDATOR_ATTRIBUTE_NAME
            )[type(command)]
        except KeyError as ke:
            log(
                LogToggles.command_validator_missing,
                "Missing command validator.",
                {"aggregate_type": str(type(self)), "command_type": str(type(command))},
                exc_info=ke,
            )
            raise ValidateCommandMethodMissingError(
                "Couldn't find a method decorated with `validate_command`."
            ) from ke
        try:
            return command_validator(command)
        except Exception as e:
            log(
                LogToggles.command_validation_errored,
                "An error occurred while validating a command",
                {
                    "command_type": str(type(command)),
                    "command": vars(command),
                    "method": command_validator.__name__,
                },
                exc_info=e,
            )
            raise CommandValidationError(
                "Encountered an error while validating a command."
            ) from e


def reconstitute_aggregate_state(event_type: type):
    """Decorates aggregate methods that reconstitute state from events.

    The implementation of the method must modify the aggregate state (self) in whatever
    way is appropriate to the specified event.  For example, a contrived event called
    'SignalReceived' might do something like:

        self.isSignalReceived = true
        self.signalContents = event.signal_data

    This method is called after an aggregate is instantiated and its events are
    retrieved from the event store.

    Args:
        event_type:
            The type of event this method will apply to the aggregate state.

    Signature:
        def func_name(self: Aggregate, event: VersionedEvent) -> None:
        
    Raises:
        CommandValidatorBadSignatureError:
            Command validation method has a bad signature.
    """

    def decorator(wrapped):
        # tag methods with an attribute to find them later
        setattr(wrapped, EVENT_TYPE_ATTRIBUTE_NAME, event_type)

        if len(inspect.signature(wrapped).parameters) != 2:
            raise CommandValidatorBadSignatureError(
                """@reconstitute_aggregate_state must decorate a method with 2 
                parameters: 
                def func_name(self: Aggregate, event: VersionedEvent) -> None"""
            )

        @functools.wraps(wrapped)
        def wrapper(self: Aggregate, event: VersionedEvent, *args, **kwargs):
            # Events should never be applied to an aggregate out of order, but if they
            # are, this will ensure the version is set correctly.  The only way this
            # would happen is if alternative version of the command_handler is used and
            # contains an error.
            self.version = (
                event.version if event.version > self.version else self.version
            )
            log(
                LogToggles.event_applied_to_aggregate,
                "Reconstituting aggregate state",
                {
                    "aggregate_type": str(type(self)),
                    "aggregate_id": self.id,
                    "event_type": str(type(event)),
                    "event": vars(event),
                },
            )
            return wrapped(self, event, *args, **kwargs)

        return wrapper

    return decorator


def validate_command(command_type: type):
    """Decorates methods in an aggregate that validate commands to produce events.

    In short, the method implementation must do three things:
    - Verify the command is valid against the current aggregate state
    - Post new events if the command succeeded
    - Return the appropriate CommandResponse.

    For example, a contrived command called
    ClearSignal might do something like:

        if not self.isSignalReceived:
            return CommandResponse(False, "The signal was not set.")
        signal_reset_event = SignalReset(version=next_version)
        self.post_new_event(signal_reset_event)
        return CommandResponse(True, "Command Succeeded!")

    If the method does not return a value, a CommandResponse(True, None) is assumed and
    automatically returned.  If a more specific CommandResponse with data is desired,
    that must be done explicitly.

    Args:
        command_type:
            The type of command this method will validate.

    Signature:
        def func_name(
            self: Aggregate,
            command: Command,
            next_version: int) -> CommandResponse:

    Raises:
        CommandValidatorBadSignatureError:
            Command validation method has a bad signature.
    """

    def decorator(wrapped):
        # tag methods with this attribute to find them later
        setattr(wrapped, COMMAND_TYPE_ATTRIBUTE_NAME, command_type)
        if len(inspect.signature(wrapped).parameters) != 3:
            raise CommandValidatorBadSignatureError(
                "@validate_command must decorate a method with 3 parameters: self, command: Command, next_version: int"
            )

        @functools.wraps(wrapped)
        def wrapper(self: Aggregate, *args, **kwargs):
            # the command validator provides the next version number to implementors
            # to facilitate creating new events
            next_aggregate_version = self.version + 1
            command = args[0]
            retVal = wrapped(self, command, next_aggregate_version)
            # if the command validator returns nothing, assume success.
            # It's a convenience feature.
            response = CommandResponse(True) if retVal == None else retVal
            if not response.is_success:
                log(
                    LogToggles.command_validation_errored,
                    "Command validation failed",
                    {
                        "aggregate_type": str(type(self)),
                        "aggregate_id": self.id,
                        "command_type": str(type(command)),
                        "command": vars(command),
                    },
                )
            if response.is_success:
                log(
                    LogToggles.command_validation_succeeded,
                    "Command validation succeeded",
                    {
                        "aggregate_type": str(type(self)),
                        "aggregate_id": self.id,
                        "command_type": str(type(command)),
                        "command": vars(command),
                    },
                )
            return response

        return wrapper

    return decorator
