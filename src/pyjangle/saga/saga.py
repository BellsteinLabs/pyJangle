from datetime import datetime, timedelta
import functools
import inspect
import logging
import os
from typing import Callable, Iterable, List

from pyjangle import (
    find_decorated_method_names,
    register_instance_methods,
    LogToggles,
    JangleError,
    log,
    Event,
    VersionedEvent,
    CommandResponse,
    CommandDispatcherNotRegisteredError,
    command_dispatcher_instance,
    get_saga_retry_interval,
)

# Name of the attribute used to tag saga methods decorated with `event_receiver.` This
# attribute holds the type of the event that the event receiver handles.
_EVENT_RECEIVER_EVENT_TYPE = "_event_receiver_event_type"
# The name of the attribute on each saga instance that holds a mapping of event_type to
# instances of methods decorated with `event_receiver`.
_EVENT_TYPE_TO_EVENT_RECEIVER_INSTANCE_METHOD = (
    "_event_type_to_event_receiver_instance_method"
)
# Name of the attribute used to tag saga methods decorated with
# `reconstitute_saga_state`. This attribute holds the type of the event that the method
# handles.
_STATE_RECONSTITUTOR_EVENT_TYPE = "_state_reconstitutor_event_type"
# Name of the attribute that each saga instance that holds a mapping of event_type to
# instances of methods decorated with `reconstitute_saga_state`.
_EVENT_TYPE_TO_STATE_RECONSTITUTORS_INSTANCE_METHOD = (
    "_event_type_to_state_reconstitutor_instance_method"
)





class ReconstituteSagaStateBadSignatureError(JangleError):
    "Method decorated with `reconstitute_saga_state` has invalid signature."
    pass


class EventReceiverBadSignatureError(JangleError):
    "Method decorated with `event_receiver` has invalid signature."
    pass


class ReconstituteSagaStateMissingError(JangleError):
    "Reconstitute saga state method missing."
    pass


class EventRceiverMissingError(JangleError):
    "Event receiver method missing."
    pass


def reconstitute_saga_state(
    type: type[VersionedEvent], add_event_type_to_flags: bool = True
):
    """Decorates saga methods that reconstitute state from events.

    When a saga is initialized, it has no idea where it left off save for the events in
    the saga store.  Methods decorated with this decorator are used to rebuild the
    saga's state.  This implies that any state change to a saga that does not happen in
    one of these methods will be erased between instantiations and is considered to be
    an error.

    Because saga events may arrive out of order, or as duplicates, or not when we
    expect them, the saga's `flags` attribute is used to track what events have
    transpired.  In order to reduce the tedium, use `add_event_type_to_flags` in order
    to automatically add `type` to flags.  When this is done in concert with the
    `require_event_type_in_flags` on methods decorated with `event_receiver`, much
    error-prone manual coding is avoided.

    Args:
        type:
            The type of event with which this method will modify the saga's state.
        add_event_type_to_flags:
            If True, add `type` to the saga's `flags` attribute.

    Signature:
        def method_name(self, event: Event) -> None

    Raises:
        ReconstituteSagaStateBadSignatureError:
            Method decorated with reconstitute_saga_state has invalid signature.
    """

    def decorator(wrapped):
        setattr(wrapped, _STATE_RECONSTITUTOR_EVENT_TYPE, type)
        if len(inspect.signature(wrapped).parameters) != 2:
            raise ReconstituteSagaStateBadSignatureError(
                """@reconstitute_saga_state must decorate a method with 2 parameters: 
                self, event: Event"""
            )

        @functools.wraps(wrapped)
        def wrapper(self: Saga, *args, **kwargs):
            if add_event_type_to_flags:
                self.flags.add(type)
            return wrapped(self, *args, **kwargs)

        return wrapper

    return decorator


def event_receiver(
    type: type,
    require_event_type_in_flags: bool = True,
    required_flags: Iterable = [],
    skip_if_any_flags_set: Iterable = [],
    default_retry_interval_in_seconds: int = None,
):
    """Decorates saga methods that receive events and drive the saga forward.

    When a saga is initialized as a result of a new event, the evaluate(event) method is
    called which looks for a corresponding method decorated with `event_receiver`.
    Because events can arrive out of order or in duplicate, or as a part of an event
    replay a determination must be made as to whether or not the `event_receiver` method
    should be invoked.  All but the first of the parameters are provided as a
    convenience to accomplish this.  They are a shortcut to manually accessing the
    `self.flags` set inside the decorated method.

    See `add_event_type_to_flags` in the `reconstitute_saga_state` decorator.  It
    synergizes with `require_event_type_in_flags`.

    When a point is reached where a command has been issued and the saga is waiting for
    a resulting event to be published, the saga can simply return from this method at
    which point it will be committed to storage until the next event in the saga's
    progression arrives which will re-instantiate the saga from storage.

    In the event of a command failure, calling `self.set_retry` will cause the saga to
    be retried at the specified interval.

    Args:
        type:
            The type of event with which this method will use to progress the saga.
        require_event_type_in_flags:
            If True, requires `type` to be present in `flags` for this method to be
            invoked.
        required_flags:
            These values are *each* required to exist in `flags` for this method to be
            invoked.
        skip_if_any_flags_set:
            If *any* of these values exist in `flags`, this method will not be invoked.
        default_retry_interval_in_seconds:
            The saga will retry after at least this length of time if this method does
            not complete successfully.

    Signature:
        async def method_name(self) -> None:

    Raises:
        EventReceiverBadSignatureError:
            Method decorated with event_receiver has invalid signature.
    """

    def decorator(wrapped):
        if not inspect.iscoroutinefunction(wrapped):
            raise EventReceiverBadSignatureError(
                "@event_receiver must be a couroutine (async)."
            )
        if len(inspect.signature(wrapped).parameters) != 1:
            raise EventReceiverBadSignatureError(
                "@event_receiver must decorate a method with 1 parameters: self"
            )
        setattr(wrapped, _EVENT_RECEIVER_EVENT_TYPE, type)

        @functools.wraps(wrapped)
        async def wrapper(self: Saga, *args, **kwargs):
            if require_event_type_in_flags and not type in self.flags:
                return
            if self.flags.issuperset(required_flags) and not self.flags.intersection(
                skip_if_any_flags_set
            ):
                try:
                    return await wrapped(self)
                except Exception as e:
                    self._log_command_failure(self._last_command, e)
                    self.set_retry(
                        default_retry_interval_in_seconds
                        if default_retry_interval_in_seconds
                        else get_saga_retry_interval()
                    )

        return wrapper

    return decorator


class Saga:
    """Represents a distributed transaction.

    Sagas are a basic tool in distributed, event-driven systems.  An architectural
    constraint is that a command should be mapped to one and only one aggregate, but
    sometimes, a command that crosses aggregated boundaries is needed.  First, ensure
    that aggregate boundaries are properly defined, but in the case that they are,
    a separate component is needed to mediate the interaction between the various
    aggregates.  Think about the case where a financial transaction is made between two
    bank accounts, or an order is placed on a travel website which involves many moving
    parts.  These are examples of cases where a saga is needed.

    To implement a saga, extend from this class and create methods decorated with both
    `event_receiver` and `reconstitute_saga_state`.  `event_receiver` methods are
    invoked when a specific event arrives or when a saga is retried after a failure.
    `reconstitute_saga_state` methods reconstitute the saga's state whenever it is
    reinstantiated by the saga store.

    Sagas, like aggregates, are ephemeral.  Any state change that occurs in a saga must
    have a corresponding event.  For example, it is ubiquitous that when a saga is
    instantiated as a result of a new event, it will issue some command.  That the
    command was issued, successfully or not, is a state change that occurred in the
    saga, and as such, it requires a corresponding event such as 'CommandSucceeded' or
    'CommandFailed'.  Without this, when the saga is next instantiated, it will have no
    record that the command was ever issued and may mistakenly re-issue the command.
    The command should be idempotent, so in the ideal case, the worst outcome would be
    wasted resources rather than an error.

    In order to facilitate the saga's ability to know where it left off after being
    reinitialized in memory, the established pattern in this library involves an
    attribute on the saga called `flags` which is a set containing data (commonly types)
    that can be interrogated to know what was accomplished on previous instantiations of
    the saga.  The following two paragraphs explain features that make this easy to
    accomplish in practice.

    The `reconstitute_saga_state` decorator is used to decorate methods that
    rebuild the saga's state (which includes `flags`).  The decorator has a property
    called `add_event_type_to_flags` which defaults to True and will automatically add
    the type of the event being rebuilt to the `flags` collection.  This eliminates the
    need to write any code in the `reconstitute_saga_state` method in most simple cases.
    Don't forget to set the value to False if this behavior is not desired.  To
    illustrate (add_event_type_to_flags is specified here but is not needed since it
    defaults to True.):

        @reconstitute_saga_state(RequestApproved, add_event_type_to_flags=True)
        def handle_request_approved(self, event: RequestApproved):
            pass

    When a new event arrives, the corresponding method decorated with `event_receiver`
    is invoked.  The decorator provides a myriad of configuration options to interrogate
    `flags` to determine if the received should be invoked.  By default, the decorator's
    `require_event_type_in_flags` property is set to True.  You may have noticed that
    this seems silly since the reason this event receiver method is being invoked is
    that the event just arrived, but consider the case where the saga is instantiated
    because a command previously failed, and the saga was configured to retry after 30
    seconds.  In this case, there is no arriving event, so *ALL* event receiver methods
    are invoked.  In this case, interrogating `flags` is important to not accomplish
    steps that are already completed.  When authoring these event receivers, always
    consider the case where *EVERY* event receiver is invoked when the saga is
    instantiated rather than just the simple case where an event arrives.  There are
    various other attributes that can be set on the decorator to control whether or not
    an event receiver is invoked.

    When an invocation of an event receiver results in an exception, the event receiver
    will automatically set retry based on either the SAGA_RETRY_INTERVAL environment
    variable, the default value, or the value set in the decorator (listed in reverse
    order of precedence).

    A saga has a notion of a timeout (not to be confused with the retry mechanism).
    This is a period of time, after which, the saga will not progress, even if new
    events arrive.  A good way to handle timeouts is to put the timeout time in the
    event that triggers the saga in the first place.  Any other components involved in
    the saga's transaction should also be aware of the timeout and act accordingly.
    Other components in the system should NOT rely on the saga in order to receive a
    notification that the timeout is reached.

    When a saga is completed, call `set_complete` in the relevant event receiver.  To
    forcibly cause a saga to be timed out, use the `set_timeout` method which will take
    precedence over the `timeout_at` property.
    """

    # Cache of the names of methods decorated with `reconstitute_saga_state` with a key
    # corresponding to the type of the saga, and a value corresponding to a list of
    # method names as strings.
    _saga_type_to_reconstitute_saga_state_method_names = dict()
    # Cache of the names of methods decorated with `event_receiver` with a
    # key corresponding to the type of the aggregate, and a value corresponding to a
    # list of method names as strings.
    _saga_type_to_event_receiver_method_names = dict()

    def __init__(
        self,
        saga_id: any,
        events: List[Event] = [],
        retry_at: datetime | str = None,
        timeout_at: datetime | str = None,
        is_complete: bool = False,
        is_timed_out: bool = False,
    ):
        saga_type = type(self)

        # Update the cache with method names if needed.
        if saga_type not in Saga._saga_type_to_reconstitute_saga_state_method_names:
            Saga._saga_type_to_reconstitute_saga_state_method_names[
                saga_type
            ] = find_decorated_method_names(
                self, lambda method: hasattr(method, _STATE_RECONSTITUTOR_EVENT_TYPE)
            )
        if saga_type not in Saga._saga_type_to_event_receiver_method_names:
            Saga._saga_type_to_event_receiver_method_names[
                saga_type
            ] = find_decorated_method_names(
                self, lambda method: hasattr(method, _EVENT_RECEIVER_EVENT_TYPE)
            )

        register_instance_methods(
            self,
            _EVENT_TYPE_TO_STATE_RECONSTITUTORS_INSTANCE_METHOD,
            _STATE_RECONSTITUTOR_EVENT_TYPE,
            Saga._saga_type_to_reconstitute_saga_state_method_names[saga_type],
        )
        register_instance_methods(
            self,
            _EVENT_TYPE_TO_EVENT_RECEIVER_INSTANCE_METHOD,
            _EVENT_RECEIVER_EVENT_TYPE,
            Saga._saga_type_to_event_receiver_method_names[saga_type],
        )

        self.saga_id = saga_id
        self.flags = set()
        self.retry_at = (
            None
            if retry_at == None
            else retry_at
            if isinstance(retry_at, datetime)
            else datetime.fromisoformat(retry_at)
        )
        self.timeout_at = (
            None
            if timeout_at == None
            else timeout_at
            if isinstance(timeout_at, datetime)
            else datetime.fromisoformat(timeout_at)
        )
        self.is_timed_out = is_timed_out
        self.is_complete = is_complete
        self.new_events: list[VersionedEvent] = []
        self.is_dirty = False
        self._apply_historical_events(events)
        try:
            self._command_dispatcher = command_dispatcher_instance()
        except CommandDispatcherNotRegisteredError:
            self._command_dispatcher = None

    async def _dispatch_command(
        self,
        command: any,
        on_success_event: Event = None,
        on_failure_event: Event = None,
        skip_if_result_event_type_in_flags: bool = True,
    ) -> CommandResponse:
        """Dispatch a command and post state-change events based on the CommandResponse.

        This is a convenience method for dispatching a command, interpreting the
        response, and posting a new event to update the saga state.

        Args:
            command:
                The command to dispatch.
            on_success_event:
                An event representing a successful command response.
            on_failure_event:
                An event representing an unsuccessful command response.
            skip_if_result_event_type_in_flags:
                If the `on_success_event` or `on_failure_event` types exist in `flags`,
                this method will return None immediately.

        Returns:
            The response of dispatching `command`, or None if the command has already
            been dispatched.

        Raises:
            CommandDispatcherNotRegisteredError:
                Command dispatcher has not been registered.
        """

        if inspect.isclass(on_success_event):
            on_success_event = on_success_event()
        if inspect.isclass(on_failure_event):
            on_failure_event = on_failure_event()
        if inspect.ismethod(command) or inspect.isfunction(command):
            command = command()

        if skip_if_result_event_type_in_flags:
            on_success_in_flags = on_success_event and self.flags_has_any(
                type(on_success_event)
            )
            on_failure_in_flags = on_failure_event and self.flags_has_any(
                type(on_failure_event)
            )
            if on_success_in_flags or on_failure_in_flags:
                return
        self._last_command = command
        if not self._command_dispatcher:
            raise CommandDispatcherNotRegisteredError(
                "No command dispatcher registerd with @RegisterCommandDispatcher"
            )
        response: CommandResponse = await self._command_dispatcher(self._last_command)
        if response.is_success and on_success_event:
            self._post_state_change_event(on_success_event)
        if not response.is_success and on_failure_event:
            self._post_state_change_event(on_failure_event)
        return response

    def flags_has_any(self, *args: any):
        "Returns true if any of `*args` exists if `flags`."

        return set(args).intersection(self.flags)

    async def evaluate(self, event: VersionedEvent = None):
        """Applies an event to the saga to progress its state.

        This method will first check that the saga is not timed out or completed in
        which case it will return immediately.  Next, it finds a
        `reconstitute_saga_state` method with a type corresponding to `event` and
        invokes it, and sets the saga's `is_dirty` to True so that `event` is persisted.
        Next, it finds an `event_receiver` with a type corresponding to `event` and
        invokes it.

        If this method was called with `event` == None, then *ALL* event receivers are
        invoked as needed based on the contents of `flags` and the attributes set on the
        various `event_receiver` decorators.
        """
        if self.timeout_at != None and self.timeout_at < self._get_current_time():
            self.set_timed_out()
            return
        if self.is_complete:
            return
        self.retry_at = None
        event_receiver_map: dict[
            VersionedEvent, Callable[[VersionedEvent], None]
        ] = getattr(self, _EVENT_TYPE_TO_EVENT_RECEIVER_INSTANCE_METHOD)
        if event:
            self._post_state_change_event(event)
            event_type = type(event)
            try:
                return await event_receiver_map[event_type](event)
            except KeyError as ke:
                raise EventRceiverMissingError(
                    "Missing event receiver (@event_receiver) for "
                    + str(event_type)
                    + "}"
                ) from ke
        else:
            for receiver_method in event_receiver_map.values():
                await receiver_method()

    def set_complete(self):
        "Call from an event receiver to mark the saga as completed."
        self.is_dirty = True
        self.is_complete = True

    def set_timeout(self, timeout_at: datetime):
        "Call from an event receiver to specify a timeout for the saga."
        if isinstance(timeout_at, str):
            timeout_at = datetime.fromisoformat(timeout_at)
        if not self.timeout_at != timeout_at:
            self.is_dirty = True
        self.timeout_at = timeout_at

    def set_timed_out(self):
        "Call from an event receiver to decalare that the timeout has been reached."
        self.is_dirty = True
        self.is_timed_out = True

    def set_retry(self, retry_at: datetime | float | int):
        "Call from an event receiver to specify when the saga should retry."
        if retry_at and not isinstance(retry_at, datetime):
            retry_at = self._get_current_time() + timedelta(seconds=retry_at)
        if self.retry_at != retry_at:
            self.is_dirty = True
        self.retry_at = retry_at

    def _get_current_time(self):
        return datetime.now()

    def _apply_historical_events(self, events: List[VersionedEvent]):
        """Applies events to rebuild saga state.

        Args:
            events:
                The events to apply to the saga.

        Raises:
            ReconstituteSagaStateMissingError:
                reconstitute_saga_state method missing.
        """
        event_to_state_reconstitutors_map = getattr(
            self, _EVENT_TYPE_TO_STATE_RECONSTITUTORS_INSTANCE_METHOD
        )
        try:
            for e in events:
                event_to_state_reconstitutors_map[type(e)](e)
        except KeyError as ke:
            raise ReconstituteSagaStateMissingError(
                "Missing state reconstitutor (@reconstitute_saga_state) for "
                + str(type(e))
                + "}"
            ) from ke

    def _post_state_change_event(self, event: Event):
        "Call from an event receiver to post new state change events."

        self._apply_historical_events([event])
        self.is_dirty = True
        self.new_events.append(event)

    def _log_command_failure(self, command: any, exception: Exception):
        log(
            LogToggles.saga_command_failed,
            "Command failed",
            {
                "command_type": str(type(command)),
                "command": vars(command) if command else None,
            },
            exc_info=exception,
        )
