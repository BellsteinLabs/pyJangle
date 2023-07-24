import abc
from datetime import datetime
import functools
import inspect
from typing import Callable, Iterable, List
from pyjangle.event.event import Event
from pyjangle.registration.utility import find_decorated_method_names, register_methods

# Name of the attribute used to tag saga methods decorated with
# event_receiver. This attribute is used to register those
# methods.
_EXTERNAL_RECEIVER_TYPE = "__external_event_types"
# Name of the attribute that each saga instance uses to map events to
# methods decorated with @event_receiver
_EVENT_TO_EXTERNAL_RECEIVED_MAP = "__event_to_external_received_map"
# Name of the attribute used to tag saga methods decorated with
# reconstitute_saga_state. This attribute is used to register those
# methods.
_STATE_RECONSTITUTOR_EVENT_TYPE = "__state_reconstitutor_event_type"
# Name of the attribute that each saga instance uses to map events to
# methods decorated with @reconstitute_saga_state
_EVENT_TO_STATE_RECONSTITUTORS_MAP = "__event_to_state_reconstitutors_map"


class SagaError(Exception):
    pass


def reconstitute_saga_state(type: type[Event], add_event_type_to_flags: bool = True):
    """Decorates saga methods that reconstitute state from events.

    PARAMETERS
    ----------
    type - the type of the event this method handles
    add_event_type_to_flags - If true, the type will
    be added to self.flags.  There is a synergy between
    this parameter and the require_event_type_in_flags
    parameter on @event_receiver.

    THROWS
    ------
    SagaError when the method signature doesn't 
    include 2 parameters: self, event: Event

    When a saga is initialized or woken from a 
    sleep, it needs to remember where it left off
    and the way it does that is via events.  For 
    each event type, decorate a method with this 
    decorator to let the saga know HOW to update
    its state using the event."""
    def decorator(wrapped):
        setattr(wrapped, _STATE_RECONSTITUTOR_EVENT_TYPE, type)
        if len(inspect.signature(wrapped).parameters) != 2:
            raise SagaError(
                "@reconstitute_saga_state must decorate a method with 2 parameters: self, event: Event")

        @functools.wraps(wrapped)
        def wrapper(self: Saga, *args, **kwargs):
            if add_event_type_to_flags:
                self.flags.add(type)
            return wrapped(self, *args, **kwargs)
        return wrapper
    return decorator


def event_receiver(type: type, require_event_type_in_flags: bool = True, required_flags: Iterable = [], skip_if_any_flags_set: Iterable = []):
    """Decorates saga methods that receive events.

    All but the first of the parameters are provided 
    as a convenience.  They are simply a shortcut to 
    manually accessing the self.flags set inside the 
    decorated method.

    When you've reached a point where a command is issued
    and the saga is waiting for a corresponding event to
    be published, the saga can return from this method at 
    which point it will be committed to storage until the 
    next event arrives meaning it's time to wake back up.

    When an action (such as sending a command) fails, use 
    the self.set_retry() method to specify when the saga 
    should wake up and try again.  After calling 
    set_retry(), the saga can return from this method at 
    which point it will be committed to storage until it's 
    time to wake up.

    PARAMETERS
    ----------
    type - the type of the event this method handles
    require_event_type_in_flags - the event's type
    must be present in self.flags for the 
    decorated method to be executed.
    required_flags - These flags MUST be present in 
    self.flags for the decorated method to be executed.
    skip_if_any_flags_set - If any of these flags are 
    set, skip this method's execution.


    THROWS
    ------
    SagaError when the method signature doesn't
    include 2 parameters: self, next_version: int

    When a saga is initialized as a result of a new 
    event, the evaluate(event) method is called which
    looks for a corresponding method decorated with
    @event_receiver(event).
    """
    def decorator(wrapped):
        if not inspect.iscoroutinefunction(wrapped):
            raise SagaError("@event_receiver must be a couroutine (async).")
        if len(inspect.signature(wrapped).parameters) != 1:
            raise SagaError(
                "@event_receiver must decorate a method with 1 parameters: self")
        setattr(wrapped, _EXTERNAL_RECEIVER_TYPE, type)

        @functools.wraps(wrapped)
        async def wrapper(self: Saga, *args, **kwargs):
            if require_event_type_in_flags and not type in self.flags:
                return
            if self.flags.issuperset(required_flags) and not self.flags.intersection(skip_if_any_flags_set):
                return await wrapped(self)
        return wrapper
    return decorator


class Saga:
    """Represents a distributed transaction.

    Sagas are critically important in distributed,
    event-driven systems.  An architectural constraint
    is that a command should be mapped to one and only
    one aggregate, but sometimes, you need to execute a
    command that crossed aggregate boundaries.  You'll
    first want to ensure that your aggregate 
    boundaries are properly defined, but in the case that 
    they are, you'll want a separate component to 
    mediate the interaction between the various aggregates.
    Think about the case where you're making a 
    financial transaction between bank accounts,
    or placing an order on a travel website which involves
    lots of moving parts.  In those cases, the saga is what 
    you need.

    A saga will 'sleep' in between events and their 
    corresponding actions.  Sleeping means that the saga
    is committed to the saga store and does not exist in 
    memory.  A saga awakens either because a relevant event
    is received, or because a retry timer has expired.  
    Typically, when a saga tries to issue a command that fails,
    it will set a retry timer to repeat the action later.
    Because a saga can awaken for a number of reasons, be sure
    to code the saga in such a way that commands are retried 
    on saga initialization.

    There is an attribute on the saga called self.flags
    which is a set() which is intended to be used to track
    the saga's progress.  For example, when a command is 
    send as a result of a new event, put the name of the
    command into flags.  Note that methods decorated with 
    @event_receiver should NEVER directly modify the saga's 
    state.  This should only occur in the 
    @reconstitute_saga_state methods. To ensure the command 
    flag is set, create a new event like the following one 
    to set the flag: 

        class CommandSent(SagaEvent):
            pass

    The next time the saga is instantiated, the flag will 
    be set since the preceding event will be retrieved.  To 
    post the new event, similar to an aggregate, use the 
    self._post_new_event(event) method.  Be sure to set the 
    event's version using the next_version argument on the 
    method decorated with @event_receiver.

    A saga has a notion of a timeout (not to be confused 
    with the retry mechanism).  This is a period of time,
    after which, the saga will not progress, even if new
    events arrive.
    A good way to handle timeouts is to put the timeout
    time in the event that triggers the saga in the first 
    place.  Any other components involved in the saga's 
    transaction should also know the timeout.  Each 
    component should then be responsible for handling the 
    timeout in whatever way makes sense.  They should NOT 
    rely on the saga to let them know when the timeout
    is reached since the saga notified them long before.

    Once a saga is timed out or completed, its state 
    will not progress, even if new events arrive.  This 
    is a great way to ensure that a saga's actions (commands)
    aren't duplicated.

    To implement a saga, extend from Saga and implement 
    def evaluate_hook(self).  You'll also want 
    @reconstitute_saga_state methods with a signature
    def method_name(self, event) and @event_receiver methods
    with a signature of def method_name(self, next_version: int)  
    Lastly, call super().__init__().
    """

    # Cache containing a map of saga-type to the method names
    # on the saga that are decorated with
    # @reconstitute_saga_state.  Because looking these up
    # can be expensive, the names are cached once for each saga
    # type.
    _saga_type_to_reconstitute_saga_state_method_names = dict()
    # Cache containing a map of saga-type to the method names
    # on the saga that are decorated with
    # @event_receiver.  Because looking these up
    # can be expensive, the names are cached once for each saga
    # type.
    _saga_type_to_event_receiver_method_names = dict()

    def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False, is_timed_out: bool = False):
        saga_type = type(self)

        # Update the cache with method names if needed.
        if saga_type not in Saga._saga_type_to_reconstitute_saga_state_method_names:
            Saga._saga_type_to_reconstitute_saga_state_method_names[saga_type] = find_decorated_method_names(
                self, lambda method: hasattr(method, _STATE_RECONSTITUTOR_EVENT_TYPE))
        if saga_type not in Saga._saga_type_to_event_receiver_method_names:
            Saga._saga_type_to_event_receiver_method_names[saga_type] = find_decorated_method_names(
                self, lambda method: hasattr(method, _EXTERNAL_RECEIVER_TYPE))

        register_methods(self, _EVENT_TO_STATE_RECONSTITUTORS_MAP, _STATE_RECONSTITUTOR_EVENT_TYPE,
                         Saga._saga_type_to_reconstitute_saga_state_method_names[saga_type])
        register_methods(self, _EVENT_TO_EXTERNAL_RECEIVED_MAP, _EXTERNAL_RECEIVER_TYPE,
                         Saga._saga_type_to_event_receiver_method_names[saga_type])

        self.saga_id = saga_id
        self.flags = set()
        self.retry_at = retry_at
        self.timeout_at = timeout_at
        self.is_timed_out = is_timed_out
        self.is_complete = is_complete
        self.new_events: list[Event] = []
        self.is_dirty = False
        self._apply_historical_events(events)

    async def evaluate(self, event: Event = None):
        """Call to process add a new event to the saga.

        When this method is called, the assumption is 
        that all of the events leading up to this one
        have been reconstituted into the saga."""
        if self.timeout_at != None and self.timeout_at < datetime.now():
            self.set_timed_out()
            return
        self.retry_at = None
        event_receiver_map: dict[Event, Callable[[Event], None]] = getattr(
            self, _EVENT_TO_EXTERNAL_RECEIVED_MAP)
        if event:
            self._post_new_event(event)
            self._apply_historical_events([event])
            event_type = type(event)
            try:
                return await event_receiver_map[event_type](event)
            except KeyError as ke:
                raise SagaError(
                    "Missing event receiver (@event_receiver) for " + str(event_type) + "}", ke)
        else:
            for receiver_method in event_receiver_map.values():
                await receiver_method()

    def set_complete(self):
        """Call from evaluate() to mark the saga as completed."""
        self.is_dirty = True
        self.is_complete = True

    def set_timeout(self, timeout_at: datetime):
        """Call from evaluate() to specify a timeout for the saga."""
        if not self.timeout_at != timeout_at:
            self.is_dirty = True
        self.timeout_at = timeout_at

    def set_timed_out(self):
        """Call to decalare that the timeout has been reached."""
        self.is_dirty = True
        self.is_timed_out = True

    def set_retry(self, retry_at: datetime):
        """Call from evaluate() to specify when the saga should retry."""
        if self.retry_at != retry_at:
            self.is_dirty = True
        self.retry_at = retry_at

    def _apply_historical_events(self, events: List[Event]):
        """Applies events to rebuild aggregate state.

        THROWS
        ------
        SagaError when @reconstitute_saga_state or 
        @event_receiver method is missing."""
        event_to_state_reconstitutors_map = getattr(
            self, _EVENT_TO_STATE_RECONSTITUTORS_MAP)
        try:
            for e in events:
                event_to_state_reconstitutors_map[type(e)](e)
        except KeyError as ke:
            raise SagaError(
                "Missing state reconstitutor (@reconstitute_saga_state) for " + str(type(e)) + "}", ke)

    def _post_new_event(self, event: Event):
        """Call from evalute() to post new state change events.  

        Events that are received to progress
        saga state are already committed by the 
        framework, so events that are posted using this method
         are generally to set flags when commands are sent. """
        self.is_dirty = True
        self.new_events.append(event)
