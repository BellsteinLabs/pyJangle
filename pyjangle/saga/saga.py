import abc
from datetime import datetime
import functools
import inspect
from typing import Callable, List
from error.error import SquirmError
from pyjangle.command.command_dispatcher import command_dispatcher_instance
from pyjangle.event.event import Event
from pyjangle.registration.utility import find_decorated_method_names, register_methods

#Name of the attribute used to tag saga methods decorated with 
#reconstitute_saga_state. This attribute is used to register those 
#methods.
STATE_RECONSTITUTOR_EVENT_TYPE = "__state_reconstitutor_event_type"
#Name of the attribute that each saga instance uses to map events to 
#methods decorated with @reconstitute_saga_state
EVENT_TO_STATE_RECONSTITUTORS_MAP = "__event_to_state_reconstitutors_map"

def _method_is_state_reconstitutor(method: Callable) -> bool:
    """Looks for decorated methods with an attribute named RECONSTITUTE_SAGA_STATE_TYPE."""
    return hasattr(method, STATE_RECONSTITUTOR_EVENT_TYPE)

class SagaError(Exception):
    pass

def reconstitute_saga_state(type: type):
    """Decorates saga methods that reconstitute state from events.
    
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
        setattr(wrapped, STATE_RECONSTITUTOR_EVENT_TYPE, type)
        if len(inspect.signature(wrapped).parameters) != 2:
            raise SagaError("@reconstitute_saga_state must decorate a method with 2 parameters: self, event: Event")
        @functools.wraps(wrapped)
        def wrapper(*args, **kwargs):
            return wrapped(*args, **kwargs)
        return wrapper
    return decorator


class Saga(metaclass=abc.ABCMeta):
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
    
    To implement a saga, extend from Saga and implement 
    def evaluate_hook(self).  You'll also want 
    @reconstitute_saga_state methods with a signature
    def method_name(self, event).  Lastly, call 
    super().__init__().
    """

    #Cache containing a map of saga-type to the method names 
    #on the saga that are decorated with 
    #@reconstitute_saga_state.  Because looking these up
    #can be expensive, the names are cached once for each saga
    #type.
    _saga_type_to_reconstitute_saga_state_method_names = dict()

    def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
        saga_type = type(self)

        #Update the cache with method names if needed.
        #See _saga_type_to_reconstitute_saga_state_method_names class field
        if saga_type not in Saga._saga_type_to_reconstitute_saga_state_method_names:
            Saga._saga_type_to_reconstitute_saga_state_method_names[saga_type] = find_decorated_method_names(self, _method_is_state_reconstitutor)

        register_methods(self, EVENT_TO_STATE_RECONSTITUTORS_MAP, STATE_RECONSTITUTOR_EVENT_TYPE, Saga._saga_type_to_reconstitute_saga_state_method_names[saga_type])

        self.saga_id = saga_id
        self.flags = set()
        self.retry_at = retry_at
        self.timeout_at = timeout_at
        self.is_timed_out = False
        self.is_complete = is_complete
        self.new_events = []
        self._apply_events(events)

    @abc.abstractmethod
    def evaluate_hook(self):
        """Customizable portion of the call to evaluate(self).
        
        When you implement this method, it should assume 
        that the saga has just woken up and has no idea where it 
        is.  You've been keeping track of where the saga is via
        events, so treat this method as a flowchart implemented 
        in code.  Here are some keys:
        
        --When you've reached a point where a command is issued
        and the saga is waiting for a corresponding event to
        bu published, the saga can return from this method at 
        which point it will be committed to storage until the 
        next event arrives meaning it's time to wake back up.
        
        --When an action fails, use the set_retry() method to 
        specify when the saga should wake up and try again.  
        After calling set_retry(), the saga can return from this
        method at which point it will be committed to storage
        until it's time to wake up.
        
        --A good way to handle timeouts is to put the timeout
        time in the event that triggers the saga in the first 
        place.  Any components involved in the saga's 
        transaction should also know the timeout.  Each 
        component should then be responsible for handling the 
        timeout in whatever way makes sense.  They should NOT 
        rely on the saga to let them know when the timeout
        is reached since the saga notified them long before.

        --There is an attribute on the saga called self.flags
        which is a set() which is intended to be used to track
        the saga's progress.  For example, when a command is 
        send as a result of a new event, put the name of the
        command into flags.  Note that evaluate() should 
        NEVER directly modify the saga's state.  This should
        only occur in the @reconstitute_saga_state methods.
        To ensure the command flag is set, create a new event
        like this one to set the flag: 

        class CommandSent(SagaEvent):
            pass
            
        The next time the saga is instantiated, the flag will 
        be set since this event will be retrieved.  To post 
        the new event, similar to an aggregate, use the 
        self._post_new_event(event) method.

        --Once a saga is timed out or completed, its state 
        will not progress, even if new events arrive.  This 
        is a great way to ensure that a saga's actions (commands)
        aren't duplicated.
            
        """
        pass

    def evaluate(self):
        """Call to possibly advance the saga state.
        
        When this method is called, the assumption is 
        that all of the latest events have been 
        reconstituted into the saga.  Some of these 
        events may require that actions be taken 
        such as dispatching commands or creating new 
        events."""
        if self.timeout_at != None and self.timeout_at < datetime.now():
            self.set_timed_out()
            return
        self.retry_at = None
        self.evaluate_hook()
    

    def set_complete(self):
        """Call from evaluate() to mark the saga as completed."""
        self.is_complete = True

    def set_timeout(self, timeout_at:datetime):
        """Call from evaluate() to specify a timeout for the saga."""
        self.timeout_at = timeout_at

    def set_timed_out(self):
        """Call to decalare that the timeout has been reached."""
        self.is_timed_out = True

    def set_retry(self, retry_at: datetime):
        """Call from evaluate() to specify when the saga should retry."""
        self.is_retry_at_updated = True
        self.retry_at = retry_at

    def _apply_events(self, events: List[Event]):
        """Applies events to rebuild aggregate state.
        
        THROWS
        ------
        SagaError when @reconstitute_saga_state method
        is missing."""
        event_to_state_reconstitutors_map = getattr(self, EVENT_TO_STATE_RECONSTITUTORS_MAP)
        try:
            for e in events:
                event_to_state_reconstitutors_map[type(e)](e)
        except KeyError as ke:
            raise SagaError("Missing state reconstitutor for " + str(type(e)) + "}", ke)
        
    def _post_new_event(self, event: Event):
        """Call from evalute() to post new state change events.  

        Events that are received to progress
        saga state are already committed by the 
        framework, so events that are posted using this method
         are generally to set flags when commands are sent. """
        
        self.new_events.append(event)
