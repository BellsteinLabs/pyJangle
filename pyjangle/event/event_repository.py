import abc
import functools
from typing import List

from pyjangle.error.error import SquirmError
from pyjangle.event.event import Event

#Holds a singleton instance of an event repository.
#Access this via event_repository_instance.
_event_repository_instance = None

def RegisterEventRepository(cls):
    """Registers a single class that implements EventRepository.
    
    It's a singleton, so there can only be one of these registered
    at a time in a process.  Make sure there's a parameterless 
    constructor.
    
    THROWS
    ------
    EventRepositoryError when multiple repositories are registered."""

    global _event_repository_instance
    if _event_repository_instance != None:
        raise EventRepositoryError("Cannot register multiple event repositories: " + str(type(_event_repository_instance)) + ", " + str(cls))
    _event_repository_instance = cls()
    @functools.wraps(cls)
    def wrapper(*args, **kwargs):
        return cls(*args, **kwargs)
    return wrapper


class EventRepository(metaclass = abc.ABCMeta):
    """A repository where events are stored.
    
    When using this framework, nothing happens if there's no a 
    corresponding event for it.  The implication is that the 
    event store is the single point of truth, so care must be 
    taken to ensure that it is ALWAYS in a consistent state.
    
    ***It is CRITICALLY important that whatever technology is 
    being used to persist events, there should be a
    uniqueness constraint on the combination of the 
    aggregate_id and version of each event!  This framework
    HINGES on this requirement to facilitate speedy
    optimistic concurrency.  It is also important that when
    the constraint is violated, a DuplicateKeyError is 
    raised!***

    Event stores enable some interesting functionality.  For
    example, if a backup of the application is needed, you'll
    only need the event store in the backup.  Any other tables 
    or views can be regenerated by replaying events through 
    the various Event Handlers by calling handle_event() in
    the event_handler module.
    """

    @abc.abstractmethod
    def get_events(self, aggregate_id: any, current_version = 0) -> List[Event]:
        """Returns events for a particular aggregate.
        
        RETURNS
        -------
        An empty list if there are no matching events.

        Aggregates ARE their events.  Sure, the code in the 
        aggregate is also a part of the aggregate, but the 
        important part is the events.  Everything else can
        be regenerated.  Commonly, snapshots can replace
        one or more events by fast-forwarding an aggregate
        to a specific version number.  In this case, only 
        events beyond the snapshot version need to be 
        retrieved, so use the "current_version" parameter
        to let the store know the CURRENT version of the 
        aggregate to get events that occurred after that 
        version."""
        pass

    @abc.abstractmethod
    def commit_events(self, aggregate_id: any, events: List[Event]):
        """Persist events to the event store.
        
        The event store enforces a uniuquesness constraint 
        on the combination of the aggregate_id and version.
        
        THROWS
        ------
        DuplicateKeyError when (aggregate_id, version)
        already exists.
        """
        pass

    @abc.abstractmethod
    def mark_event_handled(self, event: Event):
        """Marks an event as handled
        
        If an event is not marked as handled, it will
        potentially be retried at a later date."""
        pass

    @abc.abstractmethod
    def get_failed_events(self, batch_size: int) -> List[Event]:
        """Returns failed events.
        
        RETURNS
        -------
        Empty list when there are no matching events.

        When event handlers fail for whatever reason
        such as a network outage, they will not be marked 
        as completed and can be retrieved via this method.
        Because memory is generally more limited than 
        storage, there is a batch_size parameter to 
        facilitate taking only a few of these at a time.
        """
        pass

def event_repository_instance() -> EventRepository:
    """Returns the singleton instance of the registered event repository.
    
    THROWS
    ------
    EventRepositoryError when there is no event repository registered."""
    if not _event_repository_instance:
        raise EventRepositoryError("Event repository not registered")
    return _event_repository_instance

class EventRepositoryError(SquirmError):
    pass

class DuplicateKeyError(EventRepositoryError):
    pass

