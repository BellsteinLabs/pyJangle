import abc
from datetime import datetime
from typing import Iterator

from pyjangle import JangleError, VersionedEvent, LogToggles, log, get_batch_size

# Holds a singleton instance of an event repository.
# Access this via event_repository_instance.
_event_repository_instance = None


class DuplicateEventRepositoryError(JangleError):
    "Registering multiple event repositories is not allowed."
    pass


class EventRepositoryMissingError(JangleError):
    "Event repository not registered."
    pass


def RegisterEventRepository(cls):
    """Decorates and registers a class that implements `EventRepository`.

    Raises:
        DuplicateEventRepositoryError:
            Registering multiple event repositories is not allowed.
    """

    global _event_repository_instance
    if _event_repository_instance != None:
        raise DuplicateEventRepositoryError(
            "Cannot register multiple event repositories: "
            + str(type(_event_repository_instance))
            + ", "
            + str(cls)
        )
    _event_repository_instance = cls()
    log(
        LogToggles.event_repository_registration,
        "Event repository registered",
        {"event_repository_type": str(cls)},
    )
    return cls


class EventRepository(metaclass=abc.ABCMeta):
    """A repository where events are stored.

    The event repository serves as an infallibale source of truth--you could say that
    even if it's wrong, it's right. Events that are not yet committed are an opinion,
    and a committed event is a fact.  The underlying implementation ensures that the
    event store is always in a consistent state.  Some implementations may also serve as
    the event queue/bus which necessitates a mechanism for marking an event as having
    been 'completed' or 'handled' via the `mark_event_handled` method.

    The underlying implementation must also have some notion of a primary key to
    uniquely identify events which must be referenceable via the aggregate of an
    aggregate identifier and a version number.  This is the mechanism that creates the
    optimistic concurrency capability.  In other words, a `DuplicateKeyError` should be
    raised whenever an aggregate identifer + version has already been committed.
    """

    @abc.abstractmethod
    async def get_events(
        self, aggregate_id: any, current_version=0, batch_size: int = get_batch_size()
    ) -> Iterator[VersionedEvent]:
        """Returns events for a particular aggregate.

        If snaphsots are being utilized, use the `current_version` parameter to exclude
        the events covered by the snapshot.  This can save considerable bandwidth and
        time.  Many persistence mechanisms will only buffer a subset of a large result
        set in memory at a given time--use the batch-size parameter to leverage this
        mechanism when available.

        Args:
            aggregate_id:
                Events with this aggregate identifier will be returned.
            current_version:
                If aggregate state is known up to a certain point, events covering that
                portion of the state will not be returned.
            batch_size:
                For aggregates that return a large volume of events, this argument will
                limit how many events concurrently reside in memory.

        Returns:
            A list of events corresponding to an aggregate, or an empty list if there
            are no matching events.
        """
        pass

    @abc.abstractmethod
    async def commit_events(
        self, aggregate_id_and_event_tuples: list[tuple[any, VersionedEvent]]
    ):
        """Persists events to storage.

        Enforces a uniuquesness constraint on the combination of the aggregate_id and
        version.  Because commands can often result in multiple events, some of which
        may even be associated with another (new) aggregate, this method expects a list
        of tuples containing the aggregate identifier followed by the event.

        Args:
            aggregate_id_and_event_tuples:
                list of tuples containing aggregate identifier and event.

        Raises:
            DuplicateKeyError:
              Primary key constraint was violated.
        """
        pass

    @abc.abstractmethod
    async def mark_event_handled(self, event_id: any):
        """Tags an event as having been handled.

        If an event is not marked as handled, event dispatchers should continuously
        attempt to retry the event at reasonable some interval.  Marking an event as
        having been handled prohibits an event from being returned from
        `get_unhandled_events`.  No exceptions are raised if `event_id` does not exist
        or has already been marked as handled.

        Args:
            event_id:
                The id of the event to mark as handled, not to be confused with the
                aggregate identifier.
        """
        pass

    @abc.abstractmethod
    async def get_unhandled_events(
        self, batch_size: int, time_delta: datetime
    ) -> Iterator[VersionedEvent]:
        """Returns unhandled events.

        When event dispatch fails for whatever reason, an event will not be marked as
        having been handled.  If an event has not been marked handled after having been
        committed for a certain period of time, it is considered to be unhandled and is
        returned by this method.

        Args:
            batch_size:
                Maximum number of events in the result set that are buffered in memory
                at a time.
            time_delta:
                After an event has been committed for this length of time without being
                handled, it is considered unhandled.

        Returns:
            An empty list when there are no matching events.
        """
        pass


def event_repository_instance(
    raise_exception_if_not_registered: bool = True,
) -> EventRepository:
    """Returns the singleton instance of the registered event repository.

    Raises:
        EventRepositoryMissingError:
            Event repository not registered.
    """

    global _event_repository_instance
    if not _event_repository_instance and raise_exception_if_not_registered:
        raise EventRepositoryMissingError("Event repository not registered.")
    return _event_repository_instance
