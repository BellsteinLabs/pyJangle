import abc
import functools
import logging
from typing import List
from pyjangle import JangleError
from pyjangle.event.event import VersionedEvent
from pyjangle.logging.logging import LogToggles, log
from pyjangle.saga.saga import Saga

# Saga repository singleton.  Access this
# via saga_repository_instance()
__registered_saga_repository = None


class SagaRepositoryError(JangleError):
    pass


def RegisterSagaRepository(cls):
    """Registers a saga repository.

    THROWS
    ------
    SagaRepositoryError when multiple saga repositories are registered.
    """

    global __registered_saga_repository
    if __registered_saga_repository != None:
        raise SagaRepositoryError("Cannot register multiple saga repositories: " + str(
            type(__registered_saga_repository)) + ", " + str(cls))
    __registered_saga_repository = cls()
    log(LogToggles.saga_repository_registration,
        "Saga repository registered", {"saga_repository_type": str(cls)})

    @functools.wraps(cls)
    def wrapper(*args, **kwargs):
        return cls(*args, **kwargs)
    return wrapper

# TODO: Add uniquess constraints to saga store.


class SagaRepository(metaclass=abc.ABCMeta):
    """A repository for saga-specific events.

    The saga repository is basically an event store 
    for sagas.  Some events in the "vanilla" event store 
    might be duplicated here.  This store may have 
    additional events that are specific to the saga's 
    state changes such as a command being sent (which
    is an example of a state change). 
    Because sagas sleep most of the time while they wait 
    on new events, it's important that EVERY state
    change in a saga have a corresponding event.  Put 
    them in this repo.

    Like an event store, it is critical that """
    @abc.abstractmethod
    async def get_saga(self, saga_id: any) -> Saga:
        """Retrieve a saga's metadata and events.

        When a saga is instantiated, metadata 
        which includes timeouts, retry timers, 
        completed and timeout flags, along 
        with the sags's events are required
        to fully reconstitute the saga. Get
        them here via the saga_id."""
        pass

    @abc.abstractmethod
    async def commit_saga(self, saga: Saga):
        """Commits updated sagas to storage.

        THROWS
        ------
        DuplicateKeyError

        A saga essentially consists of its events 
        and metadata.  Commit them here.  If there
        is a duplicate key error, the updated saga 
        should be disposed of."""
        pass

    @abc.abstractmethod
    async def get_retry_saga_metadata(self, max_count: int) -> list[any]:
        """Returns metadata for saga's that need to be retried.

        Sometimes, a saga will fail a state transition because
        of a network outage or similar reasons.  See the 
        saga_daemon module for an entrypoint that a daemon
        can use to periodically reexecute a failed saga."""
        pass


def saga_repository_instance() -> SagaRepository:
    """Retrieve singleton instance of saga repository."""
    if not __registered_saga_repository:
        raise SagaRepositoryError()
    return __registered_saga_repository
