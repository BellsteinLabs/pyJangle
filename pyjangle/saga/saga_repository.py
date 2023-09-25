import abc
import functools

from pyjangle import LogToggles, log, Saga, JangleError

# Saga repository singleton.  Access this
# via saga_repository_instance()
_registered_saga_repository = None


class DuplicateSagaRepositoryError(JangleError):
    "Registered multiple saga repositories."
    pass


class SagaRepositoryMissingError(JangleError):
    "Saga repository is not registered."
    pass


def RegisterSagaRepository(cls):
    """Decorates and registers a `SagaRepository` implementation.

    Raises:
        DuplicateSagaRepositoryError:
            Registered multiple saga repositories.
        SagaRepositoryMissingError:
            Saga repository is not registered.
    """

    global _registered_saga_repository
    if _registered_saga_repository != None:
        raise DuplicateSagaRepositoryError(
            "Cannot register multiple saga repositories: "
            + str(type(_registered_saga_repository))
            + ", "
            + str(cls)
        )
    _registered_saga_repository = cls()
    log(
        LogToggles.saga_repository_registration,
        "Saga repository registered",
        {"saga_repository_type": str(cls)},
    )

    @functools.wraps(cls)
    def wrapper(*args, **kwargs):
        return cls(*args, **kwargs)

    return wrapper


class SagaRepository(metaclass=abc.ABCMeta):
    """A repository for saga-specific events.

    The saga repository is an event store for sagas.  Some events in the "normal" event
    store might be duplicated here.  This store may have additional events that are
    specific to the saga's state changes such as a command being sent (which is an
    example of a state change).  Because sagas sleep most of the time while they wait
    on new events, it's important that EVERY state change in a saga have a corresponding
    event which would be persisted in a saga store.

    Unlike an event store, a saga store does not rely on the version field for a
    primary key constraint.
    """

    @abc.abstractmethod
    async def get_saga(self, saga_id: any) -> Saga:
        """Retrieve a saga by `saga_id`.

        Args:
            saga_id:
                ID of the saga to retrieve.

        Returns:
            Deserialized saga instance or None if not found.
        """
        pass

    @abc.abstractmethod
    async def commit_saga(self, saga: Saga):
        """Commits updated sagas to storage.

        A primary key violation implies that the saga was executed concurrently in
        a different synchronization context.  If this is the case, it is safe to
        abandon the saga since it succeeded elsewhere.

        Args:
            saga:
                The saga to commit to storage.

        Raises:
            DuplicateKeyError:
                Primary key constraint was violated.
        """
        pass

    @abc.abstractmethod
    async def get_retry_saga_ids(self, batch_size: int) -> list[any]:
        """Returns ids for saga's that need to be retried.

        If a saga is not completed, or timed out and the retry_at is in the past, the
        saga should be retried.  This methods returns the ids of all sagas meeting
        that criteria.

        Args:
            batch_size:
                The maximum number of sagas to buffer in memory concurrently.

        Returns:
            List of saga ids.  Empty list if no sagas meet criteria.
        """
        pass


def saga_repository_instance() -> SagaRepository:
    """Retrieve singleton instance of saga repository."""
    if not _registered_saga_repository:
        raise SagaRepositoryMissingError()
    return _registered_saga_repository
