import abc

from pyjangle import JangleError, LogToggles, log

# Singleton instance of snapshot repositry
# Access via snapshot_repository_instance()
_registered_snapshot_repository = None


class DuplicateSnapshotRepositoryError(JangleError):
    "Registered multiple snapshot repositories."
    pass


class SnapshotRepositoryMissingError(JangleError):
    "No snapshot repository registered."
    pass


def RegisterSnapshotRepository(cls):
    """Decorates and registers a class that implements `SnapshotRepository`.

    Raises:
        DuplicateSnapshotRepositoryError:
            Registered multiple snapshot repositories.

        SnapshotRepositoryTypeError:
            Type is not a snapshot repository.
    """
    global _registered_snapshot_repository
    if _registered_snapshot_repository != None:
        raise DuplicateSnapshotRepositoryError(
            "Cannot register multiple snapshot repositories: "
            + str(_registered_snapshot_repository)
            + ", "
            + str(cls)
        )
    _registered_snapshot_repository = cls()
    log(
        LogToggles.snapshot_repository_registration,
        "Snapshot repository registered",
        {"snapshot_repository_type": str(cls)},
    )
    return cls


class SnapshotRepository(metaclass=abc.ABCMeta):
    """A repository where aggregate snapshots are stored.

    Some aggregates have lengthy event histories which are used to rebuild state.  An
    optimization is to snapshot the state periodically.  The snapshot accounts for all
    events up to a certain point, and only events with a higher version than the
    snapshot need be retrieved from storage.

    It is recommended to validate that snapshots are still relevant in the case that
    code changes.  In this case, the snapshot should be deleted which will cause a new
    snapshot to be created on the next valid command.
    """

    @abc.abstractmethod
    async def get_snapshot(self, aggregate_id: str) -> tuple[int, any] | None:
        """Retrieve a snapshot by aggregate_id.

        Args:
            aggregate_id:
                Aggregate id of the snapshot to retrieve.

        Returns:
            None if there is no snapshot.  Tuple[version, snapshot], otherwise."""
        pass

    @abc.abstractmethod
    async def store_snapshot(self, aggregate_id: any, version: int, snapshot: any):
        """Stores a snapshot for an aggregate.

        Args:
            aggregate_id:
                aggregate to which the snapshot belongs.
            version:
                Highest version event captured by the snapshot.
            snapshot:
                The snapshot to store.
        """
        pass

    @abc.abstractmethod
    async def delete_snapshot(self, aggregate_id: str):
        """Deletes a snapshot.

        Code changes could potentially invalidate snapshots.  If the application of a
        snapshot raises an exception, the framework will delete it using this method.
        Be careful of the case where an invalid snapshot is applied and does *NOT* raise
        an error.

        Args:
            aggregate_id:
                ID of the snapshot to delete."""
        pass


def snapshot_repository_instance() -> SnapshotRepository:
    """Returns the singleton instance of the snapshot repository.

    Raises:
        SnapshotRepositoryMissingError:
            No snapshot repository registered.
    """
    if not _registered_snapshot_repository:
        raise SnapshotRepositoryMissingError("Snapshot repository not registered")
    return _registered_snapshot_repository
