import abc
import logging

from pyjangle import JangleError
from pyjangle.logging.logging import LogToggles, log

# Singleton instance of snapshot repositry
# Access via snapshot_repository_instance()
_registered_snapshot_repository = None


class SnapshotRepositoryError(JangleError):
    pass


def RegisterSnapshotRepository(cls):
    """Registers a snapshot repository.

    THROWS
    ------
    SnapshotRepositoryError when multiple 
    repositories are registered."""
    global _registered_snapshot_repository
    if _registered_snapshot_repository != None:
        raise SnapshotRepositoryError(
            "Cannot register multiple snapshot repositories: " + str(_registered_snapshot_repository) + ", " + str(cls))
    _registered_snapshot_repository = cls()
    log(LogToggles.snapshot_repository_registration, "Snapshot repository registered", {
        "snapshot_repository_type": str(cls)})
    return cls


class SnapshotRepository(metaclass=abc.ABCMeta):
    """Contains aggregate snapshots.

    Some aggregates have lengthy event histories
    which are used to rebuild state.  But what if 
    the state was 'snapshotted' every x events?
    You could just get the single snapshot
    corresponding to thousands of events and only 
    get those events that are newer than the 
    snapshot!"""
    @abc.abstractmethod
    async def get_snapshot(self, aggregate_id: str) -> tuple[int, any] | None:
        """Retrieve a snapshot for an aggregate_id.

        RETURNS
        -------
        None if there is no snapshot.  Returns a
        tuple(version, snapshot) otherwise."""
        pass

    @abc.abstractmethod
    async def store_snapshot(self, aggregate_id: any, version: int, snapshot: any):
        """Stores a snapshot for an aggregate."""
        pass

    @abc.abstractmethod
    async def delete_snapshot(self, aggregate_id: str):
        """Deletes a snapshot.

        Sometimes, code changes invalidate snapshots 
        and cause exceptions to be thrown.  Those are
        deleted via this method, usually by the framework
        so you'll never need to call this."""
        pass


def snapshot_repository_instance() -> SnapshotRepository:
    """Retrieve the singleton instance of the snapshot repository."""
    if not _registered_snapshot_repository:
        raise SnapshotRepositoryError("Snapshot repository not registered")
    return _registered_snapshot_repository
