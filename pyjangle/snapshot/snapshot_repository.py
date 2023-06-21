import abc
import functools

from pyjangle.error.error import SquirmError
from pyjangle.snapshot.snapshottable import Snapshottable

#Singleton instance of snapshot repositry
#Access via snapshot_repository_instance()
__registered_snapshot_repository = None


class SnapshotRepositoryError(SquirmError):
    pass


def RegisterSnapshotRepository(cls):
    """Registers a snapshot repository.
    
    THROWS
    ------
    SnapshotRepositoryError when multiple 
    repositories are registered."""
    global __registered_snapshot_repository
    if __registered_snapshot_repository != None:
        raise SnapshotRepositoryError(
            "Cannot register multiple snapshot repositories: " + str(type(__registered_snapshot_repository)) + ", " + str(cls))
    __registered_snapshot_repository = cls()
    @functools.wraps(cls)
    def wrapper(*args, **kwargs):
        return cls(*args, **kwargs)
    return wrapper


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
    def get_snapshot(self, aggregate_id: str) -> tuple[int, any]:
        """Retrieve a snapshot for an aggregate_id.
        
        RETURNS
        -------
        None if there is no snapshot.  Returns a
        tuple(version, snapshot) otherwise."""
        pass

    @abc.abstractmethod
    def store_snapshot(self, aggregate_id: any, version, int, snapshot: any):
        """Stores a snapshot for an aggregate."""
        pass

    @abc.abstractmethod
    def delete_snapshot(self, aggregate_id: str):
        """Deletes a snapshot.
        
        Sometimes, code changes invalidate snapshots 
        and cause exceptions to be thrown.  Those are
        deleted via this method, usually by the framework
        so you'll never need to call this."""
        pass


def snapshot_repository_instance() -> SnapshotRepository:
    """Retrieve the singleton instance of the snapshot repository."""
    if not __registered_snapshot_repository:
        raise SnapshotRepositoryError("Snapshot repository not registered")
    return __registered_snapshot_repository
