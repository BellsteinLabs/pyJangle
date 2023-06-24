import abc
from pyjangle.error.error import JangleError

class SnapshotError(JangleError):
    pass

class Snapshottable(metaclass=abc.ABCMeta):
    """Represents an aggregate that can be snapshotted.
    
    Aggregates with long event histories can benefit 
    from this interface.  Events are used to build 
    aggregate state.  One could take a snapshot after
    say 1000 events are applied.  Instead of later
    retrieving those 1000 events, the snapshot
    could be used to restore the state at that point
    and then only the events newer than version 1000
    would need to be retrieved from storage.  Saves
    time, money, and CPU cycles."""
    @abc.abstractmethod
    def apply_snapshot_hook(self, snapshot):
        """Updates the aggregate state based on snapshot."""
        pass

    @abc.abstractmethod
    def get_snapshot(self) -> any:
        """Retrieves the current state in the form of a snapshot."""
        pass

    @abc.abstractmethod
    def get_snapshot_frequency(self) -> int:
        """Represents often should snapshots be taken?
        
        For a frequency of 10, a snapshot would be taken every 
        10 events."""
        pass

    def apply_snapshot(self, version: int, snapshot: any):
        """Applied a snapshot to an aggregate.
        
        Use apply_snapshot_hook to customize this 
        method's behavior."""
        try:
            self.apply_snapshot_hook(snapshot)
            self.version = version
        except Exception as e:
            raise SnapshotError(e)