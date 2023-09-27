import abc
from pyjangle import JangleError


class SnapshotError(JangleError):
    pass


class Snapshottable(metaclass=abc.ABCMeta):
    """Interface for an aggregate that can be snapshotted.

    Aggregates with long event histories can benefit from this interface.  A snapshot 
    captures an aggregate's state at a certain version.  A snapshot is the serialized 
    state along with the version.  The number of events retrieved from storage will 
    never be greater than the result of `get_snapshot_frequency`."""

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
        """Represents the frequency at which snapshots are taken.

        Snapshots are taken if event_count % frequency == 0."""
        pass

    def apply_snapshot(self, version: int, snapshot: any):
        """Applied a snapshot to an aggregate.

        Implement apply_snapshot_hook to customize this method's behavior."""
        try:
            self.apply_snapshot_hook(snapshot)
            self.version = version
        except Exception as e:
            raise SnapshotError(e)
