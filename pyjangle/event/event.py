import dataclasses
from datetime import datetime
import abc

from pyjangle import JangleError
from pyjangle import event_id_factory_instance


class EventError(JangleError):
    pass


@dataclasses.dataclass(
    kw_only=True,
)
class Event(metaclass=abc.ABCMeta):
    id: any = dataclasses.field(default_factory=lambda: event_id_factory_instance()())
    created_at: datetime = dataclasses.field(default_factory=lambda: datetime.now())

    @classmethod
    def deserialize(cls, data: any) -> any:
        """Converts serialized representation to an Event."""
        return cls(**data)


@dataclasses.dataclass(
    kw_only=True,
)
class VersionedEvent(Event):
    """Represents an application's change in state.

    This could be anything: NameUpdated, AccountCreated,
    EmployeeHired, WidgetsOrdered, etc.  Within the
    context of this framework, any state changes that
    do not have a corresponding event DID NOT HAPPEN.
    The reason why is that as soon as the process shuts
    down, unexpectedly or not, the only thing that
    still exists are the events written to durable
    storage."""

    version: int
