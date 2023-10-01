import dataclasses
from datetime import datetime
import abc
from pyjangle import event_id_factory_instance


@dataclasses.dataclass(
    kw_only=True,
)
class Event(metaclass=abc.ABCMeta):
    id: any = dataclasses.field(default_factory=lambda: event_id_factory_instance()())
    created_at: datetime = dataclasses.field(default_factory=lambda: datetime.now())
    """Represents a state change in a domain.
    
    NameUpdated, AccountCreated, EmployeeHired, WidgetsOrdered, etc are all examples of 
    events.  All state changes should have a corresponding event.  It is required that 
    all events are registered with the `register_event` decorator.
    """

    @classmethod
    def deserialize(cls, data: any) -> any:
        """Converts serialized representation to an Event.

        Args:
            data:
                Format depends on the persistence mechanism being used.  However, a
                dictionary is a broadly useful data structure to use for this argument.
        """
        return cls(**data)


@dataclasses.dataclass(
    kw_only=True,
)
class VersionedEvent(Event):
    """Represents an application's ordered change in state.

    Events associated to an aggregate happen in a specific order which is represents by
    the `version` attribute.  The version increases incrementally with time.  The
    distinction between versioned and unversioned events exists because events without a
    version are sufficient for representing state change internal to a saga."""

    version: int
