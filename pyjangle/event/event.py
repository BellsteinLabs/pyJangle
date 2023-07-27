import dataclasses
from datetime import datetime
import logging
import abc
from typing import Callable, List
import functools
import uuid

from pyjangle import JangleError


class EventError(JangleError):
    pass


@dataclasses.dataclass(frozen=True, kw_only=True,)
class Event(metaclass=abc.ABCMeta):
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = dataclasses.field(
        default_factory=lambda: datetime.now().isoformat())

    @classmethod
    @abc.abstractmethod
    def deserialize(data: any) -> any:
        """Converts serialized representation to an Event."""
        pass


@dataclasses.dataclass(frozen=True, kw_only=True,)
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
