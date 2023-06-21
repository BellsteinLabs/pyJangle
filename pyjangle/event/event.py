import dataclasses
from datetime import datetime
import logging
import abc
from typing import Callable, List
import functools
import uuid

from pyjangle.error.error import SquirmError


class EventError(SquirmError):
    pass


@dataclasses.dataclass(frozen=True, kw_only=True)
class Event:
    """Represents an application's change in state.
    
    This could be anything: NameUpdated, AccountCreated,
    EmployeeHired, WidgetsOrdered, etc.  Within the
    context of this framework, any state changes that 
    do not have a corresponding event DID NOT HAPPEN.
    The reason why is that as soon as the process shuts 
    down, unexpectedly or not, the only thing that 
    still exists are the events written to durable 
    storage."""
    id: uuid.uuid4()
    version: int
    created_at: datetime

@dataclasses.dataclass(frozen=True, kw_only=True)
class SagaEvent:
    """Represents an event specific to a saga state change.
    
    Sagas are state machines that handle distributed 
    transactions.  Because they go to sleep when waiting 
    for more data, they need to know where they left off.
    These state changes are represented by saga events.
    
    For example, there might be a RolledBackFundsTransfer
    event that represents to the saga that it issued a 
    command to rollback the funds transfer.  Commonly,
    each of these events will have a corrsponding command."""
    id: uuid.uuid4()
    saga_id: any
    created_at: datetime.now()