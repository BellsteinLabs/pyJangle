from dataclasses import dataclass
import dataclasses
from datetime import datetime
import functools
from typing import Iterator, List
import uuid
from pyjangle.aggregate.aggregate import Aggregate, reconstitute_aggregate_state, validate_command
from pyjangle.command.command import Command
from pyjangle.command.command_response import CommandResponse
from pyjangle.command.register import RegisterCommand
from pyjangle.event.event import Event, SagaEvent
from pyjangle.event.event_dispatcher import RegisterEventDispatcher
from pyjangle.saga.saga import Saga, event_receiver, reconstitute_saga_state
from pyjangle.snapshot.snapshot_repository import RegisterSnapshotRepository, SnapshotRepository
from pyjangle.snapshot.snapshottable import Snapshottable
from pyjangle.test.events import EventA, EventThatCausesDuplicateKeyError, EventThatCausesSagaToRetry, EventThatCompletesSaga, EventThatContinuesSaga, EventB, EventThatContinuesSaga, EventThatSetsSagaToTimedOut, EventThatTimesOutSaga

@dataclasses.dataclass(frozen=True, kw_only=True)
class TestInternalSagaEvent(SagaEvent):
    pass

class SagaForTesting(Saga):
    def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
        self.calls={"on_event_that_continues_saga": 0, "from_event_that_continues_saga": 0}
        self.internal_event_versions = set()
        super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

    @event_receiver(EventThatContinuesSaga, skip_if_any_flags_set=[TestInternalSagaEvent])
    def on_event_that_continues_saga(self, next_version: int):
        self._post_new_event(TestInternalSagaEvent(version=next_version))
        self.calls["on_event_that_continues_saga"] += 1

    @event_receiver(EventThatCompletesSaga)
    def on_event_that_completes_sagab(self, next_version: int):
        pass

    @event_receiver(EventThatTimesOutSaga)
    def on_event_that_times_out_saga(self, next_version: int):
        pass

    @event_receiver(EventThatCausesDuplicateKeyError)
    def on_event_that_causes_duplicate_key_error(self, next_version: int):
        self._post_new_event(TestInternalSagaEvent(version=list(self.internal_event_versions)[0]))

    @event_receiver(EventThatSetsSagaToTimedOut)
    def on_event_that_sets_saga_to_timed_out(self, next_version: int):
        pass

    @event_receiver(EventThatCausesSagaToRetry)
    def on_event_that_causes_saga_to_retry(self, next_version: int):
        pass

    @reconstitute_saga_state(EventThatContinuesSaga)
    def from_event_that_continues_saga(self, event: EventThatContinuesSaga):
        self.calls["from_event_that_continues_saga"] += 1

    @reconstitute_saga_state(EventThatCompletesSaga, add_event_type_to_flags=False)
    def from_event_that_completes_saga(self, event: EventThatCompletesSaga):
        self.set_complete()

    @reconstitute_saga_state(TestInternalSagaEvent)
    def from_test_internal_saga_event(self, event: TestInternalSagaEvent):
        self.internal_event_versions.add(event.version)

    @reconstitute_saga_state(EventThatCausesDuplicateKeyError)
    def from_event_that_causes_duplicate_key_error(self, event: EventThatCausesDuplicateKeyError):
        pass

    @reconstitute_saga_state(EventThatTimesOutSaga)
    def from_event_that_times_out_saga(self, event: EventThatTimesOutSaga):
        self.set_timeout(datetime.min)

    @reconstitute_saga_state(EventThatSetsSagaToTimedOut)
    def from_event_that_sets_saga_to_timed_out(self, event: EventThatSetsSagaToTimedOut):
        self.set_timed_out()

    @reconstitute_saga_state(EventThatCausesSagaToRetry)
    def from_event_that_causes_saga_to_retry(self, event: EventThatCausesSagaToRetry):
        self.set_retry(datetime.min)

@RegisterEventDispatcher
def event_dispatcher(event):
    pass

class CommandThatAlwaysSucceeds(Command):
    def get_aggregate_id(self):
        return 1
    
class CommandThatFails(Command):
    def get_aggregate_id(self):
        return 1
    
class AnotherCommandThatAlwaysSucceeds(Command):
    def get_aggregate_id(self):
        return 1
    
class CommandB(Command):
    def get_aggregate_id(self):
        pass

@RegisterCommand(CommandThatAlwaysSucceeds)
class SnapshottableTestAggregate(Aggregate, Snapshottable):

    def __init__(self, id: any):
        super().__init__(id)
        self.count = 0

    @validate_command(CommandThatAlwaysSucceeds)
    def validateA(self, command: CommandThatAlwaysSucceeds, next_version: int):
        self._post_new_event(EventA(version=next_version, created_at=datetime.now()))

    @reconstitute_aggregate_state(EventA)
    def from_event_that_continues_saga(self, event: EventA):
        self.count += 1

    def apply_snapshot_hook(self, snapshot):
        self.count = snapshot

    def get_snapshot(self) -> any:
        return self.count

    def get_snapshot_frequency(self) -> int:
        return 2
    
@RegisterCommand(AnotherCommandThatAlwaysSucceeds, CommandThatFails)
class NotSnapshottableTestAggregate(Aggregate):

    def __init__(self, id: any):
        super().__init__(id)
        self.count = 0

    @validate_command(AnotherCommandThatAlwaysSucceeds)
    def validateA(self, command: AnotherCommandThatAlwaysSucceeds, next_version: int):
        self._post_new_event(EventA(version=next_version, created_at=datetime.now()))
        return CommandResponse(True, {})

    @validate_command(CommandThatFails)
    def validateB(self, command: CommandThatFails, next_version: int):
        self._post_new_event(EventA(version=next_version, created_at=datetime.now()))
        return CommandResponse(False, {})

    @reconstitute_aggregate_state(EventA)
    def from_event_that_continues_saga(self, event: EventA):
        pass

@RegisterSnapshotRepository
class TransientSnapshotRepository(SnapshotRepository):
    def __init__(self) -> None:
        super().__init__()
        self._snapshots: dict[any, any] = dict()

    async def get_snapshot(self, aggregate_id: str) -> tuple[int, any] | None:
        value = self._snapshots[aggregate_id] if aggregate_id in self._snapshots else None
        return value
    
    async def store_snapshot(self, aggregate_id: any, version: int, snapshot: any):
        self._snapshots[aggregate_id] = (version, snapshot)

    async def delete_snapshot(self, aggregate_id: str):
        if aggregate_id in self._snapshots: #pragma no cover
            del self._snapshots[aggregate_id]
