from datetime import datetime
from pyjangle import Aggregate, reconstitute_aggregate_state, validate_command
from pyjangle import CommandResponse
from pyjangle import RegisterAggregate
from pyjangle import Snapshottable
from test_helpers.commands import (
    CommandThatShouldSucceedB,
    CommandThatShouldSucceedA,
    CommandThatShouldFail,
)
from test_helpers.events import EventA


@RegisterAggregate
class SnapshottableTestAggregate(Aggregate, Snapshottable):
    def __init__(self, id: any):
        super().__init__(id)
        self.count = 0

    @validate_command(CommandThatShouldSucceedA)
    def validateA(self, command: CommandThatShouldSucceedA, next_version: int):
        self.post_new_event(EventA(version=next_version, created_at=datetime.now()))

    @reconstitute_aggregate_state(EventA)
    def from_event_that_continues_saga(self, event: EventA):
        self.count += 1

    def apply_snapshot_hook(self, snapshot):
        self.count = snapshot

    def get_snapshot(self) -> any:
        return self.count

    def get_snapshot_frequency(self) -> int:
        return 2


@RegisterAggregate
class NotSnapshottableTestAggregate(Aggregate):
    def __init__(self, id: any):
        super().__init__(id)
        self.count = 0

    @validate_command(CommandThatShouldSucceedB)
    def validateA(self, command: CommandThatShouldSucceedB, next_version: int):
        self.post_new_event(EventA(version=next_version, created_at=datetime.now()))
        return CommandResponse(True, {})

    @validate_command(CommandThatShouldFail)
    def validateB(self, command: CommandThatShouldFail, next_version: int):
        self.post_new_event(EventA(version=next_version, created_at=datetime.now()))
        return CommandResponse(False, {})

    @reconstitute_aggregate_state(EventA)
    def from_event_that_continues_saga(self, event: EventA):
        pass
