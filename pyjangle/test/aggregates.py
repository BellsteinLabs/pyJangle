from datetime import datetime
from pyjangle import Aggregate, reconstitute_aggregate_state, validate_command
from pyjangle import CommandResponse
from pyjangle import RegisterCommand
from pyjangle import Snapshottable
from pyjangle.test.commands import AnotherCommandThatAlwaysSucceeds, CommandThatAlwaysSucceeds, CommandThatFails
from pyjangle.test.events import EventA


@RegisterCommand(CommandThatAlwaysSucceeds)
class SnapshottableTestAggregate(Aggregate, Snapshottable):

    def __init__(self, id: any):
        super().__init__(id)
        self.count = 0

    @validate_command(CommandThatAlwaysSucceeds)
    def validateA(self, command: CommandThatAlwaysSucceeds, next_version: int):
        self._post_new_event(
            EventA(version=next_version, created_at=datetime.now()))

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
        self._post_new_event(
            EventA(version=next_version, created_at=datetime.now()))
        return CommandResponse(True, {})

    @validate_command(CommandThatFails)
    def validateB(self, command: CommandThatFails, next_version: int):
        self._post_new_event(
            EventA(version=next_version, created_at=datetime.now()))
        return CommandResponse(False, {})

    @reconstitute_aggregate_state(EventA)
    def from_event_that_continues_saga(self, event: EventA):
        pass
