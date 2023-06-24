from  datetime import datetime
from typing import List
import unittest
from unittest.mock import patch

from pyjangle.aggregate.aggregate import Aggregate, reconstitute_aggregate_state, validate_command
from pyjangle.command.command_handler import handle_command
from pyjangle.command.register import RegisterCommand
from pyjangle.event.event import Event
from pyjangle.event.event_dispatcher import EventDispatcherError, RegisterEventDispatcher
from pyjangle.event.event_repository import DuplicateKeyError, EventRepository, RegisterEventRepository, event_repository_instance
from pyjangle.snapshot.snapshot_repository import RegisterSnapshotRepository, SnapshotRepository, snapshot_repository_instance
from pyjangle.snapshot.snapshottable import Snapshottable
from pyjangle.test.test_types import CommandA, EventA


class TestCommandHandler(unittest.TestCase):
    @patch("pyjangle.snapshot.snapshot_repository.__registered_snapshot_repository", None)
    @patch("pyjangle.event.event_dispatcher.__event_dispatcher", None)
    @patch("pyjangle.event.event_repository._event_repository_instance", None)
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_command_handled(self):
        @RegisterEventRepository
        class EventRepo(EventRepository):

            def get_events(self, aggregate_id: any, current_version = 0) -> List[Event]:
                return []

            def commit_events(self, aggregate_id: any, events: List[Event]):
                pass

            def mark_event_handled(self, event: Event):
                pass

            def get_failed_events(self, batch_size: int) -> List[Event]:
                pass

        @RegisterEventDispatcher
        def event_dispatcher(event, handled_callback):
            pass
        
        @RegisterCommand(CommandA)
        class A(Aggregate):
            @validate_command(CommandA)
            def validateA(self, command: CommandA, next_version: int):
                self._post_new_event(EventA(id=2, version=next_version, created_at=datetime.now()))

        response = handle_command(CommandA())
        self.assertTrue(response.is_success)

    @patch("pyjangle.snapshot.snapshot_repository.__registered_snapshot_repository", None)
    @patch("pyjangle.event.event_dispatcher.__event_dispatcher", None)
    @patch("pyjangle.event.event_repository._event_repository_instance", None)
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_no_event_dispatcher_registered(self):
        with self.assertRaises(EventDispatcherError):
            @RegisterEventRepository
            class EventRepo(EventRepository):

                def get_events(self, aggregate_id: any, current_version = 0) -> List[Event]:
                    return []

                def commit_events(self, aggregate_id: any, events: List[Event]):
                    pass

                def mark_event_handled(self, event: Event):
                    pass

                def get_failed_events(self, batch_size: int) -> List[Event]:
                    pass

            @RegisterCommand(CommandA)
            class A(Aggregate):
                @validate_command(CommandA)
                def validateA(self, command: CommandA, next_version: int):
                    self._post_new_event(EventA(id=2, version=next_version, created_at=datetime.now()))

            response = handle_command(CommandA())

    @patch("pyjangle.snapshot.snapshot_repository.__registered_snapshot_repository", None)
    @patch("pyjangle.event.event_dispatcher.__event_dispatcher", None)
    @patch("pyjangle.event.event_repository._event_repository_instance", None)
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_snapshot_applied_when_aggregate_is_snapshottable(self):
        @RegisterEventRepository
        class EventRepo(EventRepository):

            def __init__(self) -> None:
                super().__init__()
                self._events = dict()#aggregate_id[dict[version, event_data]]
                self._last_current_version = 0

            def get_events(self, aggregate_id: any, current_version = 0) -> List[Event]:
                self._last_current_version = current_version
                events_to_return = self._events[aggregate_id] if aggregate_id in self._events else dict()
                return [v for k,v in events_to_return.items() if k > current_version]

            def commit_events(self, aggregate_id: any, events: List[Event]):
                if not aggregate_id in self._events:
                    self._events[aggregate_id] = dict()
                incoming_versions = [x.version for x in events]
                existing_versions = [y.version for x,y in self._events[aggregate_id].items()]
                if set(incoming_versions).intersection(existing_versions):
                    raise DuplicateKeyError()
                for x in events:
                    self._events[aggregate_id][x.version] = x

            def mark_event_handled(self, event: Event):
                pass

            def get_failed_events(self, batch_size: int) -> List[Event]:
                pass

        @RegisterSnapshotRepository
        class SnapRepo(SnapshotRepository):
            def __init__(self) -> None:
                super().__init__()
                self._snapshots = dict()

            def get_snapshot(self, aggregate_id: str) -> tuple[int, any]:
                value = self._snapshots[aggregate_id] if aggregate_id in self._snapshots else None
                return value
            
            def store_snapshot(self, aggregate_id: any, version: int, snapshot: any):
                self._snapshots[aggregate_id] = (version, snapshot)

            def delete_snapshot(self, aggregate_id: str):
                del self._snapshots[aggregate_id]

            
        @RegisterCommand(CommandA)
        class A(Aggregate, Snapshottable):

                def __init__(self, id: any):
                    super().__init__(id)
                    self.count = 0

                @validate_command(CommandA)
                def validateA(self, command: CommandA, next_version: int):
                    self._post_new_event(EventA(id=next_version, version=next_version, created_at=datetime.now()))

                @reconstitute_aggregate_state(EventA)
                def from_event_a(self, event: EventA):
                    self.count += 1

                def apply_snapshot_hook(self, snapshot):
                    self.count = snapshot

                def get_snapshot(self) -> any:
                    return self.count

                def get_snapshot_frequency(self) -> int:
                    return 2
                
        @RegisterEventDispatcher
        def event_dispatcher(event, handled_callback):
            pass

        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 0)
        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 1)
        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 1)
        self.assertEqual(event_repository_instance()._last_current_version, 2)
        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 1)
        handle_command(CommandA())
        self.assertEqual(event_repository_instance()._last_current_version, 4)

    @patch("pyjangle.snapshot.snapshot_repository.__registered_snapshot_repository", None)  
    @patch("pyjangle.event.event_dispatcher.__event_dispatcher", None)
    @patch("pyjangle.event.event_repository._event_repository_instance", None)
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_bad_snapshots_deleted(self):
        @RegisterEventRepository
        class EventRepo(EventRepository):

            def __init__(self) -> None:
                super().__init__()
                self._events = dict()#aggregate_id[dict[version, event_data]]
                self._last_current_version = 0

            def get_events(self, aggregate_id: any, current_version = 0) -> List[Event]:
                self._last_current_version = current_version
                events_to_return = self._events[aggregate_id] if aggregate_id in self._events else dict()
                return [v for k,v in events_to_return.items() if k > current_version]

            def commit_events(self, aggregate_id: any, events: List[Event]):
                if not aggregate_id in self._events:
                    self._events[aggregate_id] = dict()
                incoming_versions = [x.version for x in events]
                existing_versions = [y.version for x,y in self._events[aggregate_id].items()]
                if set(incoming_versions).intersection(existing_versions):
                    raise DuplicateKeyError()
                for x in events:
                    self._events[aggregate_id][x.version] = x

            def mark_event_handled(self, event: Event):
                pass

            def get_failed_events(self, batch_size: int) -> List[Event]:
                pass

        @RegisterSnapshotRepository
        class SnapRepo(SnapshotRepository):
            def __init__(self) -> None:
                super().__init__()
                self._snapshots = dict()

            def get_snapshot(self, aggregate_id: str) -> tuple[int, any]:
                value = self._snapshots[aggregate_id] if aggregate_id in self._snapshots else None
                return value
            
            def store_snapshot(self, aggregate_id: any, version: int, snapshot: any):
                self._snapshots[aggregate_id] = (version, snapshot)

            def delete_snapshot(self, aggregate_id: str):
                del self._snapshots[aggregate_id]

            
        @RegisterCommand(CommandA)
        class A(Aggregate, Snapshottable):

                def __init__(self, id: any):

                    super().__init__(id)
                    self.count = 0

                @validate_command(CommandA)
                def validateA(self, command: CommandA, next_version: int):
                    self._post_new_event(EventA(id=next_version, version=next_version, created_at=datetime.now()))

                @reconstitute_aggregate_state(EventA)
                def from_event_a(self, event: EventA):
                    self.count += 1

                def apply_snapshot_hook(self, snapshot):
                    if snapshot == 4:
                        raise Exception
                    self.count = snapshot

                def get_snapshot(self) -> any:
                    return self.count

                def get_snapshot_frequency(self) -> int:
                    return 2
                
        @RegisterEventDispatcher
        def event_dispatcher(event, handled_callback):
            pass

        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 0)
        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 1)
        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 1)
        self.assertEqual(event_repository_instance()._last_current_version, 2)
        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 1)
        handle_command(CommandA())
        self.assertEqual(event_repository_instance()._last_current_version, 0)
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 0)
        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 1)
        handle_command(CommandA())
        self.assertEqual(event_repository_instance()._last_current_version, 6)
                
    @patch("pyjangle.snapshot.snapshot_repository.__registered_snapshot_repository", None)
    @patch("pyjangle.event.event_dispatcher.__event_dispatcher", None)
    @patch("pyjangle.event.event_repository._event_repository_instance", None)
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_retry_after_duplicate_key_exception(self):
        @RegisterEventRepository
        class EventRepo(EventRepository):

            def __init__(self) -> None:
                super().__init__()
                self._events = dict()#aggregate_id[dict[version, event_data]]
                self._last_current_version = 0

            def get_events(self, aggregate_id: any, current_version = 0) -> List[Event]:
                self._last_current_version = current_version
                events_to_return = self._events[aggregate_id] if aggregate_id in self._events else dict()
                return [v for k,v in events_to_return.items() if k > current_version]

            def commit_events(self, aggregate_id: any, events: List[Event]):
                if not hasattr(self, "foo"):
                    setattr(self, "foo", None)
                    raise DuplicateKeyError()
                if not aggregate_id in self._events:
                    self._events[aggregate_id] = dict()
                incoming_versions = [x.version for x in events]
                existing_versions = [y.version for x,y in self._events[aggregate_id].items()]
                if set(incoming_versions).intersection(existing_versions):
                    raise DuplicateKeyError()
                for x in events:
                    self._events[aggregate_id][x.version] = x

            def mark_event_handled(self, event: Event):
                pass

            def get_failed_events(self, batch_size: int) -> List[Event]:
                pass

        @RegisterSnapshotRepository
        class SnapRepo(SnapshotRepository):
            def __init__(self) -> None:
                super().__init__()
                self._snapshots = dict()

            def get_snapshot(self, aggregate_id: str) -> tuple[int, any]:
                value = self._snapshots[aggregate_id] if aggregate_id in self._snapshots else None
                return value
            
            def store_snapshot(self, aggregate_id: any, version: int, snapshot: any):
                self._snapshots[aggregate_id] = (version, snapshot)

            def delete_snapshot(self, aggregate_id: str):
                del self._snapshots[aggregate_id]

            
        @RegisterCommand(CommandA)
        class A(Aggregate, Snapshottable):

                def __init__(self, id: any):
                    super().__init__(id)
                    self.count = 0

                @validate_command(CommandA)
                def validateA(self, command: CommandA, next_version: int):
                    self._post_new_event(EventA(id=next_version, version=next_version, created_at=datetime.now()))

                @reconstitute_aggregate_state(EventA)
                def from_event_a(self, event: EventA):
                    self.count += 1

                def apply_snapshot_hook(self, snapshot):
                    self.count = snapshot

                def get_snapshot(self) -> any:
                    return self.count

                def get_snapshot_frequency(self) -> int:
                    return 2
                
        @RegisterEventDispatcher
        def event_dispatcher(event, handled_callback):
            pass

        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 0)
        handle_command(CommandA())
        self.assertEqual(len(snapshot_repository_instance()._snapshots), 1)
