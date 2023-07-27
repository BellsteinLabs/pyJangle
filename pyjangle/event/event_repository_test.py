import unittest
from typing import List
from unittest.mock import patch

from pyjangle import (VersionedEvent, EventRepositoryError, RegisterEventRepository,
                      event_repository_instance)
from pyjangle.test.registration_paths import EVENT_REPO


@patch(EVENT_REPO, None)
class TestEventRepository(unittest.TestCase):

    def test_can_register_event_repository(self):
        @RegisterEventRepository
        class A:
            def get_events(self, aggregate_id: any,
                           current_version=0) -> List[VersionedEvent]: pass

            def commit_events(self, events: List[VersionedEvent]): pass

            def mark_event_handled(self, event: VersionedEvent): pass

            def get_failed_events(self, batch_size: int): pass

        self.assertIsNotNone(event_repository_instance())

    def test_exception_when_none_registered(self):
        with self.assertRaises(EventRepositoryError):
            event_repository_instance()

    def test_exception_when_multiple_registered(self):
        with self.assertRaises(EventRepositoryError):
            @RegisterEventRepository
            class A:
                pass

            @RegisterEventRepository
            class B:
                pass
