from datetime import datetime
from typing import List
import unittest
from unittest.mock import patch
from pyjangle.event.event import Event
from pyjangle.saga.saga import Saga, reconstitute_saga_state
from pyjangle.saga.saga_handler import handle_saga_event
from pyjangle.saga.saga_metadata import SagaMetadata

from pyjangle.saga.saga_repository import RegisterSagaRepository, SagaRepository, saga_repository_instance
from pyjangle.test.test_types import EventA

class TestSagaHandler(unittest.TestCase):
    @patch("pyjangle.saga.saga_repository.__registered_saga_repository", None)
    def test_handle_short_circuited_on_completed_saga(self):

        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass
                
            def evaluate_hook(self):
                self.foo = True

        @RegisterSagaRepository
        class Repo(SagaRepository):
            def __init__(self) -> None:
                super().__init__()
                self.test = (SagaMetadata(id=1,type=A, retry_at=None, timeout_at=None, is_complete=True), A(saga_id=1, events=[], is_complete=True))

            def get_saga(self, saga_id: any) -> tuple[SagaMetadata, Saga]:
                return self.test

            def commit_saga(self, metadata: SagaMetadata, events: list[Event]):
                pass

            def get_retry_saga_metadata(max_count: int) -> list[SagaMetadata]:
                pass

        handle_saga_event(1, None, A)

        self.assertFalse(hasattr(saga_repository_instance().get_saga(1)[1], "foo"))

    @patch("pyjangle.saga.saga_repository.__registered_saga_repository", None)
    def test_incomplete_saga_not_short_circuited(self):

        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass
                
            def evaluate_hook(self):
                self._post_new_event(EventA(id=1, version=1, created_at=datetime.now()))
                self.set_complete()

        @RegisterSagaRepository
        class Repo(SagaRepository):
            def __init__(self) -> None:
                super().__init__()
                self.test = (SagaMetadata(id=1,type=A, retry_at=None, timeout_at=None, is_complete=False), [])

            def get_saga(self, saga_id: any) -> tuple[SagaMetadata, Saga]:
                return self.test

            def commit_saga(self, metadata: SagaMetadata, events: list[Event]):
                if metadata.is_complete and len(events) == 1:
                    self.foo = True

            def get_retry_saga_metadata(max_count: int) -> list[SagaMetadata]:
                pass

        handle_saga_event(1, [EventA(id=1, version=1, created_at=datetime.now())], A)

        self.assertTrue(hasattr(saga_repository_instance(), "foo"))

    @patch("pyjangle.saga.saga_repository.__registered_saga_repository", None)
    def test_handle_saga_event_with_empty_event(self):

        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass
                
            def evaluate_hook(self):
                self.set_complete()

        @RegisterSagaRepository
        class Repo(SagaRepository):
            def __init__(self) -> None:
                super().__init__()
                self.test = (SagaMetadata(id=1,type=A, retry_at=None, timeout_at=None, is_complete=False), [])

            def get_saga(self, saga_id: any) -> tuple[SagaMetadata, Saga]:
                return self.test

            def commit_saga(self, metadata: SagaMetadata, events: list[Event]):
                if metadata.is_complete and len(events) == 0:
                    self.foo = True

            def get_retry_saga_metadata(max_count: int) -> list[SagaMetadata]:
                pass

        handle_saga_event(1, [], A)

        self.assertTrue(hasattr(saga_repository_instance(), "foo"))

    @patch("pyjangle.saga.saga_repository.__registered_saga_repository", None)
    def test_event_is_evaluated_if_present(self):
        pass

    @patch("pyjangle.saga.saga_repository.__registered_saga_repository", None)
    def test_saga_evaluated_if_no_event(self):
        pass