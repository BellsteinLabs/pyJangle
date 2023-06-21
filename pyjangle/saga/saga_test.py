from datetime import datetime
from typing import List
import unittest
from pyjangle.event.event import Event

from pyjangle.saga.saga import Saga, SagaError, reconstitute_saga_state
from pyjangle.test.test_types import EventA, EventB

class TestSaga(unittest.TestCase):
    def test_register_state_reconstitutor(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                self._foo = True
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventA(id=1, version=1, created_at=datetime.now())])
        self.assertTrue(a._foo)

    def test_exception_when_register_state_reconstitutor_on_wrong_method_signature(self):
        with self.assertRaises(SagaError):
            class A(Saga):

                def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                    super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

                @reconstitute_saga_state(EventA)
                def from_event_a(self):
                    self._foo = True
                    
                def evaluate_hook(self):
                    pass

    def test_exception_when_missing_state_reconstitutor(self):
        with self.assertRaises(SagaError):
            class A(Saga):

                def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                    super().__init__(saga_id, events, retry_at, timeout_at, is_complete)
                    
                def evaluate_hook(self):
                    pass

            a = A(saga_id=1, events=[EventA(id=1, version=1, created_at=datetime.now())])

    def test_post_new_events(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            def evaluate_hook(self):
                self._post_new_event(EventB(id=1, version=1, created_at=datetime.now()))

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass

        a = A(saga_id=1, events=[EventA(id=1, version=1, created_at=datetime.now())])
        a.evaluate()
        self.assertEqual(len(a.new_events), 1)

    def test_evaluate_short_circuits_on_timeout(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            def evaluate_hook(self):
                self._post_new_event(EventB(id=1, version=1, created_at=datetime.now()))

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass

        a = A(saga_id=1, events=[EventA(id=1, version=1, created_at=datetime.now())], timeout_at=datetime.min)
        a.evaluate()
        self.assertEqual(len(a.new_events), 0)

    def test_init(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventA(id=1, version=1, created_at=datetime.now())])
        self.assertEqual(None, a.retry_at)
        self.assertEqual(None, a.timeout_at)
        self.assertFalse(a.is_complete)
        self.assertFalse(a.is_timed_out)

    def test_set_retry(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventA(id=1, version=1, created_at=datetime.now())])
        a.set_retry(datetime.min)
        self.assertEqual(datetime.min, a.retry_at)

    def test_set_complete(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventA(id=1, version=1, created_at=datetime.now())])
        a.set_complete()
        self.assertTrue(a.is_complete)

    def test_set_timeout(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventA(id=1, version=1, created_at=datetime.now())])
        a.set_timeout(datetime.min)
        self.assertEqual(datetime.min, a.timeout_at)

    def test_set_timed_out(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventA)
            def from_event_a(self, event: EventA):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventA(id=1, version=1, created_at=datetime.now())])
        a.set_timed_out()
        self.assertTrue(a.is_timed_out)


