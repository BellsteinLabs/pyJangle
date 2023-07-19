from datetime import datetime
from typing import List
import unittest
from pyjangle.event.event import Event

from pyjangle.saga.saga import Saga, SagaError, event_receiver, reconstitute_saga_state
from pyjangle.test.events import EventThatContinuesSaga, EventThatCompletesSaga
from pyjangle.test.sagas import SagaForTesting

class TestSaga(unittest.TestCase):
    def test_event_receiver(self):
        foo: Saga = SagaForTesting(saga_id=1, events=[])
        foo.evaluate(EventThatContinuesSaga(id=42, version=1))
        self.assertEqual(foo.calls["on_event_that_continues_saga"], 1)

    def test_exception_when_event_receiver_on_wrong_method_signature(self):
        with self.assertRaises(SagaError):
            class A(Saga):

                def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                    super().__init__(saga_id, events, retry_at, timeout_at, is_complete) #pragma no cover

                @event_receiver(EventThatContinuesSaga)
                def from_event_that_continues_saga(self, event, next_version: int):
                    pass
                    
                def evaluate_hook(self):#pragma no cover
                    pass

    def test_exception_when_missing_event_receiver(self):
        with self.assertRaises(SagaError):
            class A(Saga):

                def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                    super().__init__(saga_id, events, retry_at, timeout_at, is_complete)
                    
                @reconstitute_saga_state(EventThatContinuesSaga)
                def from_event_that_continues_saga(self, event: EventThatContinuesSaga):
                    pass

                def evaluate_hook(self):
                    pass

            A(saga_id=1, events=[]).evaluate(EventThatContinuesSaga(id=42, version=1))

    def test_register_state_reconstitutor(self):
        foo: Saga = SagaForTesting(saga_id=1, events=[EventThatContinuesSaga(id=1, version=1)])
        foo.evaluate()
        self.assertEqual(foo.calls["from_event_that_continues_saga"], 1)

    def test_exception_when_register_state_reconstitutor_on_wrong_method_signature(self):
        with self.assertRaises(SagaError):
            class A(Saga):

                def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                    super().__init__(saga_id, events, retry_at, timeout_at, is_complete) #pragma no cover

                @reconstitute_saga_state(EventThatContinuesSaga)
                def from_event_that_continues_saga(self):
                    self._foo = True #pragma no cover
                    
                @event_receiver(EventThatContinuesSaga) #pragma no cover
                def on_event_that_continues_saga(self, next_version: int): #pragma no cover
                    pass
                    
                def evaluate_hook(self): #pragma no cover
                    pass

    def test_exception_when_missing_state_reconstitutor(self):
        with self.assertRaises(SagaError):
            class A(Saga):

                def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                    super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

                @event_receiver(EventThatContinuesSaga)
                def on_event_that_continues_saga(self, next_version: int):
                    pass

            a = A(saga_id=1, events=[EventThatContinuesSaga(id=1, version=1, created_at=datetime.now())])

    def test_when_add_event_type_to_flags_is_false_on_reconstitue_saga_state_then_flag_not_set(self, *_):
        a = SagaForTesting(saga_id=1, events=[EventThatContinuesSaga(version=41), EventThatCompletesSaga(version=42)])
        self.assertTrue(EventThatContinuesSaga in a.flags)
        self.assertFalse(EventThatCompletesSaga in a.flags)

    def test_post_new_events(self):
        a = SagaForTesting(saga_id=1, events=[])
        a.evaluate(EventThatContinuesSaga(id=1, version=1))
        self.assertEqual(len(a.new_events), 2)

    def test_when_set_retry_set_to_same_value_then_saga_not_dirty(self, *_):
        a = SagaForTesting(saga_id=1, events=[])
        a.set_retry(a.retry_at)
        self.assertFalse(a.is_dirty)

    def test_evaluate_short_circuits_on_timeout(self):
        a = SagaForTesting(saga_id=1, events=[], timeout_at=datetime.min)
        a.evaluate(EventThatContinuesSaga(id=1, version=1, created_at=datetime.now()))
        self.assertEqual(len(a.new_events), 0)

    def test_init(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventThatContinuesSaga)
            def from_event_that_continues_saga(self, event):
                pass

            @event_receiver(EventThatContinuesSaga)
            def on_event_that_continues_saga(self):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventThatContinuesSaga(id=1, version=1, created_at=datetime.now())])
        self.assertEqual(None, a.retry_at)
        self.assertEqual(None, a.timeout_at)
        self.assertFalse(a.is_complete)
        self.assertFalse(a.is_timed_out)

    def test_set_retry(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventThatContinuesSaga)
            def from_event_that_continues_saga(self, event: EventThatContinuesSaga):
                pass

            @event_receiver(EventThatContinuesSaga)
            def on_event_that_continues_saga(self):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventThatContinuesSaga(id=1, version=1, created_at=datetime.now())])
        a.set_retry(datetime.min)
        self.assertEqual(datetime.min, a.retry_at)

    def test_set_complete(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventThatContinuesSaga)
            def from_event_that_continues_saga(self, event: EventThatContinuesSaga):
                pass

            @event_receiver(EventThatContinuesSaga)
            def on_event_that_continues_saga(self):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventThatContinuesSaga(id=1, version=1, created_at=datetime.now())])
        a.set_complete()
        self.assertTrue(a.is_complete)

    def test_set_timeout(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventThatContinuesSaga)
            def from_event_that_continues_saga(self, event: EventThatContinuesSaga):
                pass

            @event_receiver(EventThatContinuesSaga)
            def on_event_that_continues_saga(self):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventThatContinuesSaga(id=42, version = 1)])
        a.set_timeout(datetime.min)
        self.assertEqual(datetime.min, a.timeout_at)

    def test_set_timed_out(self):
        class A(Saga):

            def __init__(self, saga_id: any, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
                super().__init__(saga_id, events, retry_at, timeout_at, is_complete)

            @reconstitute_saga_state(EventThatContinuesSaga)
            def from_event_that_continues_saga(self, event: EventThatContinuesSaga):
                pass

            @event_receiver(EventThatContinuesSaga)
            def on_event_that_continues_saga(self):
                pass
                
            def evaluate_hook(self):
                pass

        a = A(saga_id=1, events=[EventThatContinuesSaga(id=1, version=1, created_at=datetime.now())])
        a.set_timed_out()
        self.assertTrue(a.is_timed_out)


