import unittest

from pyjangle import handle_saga_event, retry_sagas, saga_repository_instance
from test_helpers.events import (
    EventThatCausesDuplicateKeyError,
    EventThatCausesSagaToRetry,
    EventThatCompletesSaga,
    EventThatContinuesSaga,
    EventThatSetsSagaToTimedOut,
    EventThatTimesOutSaga,
)
from test_helpers.reset import ResetPyJangleState
from test_helpers.sagas import SagaForTesting

SAGA_ID = 42


@ResetPyJangleState
class TestSagaHandler(unittest.IsolatedAsyncioTestCase):
    async def test_when_event_handled_saga_created(self, *_):
        await handle_saga_event(
            SAGA_ID, EventThatCompletesSaga(version=1), SagaForTesting
        )
        saga = await saga_repository_instance().get_saga(SAGA_ID)
        self.assertEqual(saga.saga_id, SAGA_ID)
        self.assertTrue(saga.is_complete)

    async def test_when_saga_reconstituted_without_new_event_then_saga_unchanged(
        self, *_
    ):
        await handle_saga_event(
            SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting
        )
        retrieved_saga = await saga_repository_instance().get_saga(SAGA_ID)
        self.assertEqual(retrieved_saga.saga_id, SAGA_ID)
        await retry_sagas(SAGA_ID)
        retrieved_saga_2 = await saga_repository_instance().get_saga(SAGA_ID)
        self.assertEqual(retrieved_saga_2.saga_id, retrieved_saga.saga_id)
        self.assertEqual(retrieved_saga_2.timeout_at, retrieved_saga.timeout_at)
        self.assertEqual(retrieved_saga_2.is_timed_out, retrieved_saga.is_timed_out)
        self.assertEqual(retrieved_saga_2.retry_at, retrieved_saga.retry_at)
        self.assertEqual(retrieved_saga_2.is_complete, retrieved_saga.is_complete)

    async def test_when_saga_completed_then_saga_unchanged(self, *_):
        await handle_saga_event(
            SAGA_ID, EventThatCompletesSaga(version=1), SagaForTesting
        )
        retrieved_saga = await saga_repository_instance().get_saga(SAGA_ID)
        await handle_saga_event(
            SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting
        )
        retrieved_saga_2 = await saga_repository_instance().get_saga(SAGA_ID)
        self.assertEqual(retrieved_saga_2.saga_id, retrieved_saga.saga_id)
        self.assertEqual(retrieved_saga_2.timeout_at, retrieved_saga.timeout_at)
        self.assertEqual(retrieved_saga_2.is_timed_out, retrieved_saga.is_timed_out)
        self.assertEqual(retrieved_saga_2.retry_at, retrieved_saga.retry_at)
        self.assertEqual(retrieved_saga_2.is_complete, retrieved_saga.is_complete)

    async def test_when_saga_set_to_retry_then_saga_returned_when_query_retryable_sagas(
        self, *_
    ):
        await handle_saga_event(
            SAGA_ID, EventThatCausesSagaToRetry(version=1), SagaForTesting
        )
        saga_id = (await saga_repository_instance().get_retry_saga_ids(100))[0]
        self.assertEqual(SAGA_ID, saga_id)

    async def test_when_saga_not_set_to_retry_then_saga_not_returned_when_query_retryable_sagas(
        self, *_
    ):
        await handle_saga_event(
            SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting
        )
        saga_ids = await saga_repository_instance().get_retry_saga_ids(100)
        self.assertEqual(len(saga_ids), 0)

    async def test_when_saga_timed_out_then_nothing_happens(self, *_):
        await handle_saga_event(
            SAGA_ID, EventThatTimesOutSaga(version=1), SagaForTesting
        )
        retrieved_saga = await saga_repository_instance().get_saga(SAGA_ID)
        await handle_saga_event(
            SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting
        )
        retrieved_saga_2 = await saga_repository_instance().get_saga(SAGA_ID)
        self.assertEqual(retrieved_saga_2.saga_id, retrieved_saga.saga_id)
        self.assertEqual(retrieved_saga_2.timeout_at, retrieved_saga.timeout_at)
        self.assertEqual(retrieved_saga_2.is_timed_out, retrieved_saga.is_timed_out)
        self.assertEqual(retrieved_saga_2.retry_at, retrieved_saga.retry_at)
        self.assertEqual(retrieved_saga_2.is_complete, retrieved_saga.is_complete)

    async def test_when_saga_set_to_timed_out_then_nothing_happens(self, *_):
        await handle_saga_event(
            SAGA_ID, EventThatSetsSagaToTimedOut(version=1), SagaForTesting
        )
        retrieved_saga = await saga_repository_instance().get_saga(SAGA_ID)
        await handle_saga_event(
            SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting
        )
        retrieved_saga_2 = await saga_repository_instance().get_saga(SAGA_ID)
        self.assertEqual(retrieved_saga_2.saga_id, retrieved_saga.saga_id)
        self.assertEqual(retrieved_saga_2.timeout_at, retrieved_saga.timeout_at)
        self.assertEqual(retrieved_saga_2.is_timed_out, retrieved_saga.is_timed_out)
        self.assertEqual(retrieved_saga_2.retry_at, retrieved_saga.retry_at)
        self.assertEqual(retrieved_saga_2.is_complete, retrieved_saga.is_complete)

    async def test_when_reconstituted_with_duplicate_event_then_saga_unchanged(
        self, *_
    ):
        event = EventThatContinuesSaga(version=1)
        await handle_saga_event(SAGA_ID, event, SagaForTesting)
        retrieved_saga = await saga_repository_instance().get_saga(SAGA_ID)
        await handle_saga_event(SAGA_ID, event, SagaForTesting)
        retrieved_saga_2 = await saga_repository_instance().get_saga(SAGA_ID)
        self.assertEqual(retrieved_saga_2.saga_id, retrieved_saga.saga_id)
        self.assertEqual(retrieved_saga_2.timeout_at, retrieved_saga.timeout_at)
        self.assertEqual(retrieved_saga_2.is_timed_out, retrieved_saga.is_timed_out)
        self.assertEqual(retrieved_saga_2.retry_at, retrieved_saga.retry_at)
        self.assertEqual(retrieved_saga_2.is_complete, retrieved_saga.is_complete)

    async def test_when_saga_creates_duplicate_event_then_saga_unchanged(self, *_):
        await handle_saga_event(
            SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting
        )
        retrieved_saga = await saga_repository_instance().get_saga(SAGA_ID)
        await handle_saga_event(
            SAGA_ID, EventThatCausesDuplicateKeyError(version=1), SagaForTesting
        )
        retrieved_saga_2 = await saga_repository_instance().get_saga(SAGA_ID)
        self.assertEqual(retrieved_saga_2.saga_id, retrieved_saga.saga_id)
        self.assertEqual(retrieved_saga_2.timeout_at, retrieved_saga.timeout_at)
        self.assertEqual(retrieved_saga_2.is_timed_out, retrieved_saga.is_timed_out)
        self.assertEqual(retrieved_saga_2.retry_at, retrieved_saga.retry_at)
        self.assertEqual(retrieved_saga_2.is_complete, retrieved_saga.is_complete)
