from typing import List
import unittest
from unittest.mock import patch
from pyjangle.saga.saga_handler import SagaHandlerError, handle_saga_event
from pyjangle.saga.saga_metadata import SagaMetadata

from pyjangle.saga.saga_repository import saga_repository_instance
from pyjangle.test.events import EventThatCausesDuplicateKeyError, EventThatCausesSagaToRetry, EventThatCompletesSaga, EventThatContinuesSaga, EventThatSetsSagaToTimedOut, EventThatTimesOutSaga
from pyjangle.test.sagas import SagaForTesting
from pyjangle.test.transient_saga_repository import TransientSagaRepository

SAGA_ID = 42

@patch("pyjangle.saga.saga_repository.__registered_saga_repository", new_callable=lambda : TransientSagaRepository())
class TestSagaHandler(unittest.IsolatedAsyncioTestCase):
    async def test_when_event_handled_saga_created(self, *_):
        await handle_saga_event(SAGA_ID, EventThatCompletesSaga(version=1), SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        metadata = result[0]
        events = result[1]
        self.assertEqual(metadata.id, SAGA_ID)
        self.assertEqual(len(events), 1)

    

    async def test_when_saga_reconstituted_without_new_event_nothing_happens(self, *_):
        await handle_saga_event(SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        metadata = result[0]
        events = result[1]
        self.assertEqual(metadata.id, SAGA_ID)
        self.assertEqual(len(events), 2)
        await handle_saga_event(SAGA_ID, None, SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        metadata = result[0]
        events = result[1]
        self.assertEqual(metadata.id, SAGA_ID)
        self.assertEqual(len(events), 2)

    async def test_when_saga_completed_then_nothing_happens(self, *_):
        await handle_saga_event(SAGA_ID, EventThatCompletesSaga(version=1), SagaForTesting)
        await handle_saga_event(SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        metadata = result[0]
        events = result[1]
        self.assertEqual(metadata.id, SAGA_ID)
        self.assertEqual(len(events), 1)

    async def test_when_saga_set_to_retry_then_saga_returned_when_query_retryable_sagas(self, *_):
        await handle_saga_event(SAGA_ID, EventThatCausesSagaToRetry(version=1), SagaForTesting)
        retry_metadata = await saga_repository_instance().get_retry_saga_metadata(100)
        self.assertEqual(len(retry_metadata), 1)
        self.assertEqual(retry_metadata[0].id, SAGA_ID)

    async def test_when_saga_not_set_to_retry_then_saga__not_returned_when_query_retryable_sagas(self, *_):
        await handle_saga_event(SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting)
        retry_metadata = await saga_repository_instance().get_retry_saga_metadata(100)
        self.assertEqual(len(retry_metadata), 0)

    async def test_when_saga_timed_out_then_nothing_happens(self, *_):
        await handle_saga_event(SAGA_ID, EventThatTimesOutSaga(version=1), SagaForTesting)
        await handle_saga_event(SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        metadata = result[0]
        events = result[1]
        self.assertEqual(metadata.id, SAGA_ID)
        self.assertEqual(len(events), 1)

    async def test_when_saga_set_to_timed_out_then_nothing_happens(self, *_):
        await handle_saga_event(SAGA_ID, EventThatSetsSagaToTimedOut(version=1), SagaForTesting)
        await handle_saga_event(SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        metadata = result[0]
        events = result[1]
        self.assertEqual(metadata.id, SAGA_ID)
        self.assertEqual(len(events), 1)

    async def test_when_reconstituted_with_duplicate_external_event_nothing_happens(self, *_):
        event = EventThatContinuesSaga(version=1)
        await handle_saga_event(SAGA_ID, event, SagaForTesting)
        await handle_saga_event(SAGA_ID, event, SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        metadata = result[0]
        events = result[1]
        self.assertEqual(metadata.id, SAGA_ID)
        self.assertEqual(len(events), 2)

    async def test_when_saga_has_concurrent_duplicate_instance_nothing_happens(self, *_):
        await handle_saga_event(SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        events = result[1]
        self.assertEqual(len(events), 2)
        await handle_saga_event(SAGA_ID, EventThatCausesDuplicateKeyError(version=1), SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        metadata = result[0]
        events = result[1]
        self.assertEqual(metadata.id, SAGA_ID)
        self.assertEqual(len(events), 2)

    async def test_when_saga_reconstituted_with_new_event_saga_updated(self, *_):
        await handle_saga_event(SAGA_ID, EventThatContinuesSaga(version=1), SagaForTesting)
        await handle_saga_event(SAGA_ID, EventThatCompletesSaga(version=1), SagaForTesting)
        result = await saga_repository_instance().get_saga(SAGA_ID)
        metadata = result[0]
        events = result[1]
        self.assertEqual(metadata.id, SAGA_ID)
        self.assertTrue(metadata.is_complete)
        self.assertEqual(len(events), 3)

    async def test_saga_error_if_no_events_and_saga_not_exist(self, *_):
        with self.assertRaises(SagaHandlerError):
            await handle_saga_event(1, [], SagaForTesting)