from datetime import datetime
import os
import unittest
from unittest.mock import patch
from pyjangle.event.event import Event
from pyjangle.event.event_repository import DuplicateKeyError
from pyjangle.saga.register_saga import get_saga_name
from pyjangle.saga.saga import Saga
from pyjangle.saga.saga_metadata import SagaMetadata
from pyjangle.test.events import EventThatContinuesSaga, TestSagaEvent
from pyjangle.test.registration_paths import EVENT_DESERIALIZER, EVENT_DISPATCHER, EVENT_SERIALIZER, SAGA_DESERIALIZER, SAGA_SERIALIZER
from pyjangle.test.sagas import SagaForTesting
from pyjangle.test.serialization import deserialize_event, deserialize_saga_event, serialize_event, serialize_saga_event
from pyjangle_sqllite3.sql_lite_saga_repository import SqlLiteSagaRepository
from pyjangle_sqllite3.symbols import DB_SAGA_STORE_PATH

SAGA_ID = "42"

@patch(SAGA_DESERIALIZER, new_callable=lambda : deserialize_saga_event)
@patch(SAGA_SERIALIZER, new_callable=lambda : serialize_saga_event)
class TestSqlLiteSagaRepository(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self) -> None:
        if os.path.exists(DB_SAGA_STORE_PATH):#pragma no cover
            os.remove(DB_SAGA_STORE_PATH) #pragma no cover
        self.sql_lite_saga_repo = SqlLiteSagaRepository()
    
    def tearDown(self) -> None:
        if os.path.exists(DB_SAGA_STORE_PATH):#pragma no cover
            os.remove(DB_SAGA_STORE_PATH)

    async def test_when_saga_committed_then_can_be_retrieved(self, *_):
        metadata = SagaMetadata(id=SAGA_ID, type=get_saga_name(SagaForTesting), retry_at=datetime.min, timeout_at=datetime.min, is_complete=True, is_timed_out=True)
        await self.sql_lite_saga_repo.commit_saga(metadata, [EventThatContinuesSaga(version=1), TestSagaEvent(version=1)])
        meta_data, saga_events = await self.sql_lite_saga_repo.get_saga(SAGA_ID)
        self.assertDictEqual(meta_data.__dict__, metadata.__dict__)
        self.assertEqual(len(list(saga_events)), 2)

    async def test_when_event_saga_id_and_saga_version_exists_duplicate_key_error(self, *_):
        metadata = SagaMetadata(id=SAGA_ID, type=get_saga_name(SagaForTesting), retry_at=datetime.min, timeout_at=datetime.min, is_complete=True, is_timed_out=True)
        await self.sql_lite_saga_repo.commit_saga(metadata, [EventThatContinuesSaga(id="42", version=1)])
        meta_data, saga_events = await self.sql_lite_saga_repo.get_saga(SAGA_ID)
        with self.assertRaises(DuplicateKeyError):
            await self.sql_lite_saga_repo.commit_saga(metadata, [TestSagaEvent(id= "42", version=1)])

    async def test_when_event_id_exists_nothing_happens(self, *_):
        metadata = SagaMetadata(id=SAGA_ID, type=get_saga_name(SagaForTesting), retry_at=datetime.min, timeout_at=datetime.min, is_complete=True, is_timed_out=True)
        await self.sql_lite_saga_repo.commit_saga(metadata, [EventThatContinuesSaga(id="42", version=1)])
        meta_data, saga_events = await self.sql_lite_saga_repo.get_saga(SAGA_ID)
        saga: Saga = SagaForTesting(meta_data.id, saga_events, meta_data.retry_at, meta_data.timeout_at, meta_data.is_complete)
        saga.evaluate(EventThatContinuesSaga(id="42", version=2))
        self.assertFalse(saga.new_events)

    async def test_when_saga_needs_retry_then_is_returned_from_get_retry_saga_metadata_method(self, *_):
        metadata = SagaMetadata(id=SAGA_ID, type=get_saga_name(SagaForTesting), retry_at=datetime.min, timeout_at=None, is_complete=False, is_timed_out=False)
        await self.sql_lite_saga_repo.commit_saga(metadata, [EventThatContinuesSaga(id="42", version=1)])
        meta_data = await self.sql_lite_saga_repo.get_retry_saga_metadata(max_count=100)
        metadata_list = list(meta_data)
        self.assertEqual(len(metadata_list), 1)
        self.assertEqual(metadata.id, metadata_list[0].id)