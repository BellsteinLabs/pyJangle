from datetime import datetime
import os
import unittest
from unittest.mock import patch
from uuid import uuid4
from pyjangle import DuplicateKeyError
from pyjangle.saga.saga import Saga
from pyjangle.test.events import (
    EventThatCausesDuplicateKeyError,
    EventThatContinuesSaga,
    TestSagaEvent,
)
from pyjangle.test.registration_paths import (
    EVENT_DESERIALIZER,
    EVENT_DISPATCHER,
    EVENT_ID_FACTORY,
    EVENT_SERIALIZER,
    SAGA_DESERIALIZER,
    SAGA_SERIALIZER,
)
from pyjangle.test.sagas import SagaForTesting
from pyjangle.test.serialization import (
    deserialize_event,
    deserialize_saga,
    serialize_event,
    serialize_saga,
)
from pyjangle_example.custom_json_encoder import CustomJSONEncoder
from pyjangle_sqllite3.sql_lite_saga_repository import SqlLiteSagaRepository
from pyjangle_sqllite3.symbols import DB_SAGA_STORE_PATH

SAGA_ID = "42"


@patch(EVENT_ID_FACTORY, new=lambda: str(uuid4()))
@patch(SAGA_DESERIALIZER, new_callable=lambda: deserialize_saga)
@patch(
    SAGA_SERIALIZER, new_callable=lambda: lambda x: serialize_saga(x, CustomJSONEncoder)
)
class TestSqlLiteSagaRepository(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        if os.path.exists(DB_SAGA_STORE_PATH):  # pragma no cover
            os.remove(DB_SAGA_STORE_PATH)  # pragma no cover
        self.sql_lite_saga_repo = SqlLiteSagaRepository()

    def tearDown(self) -> None:
        if os.path.exists(DB_SAGA_STORE_PATH):  # pragma no cover
            os.remove(DB_SAGA_STORE_PATH)

    async def test_when_saga_committed_then_can_be_retrieved(self, *_):
        saga = SagaForTesting(
            saga_id=SAGA_ID,
            retry_at=datetime.min,
            timeout_at=datetime.min,
            is_complete=True,
        )
        await self.sql_lite_saga_repo.commit_saga(saga)
        retrieved_saga = await self.sql_lite_saga_repo.get_saga(SAGA_ID)
        self.assertSetEqual(saga.flags, retrieved_saga.flags)
        self.assertEqual(saga.saga_id, retrieved_saga.saga_id)
        self.assertEqual(saga.timeout_at, retrieved_saga.timeout_at)
        self.assertEqual(saga.is_timed_out, retrieved_saga.is_timed_out)
        self.assertEqual(saga.retry_at, retrieved_saga.retry_at)
        self.assertEqual(saga.is_complete, retrieved_saga.is_complete)

    async def test_when_event_id_exists_then_duplicate_key_error(self, *_):
        saga = SagaForTesting(saga_id=SAGA_ID)
        await saga.evaluate(EventThatContinuesSaga(id="42", version=1))
        await self.sql_lite_saga_repo.commit_saga(saga)
        retrieved_saga = await self.sql_lite_saga_repo.get_saga(SAGA_ID)
        await retrieved_saga.evaluate(EventThatCausesDuplicateKeyError(version=1))
        with self.assertRaises(DuplicateKeyError):
            await self.sql_lite_saga_repo.commit_saga(retrieved_saga)

    async def test_when_saga_not_found_then_return_none(self, *_):
        self.assertIsNone(await self.sql_lite_saga_repo.get_saga(SAGA_ID))

    async def test_when_saga_needs_retry_then_is_returned_from_get_retry_saga_metadata_method(
        self, *_
    ):
        saga_needs_retry = SagaForTesting(
            saga_id=SAGA_ID, retry_at=datetime.min, timeout_at=None, is_complete=False
        )
        saga_completed = SagaForTesting(
            saga_id=SAGA_ID + "1",
            retry_at=datetime.min,
            timeout_at=None,
            is_complete=True,
        )
        saga_timed_out = SagaForTesting(
            saga_id=SAGA_ID + "2",
            retry_at=datetime.min,
            timeout_at=None,
            is_complete=False,
            is_timed_out=True,
        )
        saga_pre_timed_out = SagaForTesting(
            saga_id=SAGA_ID + "3",
            retry_at=datetime.min,
            timeout_at=datetime.min,
            is_complete=False,
        )
        await self.sql_lite_saga_repo.commit_saga(saga_needs_retry)
        await self.sql_lite_saga_repo.commit_saga(saga_completed)
        await self.sql_lite_saga_repo.commit_saga(saga_timed_out)
        await self.sql_lite_saga_repo.commit_saga(saga_pre_timed_out)
        saga_id_list = list(
            await self.sql_lite_saga_repo.get_retry_saga_ids(batch_size=100)
        )
        self.assertEqual(len(saga_id_list), 1)
        self.assertEqual(saga_id_list[0], SAGA_ID)
