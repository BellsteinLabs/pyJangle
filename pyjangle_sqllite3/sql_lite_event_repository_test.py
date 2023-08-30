from datetime import timedelta
import os
import unittest
from unittest.mock import patch
from uuid import uuid4
from pyjangle import DuplicateKeyError
from pyjangle.test.events import EventA
from pyjangle.test.registration_paths import EVENT_DESERIALIZER, EVENT_ID_FACTORY, EVENT_REPO, EVENT_SERIALIZER, SAGA_DESERIALIZER, SAGA_SERIALIZER
from pyjangle.test.serialization import deserialize_event, deserialize_saga, serialize_event, serialize_saga
from pyjangle_example.custom_json_encoder import CustomJSONDecoder, CustomJSONEncoder
from pyjangle_sqllite3.sql_lite_event_repository import SqlLiteEventRepository
from pyjangle_sqllite3.symbols import DB_EVENT_STORE_PATH


@patch(EVENT_ID_FACTORY, new=lambda: str(uuid4()))
@patch(EVENT_DESERIALIZER, new_callable=lambda: lambda x: deserialize_event(x, CustomJSONDecoder))
@patch(EVENT_SERIALIZER, new_callable=lambda: lambda x: serialize_event(x, CustomJSONEncoder))
@patch(EVENT_REPO, new_callable=lambda: SqlLiteEventRepository())
class TestSqlLiteEventRepository(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        if os.path.exists(DB_EVENT_STORE_PATH):  # pragma no cover
            os.remove(DB_EVENT_STORE_PATH)  # pragma no cover
        self.sql_lite_event_repo = SqlLiteEventRepository()

    def tearDown(self) -> None:
        if os.path.exists(DB_EVENT_STORE_PATH):  # pragma no cover
            os.remove(DB_EVENT_STORE_PATH)

    async def test_committed_events_can_be_retrieved(self, *_):
        tuple_41 = (41, EventA(version=1))
        tuple_42 = (42, EventA(version=3))
        aggregate_id_and_event_tuples = [tuple_41, tuple_42]
        await self.sql_lite_event_repo.commit_events(aggregate_id_and_event_tuples)
        retrieved_events_41 = list(await self.sql_lite_event_repo.get_events(41))
        self.assertEqual(1, len(retrieved_events_41))
        self.assertDictEqual(
            tuple_41[1].__dict__, retrieved_events_41[0].__dict__)
        retrieved_events_42 = list(await self.sql_lite_event_repo.get_events(42))
        self.assertEqual(1, len(retrieved_events_42))
        self.assertDictEqual(
            tuple_42[1].__dict__, retrieved_events_42[0].__dict__)

    async def test_when_event_aggregate_id_and_version_exists_duplicate_key_error(self, *_):
        with self.assertRaises(DuplicateKeyError):
            events = [EventA(version=1)]
            await self.sql_lite_event_repo.commit_events([(42, event) for event in events])
            await self.sql_lite_event_repo.commit_events([(42, event) for event in events])

    async def test_when_events_not_marked_handled_retrieved_with_get_unhandled_events(self, *_):
        events = [EventA(version=1), EventA(version=2),
                  EventA(version=3), EventA(version=4)]
        await self.sql_lite_event_repo.commit_events([(42, event) for event in events])
        unhandled_events = [result async for result in self.sql_lite_event_repo.get_unhandled_events(100, time_delta=timedelta(seconds=0))]
        self.assertListEqual(sorted(events, key=lambda x: x.version), sorted(
            list(unhandled_events), key=lambda x: x.version))

    async def test_when_events_marked_handled_not_retreived_with_get_unhandled_events(self, *_):
        events = [EventA(version=1), EventA(version=2),
                  EventA(version=3), EventA(version=4)]
        await self.sql_lite_event_repo.commit_events([(42, event) for event in events])
        [await self.sql_lite_event_repo.mark_event_handled(event.id) for event in events]
        unhandled_events = [result async for result in self.sql_lite_event_repo.get_unhandled_events(100, timedelta(seconds=0))]
        self.assertFalse([_ for _ in unhandled_events])
