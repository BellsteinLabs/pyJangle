from datetime import timedelta
import os
import unittest
from unittest.mock import patch
from pyjangle import DuplicateKeyError
from pyjangle.test.events import EventA
from pyjangle.test.registration_paths import EVENT_DESERIALIZER, EVENT_REPO, EVENT_SERIALIZER, SAGA_DESERIALIZER, SAGA_SERIALIZER
from pyjangle.test.serialization import deserialize_event, deserialize_saga, serialize_event, serialize_saga
from pyjangle_sqllite3.sql_lite_event_repository import SqlLiteEventRepository
from pyjangle_sqllite3.symbols import DB_EVENT_STORE_PATH


@patch(EVENT_DESERIALIZER, new_callable=lambda: deserialize_event)
@patch(EVENT_SERIALIZER, new_callable=lambda: serialize_event)
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
        events = [EventA(version=1), EventA(version=2),
                  EventA(version=3), EventA(version=4)]
        await self.sql_lite_event_repo.commit_events(42, events)
        retrieved_events = await self.sql_lite_event_repo.get_events(42)
        self.assertListEqual(events, sorted(
            retrieved_events, key=lambda event: event.version))

    async def test_when_event_aggregate_id_and_version_exists_duplicate_key_error(self, *_):
        with self.assertRaises(DuplicateKeyError):
            events = [EventA(version=1)]
            await self.sql_lite_event_repo.commit_events(42, events)
            await self.sql_lite_event_repo.commit_events(42, events)

    async def test_when_events_not_marked_handled_retrieved_with_get_unhandled_events(self, *_):
        events = [EventA(version=1), EventA(version=2),
                  EventA(version=3), EventA(version=4)]
        await self.sql_lite_event_repo.commit_events(42, events)
        unhandled_events = await self.sql_lite_event_repo.get_unhandled_events(100, time_since_published=timedelta(seconds=0))
        self.assertListEqual(sorted(events, key=lambda x: x.version), sorted(
            unhandled_events, key=lambda x: x.version))

    async def test_when_events_marked_handled_not_retreived_with_get_unhandled_events(self, *_):
        events = [EventA(version=1), EventA(version=2),
                  EventA(version=3), EventA(version=4)]
        await self.sql_lite_event_repo.commit_events(42, events)
        [await self.sql_lite_event_repo.mark_event_handled(event.id) for event in events]
        unhandled_events = await self.sql_lite_event_repo.get_unhandled_events(100, timedelta(seconds=0))
        self.assertFalse([_ for _ in unhandled_events])
