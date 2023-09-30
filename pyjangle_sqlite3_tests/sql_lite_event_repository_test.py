from datetime import timedelta
import os
import unittest
from unittest.mock import patch
from uuid import uuid4
from json import dumps, loads
from dataclasses import asdict
from pyjangle import DuplicateKeyError
from test_helpers import (
    EventA,
    DESERIALIZER,
    EVENT_ID_FACTORY,
    EVENT_REPO,
    SERIALIZER,
    CustomJSONDecoder,
    CustomJSONEncoder,
)

from pyjangle_sqlite3 import (
    SqliteEventRepository,
    get_event_store_path,
    initialize_pyjangle_sqlite3,
)

initialize_pyjangle_sqlite3()


@patch(EVENT_ID_FACTORY, new=lambda: str(uuid4()))
@patch(
    DESERIALIZER,
    new_callable=lambda: lambda x: loads(x, cls=CustomJSONDecoder),
)
@patch(
    SERIALIZER,
    new_callable=lambda: lambda e: dumps(asdict(e), cls=CustomJSONEncoder),
)
@patch(EVENT_REPO, new_callable=lambda: SqliteEventRepository())
class TestSqliteEventRepository(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        if os.path.exists(get_event_store_path()):  # pragma no cover
            os.remove(get_event_store_path())  # pragma no cover
        self.sql_lite_event_repo = SqliteEventRepository()

    def tearDown(self) -> None:
        if os.path.exists(get_event_store_path()):  # pragma no cover
            os.remove(get_event_store_path())

    async def test_committed_events_can_be_retrieved(self, *_):
        tuple_41 = (41, EventA(version=1))
        tuple_42 = (42, EventA(version=3))
        aggregate_id_and_event_tuples = [tuple_41, tuple_42]
        await self.sql_lite_event_repo.commit_events(aggregate_id_and_event_tuples)
        retrieved_events_41 = list(await self.sql_lite_event_repo.get_events(41))
        self.assertEqual(1, len(retrieved_events_41))
        self.assertDictEqual(vars(tuple_41[1]), vars(retrieved_events_41[0]))
        retrieved_events_42 = list(await self.sql_lite_event_repo.get_events(42))
        self.assertEqual(1, len(retrieved_events_42))
        self.assertDictEqual(vars(tuple_42[1]), vars(retrieved_events_42[0]))

    async def test_when_event_aggregate_id_and_version_exists_duplicate_key_error(
        self, *_
    ):
        with self.assertRaises(DuplicateKeyError):
            events = [EventA(version=1)]
            await self.sql_lite_event_repo.commit_events(
                [(42, event) for event in events]
            )
            await self.sql_lite_event_repo.commit_events(
                [(42, event) for event in events]
            )

    async def test_when_events_not_marked_handled_retrieved_with_get_unhandled_events(
        self, *_
    ):
        events = [
            EventA(version=1),
            EventA(version=2),
            EventA(version=3),
            EventA(version=4),
        ]
        await self.sql_lite_event_repo.commit_events([(42, event) for event in events])
        unhandled_events = [
            result
            async for result in self.sql_lite_event_repo.get_unhandled_events(
                100, time_delta=timedelta(seconds=0)
            )
        ]
        self.assertListEqual(
            sorted(events, key=lambda x: x.version),
            sorted(list(unhandled_events), key=lambda x: x.version),
        )

    async def test_when_events_marked_handled_not_retreived_with_get_unhandled_events(
        self, *_
    ):
        events = [
            EventA(version=1),
            EventA(version=2),
            EventA(version=3),
            EventA(version=4),
        ]
        await self.sql_lite_event_repo.commit_events([(42, event) for event in events])
        [
            await self.sql_lite_event_repo.mark_event_handled(event.id)
            for event in events
        ]
        unhandled_events = [
            result
            async for result in self.sql_lite_event_repo.get_unhandled_events(
                100, timedelta(seconds=0)
            )
        ]
        self.assertFalse([_ for _ in unhandled_events])
