import dataclasses
from datetime import timedelta
import json
import os
import unittest
from unittest.mock import patch
from pyjangle.event.event import Event
from pyjangle.event.event_repository import DuplicateKeyError
from pyjangle.event.register import get_event_name, get_event_type
from pyjangle.serialization.register import register_event_deserializer, register_event_serializer
from pyjangle.test.events import EventA
from pyjangle.test.registration_paths import EVENT_REPO

from pyjangle_sqllite3.sql_lite_event_repository import AGGREGATE_ID, AGGREGATE_VERSION, CREATED_AT, DATA, DB_PATH, EVENT_ID, TYPE, SqlLiteEventRepository

@register_event_serializer
def serialize(event: Event) -> any:
    as_dict = dataclasses.asdict(event)
    event_id = as_dict.pop("id")    
    aggregate_version = as_dict.pop("version")
    created_at = as_dict.pop("created_at")
    data = json.dumps(as_dict)
    return {
        EVENT_ID: event_id,
        AGGREGATE_VERSION: aggregate_version,
        DATA: data,
        CREATED_AT: created_at,
        TYPE: get_event_name(type(event))
    }

@register_event_deserializer
def deserialize(fields: dict) -> Event:
    event_id = fields[EVENT_ID]
    aggregave_version = fields[AGGREGATE_VERSION]
    data = fields[DATA]
    created_at = fields[CREATED_AT]
    event_type = fields[TYPE]
    event_class_instance: type[Event] = get_event_type(event_type)
    data_dict = json.loads(data)
    data_dict["id"] = event_id
    data_dict["created_at"] = created_at
    data_dict["version"] = aggregave_version
    return event_class_instance.deserialize(data_dict)

@patch(EVENT_REPO, new_callable=lambda : SqlLiteEventRepository())
class TestSqlLiteEventRepository(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self) -> None:
        if os.path.exists(DB_PATH):#pragma no cover
            os.remove(DB_PATH) #pragma no cover
        self.sql_lite_event_repo = SqlLiteEventRepository()
    
    def tearDown(self) -> None:
        if os.path.exists(DB_PATH):#pragma no cover
            os.remove(DB_PATH)

    async def test_committed_events_can_be_retrieved(self, *_):
        events = [EventA(version=1), EventA(version=2), EventA(version=3), EventA(version=4)]
        await self.sql_lite_event_repo.commit_events(42, events)
        retrieved_events = await self.sql_lite_event_repo.get_events(42)
        self.assertListEqual(events, sorted(retrieved_events, key=lambda event : event.version))

    async def test_when_event_aggregate_id_and_version_exists_duplicate_key_error(self, *_):
        with self.assertRaises(DuplicateKeyError):
            events = [EventA(version=1)]
            await self.sql_lite_event_repo.commit_events(42, events)
            await self.sql_lite_event_repo.commit_events(42, events)

    async def test_when_events_not_marked_handled_retrieved_with_get_unhandled_events(self, *_):
        events = [EventA(version=1), EventA(version=2), EventA(version=3), EventA(version=4)]
        await self.sql_lite_event_repo.commit_events(42, events)
        unhandled_events = await self.sql_lite_event_repo.get_unhandled_events(100, time_since_published=timedelta(seconds=0))
        self.assertListEqual(sorted(events, key=lambda x : x.version), sorted(unhandled_events, key=lambda x : x.version))

    async def test_when_events_marked_handled_not_retreived_with_get_unhandled_events(self, *_):
        events = [EventA(version=1), EventA(version=2), EventA(version=3), EventA(version=4)]
        await self.sql_lite_event_repo.commit_events(42, events)
        [await self.sql_lite_event_repo.mark_event_handled(event.id) for event in events]
        unhandled_events = await self.sql_lite_event_repo.get_unhandled_events(100, timedelta(seconds=0))
        self.assertFalse([_ for _ in unhandled_events])

