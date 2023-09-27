from dataclasses import asdict
from json import dumps, loads
import os
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from pyjangle.test.registration_paths import EVENT_DESERIALIZER, EVENT_SERIALIZER
from pyjangle.test.serialization import deserialize_snapshot, serialize_snapshot
from pyjangle_example.custom_json_encoder import CustomJSONDecoder, CustomJSONEncoder
from pyjangle_sqllite3.adapters import (
    register_datetime_and_decimal_adapters_and_converters,
)

register_datetime_and_decimal_adapters_and_converters()

from pyjangle_sqllite3.sql_lite_snapshot_repository import SqliteSnapshotRepository
from pyjangle_sqllite3.symbols import DB_SNAPSHOTS_PATH

SNAPSHOT = {"foo": 42, "bar": 84}
UPDATED_SNAPSHOT = {"foo": 84, "bar": 168}
AGGREGATE_ID = 21
VERSION = 25
UPDATED_VERSION = 28


@patch(
    EVENT_DESERIALIZER,
    new_callable=lambda: lambda x: loads(x, cls=CustomJSONDecoder),
)
@patch(
    EVENT_SERIALIZER,
    new_callable=lambda: lambda e: dumps(e, cls=CustomJSONEncoder),
)
class TestSqliteSnapshotRepository(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        if os.path.exists(DB_SNAPSHOTS_PATH):  # pragma no cover
            os.remove(DB_SNAPSHOTS_PATH)  # pragma no cover
        self.snapshot_repo = SqliteSnapshotRepository()

    def tearDown(self) -> None:
        if os.path.exists(DB_SNAPSHOTS_PATH):  # pragma no cover
            os.remove(DB_SNAPSHOTS_PATH)

    async def test_when_store_snapshot_then_can_be_retrieved(self, *_):
        await self.snapshot_repo.store_snapshot(AGGREGATE_ID, VERSION, SNAPSHOT)
        version, snapshot = await self.snapshot_repo.get_snapshot(AGGREGATE_ID)
        self.assertEqual(VERSION, version)
        self.assertDictEqual(SNAPSHOT, snapshot)

    async def test_when_snapshot_not_exist_then_return_none(self, *_):
        self.assertIsNone(await self.snapshot_repo.get_snapshot(AGGREGATE_ID + 1))

    async def test_when_store_existing_snapshot_then_old_snapshot_overriden(self, *_):
        await self.snapshot_repo.store_snapshot(AGGREGATE_ID, VERSION, SNAPSHOT)
        version, snapshot = await self.snapshot_repo.get_snapshot(AGGREGATE_ID)
        self.assertEqual(VERSION, version)
        self.assertDictEqual(SNAPSHOT, snapshot)
        await self.snapshot_repo.store_snapshot(
            AGGREGATE_ID, UPDATED_VERSION, UPDATED_SNAPSHOT
        )
        version, snapshot = await self.snapshot_repo.get_snapshot(AGGREGATE_ID)
        self.assertEqual(UPDATED_VERSION, version)
        self.assertDictEqual(UPDATED_SNAPSHOT, snapshot)

    async def test_when_delete_snapshot_then_not_exists(self, *_):
        await self.snapshot_repo.store_snapshot(AGGREGATE_ID, VERSION, SNAPSHOT)
        version, snapshot = await self.snapshot_repo.get_snapshot(AGGREGATE_ID)
        self.assertEqual(VERSION, version)
        self.assertDictEqual(SNAPSHOT, snapshot)
        await self.snapshot_repo.delete_snapshot(AGGREGATE_ID)
        self.assertIsNone(await self.snapshot_repo.get_snapshot(AGGREGATE_ID))

    async def test_when_delete_snapshot_not_exists_then_no_error(self, *_):
        await self.snapshot_repo.delete_snapshot(AGGREGATE_ID)
        await self.snapshot_repo.delete_snapshot(AGGREGATE_ID)
        await self.snapshot_repo.delete_snapshot(AGGREGATE_ID)

    async def test_when_try_commit_older_snapshot_then_not_committed(self, *_):
        await self.snapshot_repo.store_snapshot(
            AGGREGATE_ID, UPDATED_VERSION, UPDATED_SNAPSHOT
        )
        version, snapshot = await self.snapshot_repo.get_snapshot(AGGREGATE_ID)
        self.assertEqual(UPDATED_VERSION, version)
        self.assertDictEqual(UPDATED_SNAPSHOT, snapshot)
        await self.snapshot_repo.store_snapshot(AGGREGATE_ID, VERSION, SNAPSHOT)
        version, snapshot = await self.snapshot_repo.get_snapshot(AGGREGATE_ID)
        self.assertEqual(UPDATED_VERSION, version)
        self.assertDictEqual(UPDATED_SNAPSHOT, snapshot)
