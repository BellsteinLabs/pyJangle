import unittest
from unittest.mock import patch

from pyjangle import (
    RegisterSnapshotRepository,
    SnapshotRepositoryMissingError,
    DuplicateSnapshotRepositoryError,
    snapshot_repository_instance,
    SnapshotRepository,
)
from pyjangle.test.registration_paths import SNAPSHOT_REPO


class TestSnapshotRepository(unittest.TestCase):
    @patch(SNAPSHOT_REPO, None)
    def test_can_register_snapshot_repository(self):
        @RegisterSnapshotRepository
        class A:
            pass

        self.assertIsNotNone(snapshot_repository_instance())

    @patch(SNAPSHOT_REPO, None)
    def test_exception_when_none_registered(self):
        with self.assertRaises(SnapshotRepositoryMissingError):
            snapshot_repository_instance()

    @patch(SNAPSHOT_REPO, None)
    def test_exception_when_multiple_registered(self):
        with self.assertRaises(DuplicateSnapshotRepositoryError):

            @RegisterSnapshotRepository
            class A(SnapshotRepository):
                async def get_snapshot(self, aggregate_id: str):
                    pass

                async def store_snapshot(
                    self, aggregate_id: any, version: int, snapshot: any
                ):
                    pass

                async def delete_snapshot(self, aggregate_id: str):
                    pass

            @RegisterSnapshotRepository
            class B(SnapshotRepository):
                async def get_snapshot(self, aggregate_id: str):
                    pass

                async def store_snapshot(
                    self, aggregate_id: any, version: int, snapshot: any
                ):
                    pass

                async def delete_snapshot(self, aggregate_id: str):
                    pass
