import unittest
from unittest.mock import patch

from pyjangle.snapshot.snapshot_repository import RegisterSnapshotRepository, SnapshotRepositoryError, snapshot_repository_instance

class TestSnapshotRepository(unittest.TestCase):
    @patch("pyjangle.snapshot.snapshot_repository._registered_snapshot_repository", None)
    def test_can_register_snapshot_repository(self):
        @RegisterSnapshotRepository
        class A:
            pass

        self.assertIsNotNone(snapshot_repository_instance())

    @patch("pyjangle.snapshot.snapshot_repository._registered_snapshot_repository", None)
    def test_exception_when_none_registered(self):
        with self.assertRaises(SnapshotRepositoryError):
            snapshot_repository_instance()

    @patch("pyjangle.snapshot.snapshot_repository._registered_snapshot_repository", None)
    def test_exception_when_multiple_registered(self):
        with self.assertRaises(SnapshotRepositoryError):
            @RegisterSnapshotRepository
            class A:
                pass

            @RegisterSnapshotRepository
            class B:
                pass