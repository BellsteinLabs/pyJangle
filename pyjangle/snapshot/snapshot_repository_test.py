import unittest
from unittest.mock import patch

from pyjangle import (RegisterSnapshotRepository, SnapshotRepositoryError,
                      snapshot_repository_instance)
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
        with self.assertRaises(SnapshotRepositoryError):
            snapshot_repository_instance()

    @patch(SNAPSHOT_REPO, None)
    def test_exception_when_multiple_registered(self):
        with self.assertRaises(SnapshotRepositoryError):
            @RegisterSnapshotRepository
            class A:
                pass

            @RegisterSnapshotRepository
            class B:
                pass
