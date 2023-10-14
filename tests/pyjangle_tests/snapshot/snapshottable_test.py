from unittest import TestCase

from pyjangle import SnapshotError, Snapshottable


class TestSnapshottable(TestCase):
    def test_when_apply_snapshot_fails_then_exception(self, *_):
        class Foo(Snapshottable):
            def apply_snapshot_hook(self, snapshot):
                raise Exception

            def get_snapshot(self) -> any:
                pass

            def get_snapshot_frequency(self) -> int:
                pass

        with self.assertRaises(SnapshotError):
            foo = Foo()
            foo.apply_snapshot(42, 42)
