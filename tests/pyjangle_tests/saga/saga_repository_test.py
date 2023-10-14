import unittest
from unittest.mock import patch

from pyjangle import (
    RegisterSagaRepository,
    DuplicateSagaRepositoryError,
    SagaRepositoryMissingError,
    saga_repository_instance,
)
from test_helpers.registration_paths import SAGA_REPO
from test_helpers.reset import ResetPyJangleState


@patch(SAGA_REPO, None)
@ResetPyJangleState
class TestSagaRepository(unittest.TestCase):
    def test_can_register_saga_repository(self, *_):
        @RegisterSagaRepository
        class A:
            pass

        self.assertIsNotNone(saga_repository_instance())

    def test_exception_when_multiple_registered(self, *_):
        with self.assertRaises(DuplicateSagaRepositoryError):

            @RegisterSagaRepository
            class A:
                pass

            @RegisterSagaRepository
            class B:
                pass

    def test_exception_when_none_registered(self, *_):
        with self.assertRaises(SagaRepositoryMissingError):
            self.assertIsNotNone(saga_repository_instance())

    def test_decorator_does_not_hide_class(self, *_):
        @RegisterSagaRepository
        class A:
            pass

        self.assertEqual(A().__class__.__name__, A.__name__)
