import unittest
from unittest.mock import patch

from pyjangle import RegisterSagaRepository, SagaRepositoryError, saga_repository_instance
from pyjangle.test.registration_paths import SAGA_REPO


@patch(SAGA_REPO, None)
class TestSagaRepository(unittest.TestCase):

    def test_can_register_saga_repository(self):
        @RegisterSagaRepository
        class A:
            pass

        self.assertIsNotNone(saga_repository_instance())

    def test_exception_when_multiple_registered(self):
        with self.assertRaises(SagaRepositoryError):
            @RegisterSagaRepository
            class A:
                pass

            @RegisterSagaRepository
            class B:
                pass

    def test_exception_when_none_registered(self):
        with self.assertRaises(SagaRepositoryError):
            self.assertIsNotNone(saga_repository_instance())

    def test_decorator_does_not_hide_class(self):
        @RegisterSagaRepository
        class A:
            pass

        self.assertEqual(A().__class__.__name__, A.__name__)
