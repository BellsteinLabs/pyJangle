import unittest
from unittest.mock import patch

from pyjangle.saga.saga_repository import RegisterSagaRepository, SagaRepositoryError, saga_repository_instance

class TestSagaRepository(unittest.TestCase):

    @patch("pyjangle.saga.saga_repository.__registered_saga_repository", None)
    def test_can_register_saga_repository(self):
        @RegisterSagaRepository
        class A:
            pass

        self.assertIsNotNone(saga_repository_instance())

    @patch("pyjangle.saga.saga_repository.__registered_saga_repository", None)
    def test_exception_when_multiple_registered(self):
        with self.assertRaises(SagaRepositoryError):
            @RegisterSagaRepository
            class A:
                pass

            @RegisterSagaRepository
            class B:
                pass

            self.assertIsNotNone(saga_repository_instance())

    @patch("pyjangle.saga.saga_repository.__registered_saga_repository", None)
    def test_exception_when_none_registered(self):
        with self.assertRaises(SagaRepositoryError):
            self.assertIsNotNone(saga_repository_instance())

    @patch("pyjangle.saga.saga_repository.__registered_saga_repository", None)
    def test_decorator_does_not_hide_class(self):
        @RegisterSagaRepository
        class A:
            pass

        self.assertEqual(A().__class__.__name__, A.__name__)