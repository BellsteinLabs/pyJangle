from unittest import TestCase

from pyjangle import (
    RegisterSaga,
    Saga,
    SagaRegistrationError,
    get_saga_name,
    get_saga_type,
    DuplicateSagaNameError,
)
from test_helpers.reset import ResetPyJangleState


@ResetPyJangleState
class TestRegisterSaga(TestCase):
    def test_non_parenthesis_form(self, *_):
        @RegisterSaga
        class Foo(Saga):
            pass

        self.assertEqual(get_saga_name(Foo), f"{Foo.__module__}.{Foo.__name__}")

    def test_parenthesis_form(self, *_):
        @RegisterSaga()
        class Foo(Saga):
            pass

        self.assertEqual(get_saga_name(Foo), f"{Foo.__module__}.{Foo.__name__}")

    def test_exception_when_register_non_saga(self, *_):
        with self.assertRaises(SagaRegistrationError):

            @RegisterSaga
            class Foo:
                pass

    def test_exception_when_saga_name_already_registered(self, *_):
        with self.assertRaises(DuplicateSagaNameError):

            @RegisterSaga(name="books.HarryPotter.characters.Hermione")
            class Foo(Saga):
                pass

            @RegisterSaga(name="books.HarryPotter.characters.Hermione")
            class Bar(Saga):
                pass

    def test_custom_saga_name(self, *_):
        SAGA_NAME = "books.HarryPotter.characters.Hermione"

        @RegisterSaga(name=SAGA_NAME)
        class Foo(Saga):
            pass

        self.assertEqual(SAGA_NAME, get_saga_name(Foo))

    def test_exception_when_name_not_exists(self, *_):
        with self.assertRaises(KeyError):
            get_saga_name(str)

    def test_exception_when_saga_not_registered(self, *_):
        with self.assertRaises(KeyError):
            get_saga_type("Voldemort")
