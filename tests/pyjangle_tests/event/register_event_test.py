from unittest import TestCase

from pyjangle import (
    VersionedEvent,
    EventRegistrationError,
    DuplicateEventNameRegistrationError,
    RegisterEvent,
    get_event_name,
    get_event_type,
)
from test_helpers.reset import ResetPyJangleState


@ResetPyJangleState
class TestRegisterEvent(TestCase):
    def test_non_parenthesis_form(self, *_):
        @RegisterEvent
        class Foo(VersionedEvent):
            pass

        self.assertEqual(get_event_name(Foo), f"{Foo.__module__}.{Foo.__name__}")

    def test_parenthesis_form(self, *_):
        @RegisterEvent()
        class Foo(VersionedEvent):
            pass

        self.assertEqual(get_event_name(Foo), f"{Foo.__module__}.{Foo.__name__}")

    def test_exception_when_register_non_event(self, *_):
        with self.assertRaises(EventRegistrationError):

            @RegisterEvent
            class Foo:
                pass

    def test_exception_when_event_name_already_registered(self, *_):
        with self.assertRaises(DuplicateEventNameRegistrationError):

            @RegisterEvent(name="books.HarryPotter.characters.Hermione")
            class Foo(VersionedEvent):
                pass

            @RegisterEvent(name="books.HarryPotter.characters.Hermione")
            class Bar(VersionedEvent):
                pass

    def test_custom_event_name(self, *_):
        EVENT_NAME = "books.HarryPotter.characters.Hermione"

        @RegisterEvent(name=EVENT_NAME)
        class Foo(VersionedEvent):
            pass

        self.assertEqual(EVENT_NAME, get_event_name(Foo))

    def test_exception_when_name_not_exists(self, *_):
        with self.assertRaises(KeyError):
            get_event_name(str)

    def test_exception_when_event_not_registered(self, *_):
        with self.assertRaises(KeyError):
            get_event_type("Voldemort")
