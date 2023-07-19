from unittest import TestCase
from unittest.mock import patch
from pyjangle.event.event import Event
from pyjangle.event.register import EventRegistrationError, RegisterEvent, get_event_name, get_event_type
from pyjangle.test.registration_paths import EVENT_REPO, EVENT_TYPE_TO_NAME_MAP, NAME_TO_EVENT_TYPE_MAP
from pyjangle.test.transient_event_repository import TransientEventRepository

@patch.dict(NAME_TO_EVENT_TYPE_MAP)
@patch.dict(EVENT_TYPE_TO_NAME_MAP)
@patch(EVENT_REPO, new_callable=lambda : TransientEventRepository())
class TestRegisterEvent(TestCase):
    def test_non_parenthesis_form(self, *_):
        @RegisterEvent
        class Foo(Event):
            pass

        self.assertEqual(get_event_name(Foo), f"{Foo.__module__}.{Foo.__name__}")
    
    def test_parenthesis_form(self, *_):
        @RegisterEvent()
        class Foo(Event):
            pass

        self.assertEqual(get_event_name(Foo), f"{Foo.__module__}.{Foo.__name__}")

    def test_exception_when_register_non_event(self, *_):
        with self.assertRaises(EventRegistrationError):
            @RegisterEvent
            class Foo:
                pass

    def test_exception_when_event_name_already_registered(self, *_):
        with self.assertRaises(EventRegistrationError):
            @RegisterEvent(name="books.HarryPotter.characters.Hermione")
            class Foo(Event):
                pass
            @RegisterEvent(name="books.HarryPotter.characters.Hermione")
            class Bar(Event):
                pass

    def test_custom_event_name(self, *_):
        EVENT_NAME = "books.HarryPotter.characters.Hermione"
        @RegisterEvent(name=EVENT_NAME)
        class Foo(Event):
            pass

        self.assertEqual(EVENT_NAME, get_event_name(Foo))

    def test_exception_when_name_not_exists(self, *_):
        with self.assertRaises(KeyError):
            get_event_name(str)

    def test_exception_when_event_not_registered(self, *_):
        with self.assertRaises(KeyError):
            get_event_type("Voldemort")