from unittest import TestCase
from unittest.mock import patch
from uuid import UUID
from pyjangle.event.register_event_id_factory import register_event_id_factory, _default_event_id_factory

from pyjangle.event.register_event_id_factory import EventIdRegistrationError, event_id_factory_instance
from pyjangle.test.registration_paths import EVENT_ID_FACTORY


@patch(EVENT_ID_FACTORY, new=_default_event_id_factory)
class TestRegisterEventIdFactory(TestCase):
    def test_when_nothing_registered_then_default_behavior(self):
        instance = event_id_factory_instance()
        self.assertIsInstance(instance(), UUID)

    def test_when_wrapped_is_not_callable_then_exception(self):
        with self.assertRaises(EventIdRegistrationError):
            register_event_id_factory(42)

    def test_when_wrapped_has_parameters_then_exception(self):
        with self.assertRaises(EventIdRegistrationError):
            @register_event_id_factory
            def another_factory(param):
                pass

    def test_when_already_registered_then_exception(self):
        @register_event_id_factory
        def factory():
            pass

        with self.assertRaises(EventIdRegistrationError):
            @register_event_id_factory
            def another_factory():
                pass

    def test_when_function_registered_then_can_be_retrieved_with_instance_method(self):
        @register_event_id_factory
        def factory():
            return 42

        self.assertEqual(event_id_factory_instance()(), 42)
