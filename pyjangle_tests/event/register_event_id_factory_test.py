from unittest import TestCase
from unittest.mock import patch
from uuid import UUID
from pyjangle import (
    register_event_id_factory,
    EventIdRegistrationFactoryBadSignatureError,
    DuplicateEventIdFactoryRegistrationError,
    event_id_factory_instance,
)
from test_helpers.reset import ResetPyJangleState


@ResetPyJangleState
class TestRegisterEventIdFactory(TestCase):
    def test_when_nothing_registered_then_default_behavior(self, *_):
        instance = event_id_factory_instance()
        self.assertIsInstance(instance(), UUID)

    def test_when_wrapped_is_not_callable_then_exception(self, *_):
        with self.assertRaises(EventIdRegistrationFactoryBadSignatureError):
            register_event_id_factory(42)

    def test_when_wrapped_has_wrong_parameters_then_exception(self, *_):
        with self.assertRaises(EventIdRegistrationFactoryBadSignatureError):

            @register_event_id_factory
            def another_factory(param):
                pass

    def test_when_already_registered_then_exception(self, *_):
        @register_event_id_factory
        def factory():
            pass

        with self.assertRaises(DuplicateEventIdFactoryRegistrationError):

            @register_event_id_factory
            def another_factory():
                pass

    def test_when_function_registered_then_can_be_retrieved_with_instance_method(
        self, *_
    ):
        @register_event_id_factory
        def factory():
            return 42

        self.assertEqual(event_id_factory_instance()(), 42)
