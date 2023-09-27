from unittest import TestCase
from unittest.mock import patch

from pyjangle import (
    EventDeserializerBadSignatureError,
    EventSerializerBadSignatureError,
    EventSerializerMissingError,
    EventDeserializerMissingError,
    get_event_deserializer,
    get_event_serializer,
    register_event_deserializer,
    register_event_serializer,
)
from pyjangle.test.registration_paths import (
    EVENT_DESERIALIZER,
    EVENT_SERIALIZER,
)


@patch(EVENT_SERIALIZER, None)
@patch(EVENT_DESERIALIZER, None)
class TestRegisterSerializer(TestCase):
    def test_serializer_happy_path(self, *_):
        @register_event_serializer
        def serializer(event):
            pass

        self.assertTrue(get_event_serializer())

    def test_cant_register_multiple(self, *_):
        with self.assertRaises(EventSerializerBadSignatureError):

            @register_event_serializer
            def serializer(event):
                pass

            @register_event_serializer
            def serializer(event):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(EventSerializerMissingError):
            get_event_serializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(EventSerializerBadSignatureError):

            @register_event_serializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(EventSerializerBadSignatureError):

            @register_event_serializer
            def serializer(event, _):
                pass


@patch(EVENT_SERIALIZER, None)
@patch(EVENT_DESERIALIZER, None)
class TestRegisterDeserializer(TestCase):
    def test_deserializer_happy_path(self, *_):
        @register_event_deserializer
        def deserializer(serialized_event):
            pass

        self.assertTrue(get_event_deserializer())

    def test_cant_register_multiple(self, *_):
        with self.assertRaises(EventDeserializerBadSignatureError):

            @register_event_deserializer
            def serializer(serialized_event):
                pass

            @register_event_deserializer
            def serializer(serialized_event):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(EventDeserializerMissingError):
            get_event_deserializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(EventDeserializerBadSignatureError):

            @register_event_deserializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(EventDeserializerBadSignatureError):

            @register_event_deserializer
            def deserializer(serialized_event, _):
                pass
