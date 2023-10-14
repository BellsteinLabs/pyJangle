from unittest import TestCase
from unittest.mock import patch

from pyjangle import (
    DeserializerBadSignatureError,
    SerializerBadSignatureError,
    SerializerMissingError,
    DeserializerMissingError,
    get_deserializer,
    get_serializer,
    register_deserializer,
    register_serializer,
)
from test_helpers.registration_paths import (
    DESERIALIZER,
    SERIALIZER,
)


@patch(SERIALIZER, None)
@patch(DESERIALIZER, None)
class TestRegisterSerializer(TestCase):
    def test_serializer_happy_path(self, *_):
        @register_serializer
        def serializer(event):
            pass

        self.assertTrue(get_serializer())

    def test_cant_register_multiple(self, *_):
        with self.assertRaises(SerializerBadSignatureError):

            @register_serializer
            def serializer(event):
                pass

            @register_serializer
            def serializer(event):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(SerializerMissingError):
            get_serializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(SerializerBadSignatureError):

            @register_serializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(SerializerBadSignatureError):

            @register_serializer
            def serializer(event, _):
                pass


@patch(SERIALIZER, None)
@patch(DESERIALIZER, None)
class TestRegisterDeserializer(TestCase):
    def test_deserializer_happy_path(self, *_):
        @register_deserializer
        def deserializer(serialized_event):
            pass

        self.assertTrue(get_deserializer())

    def test_cant_register_multiple(self, *_):
        with self.assertRaises(DeserializerBadSignatureError):

            @register_deserializer
            def serializer(serialized_event):
                pass

            @register_deserializer
            def serializer(serialized_event):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(DeserializerMissingError):
            get_deserializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(DeserializerBadSignatureError):

            @register_deserializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(DeserializerBadSignatureError):

            @register_deserializer
            def deserializer(serialized_event, _):
                pass
