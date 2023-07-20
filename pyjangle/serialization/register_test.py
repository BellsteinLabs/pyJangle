from unittest import TestCase
from unittest.mock import patch
from pyjangle.serialization.register import EventDeserializerRegistrationError, EventSerializerRegistrationError, SagaDeserializerRegistrationError, SagaSerializerRegistrationError, SnapshotDeserializerRegistrationError, SnapshotSerializerRegistrationError, get_event_deserializer, get_event_serializer, get_saga_deserializer, get_saga_serializer, get_snapshot_deserializer, get_snapshot_serializer, register_event_deserializer, register_event_serializer, register_saga_deserializer, register_saga_serializer, register_snapshot_deserializer, register_snapshot_serializer
from pyjangle.test.registration_paths import EVENT_DESERIALIZER, EVENT_SERIALIZER, SAGA_DESERIALIZER, SAGA_SERIALIZER, SNAPSHOT_DESERIALIZER, SNAPSHOT_SERIALIZER

@patch(SAGA_SERIALIZER, None)
@patch(SAGA_DESERIALIZER, None)
class TestRegisterSerializer(TestCase):
    def test_serializer_happy_path(self, *_):
        @register_saga_serializer
        def serializer(saga):
            pass

        self.assertTrue(get_saga_serializer())

    def test_cant_register_multiple(self, *_):
        with self.assertRaises(SagaSerializerRegistrationError):
            @register_saga_serializer
            def serializer(saga):
                pass

            @register_saga_serializer
            def serializer(saga):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(SagaSerializerRegistrationError):
            get_saga_serializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(SagaSerializerRegistrationError):
            @register_saga_serializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(SagaSerializerRegistrationError):
            @register_saga_serializer
            def serializer(saga, _):
                pass

@patch(SAGA_SERIALIZER, None)
@patch(SAGA_DESERIALIZER, None)
class TestRegisterDeserializer(TestCase):
    def test_deserializer_happy_path(self, *_):
        @register_saga_deserializer
        def deserializer(serialized_saga):
            pass

        self.assertTrue(get_saga_deserializer())

    def test_cant_register_multiple(self, *_):
        with self.assertRaises(SagaDeserializerRegistrationError):
            @register_saga_deserializer
            def serializer(serialized_saga):
                pass

            @register_saga_deserializer
            def serializer(serialized_saga):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(SagaDeserializerRegistrationError):
            get_saga_deserializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(SagaDeserializerRegistrationError):
            @register_saga_deserializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(SagaDeserializerRegistrationError):
            @register_saga_deserializer
            def deserializer(serialized_saga, _):
                pass

@patch(EVENT_SERIALIZER, None)
@patch(EVENT_DESERIALIZER, None)
class TestRegisterSerializer(TestCase):
    def test_serializer_happy_path(self, *_):
        @register_event_serializer
        def serializer(event):
            pass

        self.assertTrue(get_event_serializer())

    def test_cant_register_multiple(self, *_):
        with self.assertRaises(EventSerializerRegistrationError):
            @register_event_serializer
            def serializer(event):
                pass

            @register_event_serializer
            def serializer(event):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(EventSerializerRegistrationError):
            get_event_serializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(EventSerializerRegistrationError):
            @register_event_serializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(EventSerializerRegistrationError):
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
        with self.assertRaises(EventDeserializerRegistrationError):
            @register_event_deserializer
            def serializer(serialized_event):
                pass

            @register_event_deserializer
            def serializer(serialized_event):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(EventDeserializerRegistrationError):
            get_event_deserializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(EventDeserializerRegistrationError):
            @register_event_deserializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(EventDeserializerRegistrationError):
            @register_event_deserializer
            def deserializer(serialized_event, _):
                pass

@patch(SNAPSHOT_SERIALIZER, None)
@patch(SNAPSHOT_DESERIALIZER, None)
class TestRegisterSerializer(TestCase):
    def test_serializer_happy_path(self, *_):
        @register_snapshot_serializer
        def serializer(snapshot):
            pass

        self.assertTrue(get_snapshot_serializer())

    def test_cant_register_multiple(self, *_):
        with self.assertRaises(SnapshotSerializerRegistrationError):
            @register_snapshot_serializer
            def serializer(snapshot):
                pass

            @register_snapshot_serializer
            def serializer(snapshot):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(SnapshotSerializerRegistrationError):
            get_snapshot_serializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(SnapshotSerializerRegistrationError):
            @register_snapshot_serializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(SnapshotSerializerRegistrationError):
            @register_snapshot_serializer
            def serializer(snapshot, _):
                pass

@patch(SNAPSHOT_SERIALIZER, None)
@patch(SNAPSHOT_DESERIALIZER, None)
class TestRegisterDeserializer(TestCase):
    def test_deserializer_happy_path(self, *_):
        @register_snapshot_deserializer
        def deserializer(serialized_snapshot):
            pass

        self.assertTrue(get_snapshot_deserializer())

    def test_cant_register_multiple(self, *_):
        with self.assertRaises(SnapshotDeserializerRegistrationError):
            @register_snapshot_deserializer
            def serializer(serialized_snapshot):
                pass

            @register_snapshot_deserializer
            def serializer(serialized_snapshot):
                pass

    def test_exception_when_no_handler_regsitered(self, *_):
        with self.assertRaises(SnapshotDeserializerRegistrationError):
            get_snapshot_deserializer()

    def test_handler_not_function(self, *_):
        with self.assertRaises(SnapshotDeserializerRegistrationError):
            @register_snapshot_deserializer
            class Foo:
                pass

    def test_handler_has_wrong_param_count(self, *_):
        with self.assertRaises(SnapshotDeserializerRegistrationError):
            @register_snapshot_deserializer
            def deserializer(serialized_snapshot, _):
                pass