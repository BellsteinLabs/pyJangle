import unittest
from unittest.mock import patch

from pyjangle import (
    Aggregate,
    DuplicateCommandRegistrationError,
    AggregateRegistrationError,
    RegisterAggregate,
    command_to_aggregate_map_instance,
    validate_command,
)
from pyjangle.test.registration_paths import COMMAND_TO_AGGREGATE_MAP
from pyjangle.test.reset import ResetPyJangleState


@ResetPyJangleState
@patch(COMMAND_TO_AGGREGATE_MAP, dict())
class TestRegisterCommand(unittest.TestCase):
    def test_duplicate_registration(self, *_):
        self.assertTrue(len(command_to_aggregate_map_instance()) == 0)
        with self.assertRaises(DuplicateCommandRegistrationError):

            @RegisterAggregate
            class A(Aggregate):
                @validate_command(int)
                def foo1(self, _, __):
                    pass

            @RegisterAggregate
            class B(Aggregate):
                @validate_command(int)
                def foo1(self, _, __):
                    pass

    def test_registration(self, *_):
        @RegisterAggregate
        class A(Aggregate):
            @validate_command(int)
            def foo1(self, _, __):
                pass

        self.assertEqual(command_to_aggregate_map_instance()[int], A)

        @RegisterAggregate
        class B(Aggregate):
            @validate_command(str)
            def foo1(self, _, __):
                pass

        self.assertEqual(command_to_aggregate_map_instance()[str], B)

    def test_registration_on_non_aggregate(self, *_):
        with self.assertRaises(AggregateRegistrationError):

            @RegisterAggregate
            class A:
                pass

    def test_multiple_registrations_on_same_aggregate(self, *_):
        @RegisterAggregate
        class A(Aggregate):
            @validate_command(int)
            def foo1(self, _, __):
                pass

            @validate_command(str)
            def foo2(self, _, __):
                pass

            @validate_command(bool)
            def foo3(self, _, __):
                pass

        self.assertEqual(command_to_aggregate_map_instance()[int], A)
        self.assertEqual(command_to_aggregate_map_instance()[str], A)
        self.assertEqual(command_to_aggregate_map_instance()[bool], A)

    def test_decorator_does_not_hide_class(self, *_):
        @RegisterAggregate
        class A(Aggregate):
            pass

        self.assertDictEqual(vars(A(id=42)), vars(A(id=42)))
