import unittest
from unittest.mock import patch
from pyjangle import Aggregate


from pyjangle import (
    CommandRegistrationError, RegisterAggregate,
    command_to_aggregate_map_instance)
from pyjangle.aggregate.aggregate import validate_command
from pyjangle.test.registration_paths import COMMAND_TO_AGGREGATE_MAP


class TestRegisterCommand(unittest.TestCase):
    @patch(COMMAND_TO_AGGREGATE_MAP, dict())
    def test_duplicate_registration(self):
        self.assertTrue(len(command_to_aggregate_map_instance()) == 0)
        with self.assertRaises(CommandRegistrationError):
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

    @patch(COMMAND_TO_AGGREGATE_MAP, dict())
    def test_registration(self):
        @RegisterAggregate
        class A(Aggregate):
            @validate_command(int)
            def foo1(self, _, __):
                pass

        self.assertEqual(command_to_aggregate_map_instance()
                         [int].__name__, A.__name__)

        @RegisterAggregate
        class B(Aggregate):
            @validate_command(str)
            def foo1(self, _, __):
                pass

        self.assertEqual(command_to_aggregate_map_instance()
                         [str].__name__, B.__name__)

    @patch(COMMAND_TO_AGGREGATE_MAP, dict())
    def test_registration_on_non_aggregate(self):
        with self.assertRaises(CommandRegistrationError):
            @RegisterAggregate
            class A:
                pass

    @patch(COMMAND_TO_AGGREGATE_MAP, dict())
    def test_multiple_registrations_on_same_aggregate(self):
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

        self.assertEqual(command_to_aggregate_map_instance()
                         [int].__name__, A.__name__)
        self.assertEqual(command_to_aggregate_map_instance()
                         [str].__name__, A.__name__)
        self.assertEqual(command_to_aggregate_map_instance()
                         [bool].__name__, A.__name__)

    @patch(COMMAND_TO_AGGREGATE_MAP, dict())
    def test_decorator_does_not_hide_class(self):
        @RegisterAggregate
        class A(Aggregate):
            pass

        self.assertDictEqual(A(id=42).__dict__, A(id=42).__dict__)
