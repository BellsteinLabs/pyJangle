import unittest
from unittest.mock import patch
from pyjangle.aggregate.aggregate import Aggregate
from pyjangle.command.register import CommandRegistrationError, RegisterCommand, command_to_aggregate_map_instance


class TestRegisterCommand(unittest.TestCase):
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_duplicate_registration(self):
        self.assertTrue(len(command_to_aggregate_map_instance()) == 0)
        with self.assertRaises(CommandRegistrationError):
            @RegisterCommand(int)
            class A(Aggregate):
                pass

            @RegisterCommand(int)
            class B(Aggregate):
                pass   
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_registration(self):
        @RegisterCommand(int)
        class A(Aggregate):
            pass

        self.assertEqual(command_to_aggregate_map_instance()[int].__name__, A.__name__)

        @RegisterCommand(str)
        class B(Aggregate):
            pass

        self.assertEqual(command_to_aggregate_map_instance()[str].__name__, B.__name__)
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_registration_on_non_aggregate(self):
        with self.assertRaises(CommandRegistrationError):
            @RegisterCommand(int)
            class A:
                pass
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_multiple_registrations_on_same_aggregate(self):
        @RegisterCommand(int, str, bool)
        class A(Aggregate):
            pass

        self.assertEqual(command_to_aggregate_map_instance()[int].__name__, A.__name__)
        self.assertEqual(command_to_aggregate_map_instance()[str].__name__, A.__name__)
        self.assertEqual(command_to_aggregate_map_instance()[bool].__name__, A.__name__)
    @patch("pyjangle.command.register._command_to_aggregate_map", dict())
    def test_decorator_does_not_hide_class(self):
        @RegisterCommand(int, str, bool)
        class A(Aggregate):
            pass

        
        self.assertEqual(A().__name__, A.__name__)

if __name__ == '__main__':
    unittest.main()
