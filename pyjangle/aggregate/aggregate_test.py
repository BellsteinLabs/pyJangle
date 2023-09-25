import unittest
from datetime import datetime
from pyjangle import (
    Aggregate,
    reconstitute_aggregate_state,
    validate_command,
    ValidateCommandMethodMissingError,
    CommandResponse,
    ReconstituteStateMethodMissingError,
    CommandValidatorBadSignatureError,
)
from pyjangle.test.commands import CommandThatShouldSucceedA, CommandThatShouldSucceedB
from pyjangle.test.events import EventA


class TestAggregate(unittest.TestCase):
    def test_validate_command_decorator(self):
        class A(Aggregate):
            @validate_command(CommandThatShouldSucceedA)
            def validateA(self, command: CommandThatShouldSucceedA, next_version: int):
                self.a_called = True

            @validate_command(CommandThatShouldSucceedB)
            def validateB(self, command: CommandThatShouldSucceedB, next_version: int):
                self.b_called = True

        a = A(1)
        a.validate(CommandThatShouldSucceedA())
        a.validate(CommandThatShouldSucceedB())
        self.assertTrue(a.a_called)
        self.assertTrue(a.b_called)

    def test_reconstitute_aggregate_state_decorator(self):
        ATTR_NAME = "x"

        class A(Aggregate):
            @reconstitute_aggregate_state(EventA)
            def foo(self, event: EventA):
                setattr(self, ATTR_NAME, True)

        a = A(1)
        self.assertFalse(hasattr(a, ATTR_NAME))
        a.apply_events([EventA(id="a", version=1, created_at=datetime.now)])
        self.assertTrue(hasattr(a, ATTR_NAME))

    def test_validate_command_decorated_method_wrong_signature(self):
        with self.assertRaises(CommandValidatorBadSignatureError):

            class A(Aggregate):
                @validate_command(CommandThatShouldSucceedA)
                def foo(self):
                    pass

    def test_validate_command_method_is_passed_next_aggregate_version(self):
        class A(Aggregate):
            @validate_command(CommandThatShouldSucceedA)
            def validateA(self, command: CommandThatShouldSucceedA, next_version: int):
                self.updated_version = next_version

        a = A(1)
        self.assertEqual(0, a.version)
        a.validate(CommandThatShouldSucceedA())
        self.assertEqual(1, a.updated_version)

    def test_command_validator_can_post_new_event(self):
        class A(Aggregate):
            @validate_command(CommandThatShouldSucceedA)
            def validateA(self, command: CommandThatShouldSucceedA, next_version: int):
                self.updated_version = next_version
                self.post_new_event(EventA(id=2, version=2, created_at=None))

        a = A(1)
        a.validate(CommandThatShouldSucceedA())
        new_event_tuple = a.new_events[0]
        self.assertEqual(len(a.new_events), 1)
        self.assertEqual(a.id, new_event_tuple[0])
        self.assertIsInstance(new_event_tuple[1], EventA)

    def test_command_response_defaults_to_true(self):
        class A(Aggregate):
            @validate_command(CommandThatShouldSucceedA)
            def validateA(self, command: CommandThatShouldSucceedA, next_version: int):
                self.updated_version = next_version

        a = A(1)
        self.assertEqual(0, a.version)
        response = a.validate(CommandThatShouldSucceedA())
        self.assertIsInstance(response, CommandResponse)
        self.assertTrue(response.is_success)

    def test_explicit_command_response_returned(self):
        class A(Aggregate):
            @validate_command(CommandThatShouldSucceedA)
            def validateA(self, command: CommandThatShouldSucceedA, next_version: int):
                return CommandResponse(False, "Foo")

        a = A(1)
        response = a.validate(CommandThatShouldSucceedA())
        self.assertIsInstance(response, CommandResponse)
        self.assertFalse(response.is_success)
        self.assertEqual("Foo", response.data)

    def test_missing_command_validator(self):
        with self.assertRaises(ValidateCommandMethodMissingError):

            class A(Aggregate):
                @validate_command(CommandThatShouldSucceedA)
                def validateA(
                    self, command: CommandThatShouldSucceedA, next_version: int
                ):
                    pass

            a = A(1)
            a.validate(CommandThatShouldSucceedB())

    def test_missing_state_reconstitutor(self):
        with self.assertRaises(ReconstituteStateMethodMissingError):

            class A(Aggregate):
                pass

            a = A(1)
            a.apply_events([EventA(id="", version=1, created_at=None)])

    def test_version_accurate_when_events_applied_out_of_order(self):
        version_1 = EventA(id="a", version=1, created_at=datetime.now)
        version_2 = EventA(id="a", version=2, created_at=datetime.now)
        version_3 = EventA(id="a", version=3, created_at=datetime.now)

        class A(Aggregate):
            def __init__(self, id: any):
                super().__init__(id)

            @reconstitute_aggregate_state(EventA)
            def foo(self, event: EventA):
                pass

        a = A(1)
        self.assertEqual(0, a.version)
        a.apply_events([version_2])
        self.assertEqual(2, a.version)
        a.apply_events([version_1])
        self.assertEqual(2, a.version)
        a.apply_events([version_3])
        self.assertEqual(3, a.version)
