import unittest

from pyjangle.aggregate.aggregate import Aggregate, AggregateError, reconstitute_aggregate_state, validate_command
from pyjangle.command.command_response import CommandResponse
from pyjangle.event.event import Event
from pyjangle.log_tools.log_tools import initialize_jangle_logging
from pyjangle.test.test_types import CommandA, CommandB, EventA
from datetime import datetime

class TestAggregate(unittest.TestCase):

    def test_register_command_validator(self):
        class A(Aggregate):

            @validate_command(CommandA)
            def validateA(self, command: CommandA, next_version: int):
                self.a_called = True

            @validate_command(CommandB)
            def validateB(self, command: CommandB, next_version: int):
                self.b_called = True

        a = A(1)
        a.validate(CommandA())
        a.validate(CommandB())
        self.assertTrue(a.a_called)
        self.assertTrue(a.b_called)

    def test_register_reconstitue_aggregate_state(self):
        ATTR_NAME = "x"
        class A(Aggregate):
            @reconstitute_aggregate_state(EventA)
            def foo(self, event:EventA):
                setattr(self, ATTR_NAME, True)

        a = A(1)
        self.assertFalse(hasattr(a, ATTR_NAME))
        a.apply_events([EventA(id="a", version=1, created_at=datetime.now)])
        self.assertTrue(hasattr(a, ATTR_NAME))

    def test_can_reconstitute_aggregate_state(self):
        ATTR_NAME = "x"
        class A(Aggregate):

            def __init__(self, id: any):
                super().__init__(id)
                setattr(self, ATTR_NAME, 0)

            @reconstitute_aggregate_state(EventA)
            def foo(self, event:EventA):
                setattr(self, ATTR_NAME, getattr(self, ATTR_NAME) + 1)

        a = A(1)
        self.assertEqual(getattr(a, ATTR_NAME), 0)
        a.apply_events([EventA(id="a", version=1, created_at=datetime.now)])
        self.assertEqual(getattr(a, ATTR_NAME), 1)

    def test_when_commnd_validator_wrong_signature(self):
        with self.assertRaises(AggregateError):
            class A(Aggregate):

                @validate_command(CommandA)
                def foo(self):
                    pass
    def test_command_validator_supplies_next_version(self):
        class A(Aggregate):

            @validate_command(CommandA)
            def validateA(self, command: CommandA, next_version: int):
                self.updated_version = next_version

        a = A(1)
        self.assertEqual(0, a.version)
        a.validate(CommandA())
        self.assertEqual(1, a.updated_version)

    def test_command_validator_can_post_new_event(self):
        class A(Aggregate):

            @validate_command(CommandA)
            def validateA(self, command: CommandA, next_version: int):
                self.updated_version = next_version
                self._post_new_event(EventA(id=2, version=2, created_at=None))

        a = A(1)
        a.validate(CommandA())
        self.assertEqual(len(a.new_events), 1)

    def test_command_response_defaults_to_true(self):
        class A(Aggregate):

            @validate_command(CommandA)
            def validateA(self, command: CommandA, next_version: int):
                self.updated_version = next_version

        a = A(1)
        self.assertEqual(0, a.version)
        response = a.validate(CommandA())
        self.assertIsInstance(response, CommandResponse)
        self.assertTrue(response.is_success)

    def test_explicit_command_response_returned(self):
        class A(Aggregate):

            @validate_command(CommandA)
            def validateA(self, command: CommandA, next_version: int):
                return CommandResponse(False, "Foo")

        a = A(1)
        response = a.validate(CommandA())
        self.assertIsInstance(response, CommandResponse)
        self.assertFalse(response.is_success)
        self.assertEqual("Foo", response.data)

    def test_missing_command_validator(self):
        with self.assertRaises(AggregateError):
            class A(Aggregate):

                @validate_command(CommandA)
                def validateA(self, command: CommandA, next_version: int):
                    pass

            a = A(1)
            a.validate(CommandB())

    def test_missing_state_reconstitutor(self):
        with self.assertRaises(AggregateError):
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
            def foo(self, event:EventA):
                pass

        a = A(1)
        self.assertEqual(0, a.version)
        a.apply_events([version_2])
        self.assertEqual(2, a.version)
        a.apply_events([version_1])
        self.assertEqual(2, a.version)
        a.apply_events([version_3])
        self.assertEqual(3, a.version)
