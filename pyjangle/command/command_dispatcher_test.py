import unittest

from pyjangle import (
    DuplicateCommandDispatcherError,
    CommandDispatcherNotRegisteredError,
    CommandDispatcherBadSignatureError,
    CommandResponse,
    RegisterCommandDispatcher,
    command_dispatcher_instance,
)
from pyjangle.test.reset import ResetPyJangleState


@ResetPyJangleState
class TestCommandDispatcher(unittest.IsolatedAsyncioTestCase):
    async def test_register_multiple_command_dispatcher(self, *_):
        @RegisterCommandDispatcher
        async def command_dispatcher1(command) -> CommandResponse:
            pass

        with self.assertRaises(DuplicateCommandDispatcherError):

            @RegisterCommandDispatcher
            async def command_dispatcher2(command) -> CommandResponse:
                pass

    async def test_bad_signature(self, *_):
        with self.assertRaises(CommandDispatcherBadSignatureError):

            @RegisterCommandDispatcher
            def command_dispatcher2(command) -> CommandResponse:
                pass

    async def test_register_aggregate_dispatcher(self, *_):
        @RegisterCommandDispatcher
        async def command_dispatcher(command) -> CommandResponse:
            pass

        self.assertEqual(
            command_dispatcher_instance().__name__, command_dispatcher.__name__
        )

    async def test_command_dispatcher_raise_error_when_none_registered(self, *_):
        with self.assertRaises(CommandDispatcherNotRegisteredError):
            command_dispatcher_instance()
