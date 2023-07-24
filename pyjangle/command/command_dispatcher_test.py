import unittest
from unittest.mock import patch

from pyjangle import CommandDispatcherError, RegisterCommandDispatcher, command_dispatcher_instance
from pyjangle import CommandResponse
from pyjangle.test.registration_paths import COMMAND_DISPATCHER


@patch(COMMAND_DISPATCHER, None)
class TestCommandDispatcher(unittest.IsolatedAsyncioTestCase):

    async def test_register_multiple_command_dispatcher(self):
        @RegisterCommandDispatcher
        async def command_dispatcher1(command) -> CommandResponse: pass
        with self.assertRaises(CommandDispatcherError):
            @RegisterCommandDispatcher
            async def command_dispatcher2(command) -> CommandResponse: pass

    async def test_register_command_dispatcher(self):
        @RegisterCommandDispatcher
        async def command_dispatcher(command) -> CommandResponse: pass

        self.assertEqual(command_dispatcher_instance().__name__,
                         command_dispatcher.__name__)

    async def test_command_dispatcher_raise_error_when_none_registered(self):
        with self.assertRaises(CommandDispatcherError):
            command_dispatcher_instance()
