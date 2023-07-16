import unittest
from unittest.mock import patch

from pyjangle.command.command_dispatcher import CommandDispatcherError, RegisterCommandDispatcher, command_dispatcher_instance, _command_dispatcher
from pyjangle.command.command_response import CommandResponse

@patch("pyjangle.command.command_dispatcher._command_dispatcher", None)
class TestCommandDispatcher(unittest.TestCase):

    def test_register_multiple_command_dispatcher(self):
        @RegisterCommandDispatcher
        def command_dispatcher1(command) -> CommandResponse: pass
        with self.assertRaises(CommandDispatcherError): 
            @RegisterCommandDispatcher
            def command_dispatcher2(command) -> CommandResponse: pass

    def test_register_command_dispatcher(self):
        @RegisterCommandDispatcher
        def command_dispatcher(command) -> CommandResponse: pass

        self.assertEqual(command_dispatcher_instance().__name__, command_dispatcher.__name__)

    def test_command_dispatcher_raise_error_when_none_registered(self):
        with self.assertRaises(CommandDispatcherError):
            command_dispatcher_instance()
        