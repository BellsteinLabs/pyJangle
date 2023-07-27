import asyncio
import inspect
from typing import Awaitable, Callable
from pyjangle import CommandResponse

from pyjangle import JangleError
from pyjangle.logging.logging import LogToggles, log

# This is where the command dispatcher is kept.
# Access it via command_dispatcher_instance().
_command_dispatcher_instance = None


class CommandDispatcherError(JangleError):
    pass


def RegisterCommandDispatcher(wrapped: Callable[[any], CommandResponse]):
    """Decorates function that sends commands to wherever they're in a big hurry to get to.

    SIGNATURE
    ---------
    def command_dispatcher_name(command: Command) -> CommandRepsonse

    This can mean a few things.  Let's say that you have a saga that 
    needs to dispatch events to progress its state.  It would use the 
    registered command dispatcher to dispatch those commands.  It could
    be that the commands are destined to go to the local handle_command
    method, or maybe they're sent to a durable message bus for some 
    other process on a remote machine to process.  It all depends on 
    your architecture, so whatever you need, put that code in the 
    method decorated with this.  

    It MIGHT also be the case that some events are locally dispatched
    while others are dispatched remotely.  Just put that logic in this 
    method--for simplicity, this framework only supports a single 
    command dispatcher per process and leaves anything more 
    complicated than that as an exercise for the implementor.

    THROWS
    ------
    CommandDispatcherError when multiple methods are registered."""
    if not asyncio.iscoroutinefunction(wrapped):
        raise CommandDispatcherError(
            "@RegisterCommandDispatcher must decorate a coroutine (async) method with signature: async def func_name(command: Command) -> CommandResponse")
    if not len(inspect.signature(wrapped).parameters) == 1:
        raise CommandDispatcherError(
            "Command dispatcher function should only have one parameter: async def func_name(command: Command) -> CommandResponse")
    global _command_dispatcher_instance
    if _command_dispatcher_instance != None:
        raise CommandDispatcherError(
            "Cannot register multiple command dispatchers: " + str(type(_command_dispatcher_instance)) + ", " + wrapped.__name__)
    _command_dispatcher_instance = wrapped
    log(LogToggles.command_dispatcher_registration, "Registering command dispatcher", {
        "command_dispatcher_type": str(type(wrapped))})
    return wrapped


def command_dispatcher_instance() -> Callable[[any], CommandResponse]:
    """Returns the singleton instance of the registered command dispatcher."""
    if not _command_dispatcher_instance:
        raise CommandDispatcherError
    return _command_dispatcher_instance


__all__ = [CommandDispatcherError.__name__,
           RegisterCommandDispatcher.__name__, command_dispatcher_instance.__name__]
