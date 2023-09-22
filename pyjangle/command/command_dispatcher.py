import asyncio
import inspect
from typing import Awaitable, Callable
from pyjangle import CommandResponse, JangleError, LogToggles, log

# Registered command dispatcher accessible via `command_dispatcher_instance()`.
_command_dispatcher_instance = None


class CommandDispatcherBadSignatureError(JangleError):
    "Command dispatcher signature is invalid."
    pass


class DuplicateCommandDispatcherError(JangleError):
    "Registering multiple command dispatchers is not allowed."
    pass


class CommandDispatcherNotRegisteredError(JangleError):
    "Command dispatcher has not been registered."
    pass


def RegisterCommandDispatcher(wrapped: Callable[[any], CommandResponse]):
    """Decorates a function that forwards commands originating from the current process.

    When commands originate within this process, as opposed to external commands, this
    command dispatcher routes commands appropriately.  The destination could be either
    remote or local to this process depending on the system design.  In either case, the
    command could be forwarded directly to another server or to an intermediary such as
    a durable message queue.  Commonly, this component is leveraged by a saga to forward
    commands that originate from within the saga.

    Signature:
        async def command_dispatcher_func(command: Command) -> CommandRepsonse:

    Raises:
        DuplicateCommandDispatcher: Registering multiple command dispatchers is not
          allowed.
        CommandDispatcherBadSignatureError: Command dispatcher signature is invalid.
    """
    if (
        not asyncio.iscoroutinefunction(wrapped)
        or not len(inspect.signature(wrapped).parameters) == 1
    ):
        raise CommandDispatcherBadSignatureError(
            """@RegisterCommandDispatcher must decorate a coroutine (async) method with 
            signature: async def command_dispatcher_func(command: Command) -> 
            CommandRepsonse:"""
        )
    global _command_dispatcher_instance
    if _command_dispatcher_instance != None:
        raise DuplicateCommandDispatcherError(
            "Cannot register multiple command dispatchers: "
            + str(type(_command_dispatcher_instance))
            + ", "
            + wrapped.__name__
        )
    _command_dispatcher_instance = wrapped
    log(
        LogToggles.command_dispatcher_registration,
        "Registering command dispatcher",
        {"command_dispatcher_type": str(wrapped)},
    )
    return wrapped


def command_dispatcher_instance() -> Awaitable[Callable[[any], CommandResponse]]:
    """Returns the singleton instance of the registered command dispatcher.

    Raises:
        CommandDispatcherNotRegisteredError: Command dispatcher has not been registered.
    """
    if not _command_dispatcher_instance:
        raise CommandDispatcherNotRegisteredError()
    return _command_dispatcher_instance
