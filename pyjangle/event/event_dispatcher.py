from asyncio import Queue, Task, create_task
import inspect
import os
from typing import Awaitable, Callable, List

from pyjangle import (
    JangleError,
    VersionedEvent,
    EventHandlerMissingError,
    LogToggles,
    log,
    event_repository_instance,
    event_type_to_handler_instance,
    get_events_ready_for_dispatch_queue_size,
)
from pyjangle import background_tasks

# Registered event dispatcher singleton.
_event_dispatcher = None

# Queue of events that have been committed to the event store and are ready to
# dispatched elsewhere within the current process.
_committed_event_queue = Queue(maxsize=get_events_ready_for_dispatch_queue_size())


class EventDispatcherBadSignatureError(JangleError):
    "Event dispatcher signature is invalid."
    pass


class EventDispatcherMissingError(JangleError):
    "Event dispatcher not registered."
    pass


class DuplicateEventDispatcherError(JangleError):
    "Attempted to register multiple event dispatchers."
    pass


def begin_processing_committed_events() -> Task:
    """Begins processing events that are ready to be dispatched.

    Calling this method will begin a background task that continuously processes events
    that are ready to be dispatched on a background task.  A reference to the created
    task is automatically added to `tasks.background_tasks` in order to prevent it from
    being garbage collected.  The task is also returned from this function call.

    Returns:
        A reference to the background task that is created.

    Raises:
        EventDispatcherMissingError:
            Event dispatcher not registered.
    """

    event_dispatcher = event_dispatcher_instance()
    if not event_dispatcher:
        raise EventDispatcherMissingError(
            "Unable to process committed events--no event dispatcher registered"
        )
    log(LogToggles.event_dispatcher_ready, "Event dispatcher ready to process events")

    async def _task():
        while True:
            event = await _committed_event_queue.get()
            await _invoke_registered_event_dispatcher(event)

    task = create_task(_task())
    background_tasks.append(task)
    return task


async def _invoke_registered_event_dispatcher(event: VersionedEvent):
    try:
        event_repo = event_repository_instance()
        await _event_dispatcher(event, event_repo.mark_event_handled)
    except Exception as e:
        log(
            LogToggles.event_dispatching_error,
            "Encountered an error while dispatching event",
            {"event_type": str(type(event)), "event": vars(event)},
            exc_info=e,
        )


async def enqueue_committed_event_for_dispatch(event: VersionedEvent):
    "Enqueues a committed event for dispatch."
    await _committed_event_queue.put(event)


def RegisterEventDispatcher(wrapped: Callable):
    """Decorates a function that dispatches events.

    The process of dispatching an event involves moving an event to where it needs to be
    in order to be processed and, if successful, to subsequently inform the sender that
    the event was processed via the `completed_callback`.  See the default
    implementation in `default_event_dispatcher`.

    *Failure to mark an event completed will cause it to be processed repeatedly ad
    infinitum by a retry mechanism.*

    In an implementation of this library where all components live in a single process,
    a typical use-case would be that once events are committed to an event store,
    and in the absence of a durable message queue, the events could be dispatched
    elsewhere within the process (typically an event handler) for processing.  The
    dispatcher would them mark the event as completed on the event store.

    Signature:
        async def func_name(
            event: Event,
            completed_callback: Callable[[any], Awaitable[any]]
        )

    Raises:
        EventDispatcherBadSignatureError:
            Event dispatcher signature is invalid.
        DuplicateEventDispatcherError:
            Attempted to register multiple event dispatchers.
    """

    if (
        not callable(wrapped)
        or len(inspect.signature(wrapped).parameters) != 2
        or not inspect.iscoroutinefunction(wrapped)
    ):
        raise EventDispatcherBadSignatureError(
            """@RegisterEventDispatcher must decorate a method with 2 parameters: 
            async def func_name(
                event: Event, 
                completed_callback: Callable[[any], Awaitable[any]]
            )
            """
        )
    global _event_dispatcher
    if _event_dispatcher != None:
        raise DuplicateEventDispatcherError(
            "Cannot register multiple event dispatchers: "
            + str(_event_dispatcher)
            + ", "
            + str(wrapped)
        )
    _event_dispatcher = wrapped
    log(
        LogToggles.event_dispatcher_registration,
        "Event dispatcher registered",
        {"event_dispatcher_type": wrapped.__module__ + "." + wrapped.__name__},
    )
    return wrapped


def event_dispatcher_instance() -> (
    Callable[[List[VersionedEvent], Callable[[VersionedEvent], None]], None]
):
    "Returns the registered event dispatcher singleton."
    return _event_dispatcher


async def default_event_dispatcher(
    event: VersionedEvent, completed_callback: Callable[[any], Awaitable]
):
    """Dispatches events to registered events handlers.

    Searches for registered event handlers based on the event type and invokes each of
    them, in-turn.  If an event handler raises an error, all subsequent event handlers,
    in the case that multiple are registered, will *not* be invoked.

    Args:
        event:
            The event to dispatch.
        completed_callback:
            Callback to invoke with the event ID after it is successfully dispatched.

    Raises:
        EventHandlerMissingError:
            Event handler not registered.
        EventHandlerError:
            An error occurred while handling an event.
    """

    event_type = type(event)
    handler_map = event_type_to_handler_instance()
    if not event_type in handler_map:
        raise EventHandlerMissingError(
            "No event handler registered for " + str(event_type)
        )
    for handler in handler_map[event_type]:
        await handler(event)
    await completed_callback(event.id)

    return


def default_event_dispatcher_with_blacklist(
    *blacklisted_event_types: type,
) -> Awaitable:
    """Invokes the default event handler while ignoring blacklisted events.

    If an event is committed, and no event handlers are regsitered, an
    `EventHandlerMissingError` will be thrown.  To prevent this behavior, add the event
    to the `blacklisted_event_types` argument.

    See: `default_event_dispatcher`

    Args:
        blacklisted_event_types:
            A list of event types that should be ignored.
    """

    async def wrapper(event: VersionedEvent, completed_callback):
        await default_event_dispatcher(event, completed_callback) if not type(
            event
        ) in blacklisted_event_types else None

    return wrapper
