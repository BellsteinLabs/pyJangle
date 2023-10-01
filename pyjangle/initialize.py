"""Bootstrapping methods to quickly configure a pyjangle application.

`initialize_pyjangle` 
    Facilitates the registration of all necessary components to get an 
    application running.  Defaults are provided for necessary components, however, 
    the `InMemoryEventRepository`, `InMemorySagaRepository`, and the 
    `InMemorySnapshotRepository` are not suitable for production code.  They are not 
    durable, and will only persist data for as long as the process is running.  

`init_background_tasks`
    Begins several background tasks that may be needed depending on the architecture of
    the system being implemented.  For example, if there is an external process that is 
    processing committed events, set `process_committed_events` to False.  This method 
    *MUST* be called where an active event loop is running.
"""

import asyncio
from datetime import timedelta
from typing import Awaitable, Callable
from pyjangle import (
    InMemoryEventRepository,
    InMemorySnapshotRepository,
    InMemorySagaRepository,
    Event,
    default_event_dispatcher,
    handle_command,
    CommandResponse,
    Command,
    register_command_dispatcher,
    register_event_dispatcher,
    register_deserializer,
    register_serializer,
    RegisterEventRepository,
    RegisterSagaRepository,
    RegisterSnapshotRepository,
    register_event_id_factory,
    set_batch_size,
    set_saga_retry_interval,
    set_events_ready_for_dispatch_queue_size,
    begin_retry_failed_events_loop,
    begin_processing_committed_events,
    begin_retry_sagas_loop,
    get_saga_retry_interval,
    get_batch_size,
    get_failed_events_retry_interval,
    get_failed_events_max_age,
    default_event_id_factory,
    JangleError,
)


class BackgroundTasksError(JangleError):
    "Background tasks started without a running event loop."
    pass


def init_background_tasks(
    process_committed_events: bool = True,
    retry_sagas: bool = True,
    saga_retry_interval_seconds: int = get_saga_retry_interval(),
    saga_batch_size: int = get_batch_size(),
    retry_failed_events: bool = True,
    failed_events_batch_size: int = get_batch_size(),
    failed_events_retry_interval_seconds: int = get_failed_events_retry_interval(),
    failed_events_age: int = get_failed_events_max_age(),
):
    """Begins background tasks that may be useful depending on the system architecture.

    Args:
        process committed_events:
            Begins a task that processes committed events through the registered event
            dispatcher.
        retry_sagas:
            Begins a task that retries sagas that meet the retry criteria.
        saga_retry_interval_in_seconds:
            Specifies a frequency to run the `retry_sagas` daemon.
        saga_batch_size:
            The number of sagas that can be concurrently buffered in memory.
        retry_failed_events:
            Begins a task that retries events that failed during dispatch.
        failed_events_batch_size:
            The number of failed events that can be concurrently buffered in memory.
        failed_events_retry_interval_seconds:
            Specifies a frequency to run the `retry_failed_events` daemon.
        failed_events_age:
            An age, after which, an event is considered to be failed if not marked
            completed.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError as e:
        raise BackgroundTasksError("Event loop not running.") from e

    if process_committed_events:
        begin_processing_committed_events()
    if retry_sagas:
        begin_retry_sagas_loop(saga_retry_interval_seconds, saga_batch_size)
    if retry_failed_events:
        begin_retry_failed_events_loop(
            frequency_in_seconds=failed_events_retry_interval_seconds,
            batch_size=failed_events_batch_size,
            max_age_time_delta=timedelta(seconds=failed_events_age),
        )


def initialize_pyjangle(
    command_dispatcher_func: Callable[
        [Command], Awaitable[CommandResponse]
    ] = handle_command,
    event_dispatcher_func: Callable[
        [Event, Callable[[any], Awaitable[any]]], Awaitable
    ] = default_event_dispatcher,
    deserializer: Callable[[any], any] = None,
    serializer: Callable[[any], any] = None,
    event_id_factory: Callable[[None], any] = default_event_id_factory,
    event_repository_type: type = InMemoryEventRepository,
    saga_repository_type: type = InMemorySagaRepository,
    snapshot_repository_type: type = InMemorySnapshotRepository,
    batch_size: int = None,
    saga_retry_interval_seconds: int = None,
    dispatch_queue_size: int = None,
):
    """Registers all necessary components.

    Calling this method is not necessary to bootstrap an application.  Each component
    can instead be registered separately.  In fact, the implementation of this function
    merely calls those manual registration methods with reasonable defaults.

    Args:
        command_dispatcher_func:
            See the `register_command_dispatcher` decorator.
        event_dispatcher_func:
            See the `register_event_dispatcher` decorator.
        deserializer:
            See the `register_deserializer` decorator.  This component is not necessary
            when using an 'InMemory' saga, event, and snapshot repositories.
        serializer:
            See the `register_serializer` decorator.  This component is not necessary
            when using an 'InMemory' saga, event, and snapshot repositories.
        event_id_factory:
            See the `register_event_id_factory` decorator.
        event_repository_type:
            See the `RegisterEventRepository` decorator.
        saga_repository_type:
            See the `RegisterSagaRepository` decorator.
        snapshot_repository_type:
            See the `RegisterSnapshotRepository` decorator.
        batch_size:
            See `set_batch_size`.
        saga_retry_interval_seconds:
            See `set_saga_retry_interval`.
        dispatch_queue_size:
            See `set_events_ready_for_dispatch_queue_size`.
    """
    register_command_dispatcher(command_dispatcher_func)
    register_event_dispatcher(event_dispatcher_func)
    if deserializer:
        register_deserializer(deserializer)
    if serializer:
        register_serializer(serializer)
    register_event_id_factory(event_id_factory)
    RegisterEventRepository(event_repository_type)
    RegisterSagaRepository(saga_repository_type)
    RegisterSnapshotRepository(snapshot_repository_type)
    if batch_size:
        set_batch_size(batch_size)
    if saga_retry_interval_seconds:
        set_saga_retry_interval(saga_retry_interval_seconds)
    if dispatch_queue_size:
        set_events_ready_for_dispatch_queue_size(dispatch_queue_size)
