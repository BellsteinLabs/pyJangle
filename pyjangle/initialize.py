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
    RegisterCommandDispatcher,
    RegisterEventDispatcher,
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
    """Registers all necessary components."""
    RegisterCommandDispatcher(command_dispatcher_func)
    RegisterEventDispatcher(event_dispatcher_func)
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
