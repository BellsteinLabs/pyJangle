from asyncio import Task, create_task, sleep, CancelledError
from datetime import timedelta
from pyjangle import (
    EventDispatcherMissingError,
    event_dispatcher_instance,
    event_repository_instance,
    LogToggles,
    JangleError,
    get_batch_size,
    get_failed_events_retry_interval,
    log,
    get_failed_events_max_age,
)
from pyjangle.registration import background_tasks


class RetryFailedEventsError(JangleError):
    "An error occurred while retrying failed events."
    pass


async def retry_failed_events(
    batch_size: int = get_batch_size(),
    max_age_time_delta: timedelta = timedelta(seconds=30),
):
    """Retries failed events.

    When an event handler fails to process an event without an exception, the event is
    not marked as completed and will be retried.  Event repositories provide an
    interface by which failed events can be retrieved.  Calling this method retrieves
    those events and redispatches them.  Each event is guaranteed to be retried once
    independent of the outcome of other events.

    Regarding the batch_size argument, be sure to specify value that takes system memory
    into account.  And for the max_age_time_delta argument, keep in mind that just
    because an event is marked as not completed doesn't mean its completion is not
    imminent.  However, after some period of time passes, it's very likely that the
    event dispatch has failed and needs to be retried.  It is possible that
    circumstances (network intermittency) can delay successful dispatch, and so an event
    may be retried even if the first attempt is still in progress.  It is important to
    ensure that all event handlers are idempotent.  Also, ensure that a call to
    retry_failed_events does *not* overlap a previous one.

    Calling this method will probably involve using an external daemon, or by using
    `begin_retry_failed_events_loop`.

    Args:
        batch_size:
            The number of events to keep in memory at a time.
        max_age_in_seconds:
            Events are retried when this period of time has elapsed since it was
            published, and it has not been marked as completed.

    Raises:
        RetryFailedEventsError:
            An error occurred while retrying failed events.
    """

    try:
        repo = event_repository_instance()
        unhandled_events = repo.get_unhandled_events(
            batch_size=batch_size, time_delta=max_age_time_delta
        )
        log(
            LogToggles.retrying_failed_events,
            f"Retrying failed events...",
        )
        count = 0
        async for event in unhandled_events:
            count += 1
            event_repo = event_repository_instance()
            event_dispatcher = event_dispatcher_instance()
            try:
                await event_dispatcher(event, event_repo.mark_event_handled)
            except Exception as e:
                log(
                    LogToggles.event_failed_on_retry,
                    "An event failed to process on retry.",
                    {
                        "event_type": str(type(event)),
                        "event": vars(event),
                    },
                    exc_info=e,
                )
        log(
            LogToggles.retrying_failed_events,
            f"Finished retrying {count} failed events",
        )
    except Exception as e:
        log(
            LogToggles.retrying_failed_events_error,
            "Error encountered while retrying failed events.",
            exc_info=e,
        )
        raise RetryFailedEventsError from e


def begin_retry_failed_events_loop(
    frequency_in_seconds: float = get_failed_events_retry_interval(),
    batch_size: int = get_batch_size(),
    max_age_time_delta: timedelta = timedelta(seconds=get_failed_events_max_age()),
) -> Task:
    """Calls `retry_failed_events` at a specified interval.

    Instead of having an external daemon periodically retry failed events, calling this
    method will begin a background task that periodically retry failed events.  A
    reference to the created task is automatically added to `tasks.background_tasks` in
    order to prevent it from being garbage collected.  The task is also returned from
    this function call.  The first retry occurs after `frequency_in_seconds` seconds have
    elapsed.  Subsequent invocations occur `frequency_in_seconds` seconds *after* the
    previous invocation has completed meaning consecutive invocations will never
    overlap.

    Args:
        frequency_in_seconds:
            The interval *between* invocations of `retry_failed_events`.
        batch_size:
            The number of events to keep in memory at a time.
        max_age_time_delta:
            Events are retried when this period of time has elapsed since it was
            published, and it has not been marked as completed.

    Returns:
        A reference to the background task that is created.

    Raises:
        EventDispatcherMissingError:
            Event dispatcher not registered.
        EventRepositoryMissingError:
            Event repository not registered.

    Raises (Returned Background Task):
        RetryFailedEventsError:
            An error occurred while retrying failed events.
    """

    if not event_dispatcher_instance():
        raise EventDispatcherMissingError("No event dispatcher registered.")
    event_repository_instance()

    async def _task():
        while True:
            await sleep(frequency_in_seconds)
            try:
                await retry_failed_events(
                    batch_size=batch_size, max_age_time_delta=max_age_time_delta
                )
            except Exception as e:
                if isinstance(e, CancelledError):
                    log(LogToggles.cancel_retry_event_loop, "Ending retry event loop.")

    task = create_task(_task())
    background_tasks.background_tasks.append(task)
    return task
