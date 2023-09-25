from asyncio import create_task, events, sleep
import asyncio
from pyjangle import (
    LogToggles,
    log,
    JangleError,
    saga_repository_instance,
    tasks,
    DuplicateKeyError,
    SagaNotFoundError,
)


class SagaRetryError(JangleError):
    "Error occurred while retrying saga."
    pass


async def retry_sagas(max_batch_size: int):
    """Retries eligible sagas.

    There are many reasons why a saga would need to be retried such as a network or
    server outage.  It could also be a part of the normal flow of a saga that some
    command is expected to fail until it doesn't.  Regardless of the reason, this
    method will retry all sagas that have signaled that a retry is necessary.  Each saga
    is guaranteed to be retried independent of the outcome of the other sagas.

    Regarding the max_batch_size argument, be sure to specify value that takes system
    memory into account.

    Calling this method will probably involve using an external daemon, or by using
    `begin_retry_sagas_loop`.

    Args:
        max_batch_size:
            The maximum number of sagas to keep in memory at a time.

    """
    repo = saga_repository_instance()
    saga_ids = await repo.get_retry_saga_ids(max_batch_size)
    count = len(saga_ids)
    log(
        LogToggles.retrying_sagas,
        f"Retrying {count} sagas." if count else "Retryable sagas not found.",
    )
    for id in saga_ids:
        try:
            await retry_saga(id)
        except Exception as e:
            log(
                LogToggles.retrying_sagas_error,
                f"Error retrying saga with id '{id}'",
                exc_info=e,
            )


def begin_retry_sagas_loop(frequency_in_seconds: float, batch_size: int = 100):
    """Calls `retry_sagas` at a specified interval.

    Calling this method is roughly equivalent to using an external daemon to
    periodically call `retry_sagas`.  A reference to the created task is automatically a
    dded to `tasks.background_tasks` in order to prevent it from being garbage
    collected.  The task is also returned from this function call.  The first retry
    occurs after `frequency_in_seconds` seconds have elapsed.  Subsequent invocations
    occur `frequency_in_seconds` seconds *after* the previous invocation has completed
    meaning consecutive invocations will never overlap.

    Args:
        frequency_in_seconds:
            The interval *between* invocations of `retry_sagas`.
        batch_size:
            The maximum number of sagas to keep in memory at a time.

    Returns:
        A reference to the background task that is created.

    Raises:
        SagaRepositoryMissingError:
            Saga repository is not registered.
    """
    saga_repository_instance()

    async def _task():
        try:
            while True:
                await sleep(frequency_in_seconds)
                await retry_sagas(batch_size)
        except asyncio.CancelledError as e:
            log(LogToggles.cancel_retry_saga_loop, "Ended retry saga loop.")

    task = create_task(_task())
    tasks.background_tasks.append(task)
    return task


async def retry_saga(saga_id: any):
    """Retries a saga.

    Args:
        saga_id:
            The id of the saga to retry.

    Raises:
        SagaRetryError:
            Error occurred while retrying saga.
    """

    try:
        saga_repository = saga_repository_instance()
    except Exception as e:
        raise SagaRetryError() from e

    saga = await saga_repository.get_saga(saga_id)

    if not saga:
        raise SagaNotFoundError(
            f"Attempted to retry non-existent saga with id '{saga_id}'."
        )
    log(
        LogToggles.saga_retrieved,
        "Retrieved saga",
        {"saga_id": saga_id, "saga": vars(saga)},
    )
    if saga.is_complete or saga.is_timed_out:
        return
    await saga.evaluate()
    if saga.is_dirty:
        try:
            await saga_repository.commit_saga(saga)
        except DuplicateKeyError as e:
            log(
                LogToggles.saga_duplicate_key,
                "Concurrent saga execution detected.  This is unlikely and could indicate an issue.",
                {
                    "saga_id": saga_id,
                    "saga_type": str(type(saga)),
                    "saga": vars(saga),
                },
            )
            return
        log(
            LogToggles.saga_committed,
            "Committed saga to saga store.",
            {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": vars(saga)},
        )
    else:
        log(
            LogToggles.saga_nothing_happened,
            "Saga state was not changed.",
            {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": vars(saga)},
        )
