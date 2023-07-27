from asyncio import events, sleep
import asyncio
from pyjangle.logging.logging import LogToggles, log
from pyjangle.saga.saga_handler import handle_saga_event, retry_saga
from pyjangle.saga.saga_repository import SagaRepositoryError, saga_repository_instance

# TODO: Need a solution that can retry all sagas
# by streaming them from the db and letting
# the caller know when the operation is done
# to avoid overlapping calls


async def retry_sagas(max_batch_size: int):
    """Entrypoint for saga retry daemon

    This is an entrypiont for a daemon (not supplied) 
    that periodically queries for sagas that require
    retry, likely because of a network outage or 
    similar issue.  Because memory is limited,
    use the max_batch_size parameter to process only 
    a few at a time."""
    repo = saga_repository_instance()
    saga_ids: list[str] = await repo.get_retry_saga_metadata(max_batch_size)
    count = len(saga_ids)
    log(LogToggles.retrying_sagas,
        f"Retrying {count} sagas." if count else "Retryable sagas not found.")
    for id in saga_ids:
        await retry_saga(id)


async def begin_retry_sagas_loop(frequency_in_seconds: float, batch_size: int = 100):
    try:
        while True:
            await sleep(frequency_in_seconds)
            await retry_sagas(batch_size)
    except asyncio.CancelledError as e:
        log(LogToggles.cancel_retry_saga_loop, "Ended retry saga loop.")
