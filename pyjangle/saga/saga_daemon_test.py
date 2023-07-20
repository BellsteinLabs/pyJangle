from asyncio import Queue, create_task
import asyncio
from datetime import datetime
from typing import List
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from pyjangle.command.command_response import CommandResponse
from pyjangle.saga.saga_daemon import begin_retry_sagas_loop
from pyjangle.saga.saga_metadata import SagaMetadata
from pyjangle.saga.saga_repository import SagaRepositoryError, saga_repository_instance
from pyjangle.test.events import EventThatContinuesSaga
from pyjangle.test.registration_paths import COMMAND_DISPATCHER, SAGA_REPO
from pyjangle.test.sagas import SagaForTesting, SagaForTestingRetryLogic
from pyjangle.test.transient_saga_repository import TransientSagaRepository

SAGA_ID = 42
_message_queue = Queue()
EXCEPTION_RAISED = 1
COMMAND_RESPONSE_SENT = 2

def get_command_dispatcher_that_fails_on_first_call():
    count = 0
    async def inner(command):
        nonlocal count
        global _message_queue
        if count == 0:
            count += 1
            await _message_queue.put(EXCEPTION_RAISED)
            raise Exception()
        await _message_queue.put(COMMAND_RESPONSE_SENT)
        return CommandResponse(True)
    return inner

@patch(COMMAND_DISPATCHER, new_callable=lambda : get_command_dispatcher_that_fails_on_first_call())
@patch(SAGA_REPO, new_callable=lambda : TransientSagaRepository())
class TestSagaDaemon(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        global _message_queue
        _message_queue = Queue()

    async def test_when_saga_needs_retry_then_retried_by_saga_daemon_and_no_longer_marked_as_needs_retry(self, *_):
        global _message_queue
        events = [EventThatContinuesSaga(version=1)]
        metadata = SagaMetadata(id=SAGA_ID, type=SagaForTestingRetryLogic, retry_at=datetime.min.isoformat(), timeout_at=None, is_complete=False, is_timed_out=False)
        await saga_repository_instance().commit_saga(metadata=metadata, events=events)
        task = create_task(begin_retry_sagas_loop(0))
        first = await asyncio.wait_for(_message_queue.get(), 2)
        second = await asyncio.wait_for(_message_queue.get(), 2)
        self.assertEqual(first, EXCEPTION_RAISED)
        self.assertEqual(second, COMMAND_RESPONSE_SENT)
        async def get_retryable_sagas():
            while True:
                if not await saga_repository_instance().get_retry_saga_metadata(max_count=10):
                    return True
        self.assertTrue(await asyncio.wait_for(get_retryable_sagas(), 2))
        task.cancel()
        await asyncio.wait_for(task, 2)


    async def test_when_no_saga_repository_then_exception(self, *_):
        with patch(SAGA_REPO, new=None):
            with self.assertRaises(SagaRepositoryError):
                await begin_retry_sagas_loop(0)