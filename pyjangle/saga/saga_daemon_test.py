from asyncio import create_task
from datetime import datetime
from typing import List
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from pyjangle.event.event import Event
from pyjangle.saga.saga import Saga
from pyjangle.saga.saga_daemon import begin_retry_sagas_loop
from pyjangle.saga.saga_metadata import SagaMetadata
from pyjangle.saga.saga_repository import saga_repository_instance
from pyjangle.test.events import EventThatContinuesSaga
from pyjangle.test.sagas import SagaForTesting
from pyjangle.test.transient_saga_repository import TransientSagaRepository

SAGA_ID = 42

@patch("pyjangle.saga.saga_repository.__registered_saga_repository", new_callable=lambda : TransientSagaRepository())
class TestSagaDaemon(IsolatedAsyncioTestCase):
    async def test_when_saga_needs_retry_then_retried_by_saga_daemon(seld, *_):
        events = [EventThatContinuesSaga(version=1)]
        metadata = SagaMetadata(id=SAGA_ID, type=SagaForTesting, retry_at=datetime.min, timeout_at=None, is_complete=False, is_timed_out=False)
        await saga_repository_instance().commit_saga(metadata=metadata, events=events)
        task = create_task(begin_retry_sagas_loop(0))

    async def test_when_no_saga_repository_then_exception(self, *_):
        pass