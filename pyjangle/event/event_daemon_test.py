from asyncio import Queue, create_task
import asyncio
from datetime import timedelta
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
import pyjangle
from pyjangle.command.command_handler import handle_command
from pyjangle.event.event import Event
from pyjangle.event.event_daemon import begin_retry_failed_events_loop

from pyjangle.event.event_dispatcher import EventDispatcherError, RegisterEventDispatcher, begin_processing_committed_events
from pyjangle.event.event_repository import EventRepositoryError, event_repository_instance
from pyjangle.test.commands import CommandThatAlwaysSucceeds
from pyjangle.test.registration_paths import COMMITTED_EVENT_QUEUE, EVENT_DISPATCHER, EVENT_REPO
from pyjangle.test.transient_event_repository import TransientEventRepository

@patch(EVENT_DISPATCHER, None)
@patch(EVENT_REPO, new_callable=lambda : TransientEventRepository())
@patch(COMMITTED_EVENT_QUEUE, new_callable=lambda : Queue())
class TestEventDaemon(IsolatedAsyncioTestCase):
    async def test_daemon_retries_failed_events(self, *_):
        q = Queue()
        event_repo = event_repository_instance()
        @RegisterEventDispatcher
        async def foo(event: Event):
            if not hasattr(foo, "count"): foo.count = 0
            foo.count += 1
            if foo.count < 2:
                await q.put(True)
                raise Exception()
            await q.put(True)
        process_events_task = create_task(begin_processing_committed_events())
        await handle_command(CommandThatAlwaysSucceeds())
        await q.get()
        self.assertEqual(pyjangle.event.event_dispatcher._committed_event_queue.qsize(), 0)
        process_events_task = create_task(begin_retry_failed_events_loop(frequency_in_seconds=0, max_age_in_seconds=0))
        unhandled_events = [event async for event in event_repo.get_unhandled_events(time_delta=timedelta(seconds=0), batch_size=100)]
        self.assertEqual(len(list(unhandled_events)), 1)
        await q.get()
        self.assertEqual(foo.count, 2)

    async def test_when_no_event_repository_then_exception(self, *_):
        @RegisterEventDispatcher
        async def foo(event: Event):
            pass
        with patch(EVENT_REPO, None):
            with self.assertRaises(EventRepositoryError):
                await begin_retry_failed_events_loop(frequency_in_seconds=0)

    async def test_when_no_event_dispatcher_then_exception(self, *_):
        with patch(EVENT_DISPATCHER, None):
            with self.assertRaises(EventDispatcherError):
                await begin_retry_failed_events_loop(frequency_in_seconds=0)