from asyncio import Queue, create_task, wait_for, Task
from datetime import timedelta
import unittest
from unittest.mock import patch
from pyjangle import tasks
from pyjangle.command.command_handler import handle_command
from pyjangle.event.event import Event

from pyjangle.event.event_dispatcher import EventDispatcherError, RegisterEventDispatcher, begin_processing_committed_events, event_dispatcher_instance
from pyjangle.event.event_repository import event_repository_instance
from pyjangle.test.registration_paths import COMMITTED_EVENT_QUEUE, EVENT_DISPATCHER, EVENT_REPO
from pyjangle.test.test_types import CommandThatAlwaysSucceeds
from pyjangle.test.transient_event_repository import TransientEventRepository

@patch(COMMITTED_EVENT_QUEUE, new_callable=lambda : Queue())
@patch(EVENT_REPO, new_callable=lambda : TransientEventRepository())
@patch(EVENT_DISPATCHER, None)
@patch("pyjangle.event.event_dispatcher._event_dispatcher", None)
class TestEventDispatcher(unittest.IsolatedAsyncioTestCase):

    def test_register_multiple_event_dispatcher(self, *_): 
        @RegisterEventDispatcher
        def Event_dispatcher1(_): pass
        with self.assertRaises(EventDispatcherError):
            @RegisterEventDispatcher
            def Event_dispatcher2(_): pass

    def test_register_event_dispatcher_with_wrong_function_signature_raises_error(self, *_):
        with self.assertRaises(EventDispatcherError):
            @RegisterEventDispatcher
            def Event_dispatcher(): pass

    def test_register_event_dispatcher(self, *_):
        @RegisterEventDispatcher
        def Event_dispatcher(event: Event): pass

        self.assertEqual(event_dispatcher_instance().__name__, Event_dispatcher.__name__)

    async def test_process_committed_events(self, *_):
        q = Queue()
        @RegisterEventDispatcher
        async def foo(event: Event):
            await q.put(event)

        await handle_command(CommandThatAlwaysSucceeds())
        task = create_task(begin_processing_committed_events())
        tasks.background_tasks.append(task)
        event = await wait_for(q.get(), 1)

        self.assertTrue(event)

    async def test_error_when_process_committed_events_and_no_event_dispatcher_registered(self, *_):
        with self.assertRaises(EventDispatcherError):
            task = create_task(begin_processing_committed_events())
            await task

    async def test_error_when_processing_committed_events_doesnt_kill_processing_loop(self, *_):
        q = Queue()
        @RegisterEventDispatcher
        async def foo(event: Event):
            await q.put(True)
            raise Exception()
        task = create_task(begin_processing_committed_events())
        await handle_command(CommandThatAlwaysSucceeds())
        await handle_command(CommandThatAlwaysSucceeds())
        result = await q.get()
        result2 = await q.get()
        self.assertTrue(result and result2)

    async def test_error_when_processing_committed_events_doesnt_mark_events_as_handled(self, *_):
        q = Queue()
        event_repo = event_repository_instance()
        @RegisterEventDispatcher
        async def foo(event: Event):
            try:
                raise Exception()
            finally:
                await q.put(True)
        task = create_task(begin_processing_committed_events())
        await handle_command(CommandThatAlwaysSucceeds())
        await handle_command(CommandThatAlwaysSucceeds())
        await q.get()
        await q.get()
        unhandled_events = [event async for event in event_repo.get_unhandled_events(time_delta=timedelta(seconds=0), batch_size=100)]
        self.assertEqual(len(list(unhandled_events)), 2)

    async def test_processing_committed_events_successfully_marks_them_as_handled(self, *_):
        q = Queue()
        @RegisterEventDispatcher
        async def foo(event: Event):
            await q.put(True)
        task = create_task(begin_processing_committed_events())
        await handle_command(CommandThatAlwaysSucceeds())
        await handle_command(CommandThatAlwaysSucceeds())
        await q.get()
        await q.get()
        event_repo = event_repository_instance()
        unhandled_events = [event async for event in event_repo.get_unhandled_events(time_delta=timedelta(seconds=0), batch_size=100)]
        self.assertEqual(len(list(unhandled_events)), 0)