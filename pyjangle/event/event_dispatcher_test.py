import unittest
from asyncio import Queue, create_task, wait_for
from datetime import timedelta
from unittest.mock import patch

import pyjangle.test.aggregates
from pyjangle import (VersionedEvent, EventDispatcherError, RegisterEventDispatcher,
                      begin_processing_committed_events,
                      event_dispatcher_instance, event_repository_instance,
                      handle_command, tasks)
from pyjangle.test.commands import CommandThatAlwaysSucceeds
from pyjangle.test.registration_paths import (COMMITTED_EVENT_QUEUE,
                                              EVENT_DISPATCHER, EVENT_REPO)
from pyjangle.test.transient_event_repository import TransientEventRepository


@patch(COMMITTED_EVENT_QUEUE, new_callable=lambda: Queue())
@patch(EVENT_REPO, new_callable=lambda: TransientEventRepository())
@patch(EVENT_DISPATCHER, None)
class TestEventDispatcher(unittest.IsolatedAsyncioTestCase):

    async def test_register_multiple_event_dispatcher(self, *_):
        @RegisterEventDispatcher
        async def Event_dispatcher1(_): pass
        with self.assertRaises(EventDispatcherError):
            @RegisterEventDispatcher
            def Event_dispatcher2(_): pass

    async def test_register_event_dispatcher_with_wrong_function_signature_raises_error(self, *_):
        with self.assertRaises(EventDispatcherError):
            @RegisterEventDispatcher
            async def Event_dispatcher(): pass

    async def test_register_event_dispatcher_not_coroutine_then_error(self, *_):
        with self.assertRaises(EventDispatcherError):
            @RegisterEventDispatcher
            def Event_dispatcher(): pass

    async def test_register_event_dispatcher(self, *_):
        @RegisterEventDispatcher
        async def Event_dispatcher(event: VersionedEvent): pass

        self.assertEqual(event_dispatcher_instance().__name__,
                         Event_dispatcher.__name__)

    async def test_process_committed_events(self, *_):
        q = Queue()

        @RegisterEventDispatcher
        async def foo(event: VersionedEvent):
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
        async def foo(event: VersionedEvent):
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
        async def foo(event: VersionedEvent):
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
        async def foo(event: VersionedEvent):
            await q.put(True)
        task = create_task(begin_processing_committed_events())
        await handle_command(CommandThatAlwaysSucceeds())
        await handle_command(CommandThatAlwaysSucceeds())
        await q.get()
        await q.get()
        event_repo = event_repository_instance()
        unhandled_events = [event async for event in event_repo.get_unhandled_events(time_delta=timedelta(seconds=0), batch_size=100)]
        self.assertEqual(len(list(unhandled_events)), 0)
