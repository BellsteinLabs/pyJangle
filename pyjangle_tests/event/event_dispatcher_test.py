from typing import Awaitable, Callable
import unittest
from asyncio import Queue, wait_for
from datetime import timedelta

from pyjangle import (
    VersionedEvent,
    EventDispatcherMissingError,
    DuplicateEventDispatcherError,
    EventDispatcherBadSignatureError,
    RegisterEventDispatcher,
    begin_processing_committed_events,
    event_dispatcher_instance,
    event_repository_instance,
    handle_command,
)
from pyjangle.registration import background_tasks
import test_helpers.aggregates  # Importing module here registers the aggregates
from test_helpers.commands import CommandThatShouldSucceedA
from test_helpers.reset import ResetPyJangleState


@ResetPyJangleState
class TestEventDispatcher(unittest.IsolatedAsyncioTestCase):
    async def test_register_multiple_event_dispatcher(self, *_):
        @RegisterEventDispatcher
        async def Event_dispatcher1(_, __):
            pass

        with self.assertRaises(DuplicateEventDispatcherError):

            @RegisterEventDispatcher
            async def Event_dispatcher2(_, __):
                pass

    async def test_register_event_dispatcher_with_wrong_function_signature_raises_error(
        self, *_
    ):
        with self.assertRaises(EventDispatcherBadSignatureError):

            @RegisterEventDispatcher
            async def Event_dispatcher():
                pass

    async def test_register_event_dispatcher_not_coroutine_then_error(self, *_):
        with self.assertRaises(EventDispatcherBadSignatureError):

            @RegisterEventDispatcher
            def Event_dispatcher():
                pass

    async def test_register_event_dispatcher(self, *_):
        @RegisterEventDispatcher
        async def Event_dispatcher(event: VersionedEvent, callback):
            pass

        self.assertEqual(
            event_dispatcher_instance().__name__, Event_dispatcher.__name__
        )

    async def test_process_committed_events(self, *_):
        q = Queue()

        @RegisterEventDispatcher
        async def foo(
            event: VersionedEvent, completed_callback: Callable[[any], Awaitable[None]]
        ):
            await q.put(event)
            await completed_callback(event.id)

        await handle_command(CommandThatShouldSucceedA())
        task = begin_processing_committed_events()
        background_tasks.background_tasks.append(task)
        event = await wait_for(q.get(), 1)

        self.assertTrue(event)

    async def test_error_when_process_committed_events_and_no_event_dispatcher_registered(
        self, *_
    ):
        with self.assertRaises(EventDispatcherMissingError):
            task = begin_processing_committed_events()
            await task

    async def test_error_when_processing_committed_events_doesnt_kill_processing_loop(
        self, *_
    ):
        q = Queue()

        @RegisterEventDispatcher
        async def foo(
            event: VersionedEvent, completed_callback: Callable[[any], Awaitable[None]]
        ):
            await q.put(True)
            raise Exception()

        begin_processing_committed_events()
        await handle_command(CommandThatShouldSucceedA())
        await handle_command(CommandThatShouldSucceedA())
        result = await q.get()
        result2 = await q.get()
        self.assertTrue(result and result2)

    async def test_error_when_processing_committed_events_doesnt_mark_events_as_handled(
        self, *_
    ):
        q = Queue()
        event_repo = event_repository_instance()

        @RegisterEventDispatcher
        async def foo(
            event: VersionedEvent, completed_callback: Callable[[any], Awaitable[None]]
        ):
            try:
                raise Exception()
            finally:
                await q.put(True)

        begin_processing_committed_events()
        await handle_command(CommandThatShouldSucceedA())
        await handle_command(CommandThatShouldSucceedA())
        await q.get()
        await q.get()
        unhandled_events = [
            event
            async for event in event_repo.get_unhandled_events(
                time_delta=timedelta(seconds=0), batch_size=100
            )
        ]
        self.assertEqual(len(list(unhandled_events)), 2)

    async def test_processing_committed_events_successfully_marks_them_as_handled(
        self, *_
    ):
        q = Queue()

        @RegisterEventDispatcher
        async def foo(
            event: VersionedEvent, completed_callback: Callable[[any], Awaitable[None]]
        ):
            await q.put(True)
            await completed_callback(event.id)

        begin_processing_committed_events()
        await handle_command(CommandThatShouldSucceedA())
        await handle_command(CommandThatShouldSucceedA())
        await q.get()
        await q.get()
        event_repo = event_repository_instance()
        unhandled_events = [
            event
            async for event in event_repo.get_unhandled_events(
                time_delta=timedelta(seconds=0), batch_size=100
            )
        ]
        self.assertEqual(len(list(unhandled_events)), 0)
