from asyncio import Queue, create_task
from datetime import timedelta
from typing import Awaitable, Callable
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import pyjangle
from pyjangle import (
    VersionedEvent,
    EventRepositoryError,
    EventDispatcherMissingError,
    RegisterEventDispatcher,
    begin_processing_committed_events,
    begin_retry_failed_events_loop,
    event_repository_instance,
    handle_command,
)
from pyjangle.test.commands import CommandThatShouldSucceedA
from pyjangle.test.registration_paths import (
    EVENT_DISPATCHER,
    EVENT_REPO,
)
from pyjangle.test.reset import ResetPyJangleState


@ResetPyJangleState
class TestEventDaemon(IsolatedAsyncioTestCase):
    async def test_daemon_retries_failed_events(self, *_):
        # This test will cause an event to be committed and dispatched, but the first
        # attempt dispatching will fail. (The exception thrown in the dispatcher below)

        q = Queue()
        event_repo = event_repository_instance()

        @RegisterEventDispatcher
        async def foo(
            event: VersionedEvent, completed_callback: Callable[[any], Awaitable[None]]
        ):
            "The event dispatcher puts a True value onto q."

            try:
                if foo.count == 0:
                    # The test will wait for this with a call to q.get() to proceed
                    await q.put(True)
                    raise Exception()  # The event should be requeued here.
                await q.put(True)
                await completed_callback(event.id)
            finally:
                foo.count += 1

        foo.count = 0

        # Keep reference to the task so that it's not garbage collected per the docs
        begin_processing_committed_events()
        await handle_command(CommandThatShouldSucceedA())
        await q.get()  # Wait for the first boolean that the dispatcher puts on the q.
        self.assertEqual(
            pyjangle.event.event_dispatcher._committed_event_queue.qsize(), 0
        )  # After calling get(), the q should be empty.
        begin_retry_failed_events_loop(
            frequency_in_seconds=0, max_age_time_delta=timedelta(seconds=0)
        )
        unhandled_events = [
            event
            async for event in event_repo.get_unhandled_events(
                time_delta=timedelta(seconds=0), batch_size=100
            )
        ]
        self.assertEqual(len(list(unhandled_events)), 1)
        await q.get()  # Block until the event is processed by the dispatcher again
        unhandled_events = [
            event
            async for event in event_repo.get_unhandled_events(
                time_delta=timedelta(seconds=0), batch_size=100
            )
        ]
        self.assertEqual(len(list(unhandled_events)), 0)
        self.assertEqual(foo.count, 2)  # The dispatcher should have been called twice.

    async def test_when_no_event_repository_then_exception(self, *_):
        @RegisterEventDispatcher
        async def foo(
            event: VersionedEvent, completed_callback: Callable[[any], Awaitable[None]]
        ):
            pass

        with patch(EVENT_REPO, None):
            with self.assertRaises(EventRepositoryError):
                begin_retry_failed_events_loop(frequency_in_seconds=0)

    async def test_when_no_event_dispatcher_then_exception(self, *_):
        with patch(EVENT_DISPATCHER, None):
            with self.assertRaises(EventDispatcherMissingError):
                begin_retry_failed_events_loop(frequency_in_seconds=0)
