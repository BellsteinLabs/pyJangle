import asyncio
import unittest
from unittest.mock import MagicMock, Mock, patch
import pyjangle
from pyjangle import handle_command, DuplicateKeyError, event_repository_instance
import pyjangle.test.aggregates
from pyjangle.test.commands import (
    CommandThatShouldSucceedB,
    CommandThatShouldSucceedA,
    CommandThatShouldFail,
)
from pyjangle.test.registration_paths import EVENT_DISPATCHER
from pyjangle.test.reset import ResetPyJangleState


@ResetPyJangleState
class TestCommandHandler(unittest.IsolatedAsyncioTestCase):
    # CommandThatShouldSucceedA is registered to a snapshottable aggregate.
    # CommandThatShouldSucceedB is not registered to a snapshottable aggregate.
    # See: pyjangle.test.aggregates

    async def test_command_handled_snapshottable_aggregate(self, *_):
        command_response = await handle_command(CommandThatShouldSucceedA())
        self.assertTrue(command_response.is_success)

    async def test_command_handled_not_snapshottable_aggregate(self, *_):
        command_response = await handle_command(CommandThatShouldSucceedB())
        self.assertTrue(command_response.is_success)

    async def test_command_failure(self, *_):
        command_repsponse = await handle_command(CommandThatShouldFail())
        self.assertFalse(command_repsponse.is_success)

    async def test_dispatch_locally_enabled(self, *_):
        with patch(EVENT_DISPATCHER, lambda x: x):
            response = await handle_command(CommandThatShouldSucceedA())
            self.assertTrue(response.is_success)
            self.assertTrue(
                pyjangle.event.event_dispatcher._committed_event_queue.qsize()
            )

    async def test_event_not_queued_for_dispatch_when_no_dispatcher_registered(
        self, *_
    ):
        await handle_command(CommandThatShouldSucceedA())
        self.assertFalse(pyjangle.event.event_dispatcher._committed_event_queue.qsize())

    async def test_when_same_command_executes_concurrently_then_only_one_wins(self, *_):
        actual = event_repository_instance().commit_events

        async def commit_events_with_delay(*args, **kwargs):
            await asyncio.sleep(0.2)
            return await actual(*args, **kwargs)

        with patch.object(
            event_repository_instance(), "commit_events"
        ) as mock_commit_events:
            mock_commit_events.side_effect = commit_events_with_delay
            await asyncio.gather(
                handle_command(CommandThatShouldSucceedA()),
                handle_command(CommandThatShouldSucceedA()),
            )

        self.assertEqual(mock_commit_events.call_count, 3)

    async def test_snapshot_applied_when_aggregate_is_snapshottable(self, *_):
        actual = (
            pyjangle.snapshot.snapshot_repository._registered_snapshot_repository.store_snapshot
        )

        async def store_snapshots_real(*args, **kwargs):
            await actual(*args, **kwargs)

        # mocking store_snapshots only to count method calls--call is forwarded to the actual method.
        with patch.object(
            pyjangle.snapshot.snapshot_repository._registered_snapshot_repository,
            "store_snapshot",
        ) as store_snapshot_mock:
            store_snapshot_mock.side_effect = store_snapshots_real
            # snapshot created every 2 commands
            await handle_command(CommandThatShouldSucceedA())
            await handle_command(CommandThatShouldSucceedA())
            await handle_command(CommandThatShouldSucceedA())
            await handle_command(CommandThatShouldSucceedA())
            await handle_command(CommandThatShouldSucceedA())
            self.assertEqual(store_snapshot_mock.call_count, 2)
            """when new snapshot is created for the same aggregate, 
            it overwrites the previous one."""
            self.assertEqual(
                len(
                    pyjangle.snapshot.snapshot_repository._registered_snapshot_repository._snapshots
                ),
                1,
            )

    async def test_bad_snapshots_deleted(self, *_):
        # Snapshot created every 2 commands/events
        await handle_command(CommandThatShouldSucceedA())
        await handle_command(CommandThatShouldSucceedA())
        self.assertEqual(
            len(
                pyjangle.snapshot.snapshot_repository._registered_snapshot_repository._snapshots
            ),
            1,
        )
        # Throw exception when this snapshot is applied
        with patch.object(
            pyjangle.aggregate.register_aggregate._command_to_aggregate_map[
                CommandThatShouldSucceedA
            ],
            "apply_snapshot",
            MagicMock(side_effect=Exception),
        ):
            await handle_command(CommandThatShouldSucceedA())
        self.assertEqual(
            len(
                pyjangle.snapshot.snapshot_repository._registered_snapshot_repository._snapshots
            ),
            0,
        )

    async def test_snapshotting_reduces_events_retrieved_from_event_store(self, *_):
        async def raise_error_if_too_many_events_returned_side_effect(
            real_method, event_count_threshold: int
        ):
            async def side_effect_method(*args, **kwargs):
                returned_events = await real_method(*args, **kwargs)
                if len(returned_events) > event_count_threshold:
                    raise Exception()  # pragma no cover
                return returned_events

            return side_effect_method

        get_events_real = (
            pyjangle.event.event_repository._event_repository_instance.get_events
        )
        with patch.object(
            pyjangle.event.event_repository._event_repository_instance, "get_events"
        ) as get_events_mock:
            get_events_mock.side_effect = (
                await raise_error_if_too_many_events_returned_side_effect(
                    get_events_real, 2
                )
            )
            for _ in range(100):
                await handle_command(CommandThatShouldSucceedA())

    async def test_retry_after_duplicate_key_exception(self, _, event_repo, __):
        def dupliate_key_error_on_2nd_call_side_effect(real_function, *args):
            dupliate_key_error_on_2nd_call_side_effect.counter = (
                0
                if not hasattr(dupliate_key_error_on_2nd_call_side_effect, "counter")
                else dupliate_key_error_on_2nd_call_side_effect.counter
            )
            dupliate_key_error_on_2nd_call_side_effect.counter += 1
            if dupliate_key_error_on_2nd_call_side_effect.counter == 2:
                raise DuplicateKeyError()
            else:
                return real_function(*args)

        real_func = event_repo.commit_events
        event_repo.commit_events = Mock(
            side_effect=lambda *args: dupliate_key_error_on_2nd_call_side_effect(
                real_func, *args
            )
        )

        # first call
        await handle_command(CommandThatShouldSucceedA())
        # second call, raises exception and retries (3rd call)
        await handle_command(CommandThatShouldSucceedA())

        self.assertEqual(event_repo.commit_events.call_count, 3)
