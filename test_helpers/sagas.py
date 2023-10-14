from datetime import datetime
from typing import List
from pyjangle import command_dispatcher_instance
from pyjangle.event.event import VersionedEvent
from pyjangle.saga.register_saga import RegisterSaga
from pyjangle.saga.saga import Saga, event_receiver, reconstitute_saga_state
from test_helpers.commands import CommandThatShouldErrorOnlyFirstTime
from test_helpers.events import (
    EventThatCausesDuplicateKeyError,
    EventThatCausesSagaToRetry,
    EventThatCompletesACommand,
    EventThatCompletesSaga,
    EventThatContinuesSaga,
    EventThatSetsSagaToTimedOut,
    EventThatTimesOutSaga,
    TestSagaEvent,
)


@RegisterSaga
class SagaForTestingRetryLogic(Saga):
    def __init__(
        self,
        saga_id: any,
        events: List[VersionedEvent] = [],
        retry_at: datetime = None,
        timeout_at: datetime = None,
        is_complete: bool = False,
        is_timed_out: bool = False,
    ):
        super().__init__(
            saga_id, events, retry_at, timeout_at, is_complete, is_timed_out
        )

    @event_receiver(
        EventThatContinuesSaga, skip_if_any_flags_set=[EventThatCompletesACommand]
    )
    async def on_event_that_continues_saga(self):
        try:
            await command_dispatcher_instance()(CommandThatShouldErrorOnlyFirstTime())
            self._post_state_change_event(EventThatCompletesACommand(version=1))
        except Exception as e:
            self.set_retry(datetime.min)

    @reconstitute_saga_state(EventThatContinuesSaga)
    def from_event_that_continues_saga(self, event):
        pass

    @reconstitute_saga_state(EventThatCompletesACommand)
    def from_event_that_completes_a_command(self, event):
        pass


@RegisterSaga
class SagaForTesting(Saga):
    def __init__(
        self,
        saga_id: any,
        events: List[VersionedEvent] = [],
        retry_at: datetime = None,
        timeout_at: datetime = None,
        is_complete: bool = False,
        is_timed_out: bool = False,
    ):
        self.calls = {
            "on_event_that_continues_saga": 0,
            "from_event_that_continues_saga": 0,
        }
        self._used_event_ids = set()
        super().__init__(
            saga_id, events, retry_at, timeout_at, is_complete, is_timed_out
        )

    @event_receiver(EventThatContinuesSaga, skip_if_any_flags_set=[TestSagaEvent])
    async def on_event_that_continues_saga(self):
        self._post_state_change_event(TestSagaEvent())
        self.calls["on_event_that_continues_saga"] += 1

    @event_receiver(EventThatCompletesSaga)
    async def on_event_that_completes_saga(self):
        pass

    @event_receiver(EventThatTimesOutSaga)
    async def on_event_that_times_out_saga(self):
        pass

    @event_receiver(EventThatCausesDuplicateKeyError)
    async def on_event_that_causes_duplicate_key_error(self):
        self._post_state_change_event(TestSagaEvent(id=self._used_event_ids.pop()))

    @event_receiver(EventThatSetsSagaToTimedOut)
    async def on_event_that_sets_saga_to_timed_out(self):
        pass

    @event_receiver(EventThatCausesSagaToRetry)
    async def on_event_that_causes_saga_to_retry(self):
        pass

    @reconstitute_saga_state(EventThatContinuesSaga)
    def from_event_that_continues_saga(self, event: EventThatContinuesSaga):
        self._used_event_ids.add(event.id)
        self.calls["from_event_that_continues_saga"] += 1

    @reconstitute_saga_state(EventThatCompletesSaga, add_event_type_to_flags=False)
    def from_event_that_completes_saga(self, event: EventThatCompletesSaga):
        self._used_event_ids.add(event.id)
        self.set_complete()

    @reconstitute_saga_state(TestSagaEvent)
    def from_test_internal_saga_event(self, event: TestSagaEvent):
        self._used_event_ids.add(event.id)

    @reconstitute_saga_state(EventThatCausesDuplicateKeyError)
    def from_event_that_causes_duplicate_key_error(
        self, event: EventThatCausesDuplicateKeyError
    ):
        self._used_event_ids.add(event.id)

    @reconstitute_saga_state(EventThatTimesOutSaga)
    def from_event_that_times_out_saga(self, event: EventThatTimesOutSaga):
        self._used_event_ids.add(event.id)
        self.set_timeout(datetime.min)

    @reconstitute_saga_state(EventThatSetsSagaToTimedOut)
    def from_event_that_sets_saga_to_timed_out(
        self, event: EventThatSetsSagaToTimedOut
    ):
        self._used_event_ids.add(event.id)
        self.set_timed_out()

    @reconstitute_saga_state(EventThatCausesSagaToRetry)
    def from_event_that_causes_saga_to_retry(self, event: EventThatCausesSagaToRetry):
        self._used_event_ids.add(event.id)
        self.set_retry(datetime.min)
