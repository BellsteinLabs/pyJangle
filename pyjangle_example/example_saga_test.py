from datetime import datetime, timedelta
from typing import Callable
from unittest import IsolatedAsyncioTestCase, TestCase
import unittest
from unittest.mock import patch
from uuid import uuid4
from pyjangle.command.command_dispatcher import RegisterCommandDispatcher
from pyjangle.command.command_response import CommandResponse
from pyjangle import Saga
from pyjangle import Event, VersionedEvent
from pyjangle.test.registration_paths import COMMAND_DISPATCHER
from pyjangle_example.example_commands import CreditReceiveFunds, DebitReceiveFunds, NotifyReceiveFundsRejected, RollbackReceiveFundsDebit, TryObtainReceiveFundsApproval
from pyjangle_example.example_events import ReceiveFundsApproved, ReceiveFundsRejected, ReceiveFundsRequested

from pyjangle_example.example_saga import CreditReceiveFundsCommandFailed, CreditReceiveFundsCommandSucceeded, DebitReceiveFundsCommandFailed, DebitReceiveFundsCommandSucceeded, NotifyReceiveFundsRejectedCommandAcknowledged, RequestFundsFromAnotherAccount, RollbackReceiveFundsDebitCommandAcknowledged, TryObtainReceiveFundsApprovalCommandFailed, TryObtainReceiveFundsApprovalCommandSucceeded


@patch(COMMAND_DISPATCHER, new=None)
class TestRequestFundsFromAnotherAccountSaga_ReceiveFundsRequestedEventReceived(IsolatedAsyncioTestCase):

    async def test_happy_path(self, *_):
        await saga_state_verifier(test_case=self, incoming_event=receive_funds_requested_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsRequested, TryObtainReceiveFundsApprovalCommandSucceeded],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, TryObtainReceiveFundsApprovalCommandSucceeded],
            SAGA_FLD_IS_COMPLETE: False,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_TryObtainReceiveFundsApproval_exception(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[], exception_commands=[TryObtainReceiveFundsApproval], incoming_event=receive_funds_requested_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsRequested],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested],
            SAGA_FLD_IS_COMPLETE: False,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_NotifyReceiveFundsRejected_exception(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[TryObtainReceiveFundsApproval], exception_commands=[NotifyReceiveFundsRejected], incoming_event=receive_funds_requested_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsRequested, TryObtainReceiveFundsApprovalCommandFailed],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, TryObtainReceiveFundsApprovalCommandFailed],
            SAGA_FLD_IS_COMPLETE: False,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_TryObtainReceiveFundsApproval_failed(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[TryObtainReceiveFundsApproval], incoming_event=receive_funds_requested_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsRequested, TryObtainReceiveFundsApprovalCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, TryObtainReceiveFundsApprovalCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_NotifyReceiveFundsRejected_failed(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[TryObtainReceiveFundsApproval, NotifyReceiveFundsRejected], incoming_event=receive_funds_requested_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsRequested, TryObtainReceiveFundsApprovalCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, TryObtainReceiveFundsApprovalCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })


@patch(COMMAND_DISPATCHER, new=None)
class TestRequestFundsFromAnotherAccountSaga_ReceiveFundsApproved(IsolatedAsyncioTestCase):
    async def test_happy_path(self, *_):
        await saga_state_verifier(test_case=self, preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandSucceeded],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandSucceeded],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_DebitReceiveFundsCommandSucceeded_failed(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[DebitReceiveFunds], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_DebitReceiveFundsCommandSucceeded_exception(self, *_):
        await saga_state_verifier(test_case=self, exception_commands=[DebitReceiveFunds], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved],
            SAGA_FLD_IS_COMPLETE: False,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_NotifyReceiveFundsRejectedCommandAcknowledged_succeeded(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[DebitReceiveFunds], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_NotifyReceiveFundsRejectedCommandAcknowledged_failed(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[DebitReceiveFunds, NotifyReceiveFundsRejected], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandFailed, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_NotifyReceiveFundsRejectedCommandAcknowledged_exception(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[DebitReceiveFunds], exception_commands=[NotifyReceiveFundsRejected], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandFailed],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandFailed],
            SAGA_FLD_IS_COMPLETE: False,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_CreditReceiveFunds_failed(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[CreditReceiveFunds], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandFailed, RollbackReceiveFundsDebitCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandFailed, RollbackReceiveFundsDebitCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_CreditReceiveFunds_exception(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[], exception_commands=[CreditReceiveFunds], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded],
            SAGA_FLD_IS_COMPLETE: False,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_RollbackReceiveFundsDebitCommandAcknowledged_succeeded(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[CreditReceiveFunds], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandFailed, RollbackReceiveFundsDebitCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandFailed, RollbackReceiveFundsDebitCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_RollbackReceiveFundsDebitCommandAcknowledged_failed(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[CreditReceiveFunds, RollbackReceiveFundsDebit], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandFailed, RollbackReceiveFundsDebitCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandFailed, RollbackReceiveFundsDebitCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_RollbackReceiveFundsDebitCommandAcknowledged_exception(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[CreditReceiveFunds], exception_commands=[RollbackReceiveFundsDebit], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_approved_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandFailed],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsApproved, DebitReceiveFundsCommandSucceeded, CreditReceiveFundsCommandFailed],
            SAGA_FLD_IS_COMPLETE: False,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })


@patch(COMMAND_DISPATCHER, new=None)
class TestRequestFundsFromAnotherAccountSaga_ReceiveFundsRejected(IsolatedAsyncioTestCase):
    async def test_happy_path(self, *_):
        await saga_state_verifier(test_case=self, preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_rejected_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsRejected, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsRejected, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_NotifyReceiveFundsRejected_failed(self, *_):
        await saga_state_verifier(test_case=self, fail_commands=[NotifyReceiveFundsRejected], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_rejected_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsRejected, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsRejected, NotifyReceiveFundsRejectedCommandAcknowledged],
            SAGA_FLD_IS_COMPLETE: True,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: None,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })

    async def test_NotifyReceiveFundsRejected_exception(self, *_):
        await saga_state_verifier(test_case=self, exception_commands=[NotifyReceiveFundsRejected], preexisting_saga_events=[receive_funds_requested_event], incoming_event=receive_funds_rejected_event, saga_factory=get_saga, expected_saga_state={
            SAGA_FLD_NEW_EVENTS: [ReceiveFundsRejected],
            SAGA_FLD_FLAGS: [ReceiveFundsRequested, ReceiveFundsRejected],
            SAGA_FLD_IS_COMPLETE: False,
            SAGA_FLD_IS_DIRTY: True,
            SAGA_FLD_IS_TIMED_OUT: False,
            SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
            SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES
        })


async def saga_state_verifier(test_case: TestCase, incoming_event: VersionedEvent, saga_factory: Callable[[list[Event]], Saga], expected_saga_state: dict[str, any], fail_commands: list = [], exception_commands: list = [], preexisting_saga_events: list[VersionedEvent] = []):
    with patch(COMMAND_DISPATCHER, new=get_command_dispatcher(fail_commands=fail_commands, exception_commands=exception_commands)):
        saga = saga_factory(preexisting_saga_events)
        with patch.object(saga, attribute="_get_current_time",
                          return_value=FAKE_CURRENT_TIME):
            await saga.evaluate(incoming_event)
            for k, v in expected_saga_state.items():

                actual = getattr(saga, k)
                if k == SAGA_FLD_NEW_EVENTS:
                    actual = set([type(event) for event in actual])
                    v = set(v)
                    missing_from_expected = actual.difference(v)
                    extra_in_expected = v.difference(actual)
                    test_case.assertFalse(
                        missing_from_expected, f"Actual new events contains more than was expected: {str(missing_from_expected)}")
                    test_case.assertFalse(
                        extra_in_expected, f"Actual new events was missing some expected items: {str(extra_in_expected)}")
                    continue
                if k == SAGA_FLD_FLAGS:
                    v = set(v)
                    missing_from_expected = actual.difference(v)
                    extra_in_expected = v.difference(actual)
                    test_case.assertFalse(
                        missing_from_expected, f"Actual flags contains more than was expected: {str(missing_from_expected)}")
                    test_case.assertFalse(
                        extra_in_expected, f"Actual flags was missing some expected items: {str(extra_in_expected)}")
                test_case.assertEqual(
                    actual, v, f"Attribute '{k}', expected '{str(v)}', actual '{str(actual)}")

TRANSACTION_ID = str(uuid4())
FUNDED_ACCOUNT_ID = str(uuid4())
FUNDING_ACCOUNT_ID = str(uuid4())
AMOUNT = 50
FAKE_CURRENT_TIME = datetime.min
FAKE_CURRENT_TIME_PLUS_30_MINUTES = FAKE_CURRENT_TIME + timedelta(minutes=30)
FAKE_CURRENT_TIME_PLUS_30_SECONDS = FAKE_CURRENT_TIME + timedelta(seconds=30)

SAGA_FLD_FLAGS = "flags"
SAGA_FLD_RETRY_AT = "retry_at"
SAGA_FLD_TIMEOUT_AT = "timeout_at"
SAGA_FLD_IS_TIMED_OUT = "is_timed_out"
SAGA_FLD_IS_COMPLETE = "is_complete"
SAGA_FLD_NEW_EVENTS = "new_events"
SAGA_FLD_IS_DIRTY = "is_dirty"


def get_command_dispatcher(fail_commands: tuple[type] = (), exception_commands: tuple[type] = ()):
    async def command_dispatcher(command: any) -> CommandResponse:
        if type(command) in exception_commands:
            raise Exception()
        return CommandResponse(not type(command) in fail_commands)
    return command_dispatcher


def get_saga(events: list[Event]):
    return RequestFundsFromAnotherAccount(saga_id=TRANSACTION_ID, events=events)


receive_funds_requested_event = ReceiveFundsRequested(
    version=3, funded_account_id=FUNDED_ACCOUNT_ID, funding_account_id=FUNDING_ACCOUNT_ID, amount=AMOUNT, transaction_id=TRANSACTION_ID, timeout_at=FAKE_CURRENT_TIME_PLUS_30_MINUTES)
receive_funds_approved_event = ReceiveFundsApproved(
    version=42, funding_account_id=FUNDING_ACCOUNT_ID, transaction_id=TRANSACTION_ID)
receive_funds_rejected_event = ReceiveFundsRejected(
    version=42, funding_account_id=FUNDING_ACCOUNT_ID, transaction_id=TRANSACTION_ID)
