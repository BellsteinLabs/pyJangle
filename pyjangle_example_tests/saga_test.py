from datetime import datetime, timedelta
from decimal import Decimal
from typing import Callable
from unittest import IsolatedAsyncioTestCase, TestCase
import unittest
from unittest.mock import patch
from uuid import uuid4
from pyjangle.command.command_dispatcher import register_command_dispatcher
from pyjangle.command.command_response import CommandResponse
from pyjangle import Saga
from pyjangle import Event, VersionedEvent
from test_helpers.registration_paths import COMMAND_DISPATCHER
from pyjangle_example.commands import (
    CreditRequest,
    DebitRequest,
    NotifyRequestRejected,
    RollbackRequestDebit,
    GetRequestApproval,
)
from pyjangle_example.events import RequestApproved, RequestRejected, RequestCreated

from pyjangle_example.saga import (
    CreditRequestCommandFailed,
    CreditRequestCommandSucceeded,
    DebitRequestCommandFailed,
    DebitRequestCommandSucceeded,
    NotifyRequestRejectedCommandAcknowledged,
    RequestSaga,
    RollbackRequestCommandAcknowledged,
    GetRequestApprovalCommandFailed,
    GetRequestApprovalCommandSucceeded,
)


@patch(COMMAND_DISPATCHER, new=None)
class TestRequestSaga_RequestCreatedEventReceived(IsolatedAsyncioTestCase):
    async def test_happy_path(self, *_):
        await saga_state_verifier(
            test_case=self,
            incoming_event=receive_funds_requested_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestCreated,
                    GetRequestApprovalCommandSucceeded,
                ],
                SAGA_FLD_FLAGS: [RequestCreated, GetRequestApprovalCommandSucceeded],
                SAGA_FLD_IS_COMPLETE: False,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_get_request_approval_exception(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[],
            commands_that_will_throw_an_exception=[GetRequestApproval],
            incoming_event=receive_funds_requested_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [RequestCreated],
                SAGA_FLD_FLAGS: [RequestCreated],
                SAGA_FLD_IS_COMPLETE: False,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_get_request_approval_exception(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[GetRequestApproval],
            commands_that_will_throw_an_exception=[NotifyRequestRejected],
            incoming_event=receive_funds_requested_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [RequestCreated, GetRequestApprovalCommandFailed],
                SAGA_FLD_FLAGS: [RequestCreated, GetRequestApprovalCommandFailed],
                SAGA_FLD_IS_COMPLETE: False,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_get_request_approval_failed(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[GetRequestApproval],
            incoming_event=receive_funds_requested_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestCreated,
                    GetRequestApprovalCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    GetRequestApprovalCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_notify_request_rejected_failed(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[GetRequestApproval, NotifyRequestRejected],
            incoming_event=receive_funds_requested_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestCreated,
                    GetRequestApprovalCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    GetRequestApprovalCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )


@patch(COMMAND_DISPATCHER, new=None)
class TestRequestSaga_RequestApproved(IsolatedAsyncioTestCase):
    async def test_happy_path(self, *_):
        await saga_state_verifier(
            test_case=self,
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandSucceeded,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandSucceeded,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_debit_request_failed(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[DebitRequest],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestApproved,
                    DebitRequestCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_debit_request_exception(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_throw_an_exception=[DebitRequest],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [RequestApproved],
                SAGA_FLD_FLAGS: [RequestCreated, RequestApproved],
                SAGA_FLD_IS_COMPLETE: False,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_notify_request_rejected_succeeded(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[DebitRequest],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestApproved,
                    DebitRequestCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_notify_request_rejected_failed(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[DebitRequest, NotifyRequestRejected],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestApproved,
                    DebitRequestCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandFailed,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_notify_request_rejected_exception(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[DebitRequest],
            commands_that_will_throw_an_exception=[NotifyRequestRejected],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [RequestApproved, DebitRequestCommandFailed],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandFailed,
                ],
                SAGA_FLD_IS_COMPLETE: False,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_credit_request_failed(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[CreditRequest],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandFailed,
                    RollbackRequestCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandFailed,
                    RollbackRequestCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_credit_request_exception(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[],
            commands_that_will_throw_an_exception=[CreditRequest],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [RequestApproved, DebitRequestCommandSucceeded],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                ],
                SAGA_FLD_IS_COMPLETE: False,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_rollback_request_debit_succeeded(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[CreditRequest],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandFailed,
                    RollbackRequestCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandFailed,
                    RollbackRequestCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_rollback_request_debit_failed(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[CreditRequest, RollbackRequestDebit],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandFailed,
                    RollbackRequestCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandFailed,
                    RollbackRequestCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_rollback_request_debit_exception(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[CreditRequest],
            commands_that_will_throw_an_exception=[RollbackRequestDebit],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_approved_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandFailed,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestApproved,
                    DebitRequestCommandSucceeded,
                    CreditRequestCommandFailed,
                ],
                SAGA_FLD_IS_COMPLETE: False,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )


@patch(COMMAND_DISPATCHER, new=None)
class TestRequestSaga_RequestRejected(IsolatedAsyncioTestCase):
    async def test_happy_path(self, *_):
        await saga_state_verifier(
            test_case=self,
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_rejected_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestRejected,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestRejected,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_notify_request_rejected_failed(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_fail=[NotifyRequestRejected],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_rejected_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [
                    RequestRejected,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_FLAGS: [
                    RequestCreated,
                    RequestRejected,
                    NotifyRequestRejectedCommandAcknowledged,
                ],
                SAGA_FLD_IS_COMPLETE: True,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: None,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )

    async def test_notify_request_rejected_exception(self, *_):
        await saga_state_verifier(
            test_case=self,
            commands_that_will_throw_an_exception=[NotifyRequestRejected],
            preexisting_saga_events=[receive_funds_requested_event],
            incoming_event=receive_funds_rejected_event,
            saga_factory=get_saga,
            expected_saga_state={
                SAGA_FLD_NEW_EVENTS: [RequestRejected],
                SAGA_FLD_FLAGS: [RequestCreated, RequestRejected],
                SAGA_FLD_IS_COMPLETE: False,
                SAGA_FLD_IS_DIRTY: True,
                SAGA_FLD_IS_TIMED_OUT: False,
                SAGA_FLD_RETRY_AT: FAKE_CURRENT_TIME_PLUS_30_SECONDS,
                SAGA_FLD_TIMEOUT_AT: FAKE_CURRENT_TIME_PLUS_30_MINUTES,
            },
        )


async def saga_state_verifier(
    test_case: TestCase,
    incoming_event: VersionedEvent,
    saga_factory: Callable[[list[Event]], Saga],
    expected_saga_state: dict[str, any],
    commands_that_will_fail: list = [],
    commands_that_will_throw_an_exception: list = [],
    preexisting_saga_events: list[VersionedEvent] = [],
):
    with patch(
        COMMAND_DISPATCHER,
        new=get_command_dispatcher(
            fail_commands=commands_that_will_fail,
            exception_commands=commands_that_will_throw_an_exception,
        ),
    ):
        saga = saga_factory(preexisting_saga_events)
        with patch.object(
            saga, attribute="_get_current_time", return_value=FAKE_CURRENT_TIME
        ):
            await saga.evaluate(incoming_event)
            for k, v in expected_saga_state.items():
                actual = getattr(saga, k)
                if k == SAGA_FLD_NEW_EVENTS:
                    actual = set([type(event) for event in actual])
                    v = set(v)
                    missing_from_expected = actual.difference(v)
                    extra_in_expected = v.difference(actual)
                    test_case.assertFalse(
                        missing_from_expected,
                        f"""Actual new events contains more than was expected: 
                        {str(missing_from_expected)}""",
                    )
                    test_case.assertFalse(
                        extra_in_expected,
                        f"""Actual new events was missing some expected items: 
                        {str(extra_in_expected)}""",
                    )
                    continue
                if k == SAGA_FLD_FLAGS:
                    v = set(v)
                    missing_from_expected = actual.difference(v)
                    extra_in_expected = v.difference(actual)
                    test_case.assertFalse(
                        missing_from_expected,
                        f"""Actual flags contains more than was expected: 
                        {str(missing_from_expected)}""",
                    )
                    test_case.assertFalse(
                        extra_in_expected,
                        f"""Actual flags was missing some expected items: 
                        {str(extra_in_expected)}""",
                    )
                test_case.assertEqual(
                    actual,
                    v,
                    f"Attribute '{k}', expected '{str(v)}', actual '{str(actual)}",
                )


TRANSACTION_ID = str(uuid4())
FUNDED_ACCOUNT_ID = "000001"
FUNDING_ACCOUNT_ID = "000002"
AMOUNT = Decimal(50)
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


def get_command_dispatcher(
    fail_commands: tuple[type] = (), exception_commands: tuple[type] = ()
):
    async def command_dispatcher(command: any) -> CommandResponse:
        if type(command) in exception_commands:
            raise Exception()
        return CommandResponse(not type(command) in fail_commands)

    return command_dispatcher


def get_saga(events: list[Event]):
    return RequestSaga(saga_id=TRANSACTION_ID, events=events)


receive_funds_requested_event = RequestCreated(
    version=3,
    funded_account_id=FUNDED_ACCOUNT_ID,
    funding_account_id=FUNDING_ACCOUNT_ID,
    amount=AMOUNT,
    transaction_id=TRANSACTION_ID,
    timeout_at=FAKE_CURRENT_TIME_PLUS_30_MINUTES,
)
receive_funds_approved_event = RequestApproved(
    version=42, funding_account_id=FUNDING_ACCOUNT_ID, transaction_id=TRANSACTION_ID
)
receive_funds_rejected_event = RequestRejected(
    version=42, funding_account_id=FUNDING_ACCOUNT_ID, transaction_id=TRANSACTION_ID
)
