from decimal import Decimal
from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock, patch
from uuid import uuid4
import uuid
from pyjangle_example.aggregates.account_aggregate import AccountAggregate
from pyjangle_example.commands import (
    AcceptRequest,
    CreditRequest,
    CreditTransfer,
    DebitRequest,
    DeleteAccount,
    DepositFunds,
    NotifyRequestRejected,
    Request,
    RejectRequest,
    ForgiveDebt,
    RollbackRequestDebit,
    RollbackTransferDebit,
    Transfer,
    GetRequestApproval,
    WithdrawFunds,
)
from pyjangle_example.events import (
    AccountCreated,
    AccountDeleted,
    DebtForgiven,
    FundsDeposited,
    FundsWithdrawn,
    RequestReceived,
    RequestRejectionReceived,
    RequestApproved,
    RequestCredited,
    RequestDebited,
    RequestDebitRolledBack,
    RequestRejected,
    RequestCreated,
    TransferCredited,
    TransferDebited,
    TransferDebitRolledBack,
)
from pyjangle_example.test_helpers import (
    FAKE_CURRENT_TIME,
    FUNDING_ACCOUNT_ID_FLD,
    FUNDED_ACCOUNT_ID_FLD,
    ACCOUNT_ID,
    ACCOUNT_ID_FLD,
    ACCOUNT_NAME,
    AMOUNT_FLD,
    BALANCE_FLD,
    THIRTY_MINUTES_FROM_FAKE_NOW,
    LARGE_AMOUNT,
    OTHER_ACCOUNT_ID,
    SMALL_AMOUNT,
    TIMEOUT_AT_FLD,
    TRANSACTION_ID_FLD,
    ExpectedEvent,
    verify_events,
    datetime_mock,
    KNOWN_UUID,
)
from pyjangle_example.validation.descriptors import TransactionId


uuid_mock = Mock(wraps=uuid.uuid4, return_value=KNOWN_UUID)


@patch(f"{AccountAggregate.__module__}.datetime", new=datetime_mock)
@patch(f"{TransactionId.__module__}.uuid4", new=uuid_mock)
class TestAccountAggregate(IsolatedAsyncioTestCase):
    """Tests account aggregate.

    Tests the account aggregate by issuing commands and verifying that the resulting
    events match expectations."""

    def setUp(self) -> None:
        self.aggregate = AccountAggregate(ACCOUNT_ID)

    def test_when_account_not_exists_then_commands_fail_and_no_new_events(self, *_):
        self.assertFalse(
            self.aggregate.validate(
                DepositFunds(account_id=ACCOUNT_ID, amount=SMALL_AMOUNT)
            ).is_success
        )
        self.assertFalse(self.aggregate.new_events)

    def test_when_deposit_funds_then_funds_deposited_event(self, *_):
        c = DepositFunds(account_id=ACCOUNT_ID, amount=SMALL_AMOUNT)
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_length=1,
            expected_events=[
                ExpectedEvent(
                    event_type=FundsDeposited,
                    agg_id=ACCOUNT_ID,
                    version=2,
                    attributes={
                        ACCOUNT_ID_FLD: ACCOUNT_ID,
                        AMOUNT_FLD: SMALL_AMOUNT,
                        BALANCE_FLD: SMALL_AMOUNT,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                    },
                )
            ],
        )

    def test_when_withdraw_insufficient_funds_then_command_failed_and_no_new_events(
        self, *_
    ):
        c = WithdrawFunds(account_id=ACCOUNT_ID, amount=LARGE_AMOUNT)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_withdraw_sufficient_funds_then_fudns_withdrawn_event(self, *_):
        c = WithdrawFunds(account_id=ACCOUNT_ID, amount=SMALL_AMOUNT)
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c))
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_length=1,
            expected_events=[
                ExpectedEvent(
                    event_type=FundsWithdrawn,
                    agg_id=ACCOUNT_ID,
                    version=2,
                    attributes={
                        ACCOUNT_ID_FLD: ACCOUNT_ID,
                        AMOUNT_FLD: SMALL_AMOUNT,
                        BALANCE_FLD: -SMALL_AMOUNT,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                    },
                )
            ],
        )

    def test_when_send_funds_and_balance_insufficient_then_no_new_events_and_command_fails(
        self, *_
    ):
        c = Transfer(
            funded_account_id=OTHER_ACCOUNT_ID,
            funding_account_id=ACCOUNT_ID,
            amount=LARGE_AMOUNT,
        )
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_send_funds_then_send_funds_debited(self, *_):
        c = Transfer(
            funded_account_id=OTHER_ACCOUNT_ID,
            funding_account_id=ACCOUNT_ID,
            amount=SMALL_AMOUNT,
        )
        self.aggregate.apply_events(
            [account_created_event, small_funds_deposited_event]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)

        verify_events(
            events=self.aggregate.new_events,
            test_case=self,
            expected_events=[
                ExpectedEvent(
                    event_type=TransferDebited,
                    agg_id=ACCOUNT_ID,
                    version=3,
                    attributes={
                        FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        FUNDED_ACCOUNT_ID_FLD: OTHER_ACCOUNT_ID,
                        AMOUNT_FLD: SMALL_AMOUNT,
                        BALANCE_FLD: 0,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                    },
                )
            ],
        )

    def test_when_receive_funds_then_receive_funds_requested_event(self, *_):
        c = Request(
            funded_account_id=ACCOUNT_ID,
            funding_account_id=OTHER_ACCOUNT_ID,
            amount=LARGE_AMOUNT,
        )
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)

        verify_events(
            events=self.aggregate.new_events,
            test_case=self,
            expected_events=[
                ExpectedEvent(
                    RequestCreated,
                    agg_id=ACCOUNT_ID,
                    version=self.aggregate.version + 1,
                    attributes={
                        FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        FUNDING_ACCOUNT_ID_FLD: OTHER_ACCOUNT_ID,
                        AMOUNT_FLD: LARGE_AMOUNT,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                        TIMEOUT_AT_FLD: THIRTY_MINUTES_FROM_FAKE_NOW,
                    },
                )
            ],
        )

    def test_when_credit_send_funds_and_no_corresponding_transaction_then_no_new_events_and_command_fails(
        self, *_
    ):
        c = CreditRequest(funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_credit_send_funds_then_send_funds_credited_event(self, *_):
        c = CreditRequest(funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, receive_large_funds_requested_event]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    RequestCredited,
                    agg_id=ACCOUNT_ID,
                    version=self.aggregate.version + 1,
                    attributes={
                        FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                        BALANCE_FLD: LARGE_AMOUNT,
                        AMOUNT_FLD: LARGE_AMOUNT,
                    },
                )
            ],
        )

    def test_when_request_forgiveness_and_not_needed_then_command_fails_and_no_new_events(
        self, *_
    ):
        c = ForgiveDebt(account_id=ACCOUNT_ID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_request_forgiveness_then_debt_forgiven_event(self, *_):
        c = ForgiveDebt(account_id=ACCOUNT_ID)
        self.aggregate.apply_events(
            [account_created_event, small_funds_withdrawn_event]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    DebtForgiven,
                    ACCOUNT_ID,
                    self.aggregate.version + 1,
                    attributes={
                        ACCOUNT_ID_FLD: ACCOUNT_ID,
                        AMOUNT_FLD: SMALL_AMOUNT,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                    },
                )
            ],
        )

    def test_when_request_forgiveness_too_many_times_then_command_fails_and_no_new_events(
        self, *_
    ):
        c = ForgiveDebt(account_id=ACCOUNT_ID)
        self.aggregate.apply_events(
            [
                account_created_event,
                small_funds_withdrawn_event,
                debt_forgiven_event,
                second_small_funds_withdrawn_event,
                second_debt_forgiven_event,
            ]
        )
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_delete_account_then_account_deleted_event(self, *_):
        c = DeleteAccount(account_id=ACCOUNT_ID)
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    AccountDeleted,
                    ACCOUNT_ID,
                    2,
                    attributes={ACCOUNT_ID_FLD: ACCOUNT_ID},
                )
            ],
        )

    def test_when_delete_account_and_already_deleted_no_new_events_and_command_succeeds(
        self, *_
    ):
        c = DeleteAccount(account_id=ACCOUNT_ID)
        self.aggregate.apply_events([account_created_event, account_deleted_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_try_obtain_receive_funds_approval_then_notified_receive_funds_request(
        self, *_
    ):
        c = GetRequestApproval(
            funded_account_id=OTHER_ACCOUNT_ID,
            funding_account_id=ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
            timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW,
            amount=LARGE_AMOUNT,
        )
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    event_type=RequestReceived,
                    agg_id=ACCOUNT_ID,
                    version=2,
                    attributes={
                        FUNDED_ACCOUNT_ID_FLD: OTHER_ACCOUNT_ID,
                        FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        AMOUNT_FLD: LARGE_AMOUNT,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                        TIMEOUT_AT_FLD: THIRTY_MINUTES_FROM_FAKE_NOW,
                    },
                )
            ],
        )

    def test_when_try_obtain_receive_funds_approval_duplicate_then_no_events_and_command_succeeds(
        self, *_
    ):
        c = GetRequestApproval(
            funded_account_id=OTHER_ACCOUNT_ID,
            funding_account_id=ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
            timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW,
            amount=LARGE_AMOUNT,
        )
        self.aggregate.apply_events(
            [account_created_event, notified_receive_funds_requested_event_v2]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_reject_receive_funds_request_and_transaction_not_found_then_no_new_events_and_command_fails(
        self, *_
    ):
        c = RejectRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_reject_receive_funds_request_then_receive_funds_rejected_event(
        self, *_
    ):
        c = RejectRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, notified_receive_funds_requested_event_v2]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    RequestRejected,
                    agg_id=ACCOUNT_ID,
                    version=self.aggregate.version + 1,
                    attributes={
                        FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                    },
                )
            ],
        )

    def test_when_reject_receive_funds_request_duplicate_then_no_new_events_and_command_succeeds(
        self, *_
    ):
        c = RejectRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [
                account_created_event,
                notified_receive_funds_requested_event_v2,
                receive_funds_rejected_event,
            ]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_accept_receive_funds_request_and_transaction_not_found_then_no_new_events_and_command_fails(
        self, *_
    ):
        c = AcceptRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_accept_receive_funds_request_then_receive_funds_approved_event(
        self, *_
    ):
        c = AcceptRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [
                account_created_event,
                large_funds_deposited_event,
                notified_receive_funds_requested_event_v3,
            ]
        )
        self.assertTrue(self.aggregate.validate(c))
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    event_type=RequestApproved,
                    agg_id=ACCOUNT_ID,
                    version=self.aggregate.version + 1,
                    attributes={
                        FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                    },
                )
            ],
        )

    def test_when_accept_receive_funds_request_duplicate_then_no_new_events_and_command_succeeds(
        self, *_
    ):
        c = AcceptRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [
                account_created_event,
                large_funds_deposited_event,
                notified_receive_funds_requested_event_v3,
                receive_funds_approved_event,
            ]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_notify_receive_funds_rejected_and_transaction_not_found_then_command_fails_and_no_new_events(
        self, *_
    ):
        c = NotifyRequestRejected(
            funded_account_id=ACCOUNT_ID,
            funding_account_id=OTHER_ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
        )
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_notify_receive_funds_rejected_then_notified_receive_funds_rejected_event(
        self, *_
    ):
        c = NotifyRequestRejected(
            funded_account_id=ACCOUNT_ID,
            funding_account_id=OTHER_ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
        )
        self.aggregate.apply_events(
            [account_created_event, receive_large_funds_requested_event]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    event_type=RequestRejectionReceived,
                    agg_id=ACCOUNT_ID,
                    version=self.aggregate.version + 1,
                    attributes={
                        FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                    },
                )
            ],
        )

    def test_when_notify_receive_funds_rejected_duplicate_then_no_new_events_and_command_succeeds(
        self, *_
    ):
        c = NotifyRequestRejected(
            funded_account_id=ACCOUNT_ID,
            funding_account_id=OTHER_ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
        )
        self.aggregate.apply_events(
            [
                account_created_event,
                receive_large_funds_requested_event,
                notified_receive_funds_rejected_event,
            ]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_debit_receive_funds_and_transaction_not_found_then_command_fails_and_no_new_events(
        self, *_
    ):
        c = DebitRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_debit_receive_funds_then_receive_funds_debited_event(self, *_):
        c = DebitRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [
                account_created_event,
                large_funds_deposited_event,
                notified_receive_funds_requested_event_v3,
                receive_funds_approved_event,
            ]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    event_type=RequestDebited,
                    agg_id=ACCOUNT_ID,
                    version=self.aggregate.version + 1,
                    attributes={
                        FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        BALANCE_FLD: 0,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                        AMOUNT_FLD: LARGE_AMOUNT,
                    },
                )
            ],
        )

    def test_when_debit_receive_funds_duplicate_command_then_command_succeeds_and_no_new_events(
        self, *_
    ):
        c = DebitRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [
                account_created_event,
                large_funds_deposited_event,
                notified_receive_funds_requested_event_v3,
                receive_funds_approved_event,
                receive_funds_debited_event,
            ]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_credit_receive_funds_and_transaction_id_not_found_then_command_fails_and_no_new_events(
        self, *_
    ):
        c = CreditRequest(funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_credit_receive_funds_then_receive_funds_credited_event(self, *_):
        c = CreditRequest(funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, receive_large_funds_requested_event]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    event_type=RequestCredited,
                    agg_id=ACCOUNT_ID,
                    version=3,
                    attributes={
                        FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                        BALANCE_FLD: LARGE_AMOUNT,
                        AMOUNT_FLD: LARGE_AMOUNT,
                    },
                )
            ],
        )

    def test_when_credit_receive_funds_duplicate_then_command_succeeds_and_no_new_events(
        self, *_
    ):
        receive_funds_credited_event = RequestCredited(
            version=3,
            funded_account_id=ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
            balance=LARGE_AMOUNT,
            amount=LARGE_AMOUNT,
        )
        c = CreditRequest(funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [
                account_created_event,
                receive_large_funds_requested_event,
                receive_funds_credited_event,
            ]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_credit_send_funds_then_send_funds_credited_event(self, *_):
        c = CreditTransfer(
            funding_account_id=OTHER_ACCOUNT_ID,
            funded_account_id=ACCOUNT_ID,
            amount=SMALL_AMOUNT,
            transaction_id=KNOWN_UUID,
        )
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    event_type=TransferCredited,
                    agg_id=ACCOUNT_ID,
                    version=self.aggregate.version + 1,
                    attributes={
                        FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        FUNDING_ACCOUNT_ID_FLD: OTHER_ACCOUNT_ID,
                        AMOUNT_FLD: SMALL_AMOUNT,
                        BALANCE_FLD: SMALL_AMOUNT,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                    },
                )
            ],
        )

    def test_when_credit_send_funds_duplicate_then_command_succeeds_and_no_new_events(
        self, *_
    ):
        send_funds_credited_event = TransferCredited(
            version=2,
            funded_account_id=ACCOUNT_ID,
            funding_account_id=OTHER_ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
            amount=SMALL_AMOUNT,
            balance=SMALL_AMOUNT,
        )
        c = CreditTransfer(
            funding_account_id=OTHER_ACCOUNT_ID,
            funded_account_id=ACCOUNT_ID,
            amount=SMALL_AMOUNT,
            transaction_id=KNOWN_UUID,
        )
        self.aggregate.apply_events([account_created_event, send_funds_credited_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_rollback_send_funds_debit_and_transaction_id_not_found_then_command_fails_and_no_new_events(
        self, *_
    ):
        c = RollbackTransferDebit(
            amount=SMALL_AMOUNT,
            funding_account_id=ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
        )
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_rollback_send_funds_debit_then_send_funds_debit_rolled_back_event(
        self, *_
    ):
        send_transaction_id = str(uuid4())
        funds_deposited_event = FundsDeposited(
            version=2,
            account_id=ACCOUNT_ID,
            amount=LARGE_AMOUNT,
            balance=LARGE_AMOUNT,
            transaction_id=send_transaction_id,
        )
        send_funds_debited = TransferDebited(
            version=3,
            funded_account_id=OTHER_ACCOUNT_ID,
            funding_account_id=ACCOUNT_ID,
            amount=SMALL_AMOUNT,
            balance=LARGE_AMOUNT - SMALL_AMOUNT,
            transaction_id=KNOWN_UUID,
        )
        c = RollbackTransferDebit(
            amount=SMALL_AMOUNT,
            funding_account_id=ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
        )
        self.aggregate.apply_events(
            [account_created_event, funds_deposited_event, send_funds_debited]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    event_type=TransferDebitRolledBack,
                    agg_id=ACCOUNT_ID,
                    version=4,
                    attributes={
                        AMOUNT_FLD: SMALL_AMOUNT,
                        FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        BALANCE_FLD: LARGE_AMOUNT,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                    },
                )
            ],
        )

    def test_when_rollback_send_funds_debit_duplicate_then_command_succeeds_and_no_new_events(
        self, *_
    ):
        send_transaction_id = str(uuid4())
        funds_deposited_event = FundsDeposited(
            version=2,
            account_id=ACCOUNT_ID,
            amount=LARGE_AMOUNT,
            balance=LARGE_AMOUNT,
            transaction_id=send_transaction_id,
        )
        send_funds_debited = TransferDebited(
            version=3,
            funded_account_id=OTHER_ACCOUNT_ID,
            funding_account_id=ACCOUNT_ID,
            amount=SMALL_AMOUNT,
            balance=LARGE_AMOUNT - SMALL_AMOUNT,
            transaction_id=KNOWN_UUID,
        )
        send_funds_debited_rolled_back_event = TransferDebitRolledBack(
            version=4,
            amount=SMALL_AMOUNT,
            funding_account_id=ACCOUNT_ID,
            balance=LARGE_AMOUNT,
            transaction_id=KNOWN_UUID,
        )
        c = RollbackTransferDebit(
            amount=SMALL_AMOUNT,
            funding_account_id=ACCOUNT_ID,
            transaction_id=KNOWN_UUID,
        )
        self.aggregate.apply_events(
            [
                account_created_event,
                funds_deposited_event,
                send_funds_debited,
                send_funds_debited_rolled_back_event,
            ]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_rollback_receive_funds_debit_and_transaction_not_found_then_command_fails_and_no_new_events(
        self, *_
    ):
        c = RollbackRequestDebit(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID
        )
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_rollback_receive_funds_debit_then_receive_funds_debit_rolled_back_event(
        self, *_
    ):
        receive_funds_approved_event = RequestApproved(
            version=3, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID
        )
        receive_funds_debited_event = RequestDebited(
            version=5,
            funding_account_id=ACCOUNT_ID,
            balance=Decimal(0),
            transaction_id=KNOWN_UUID,
            amount=LARGE_AMOUNT,
        )
        c = RollbackRequestDebit(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID
        )
        self.aggregate.apply_events(
            [
                account_created_event,
                large_funds_deposited_event,
                notified_receive_funds_requested_event_v3,
                receive_funds_approved_event,
                receive_funds_debited_event,
            ]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(
            test_case=self,
            events=self.aggregate.new_events,
            expected_events=[
                ExpectedEvent(
                    event_type=RequestDebitRolledBack,
                    agg_id=ACCOUNT_ID,
                    version=self.aggregate.version + 1,
                    attributes={
                        FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                        TRANSACTION_ID_FLD: KNOWN_UUID,
                        BALANCE_FLD: LARGE_AMOUNT,
                        AMOUNT_FLD: LARGE_AMOUNT,
                    },
                )
            ],
        )

    def test_when_rollback_receive_funds_debit_duplicate_then_command_succeeds_and_no_new_events(
        self, *_
    ):
        receive_funds_approved_event = RequestApproved(
            version=3, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID
        )
        receive_funds_debited_event = RequestDebited(
            version=5,
            funding_account_id=ACCOUNT_ID,
            balance=Decimal(0),
            transaction_id=KNOWN_UUID,
            amount=LARGE_AMOUNT,
        )
        receive_funds_debited_rolled_back = RequestDebitRolledBack(
            version=6,
            funding_account_id=ACCOUNT_ID,
            balance=LARGE_AMOUNT,
            amount=LARGE_AMOUNT,
            transaction_id=KNOWN_UUID,
        )
        c = RollbackRequestDebit(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID
        )
        self.aggregate.apply_events(
            [
                account_created_event,
                large_funds_deposited_event,
                notified_receive_funds_requested_event_v3,
                receive_funds_approved_event,
                receive_funds_debited_event,
                receive_funds_debited_rolled_back,
            ]
        )
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_transaction_timed_out_then_command_fails(self, *_):
        notified_receive_funds_requested_event = RequestReceived(
            version=3,
            funded_account_id=OTHER_ACCOUNT_ID,
            funding_account_id=ACCOUNT_ID,
            amount=LARGE_AMOUNT,
            transaction_id=KNOWN_UUID,
            timeout_at=FAKE_CURRENT_TIME,
        )
        c = AcceptRequest(funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [
                account_created_event,
                large_funds_deposited_event,
                notified_receive_funds_requested_event,
            ]
        )
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)


account_created_event = AccountCreated(
    account_id=ACCOUNT_ID, version=1, name=ACCOUNT_NAME
)
small_funds_deposited_event = FundsDeposited(
    version=2,
    account_id=ACCOUNT_ID,
    amount=SMALL_AMOUNT,
    balance=SMALL_AMOUNT,
    transaction_id=KNOWN_UUID,
)
large_funds_deposited_event = FundsDeposited(
    version=2,
    account_id=ACCOUNT_ID,
    amount=LARGE_AMOUNT,
    balance=LARGE_AMOUNT,
    transaction_id=KNOWN_UUID,
)
receive_large_funds_requested_event = RequestCreated(
    version=2,
    funded_account_id=ACCOUNT_ID,
    funding_account_id=OTHER_ACCOUNT_ID,
    amount=LARGE_AMOUNT,
    transaction_id=KNOWN_UUID,
    timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW,
)
small_funds_withdrawn_event = FundsWithdrawn(
    version=2,
    account_id=ACCOUNT_ID,
    amount=SMALL_AMOUNT,
    balance=-SMALL_AMOUNT,
    transaction_id=KNOWN_UUID,
)
debt_forgiven_event = DebtForgiven(
    version=3, account_id=ACCOUNT_ID, amount=SMALL_AMOUNT, transaction_id=KNOWN_UUID
)
second_small_funds_withdrawn_event = FundsWithdrawn(
    version=4,
    account_id=ACCOUNT_ID,
    amount=SMALL_AMOUNT,
    balance=-SMALL_AMOUNT,
    transaction_id=KNOWN_UUID,
)
second_debt_forgiven_event = DebtForgiven(
    version=5, account_id=ACCOUNT_ID, amount=SMALL_AMOUNT, transaction_id=KNOWN_UUID
)
account_deleted_event = AccountDeleted(account_id=ACCOUNT_ID, version=2)
notified_receive_funds_requested_event_v2 = RequestReceived(
    version=2,
    funded_account_id=OTHER_ACCOUNT_ID,
    funding_account_id=ACCOUNT_ID,
    amount=LARGE_AMOUNT,
    transaction_id=KNOWN_UUID,
    timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW,
)
notified_receive_funds_requested_event_v3 = RequestReceived(
    version=3,
    funded_account_id=OTHER_ACCOUNT_ID,
    funding_account_id=ACCOUNT_ID,
    amount=LARGE_AMOUNT,
    transaction_id=KNOWN_UUID,
    timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW,
)
receive_funds_rejected_event = RequestRejected(
    version=3, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID
)
receive_funds_approved_event = RequestApproved(
    version=4, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID
)
notified_receive_funds_rejected_event = RequestRejectionReceived(
    funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID, version=3
)
receive_funds_debited_event = RequestDebited(
    version=5,
    funding_account_id=ACCOUNT_ID,
    balance=Decimal(0),
    transaction_id=KNOWN_UUID,
    amount=LARGE_AMOUNT,
)
