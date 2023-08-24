from datetime import datetime
from typing import Callable
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import Mock, patch
from uuid import uuid4
from pyjangle import Aggregate, Event
from pyjangle_example.aggregates.account_aggregate import AccountAggregate

from pyjangle_example.aggregates.account_creation_aggregate_test import KNOWN_UUID
from pyjangle_example.commands import AcceptReceiveFundsRequest, CreditReceiveFunds, CreditSendFunds, DebitReceiveFunds, DeleteAccount, DepositFunds, NotifyReceiveFundsRejected, ReceiveFunds, RejectReceiveFundsRequest, RequestForgiveness, RollbackReceiveFundsDebit, RollbackSendFundsDebit, SendFunds, TryObtainReceiveFundsApproval, WithdrawFunds
from pyjangle_example.events import AccountCreated, AccountDeleted, DebtForgiven, FundsDeposited, FundsWithdrawn, NotifiedReceiveFundsRequested, NotifiedReceivedFundsRejected, ReceiveFundsApproved, ReceiveFundsCredited, ReceiveFundsDebited, ReceiveFundsDebitedRolledBack, ReceiveFundsRejected, ReceiveFundsRequested, SendFundsCredited, SendFundsDebited, SendFundsDebitedRolledBack
from pyjangle_example.test_helpers import CREATED_AT_FLD, FAKE_CURRENT_TIME, FUNDING_ACCOUNT_ID_FLD, FUNDED_ACCOUNT_ID_FLD, ACCOUNT_ID, ACCOUNT_ID_FLD, ACCOUNT_NAME, AMOUNT_FLD, BALANCE_FLD, THIRTY_MINUTES_FROM_FAKE_NOW, LARGE_AMOUNT, OTHER_ACCOUNT_ID, SMALL_AMOUNT, TIMEOUT_AT_FLD, TRANSACTION_ID_FLD, ExpectedEvent, verify_events, datetime_mock

account_created_event = AccountCreated(
    account_id=ACCOUNT_ID, version=1, name=ACCOUNT_NAME)
small_funds_deposited_event = FundsDeposited(
    version=2, account_id=ACCOUNT_ID, amount=SMALL_AMOUNT, balance=SMALL_AMOUNT, transaction_id=KNOWN_UUID)
large_funds_deposited_event = FundsDeposited(
    version=2, account_id=ACCOUNT_ID, amount=LARGE_AMOUNT, balance=LARGE_AMOUNT, transaction_id=KNOWN_UUID)
receive_large_funds_requested_event = ReceiveFundsRequested(
    version=2, funded_account_id=ACCOUNT_ID, funding_account_id=OTHER_ACCOUNT_ID, amount=LARGE_AMOUNT, transaction_id=KNOWN_UUID, timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW)
small_funds_withdrawn_event = FundsWithdrawn(
    version=2, account_id=ACCOUNT_ID, amount=SMALL_AMOUNT, balance=-SMALL_AMOUNT, transaction_id=KNOWN_UUID)
debt_forgiven_event = DebtForgiven(
    version=3, account_id=ACCOUNT_ID, amount=SMALL_AMOUNT, transaction_id=KNOWN_UUID)
second_small_funds_withdrawn_event = FundsWithdrawn(
    version=4, account_id=ACCOUNT_ID, amount=SMALL_AMOUNT, balance=-SMALL_AMOUNT, transaction_id=KNOWN_UUID)
second_debt_forgiven_event = DebtForgiven(
    version=5, account_id=ACCOUNT_ID, amount=SMALL_AMOUNT, transaction_id=KNOWN_UUID)
account_deleted_event = AccountDeleted(
    account_id=ACCOUNT_ID, version=2)
notified_receive_funds_requested_event_v2 = NotifiedReceiveFundsRequested(version=2, funded_account_id=OTHER_ACCOUNT_ID,
                                                                          funding_account_id=ACCOUNT_ID, amount=LARGE_AMOUNT, transaction_id=KNOWN_UUID, timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW)
notified_receive_funds_requested_event_v3 = NotifiedReceiveFundsRequested(version=3, funded_account_id=OTHER_ACCOUNT_ID,
                                                                          funding_account_id=ACCOUNT_ID, amount=LARGE_AMOUNT, transaction_id=KNOWN_UUID, timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW)
receive_funds_rejected_event = ReceiveFundsRejected(
    version=3, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
receive_funds_approved_event = ReceiveFundsApproved(
    version=4, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
notified_receive_funds_rejected_event = NotifiedReceivedFundsRejected(
    funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID, version=3)
receive_funds_debited_event = ReceiveFundsDebited(
    version=5, funding_account_id=ACCOUNT_ID, balance=0, transaction_id=KNOWN_UUID, amount=LARGE_AMOUNT)


@patch(f"{AccountAggregate.__module__}.datetime", new=datetime_mock)
@patch("uuid.uuid4", return_value=KNOWN_UUID)
class TestAccountAggregate(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.aggregate = AccountAggregate(ACCOUNT_ID)

    def test_when_account_not_exists_then_commands_fail_and_no_new_events(self, *_):
        self.assertFalse(self.aggregate.validate(DepositFunds(
            account_id=ACCOUNT_ID, amount=SMALL_AMOUNT)).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_deposit_funds_then_funds_deposited_event(self, *_):
        c = DepositFunds(account_id=ACCOUNT_ID, amount=SMALL_AMOUNT)
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_length=1, expected_events=[
            ExpectedEvent(event_type=FundsDeposited, agg_id=ACCOUNT_ID, version=2, attributes={
                ACCOUNT_ID_FLD: ACCOUNT_ID,
                AMOUNT_FLD: SMALL_AMOUNT,
                BALANCE_FLD: SMALL_AMOUNT,
                TRANSACTION_ID_FLD: KNOWN_UUID
            })
        ])

    def test_when_withdraw_insufficient_funds_then_command_failed_and_no_new_events(self, *_):
        c = WithdrawFunds(account_id=ACCOUNT_ID, amount=LARGE_AMOUNT)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_withdraw_sufficient_funds_then_fudns_withdrawn_event(self, *_):
        c = WithdrawFunds(account_id=ACCOUNT_ID, amount=SMALL_AMOUNT)
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c))
        verify_events(test_case=self, events=self.aggregate.new_events, expected_length=1, expected_events=[
            ExpectedEvent(event_type=FundsWithdrawn, agg_id=ACCOUNT_ID, version=2, attributes={
                ACCOUNT_ID_FLD: ACCOUNT_ID,
                AMOUNT_FLD: SMALL_AMOUNT,
                BALANCE_FLD: -SMALL_AMOUNT,
                TRANSACTION_ID_FLD: KNOWN_UUID
            })
        ])

    def test_when_send_funds_and_balance_insufficient_then_no_new_events_and_command_fails(self, *_):
        c = SendFunds(funded_account_id=OTHER_ACCOUNT_ID,
                      funding_account_id=ACCOUNT_ID, amount=LARGE_AMOUNT)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_send_funds_then_send_funds_debited(self, *_):
        c = SendFunds(funded_account_id=OTHER_ACCOUNT_ID,
                      funding_account_id=ACCOUNT_ID, amount=SMALL_AMOUNT)
        self.aggregate.apply_events([account_created_event,
                                    small_funds_deposited_event])
        self.assertTrue(self.aggregate.validate(c).is_success)

        verify_events(events=self.aggregate.new_events, test_case=self, expected_events=[
            ExpectedEvent(event_type=SendFundsDebited, agg_id=ACCOUNT_ID, version=3, attributes={
                FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                FUNDED_ACCOUNT_ID_FLD: OTHER_ACCOUNT_ID,
                AMOUNT_FLD: SMALL_AMOUNT,
                BALANCE_FLD: 0,
                TRANSACTION_ID_FLD: KNOWN_UUID
            })
        ])

    def test_when_receive_funds_then_receive_funds_requested_event(self, *_):
        c = ReceiveFunds(funded_account_id=ACCOUNT_ID,
                         funding_account_id=OTHER_ACCOUNT_ID, amount=LARGE_AMOUNT)
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)

        verify_events(events=self.aggregate.new_events, test_case=self, expected_events=[
            ExpectedEvent(ReceiveFundsRequested, agg_id=ACCOUNT_ID, version=self.aggregate.version + 1, attributes={
                FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                FUNDING_ACCOUNT_ID_FLD: OTHER_ACCOUNT_ID,
                AMOUNT_FLD: LARGE_AMOUNT,
                TRANSACTION_ID_FLD: KNOWN_UUID,
                TIMEOUT_AT_FLD: THIRTY_MINUTES_FROM_FAKE_NOW.isoformat()
            })
        ])

    def test_when_credit_send_funds_and_no_corresponding_transaction_then_no_new_events_and_command_fails(self, *_):
        c = CreditReceiveFunds(
            funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_credit_send_funds_then_send_funds_credited_event(self, *_):
        c = CreditReceiveFunds(
            funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event,
                                    receive_large_funds_requested_event])
        self.assertTrue(
            self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(ReceiveFundsCredited, agg_id=ACCOUNT_ID, version=self.aggregate.version + 1, attributes={
                FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                TRANSACTION_ID_FLD: KNOWN_UUID,
                BALANCE_FLD: LARGE_AMOUNT,
                AMOUNT_FLD: LARGE_AMOUNT
            })
        ])

    def test_when_request_forgiveness_and_not_needed_then_command_fails_and_no_new_events(self, *_):
        c = RequestForgiveness(account_id=ACCOUNT_ID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_request_forgiveness_then_debt_forgiven_event(self, *_):
        c = RequestForgiveness(account_id=ACCOUNT_ID)
        self.aggregate.apply_events([account_created_event,
                                    small_funds_withdrawn_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(DebtForgiven, ACCOUNT_ID, self.aggregate.version + 1, attributes={
                ACCOUNT_ID_FLD: ACCOUNT_ID,
                AMOUNT_FLD: SMALL_AMOUNT,
                TRANSACTION_ID_FLD: KNOWN_UUID
            })
        ])

    def test_when_request_forgiveness_too_many_times_then_command_fails_and_no_new_events(self, *_):
        c = RequestForgiveness(account_id=ACCOUNT_ID)
        self.aggregate.apply_events([account_created_event, small_funds_withdrawn_event,
                                    debt_forgiven_event, second_small_funds_withdrawn_event, second_debt_forgiven_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_delete_account_then_account_deleted_event(self, *_):
        c = DeleteAccount(account_id=ACCOUNT_ID)
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(AccountDeleted, ACCOUNT_ID, 2, attributes={
                ACCOUNT_ID_FLD: ACCOUNT_ID
            })
        ])

    def test_when_delete_account_and_already_deleted_no_new_events_and_command_succeeds(self, *_):
        c = DeleteAccount(account_id=ACCOUNT_ID)
        self.aggregate.apply_events(
            [account_created_event, account_deleted_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_try_obtain_receive_funds_approval_then_notified_receive_funds_request(self, *_):
        c = TryObtainReceiveFundsApproval(funded_account_id=OTHER_ACCOUNT_ID, funding_account_id=ACCOUNT_ID,
                                          transaction_id=KNOWN_UUID, timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW, amount=LARGE_AMOUNT)
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(event_type=NotifiedReceiveFundsRequested, agg_id=ACCOUNT_ID, version=2, attributes={
                FUNDED_ACCOUNT_ID_FLD: OTHER_ACCOUNT_ID,
                FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                AMOUNT_FLD: LARGE_AMOUNT,
                TRANSACTION_ID_FLD: KNOWN_UUID,
                TIMEOUT_AT_FLD: THIRTY_MINUTES_FROM_FAKE_NOW
            })
        ])

    def test_when_try_obtain_receive_funds_approval_duplicate_then_no_events_and_command_succeeds(self, *_):
        c = TryObtainReceiveFundsApproval(funded_account_id=OTHER_ACCOUNT_ID, funding_account_id=ACCOUNT_ID,
                                          transaction_id=KNOWN_UUID, timeout_at=THIRTY_MINUTES_FROM_FAKE_NOW, amount=LARGE_AMOUNT)
        self.aggregate.apply_events(
            [account_created_event, notified_receive_funds_requested_event_v2])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_reject_receive_funds_request_and_transaction_not_found_then_no_new_events_and_command_fails(self, *_):
        c = RejectReceiveFundsRequest(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_reject_receive_funds_request_then_receive_funds_rejected_event(self, *_):
        c = RejectReceiveFundsRequest(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, notified_receive_funds_requested_event_v2])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(ReceiveFundsRejected, agg_id=ACCOUNT_ID, version=self.aggregate.version + 1, attributes={
                FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                TRANSACTION_ID_FLD: KNOWN_UUID
            })
        ])

    def test_when_reject_receive_funds_request_duplicate_then_no_new_events_and_command_succeeds(self, *_):
        c = RejectReceiveFundsRequest(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, notified_receive_funds_requested_event_v2, receive_funds_rejected_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_accept_receive_funds_request_and_transaction_not_found_then_no_new_events_and_command_fails(self, *_):
        c = AcceptReceiveFundsRequest(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_accept_receive_funds_request_then_receive_funds_approved_event(self, *_):
        c = AcceptReceiveFundsRequest(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, large_funds_deposited_event, notified_receive_funds_requested_event_v3])
        self.assertTrue(self.aggregate.validate(c))
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(event_type=ReceiveFundsApproved, agg_id=ACCOUNT_ID, version=self.aggregate.version + 1, attributes={
                FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                TRANSACTION_ID_FLD: KNOWN_UUID
            })
        ])

    def test_when_accept_receive_funds_request_duplicate_then_no_new_events_and_command_succeeds(self, *_):
        c = AcceptReceiveFundsRequest(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, large_funds_deposited_event, notified_receive_funds_requested_event_v3, receive_funds_approved_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_notify_receive_funds_rejected_and_transaction_not_found_then_command_fails_and_no_new_events(self, *_):
        c = NotifyReceiveFundsRejected(
            funded_account_id=ACCOUNT_ID, funding_account_id=OTHER_ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_notify_receive_funds_rejected_then_notified_receive_funds_rejected_event(self, *_):
        c = NotifyReceiveFundsRejected(
            funded_account_id=ACCOUNT_ID, funding_account_id=OTHER_ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, receive_large_funds_requested_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(event_type=NotifiedReceivedFundsRejected, agg_id=ACCOUNT_ID, version=self.aggregate.version + 1, attributes={
                FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                TRANSACTION_ID_FLD: KNOWN_UUID
            })
        ])

    def test_when_notify_receive_funds_rejected_duplicate_then_no_new_events_and_command_succeeds(self, *_):
        c = NotifyReceiveFundsRejected(
            funded_account_id=ACCOUNT_ID, funding_account_id=OTHER_ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, receive_large_funds_requested_event, notified_receive_funds_rejected_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_debit_receive_funds_and_transaction_not_found_then_command_fails_and_no_new_events(self, *_):
        c = DebitReceiveFunds(funding_account_id=ACCOUNT_ID,
                              transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_debit_receive_funds_then_receive_funds_debited_event(self, *_):
        c = DebitReceiveFunds(funding_account_id=ACCOUNT_ID,
                              transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event, large_funds_deposited_event,
                                    notified_receive_funds_requested_event_v3, receive_funds_approved_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(event_type=ReceiveFundsDebited, agg_id=ACCOUNT_ID, version=self.aggregate.version + 1, attributes={
                FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                BALANCE_FLD: 0,
                TRANSACTION_ID_FLD: KNOWN_UUID,
                AMOUNT_FLD: LARGE_AMOUNT
            })
        ])

    def test_when_debit_receive_funds_duplicate_command_then_command_succeeds_and_no_new_events(self, *_):
        c = DebitReceiveFunds(funding_account_id=ACCOUNT_ID,
                              transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event, large_funds_deposited_event,
                                    notified_receive_funds_requested_event_v3, receive_funds_approved_event, receive_funds_debited_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_credit_receive_funds_and_transaction_id_not_found_then_command_fails_and_no_new_events(self, *_):
        c = CreditReceiveFunds(
            funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_credit_receive_funds_then_receive_funds_credited_event(self, *_):
        c = CreditReceiveFunds(
            funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, receive_large_funds_requested_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(event_type=ReceiveFundsCredited, agg_id=ACCOUNT_ID, version=3, attributes={
                FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                TRANSACTION_ID_FLD: KNOWN_UUID,
                BALANCE_FLD: LARGE_AMOUNT,
                AMOUNT_FLD: LARGE_AMOUNT
            })
        ])

    def test_when_credit_receive_funds_duplicate_then_command_succeeds_and_no_new_events(self, *_):
        receive_funds_credited_event = ReceiveFundsCredited(
            version=3, funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID, balance=LARGE_AMOUNT, amount=LARGE_AMOUNT)
        c = CreditReceiveFunds(
            funded_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, receive_large_funds_requested_event, receive_funds_credited_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_credit_send_funds_then_send_funds_credited_event(self, *_):
        c = CreditSendFunds(funding_account_id=OTHER_ACCOUNT_ID, funded_account_id=ACCOUNT_ID,
                            amount=SMALL_AMOUNT, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(event_type=SendFundsCredited, agg_id=ACCOUNT_ID, version=self.aggregate.version + 1, attributes={
                FUNDED_ACCOUNT_ID_FLD: ACCOUNT_ID,
                FUNDING_ACCOUNT_ID_FLD: OTHER_ACCOUNT_ID,
                AMOUNT_FLD: SMALL_AMOUNT,
                BALANCE_FLD: SMALL_AMOUNT,
                TRANSACTION_ID_FLD: KNOWN_UUID
            })
        ])

    def test_when_credit_send_funds_duplicate_then_command_succeeds_and_no_new_events(self, *_):
        send_funds_credited_event = SendFundsCredited(
            version=2, funded_account_id=ACCOUNT_ID, funding_account_id=OTHER_ACCOUNT_ID, transaction_id=KNOWN_UUID, amount=SMALL_AMOUNT, balance=SMALL_AMOUNT)
        c = CreditSendFunds(funding_account_id=OTHER_ACCOUNT_ID, funded_account_id=ACCOUNT_ID,
                            amount=SMALL_AMOUNT, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, send_funds_credited_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_rollback_send_funds_debit_and_transaction_id_not_found_then_command_fails_and_no_new_events(self, *_):
        c = RollbackSendFundsDebit(
            amount=SMALL_AMOUNT, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_rollback_send_funds_debit_then_send_funds_debit_rolled_back_event(self, *_):
        send_transaction_id = uuid4()
        funds_deposited_event = FundsDeposited(
            version=2, account_id=ACCOUNT_ID, amount=LARGE_AMOUNT, balance=LARGE_AMOUNT, transaction_id=send_transaction_id)
        send_funds_debited = SendFundsDebited(version=3, funded_account_id=OTHER_ACCOUNT_ID, funding_account_id=ACCOUNT_ID,
                                              amount=SMALL_AMOUNT, balance=LARGE_AMOUNT - SMALL_AMOUNT, transaction_id=KNOWN_UUID)
        c = RollbackSendFundsDebit(
            amount=SMALL_AMOUNT, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, funds_deposited_event, send_funds_debited])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(event_type=SendFundsDebitedRolledBack, agg_id=ACCOUNT_ID, version=4, attributes={
                AMOUNT_FLD: SMALL_AMOUNT,
                FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                BALANCE_FLD: LARGE_AMOUNT,
                TRANSACTION_ID_FLD: KNOWN_UUID
            })
        ])

    def test_when_rollback_send_funds_debit_duplicate_then_command_succeeds_and_no_new_events(self, *_):
        send_transaction_id = uuid4()
        funds_deposited_event = FundsDeposited(
            version=2, account_id=ACCOUNT_ID, amount=LARGE_AMOUNT, balance=LARGE_AMOUNT, transaction_id=send_transaction_id)
        send_funds_debited = SendFundsDebited(version=3, funded_account_id=OTHER_ACCOUNT_ID, funding_account_id=ACCOUNT_ID,
                                              amount=SMALL_AMOUNT, balance=LARGE_AMOUNT - SMALL_AMOUNT, transaction_id=KNOWN_UUID)
        send_funds_debited_rolled_back_event = SendFundsDebitedRolledBack(
            version=4, amount=SMALL_AMOUNT, funding_account_id=ACCOUNT_ID, balance=LARGE_AMOUNT, transaction_id=KNOWN_UUID)
        c = RollbackSendFundsDebit(
            amount=SMALL_AMOUNT, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events(
            [account_created_event, funds_deposited_event, send_funds_debited, send_funds_debited_rolled_back_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_rollback_receive_funds_debit_and_transaction_not_found_then_command_fails_and_no_new_events(self, *_):
        c = RollbackReceiveFundsDebit(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_rollback_receive_funds_debit_then_receive_funds_debit_rolled_back_event(self, *_):
        receive_funds_approved_event = ReceiveFundsApproved(
            version=3, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        receive_funds_debited_event = ReceiveFundsDebited(
            version=5, funding_account_id=ACCOUNT_ID, balance=0, transaction_id=KNOWN_UUID, amount=LARGE_AMOUNT)
        c = RollbackReceiveFundsDebit(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event, large_funds_deposited_event,
                                    notified_receive_funds_requested_event_v3, receive_funds_approved_event, receive_funds_debited_event])
        self.assertTrue(self.aggregate.validate(c).is_success)
        verify_events(test_case=self, events=self.aggregate.new_events, expected_events=[
            ExpectedEvent(event_type=ReceiveFundsDebitedRolledBack, agg_id=ACCOUNT_ID, version=self.aggregate.version + 1, attributes={
                FUNDING_ACCOUNT_ID_FLD: ACCOUNT_ID,
                TRANSACTION_ID_FLD: KNOWN_UUID,
                BALANCE_FLD: LARGE_AMOUNT,
                AMOUNT_FLD: LARGE_AMOUNT,
            })
        ])

    def test_when_rollback_receive_funds_debit_duplicate_then_command_succeeds_and_no_new_events(self, *_):
        receive_funds_approved_event = ReceiveFundsApproved(
            version=3, funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        receive_funds_debited_event = ReceiveFundsDebited(
            version=5, funding_account_id=ACCOUNT_ID, balance=0, transaction_id=KNOWN_UUID, amount=LARGE_AMOUNT)
        receive_funds_debited_rolled_back = ReceiveFundsDebitedRolledBack(
            version=6, funding_account_id=ACCOUNT_ID, balance=LARGE_AMOUNT, amount=LARGE_AMOUNT, transaction_id=KNOWN_UUID)
        c = RollbackReceiveFundsDebit(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event, large_funds_deposited_event,
                                    notified_receive_funds_requested_event_v3, receive_funds_approved_event, receive_funds_debited_event, receive_funds_debited_rolled_back])
        self.assertTrue(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)

    def test_when_transaction_timed_out_then_command_fails(self, *_):
        notified_receive_funds_requested_event = NotifiedReceiveFundsRequested(version=3, funded_account_id=OTHER_ACCOUNT_ID,
                                                                               funding_account_id=ACCOUNT_ID, amount=LARGE_AMOUNT, transaction_id=KNOWN_UUID, timeout_at=FAKE_CURRENT_TIME)
        c = AcceptReceiveFundsRequest(
            funding_account_id=ACCOUNT_ID, transaction_id=KNOWN_UUID)
        self.aggregate.apply_events([account_created_event,
                                    large_funds_deposited_event, notified_receive_funds_requested_event])
        self.assertFalse(self.aggregate.validate(c).is_success)
        self.assertFalse(self.aggregate.new_events)
