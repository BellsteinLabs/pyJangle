# from datetime import datetime, timedelta
# from decimal import Decimal
# import os
# from unittest import IsolatedAsyncioTestCase
# from unittest.mock import Mock, patch
# import uuid
# from pyjangle.event.event import Event, VersionedEvent
# from pyjangle import default_event_dispatcher
# from pyjangle.query.handlers import handle_query
# from example.data_access.db_settings import get_db_jangle_banking_path
# from test_helpers.registration_paths import EVENT_ID_FACTORY
# from example.events import (
#     AccountCreated,
#     AccountDeleted,
#     DebtForgiven,
#     FundsDeposited,
#     FundsWithdrawn,
#     RequestReceived,
#     RequestRejectionReceived,
#     RequestApproved,
#     RequestCredited,
#     RequestDebited,
#     RequestDebitRolledBack,
#     RequestRejected,
#     RequestCreated,
#     TransferCredited,
#     TransferDebited,
#     TransferDebitRolledBack,
# )
# from example.queries import (
#     AccountLedger,
#     AccountSummary,
#     BankStats,
#     AccountsList,
# )
# from example.query_responses import (
#     AccountResponse,
#     AccountSummaryResponse,
#     BankStatsResponse,
#     TransactionResponse,
#     TransferResponse,
# )
# from example.data_access.sqlite3_bank_data_access_object import (
#     Sqlite3BankDataAccessObject,
#     create_database,
# )

# ACCOUNT_ID = "000005"
# OTHER_ACCOUNT_ID = "000006"
# NAME = "HERMIONE"
# OTHER_NAME = "RONALD"
# AMOUNT = Decimal("50.55")
# TRANSACTION_ID_1 = str(uuid.uuid4())
# TRANSACTION_ID_2 = str(uuid.uuid4())
# FAKE_CURRENT_TIME = datetime.min.isoformat()
# FAKE_CURRENT_TIME_PLUS = datetime.min + timedelta(seconds=30)
# datetime_mock = Mock(wraps=datetime)
# datetime_mock.now = Mock(return_value=datetime.min)


# @patch(EVENT_ID_FACTORY, new=lambda: str(uuid.uuid4()))
# @patch(f"{Event.__module__}.datetime", new=datetime_mock)
# class TestSqlite3BankDataAccessObjectTest(IsolatedAsyncioTestCase):
#     def setUp(self) -> None:
#         if os.path.exists(get_db_jangle_banking_path()):  # pragma no cover
#             Sqlite3BankDataAccessObject.clear()  # pragma no cover
#         else:
#             create_database()

#     def tearDown(self) -> None:
#         if os.path.exists(get_db_jangle_banking_path()):  # pragma no cover
#             os.remove(get_db_jangle_banking_path())

#     async def test_when_no_events_then_queries_are_empty(self, *_):
#         bank_summary_response: list[AccountResponse] = await handle_query(
#             AccountsList()
#         )
#         bank_stats_response = await handle_query(BankStats())
#         account_summary_response = await handle_query(
#             AccountSummary(account_id=ACCOUNT_ID)
#         )
#         account_ledger_response = await handle_query(
#             AccountLedger(account_id=ACCOUNT_ID)
#         )
#         expected_account_list = []
#         expected_bank_stats = BankStatsResponse(
#             active_accounts_count=0,
#             deleted_accounts_count=0,
#             net_account_balance=0,
#             transaction_count=0,
#             average_deposit_amount=0,
#             average_withdrawal_amount=0,
#             debt_forgiven_amount=0,
#         )
#         expected_account_summary = None
#         expected_ledger_response = []

#         self.assertListEqual(bank_summary_response, expected_account_list)
#         self.assertEqual(bank_stats_response, expected_bank_stats)
#         self.assertEqual(account_summary_response, expected_account_summary)
#         self.assertListEqual(account_ledger_response, expected_ledger_response)

#     async def test_when_account_created_event_then_account_queryable(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         await publish_events(account_created)
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(0),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=0,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(0),
#                 transfer_requests=[],
#             ),
#             account_ledger=[],
#         )

#     async def test_when_account_deleted_then_account_not_queryable(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         account_deleted = AccountDeleted(version=2, account_id=ACCOUNT_ID)
#         await publish_events(account_created, account_deleted)
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=0,
#                 deleted_accounts_count=1,
#                 net_account_balance=0,
#                 transaction_count=0,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=None,
#             account_ledger=[],
#         )

#     async def test_when_funds_deposited_then_funds_queryable(self, *_):
#         account_created = AccountCreated(
#             version=1,
#             account_id=ACCOUNT_ID,
#             name=NAME,
#             created_at=datetime.now(),
#         )
#         funds_deposited = FundsDeposited(
#             version=2,
#             account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             balance=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             created_at=FAKE_CURRENT_TIME,
#         )
#         await publish_events(account_created, funds_deposited)
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(AMOUNT),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=float(AMOUNT),
#                 transaction_count=1,
#                 average_deposit_amount=float(AMOUNT),
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(AMOUNT),
#                 transfer_requests=[],
#             ),
#             account_ledger=[
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="deposit",
#                 )
#             ],
#         )

#     async def test_when_funds_withdrawn_then_funds_queryable(self, *_):
#         account_created = AccountCreated(
#             version=1,
#             account_id=ACCOUNT_ID,
#             name=NAME,
#             created_at=datetime.now(),
#         )
#         funds_withdrawn = FundsWithdrawn(
#             version=2,
#             account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             balance=-AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             created_at=FAKE_CURRENT_TIME,
#         )
#         await publish_events(account_created, funds_withdrawn)
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(-AMOUNT),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=float(-AMOUNT),
#                 transaction_count=1,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=float(AMOUNT),
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(-AMOUNT),
#                 transfer_requests=[],
#             ),
#             account_ledger=[
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="withdrawal",
#                 )
#             ],
#         )

#     async def test_when_debt_forgiven_then_balance_zero(self, *_):
#         account_created = AccountCreated(
#             version=1,
#             account_id=ACCOUNT_ID,
#             name=NAME,
#             created_at=datetime.now(),
#         )
#         funds_withdrawn = FundsWithdrawn(
#             version=2,
#             account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             balance=-AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             created_at=FAKE_CURRENT_TIME,
#         )
#         debt_forgiven = DebtForgiven(
#             version=3,
#             account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_2,
#             created_at=FAKE_CURRENT_TIME,
#         )
#         await publish_events(account_created, funds_withdrawn, debt_forgiven)
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(0),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=2,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=float(AMOUNT),
#                 debt_forgiven_amount=float(AMOUNT),
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(0),
#                 transfer_requests=[],
#             ),
#             account_ledger=[
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="withdrawal",
#                 ),
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="debt_forgiveness",
#                 ),
#             ],
#         )

#     async def test_when_receive_funds_requested_then_nothing_really_happens(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         receive_funds_requested = RequestCreated(
#             version=2,
#             funded_account_id=ACCOUNT_ID,
#             funding_account_id=OTHER_ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         await publish_events(account_created, receive_funds_requested)
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(0),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=0,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(0),
#                 transfer_requests=[],
#             ),
#             account_ledger=[],
#         )

#     async def test_when_notified_receive_funds_requested_then_pending_request_added(
#         self, *_
#     ):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         notified_receive_funds_requested = RequestReceived(
#             version=2,
#             funded_account_id=OTHER_ACCOUNT_ID,
#             funding_account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         await publish_events(account_created, notified_receive_funds_requested)
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(0),
#                     pending_request_count=1,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=0,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(0),
#                 transfer_requests=[
#                     TransferResponse(
#                         funded_account=OTHER_ACCOUNT_ID,
#                         amount=str(AMOUNT),
#                         state="request_received",
#                         timeout_at=FAKE_CURRENT_TIME_PLUS.isoformat(),
#                         transaction_id=TRANSACTION_ID_1,
#                     )
#                 ],
#             ),
#             account_ledger=[],
#         )

#     async def test_when_receive_funds_approved_then_pending_request_removed(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         notified_receive_funds_requested = RequestReceived(
#             version=2,
#             funded_account_id=OTHER_ACCOUNT_ID,
#             funding_account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         receive_funds_approved = RequestApproved(
#             version=3,
#             funding_account_id=ACCOUNT_ID,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         await publish_events(
#             account_created,
#             notified_receive_funds_requested,
#             receive_funds_approved,
#         )
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(0),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=0,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(0),
#                 transfer_requests=[],
#             ),
#             account_ledger=[],
#         )

#     async def test_when_receive_funds_rejected_then_pending_request_removed(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         notified_receive_funds_requested = RequestReceived(
#             version=2,
#             funded_account_id=OTHER_ACCOUNT_ID,
#             funding_account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         receive_funds_rejected = RequestRejected(
#             version=3,
#             funding_account_id=ACCOUNT_ID,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         await publish_events(
#             account_created,
#             notified_receive_funds_requested,
#             receive_funds_rejected,
#         )
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(0),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=0,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(0),
#                 transfer_requests=[],
#             ),
#             account_ledger=[],
#         )

#     async def test_when_notified_receive_funds_rejected_then_pending_request_removed(
#         self, *_
#     ):
#         other_account_created = AccountCreated(
#             version=1, account_id=OTHER_ACCOUNT_ID, name=OTHER_NAME
#         )
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         receive_funds_requested = RequestCreated(
#             version=2,
#             funded_account_id=ACCOUNT_ID,
#             funding_account_id=OTHER_ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         notified_receive_funds_requested = RequestReceived(
#             version=2,
#             funded_account_id=ACCOUNT_ID,
#             funding_account_id=OTHER_ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         receive_funds_rejected = RequestRejected(
#             version=3,
#             funding_account_id=ACCOUNT_ID,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         notified_receive_funds_rejected = RequestRejectionReceived(
#             version=3,
#             funded_account_id=ACCOUNT_ID,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         await publish_events(
#             other_account_created,
#             account_created,
#             receive_funds_requested,
#             notified_receive_funds_requested,
#             receive_funds_rejected,
#             notified_receive_funds_rejected,
#         )
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=OTHER_ACCOUNT_ID,
#                     name=OTHER_NAME,
#                     balance=str(0),
#                     pending_request_count=0,
#                 ),
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(0),
#                     pending_request_count=0,
#                 ),
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=2,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=0,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(0),
#                 transfer_requests=[],
#             ),
#             account_ledger=[],
#         )

#     async def test_when_receive_funds_debited_then_transaction_added(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         notified_receive_funds_requested = RequestReceived(
#             version=2,
#             funded_account_id=OTHER_ACCOUNT_ID,
#             funding_account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         receive_funds_approved = RequestApproved(
#             version=3,
#             funding_account_id=ACCOUNT_ID,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         receive_funds_debited = RequestDebited(
#             version=4,
#             funding_account_id=ACCOUNT_ID,
#             balance=-AMOUNT,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         await publish_events(
#             account_created,
#             notified_receive_funds_requested,
#             receive_funds_approved,
#             receive_funds_debited,
#         )
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(-AMOUNT),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=float(-AMOUNT),
#                 transaction_count=1,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(-AMOUNT),
#                 transfer_requests=[],
#             ),
#             account_ledger=[
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="request_debit",
#                 )
#             ],
#         )

#     async def test_when_receive_funds_debit_rolled_back_then_transaction_added(
#         self, *_
#     ):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         notified_receive_funds_requested = RequestReceived(
#             version=2,
#             funded_account_id=OTHER_ACCOUNT_ID,
#             funding_account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         receive_funds_approved = RequestApproved(
#             version=3,
#             funding_account_id=ACCOUNT_ID,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         receive_funds_debited = RequestDebited(
#             version=4,
#             funding_account_id=ACCOUNT_ID,
#             balance=-AMOUNT,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         receive_funds_debit_rolled_back = RequestDebitRolledBack(
#             version=5,
#             funding_account_id=ACCOUNT_ID,
#             transaction_id=TRANSACTION_ID_1,
#             amount=AMOUNT,
#             balance=Decimal(0),
#         )
#         await publish_events(
#             account_created,
#             notified_receive_funds_requested,
#             receive_funds_approved,
#             receive_funds_debited,
#             receive_funds_debit_rolled_back,
#         )
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(0),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=2,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(0),
#                 transfer_requests=[],
#             ),
#             account_ledger=[
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="request_debit",
#                 ),
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="request_debit_rollback",
#                 ),
#             ],
#         )

#     async def test_when_receive_funds_credited_then_transaction_added(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         other_account_created = AccountCreated(
#             version=1, account_id=OTHER_ACCOUNT_ID, name=OTHER_NAME
#         )
#         receive_funds_requested = RequestCreated(
#             version=2,
#             funded_account_id=ACCOUNT_ID,
#             funding_account_id=OTHER_ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         notified_receive_funds_requested = RequestReceived(
#             version=2,
#             funded_account_id=ACCOUNT_ID,
#             funding_account_id=OTHER_ACCOUNT_ID,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#             timeout_at=FAKE_CURRENT_TIME_PLUS,
#         )
#         receive_funds_approved = RequestApproved(
#             version=3,
#             funding_account_id=OTHER_ACCOUNT_ID,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         receive_funds_debited = RequestDebited(
#             version=4,
#             funding_account_id=OTHER_ACCOUNT_ID,
#             balance=-AMOUNT,
#             amount=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         receive_funds_credited = RequestCredited(
#             version=3,
#             funded_account_id=ACCOUNT_ID,
#             transaction_id=TRANSACTION_ID_1,
#             balance=AMOUNT,
#             amount=AMOUNT,
#         )
#         await publish_events(
#             account_created,
#             other_account_created,
#             receive_funds_requested,
#             notified_receive_funds_requested,
#             receive_funds_approved,
#             receive_funds_debited,
#             receive_funds_credited,
#         )
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(AMOUNT),
#                     pending_request_count=0,
#                 ),
#                 AccountResponse(
#                     account_id=OTHER_ACCOUNT_ID,
#                     name=OTHER_NAME,
#                     balance=str(-AMOUNT),
#                     pending_request_count=0,
#                 ),
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=2,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=2,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(AMOUNT),
#                 transfer_requests=[],
#             ),
#             account_ledger=[
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="request_credit",
#                 )
#             ],
#         )

#     async def test_when_transfer_debited_transaction_added(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         other_account_created = AccountCreated(
#             version=1, account_id=OTHER_ACCOUNT_ID, name=OTHER_NAME
#         )
#         transfer_debited = TransferDebited(
#             version=2,
#             funding_account_id=ACCOUNT_ID,
#             funded_account_id=OTHER_ACCOUNT_ID,
#             amount=AMOUNT,
#             balance=-AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         await publish_events(account_created, other_account_created, transfer_debited)
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(-AMOUNT),
#                     pending_request_count=0,
#                 ),
#                 AccountResponse(
#                     account_id=OTHER_ACCOUNT_ID,
#                     name=OTHER_NAME,
#                     balance="0",
#                     pending_request_count=0,
#                 ),
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=2,
#                 deleted_accounts_count=0,
#                 net_account_balance=float(-AMOUNT),
#                 transaction_count=1,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(-AMOUNT),
#                 transfer_requests=[],
#             ),
#             account_ledger=[
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="transfer_debit",
#                 )
#             ],
#         )

#     async def test_when_transfer_debit_rolled_back_transaction_added(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         transfer_debited = TransferDebited(
#             version=2,
#             funding_account_id=ACCOUNT_ID,
#             funded_account_id=OTHER_ACCOUNT_ID,
#             amount=AMOUNT,
#             balance=-AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         transfer_debit_rolled_back = TransferDebitRolledBack(
#             version=3,
#             amount=AMOUNT,
#             funding_account_id=ACCOUNT_ID,
#             balance=Decimal(0),
#             transaction_id=TRANSACTION_ID_1,
#         )
#         await publish_events(
#             account_created, transfer_debited, transfer_debit_rolled_back
#         )
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(0),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=0,
#                 transaction_count=2,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(0),
#                 transfer_requests=[],
#             ),
#             account_ledger=[
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="transfer_debit",
#                 ),
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="transfer_debit_rollback",
#                 ),
#             ],
#         )

#     async def test_when_transfer_credit_transaction_added(self, *_):
#         account_created = AccountCreated(version=1, account_id=ACCOUNT_ID, name=NAME)
#         transfer_credited = TransferCredited(
#             version=2,
#             funding_account_id=OTHER_ACCOUNT_ID,
#             funded_account_id=ACCOUNT_ID,
#             amount=AMOUNT,
#             balance=AMOUNT,
#             transaction_id=TRANSACTION_ID_1,
#         )
#         await publish_events(account_created, transfer_credited)
#         await expect_these_query_responses(
#             test_case=self,
#             bank_summary=[
#                 AccountResponse(
#                     account_id=ACCOUNT_ID,
#                     name=NAME,
#                     balance=str(AMOUNT),
#                     pending_request_count=0,
#                 )
#             ],
#             bank_stats=BankStatsResponse(
#                 active_accounts_count=1,
#                 deleted_accounts_count=0,
#                 net_account_balance=float(AMOUNT),
#                 transaction_count=1,
#                 average_deposit_amount=0,
#                 average_withdrawal_amount=0,
#                 debt_forgiven_amount=0,
#             ),
#             account_summary=AccountSummaryResponse(
#                 account_id=ACCOUNT_ID,
#                 name=NAME,
#                 balance=str(AMOUNT),
#                 transfer_requests=[],
#             ),
#             account_ledger=[
#                 TransactionResponse(
#                     initiated_at=FAKE_CURRENT_TIME,
#                     amount=str(AMOUNT),
#                     transaction_type="transfer_credit",
#                 )
#             ],
#         )


# async def publish_events(
#     *events: tuple[VersionedEvent], num_times_publish_each_event: int = 3
# ):
#     for e in events:
#         for _ in range(num_times_publish_each_event):
#             await default_event_dispatcher(e, empty_event_completer)


# async def empty_event_completer(_):
#     pass


# async def expect_these_query_responses(
#     test_case: IsolatedAsyncioTestCase,
#     bank_summary: list[AccountResponse],
#     bank_stats: BankStatsResponse,
#     account_summary: AccountSummaryResponse,
#     account_ledger: list[TransactionResponse],
# ):
#     actual_bank_summary = await handle_query(AccountsList())
#     actual_bank_stats = await handle_query(BankStats())
#     actual_account_summary = await handle_query(AccountSummary(account_id=ACCOUNT_ID))
#     actual_account_ledger = await handle_query(AccountLedger(account_id=ACCOUNT_ID))

#     test_case.assertListEqual(actual_bank_summary, bank_summary)
#     test_case.assertEqual(actual_bank_stats, bank_stats)
#     test_case.assertEqual(actual_account_summary, account_summary)
#     test_case.assertListEqual(actual_account_ledger, account_ledger)
