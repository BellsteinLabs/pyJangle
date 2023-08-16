import sqlite3
from pyjangle.event.event_handler import register_event_handler
from pyjangle.query.handlers import register_query_handler
from pyjangle_example.data_access.bank_data_access_object import BankDataAccessObject
from pyjangle_example.events import AccountCreated, AccountDeleted, DebtForgiven, FundsDeposited, FundsWithdrawn, NotifiedReceiveFundsRequested, NotifiedReceivedFundsRejected, ReceiveFundsApproved, ReceiveFundsCredited, ReceiveFundsDebited, ReceiveFundsDebitedRolledBack, ReceiveFundsRejected, ReceiveFundsRequested, SendFundsCredited, SendFundsDebited, SendFundsDebitedRolledBack
from pyjangle_example.queries import AccountLedger, AccountSummary, BankStats, BankSummary
from pyjangle_sqllite3.event_handler_query_builder import SqlLite3QueryBuilder as q_bldr
from pyjangle_example.data_access.db_schema import TABLES, COLUMNS, TRANSACTION_STATES, TRANSACTION_TYPE
from pyjangle_example.data_access.db_settings import DB_JANGLE_BANKING_PATH
from pyjangle_example.data_access.db_utility import fetch_multiple_rows, upsert_multiple_rows, upsert_single_row, make_update_balance_query, make_update_transactions_query

with open('pyjangle_example/data_access/create_tables.sql', 'r') as create_tables_file:
    create_tables_sql_script = create_tables_file.read()
with sqlite3.connect(DB_JANGLE_BANKING_PATH) as conn:
    conn.executescript(create_tables_sql_script)
conn.close()


class Sqlite3BankDataAccessObject(BankDataAccessObject):

    @register_query_handler(BankSummary)
    @staticmethod
    @fetch_multiple_rows
    async def bank_summary(_: BankSummary) -> list[dict[str, str]]:
        return f"""
            SELECT 
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.ACCOUNT_ID}, 
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.NAME}, 
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.BALANCE},
                COUNT({TABLES.TRANSFERS}.*) as \"Pending Transfers\"
            FROM 
                {TABLES.BANK_SUMMARY}
            LEFT JOIN
                {TABLES.TRANSFER_REQUESTS} ON {TABLES.TRANSFER_REQUESTS}.{COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT} = {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.ACCOUNT_ID}
            WHERE 
                {COLUMNS.BANK_SUMMARY.IS_DELETED} = 0 AND {TABLES.TRANSFERS}.{COLUMNS.TRANSFER_REQUESTS.STATE} NOT IN (5,7)
            """

    @register_query_handler(BankStats)
    @staticmethod
    @fetch_multiple_rows
    async def bank_stats(_: BankStats) -> list[dict[str, str]]:
        return [
            f"SELECT COUNT(*) as \"Total Active Accounts\" FROM {TABLES.BANK_SUMMARY} WHERE {COLUMNS.BANK_SUMMARY.IS_DELETED} <> 1",
            f"SELECT COUNT(*) as \"Total Deleted Accounts\" FROM {TABLES.BANK_SUMMARY} WHERE {COLUMNS.BANK_SUMMARY.IS_DELETED} = 0",
            f"SELECT SUM({COLUMNS.BANK_SUMMARY.BALANCE}) as \"NET ACCOUNT BALANCE\" FROM {TABLES.BANK_SUMMARY}"
            f"SELECT COUNT(*) as \"Total Transactions\" from {TABLES.TRANSACTIONS}"
            f"SELECT AVG({COLUMNS.DEPOSITS.AMOUNT}) as \"Average Deposit\" from {TABLES.DEPOSITS}"
            f"SELECT AVG({COLUMNS.WITHDRAWALS.AMOUNT}) as \"Average Deposit\" from {TABLES.WITHDRAWALS}"
            f"SELECT SUM({COLUMNS.DEBTS_FORGIVEN.AMOUNT}) as \"Debt Forgiven\" from {TABLES.DEBTS_FORGIVEN}"
        ]

    @register_query_handler(AccountSummary)
    @staticmethod
    @fetch_multiple_rows
    async def account_summary(event: AccountSummary):
        return [
            f"SELECT {COLUMNS.BANK_SUMMARY.ACCOUNT_ID}, {COLUMNS.BANK_SUMMARY.NAME}, {COLUMNS.BANK_SUMMARY.BALANCE} FROM {TABLES.BANK_SUMMARY} WHERE {COLUMNS.BANK_SUMMARY.IS_DELETED} = 0"
            f"SELECT {COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT}, {COLUMNS.TRANSFER_REQUESTS.AMOUNT} FROM {TABLES.TRANSFERS} WHERE {COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT} = {event.account_id}"
        ]

    @register_query_handler(AccountLedger)
    @staticmethod
    @fetch_multiple_rows
    async def account_ledger(event: AccountLedger):
        return f"SELECT {COLUMNS.TRANSACTIONS.ACCOUNT_ID}, {COLUMNS.TRANSACTIONS.INITIATED_AT}, {COLUMNS.TRANSACTIONS.AMOUNT}, {COLUMNS.TRANSACTIONS.DESCRIPTION} FROM {TABLES.TRANSACTIONS} WHERE {COLUMNS.TRANSACTIONS.ACCOUNT_ID} = {event.account_id}"

    @register_event_handler(AccountCreated)
    @staticmethod
    @upsert_single_row
    async def handle_account_created(event: AccountCreated):
        return q_bldr(TABLES.BANK_SUMMARY) \
            .at(COLUMNS.BANK_SUMMARY.ACCOUNT_ID, event.account_id) \
            .upsert(COLUMNS.BANK_SUMMARY.NAME, event.name) \
            .upsert(COLUMNS.BANK_SUMMARY.BALANCE, 0, COLUMNS.BANK_SUMMARY.BALANCE_VERSION, event.version) \
            .upsert_if_greater(COLUMNS.BANK_SUMMARY.IS_DELETED, 0) \
            .done()

    @register_event_handler(AccountDeleted)
    @staticmethod
    @upsert_single_row
    async def handle_account_deleted(event: AccountDeleted):
        return q_bldr(TABLES.BANK_SUMMARY) \
            .at(COLUMNS.BANK_SUMMARY.ACCOUNT_ID, event.account_id) \
            .upsert(COLUMNS.BANK_SUMMARY.IS_DELETED, 1) \
            .done()

    @register_event_handler(FundsDeposited)
    @staticmethod
    @upsert_multiple_rows
    async def handle_funds_deposited(event: FundsDeposited):
        bank_summary_q = make_update_balance_query(event.account_id, event)
        transactions_q = make_update_transactions_query(
            event.account_id, TRANSACTION_TYPE.DEPOSIT, event)
        deposit_q = q_bldr(TABLES.DEPOSITS) \
            .at(COLUMNS.DEPOSITS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.DEPOSITS.AMOUNT, event. amount) \
            .done()

        return [(bank_summary_q, None), (transactions_q, None), (deposit_q, None)]

    @register_event_handler(FundsWithdrawn)
    @staticmethod
    @upsert_multiple_rows
    async def handle_funds_withdrawn(event: FundsWithdrawn):
        bank_summary_q = make_update_balance_query(event.account_id, event)
        transactions_q = make_update_transactions_query(
            event.account_id, TRANSACTION_TYPE.WITHDRAWAL, event)
        withdrawal_q = q_bldr(TABLES.WITHDRAWALS) \
            .at(COLUMNS.DEPOSITS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.WITHDRAWALS.AMOUNT, event.amount) \
            .done()

        return [(bank_summary_q, None), (transactions_q, None), (withdrawal_q, None)]

    @register_event_handler(DebtForgiven)
    @staticmethod
    @upsert_multiple_rows
    async def handle_debt_forgiven(event: DebtForgiven):
        bank_summary_q = make_update_balance_query(event.account_id, event)
        transactions_q = q_bldr(TABLES.TRANSACTIONS) \
            .at(COLUMNS.TRANSACTIONS.TRANSACTION_ID, event.transaction_id,) \
            .upsert(COLUMNS.TRANSACTIONS.ACCOUNT_ID, event.account_id) \
            .upsert(COLUMNS.TRANSACTIONS.INITIATED_AT, event.created_at) \
            .upsert(COLUMNS.TRANSACTIONS.AMOUNT, event.amount) \
            .upsert(COLUMNS.TRANSACTIONS.DESCRIPTION, TRANSACTION_TYPE.DEBT_FORGIVENESS) \
            .done()
        debt_forgive_q = q_bldr(TABLES.DEBTS_FORGIVEN) \
            .at(COLUMNS.DEPOSITS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.WITHDRAWALS.AMOUNT, event.amount) \
            .done()

        return [(bank_summary_q, None), (transactions_q, None), (debt_forgive_q, None)]

    @register_event_handler(ReceiveFundsRequested)
    @staticmethod
    @upsert_single_row
    async def handle_receive_funds_requested(event: ReceiveFundsRequested):
        return q_bldr(TABLES.TRANSFER_REQUESTS) \
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT, event.funded_account_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount) \
            .upsert_if_greater(COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_SENT) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.TIMEOUT_AT, event.timeout_at) \
            .done()

    @register_event_handler(NotifiedReceiveFundsRequested)
    @staticmethod
    @upsert_single_row
    async def handle_notified_receive_funds_requested(event: NotifiedReceiveFundsRequested):
        return q_bldr(TABLES.TRANSFER_REQUESTS) \
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT, event.funded_account_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount) \
            .upsert_if_greater(COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_RECEIVED) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.TIMEOUT_AT, event.timeout_at) \
            .done()

    @register_event_handler(ReceiveFundsApproved)
    @upsert_single_row
    async def handle_receive_funds_approved(event: ReceiveFundsApproved):
        return q_bldr(TABLES.TRANSFER_REQUESTS) \
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id) \
            .upsert_if_greater(COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_SENT) \
            .done()

    @register_event_handler(ReceiveFundsRejected)
    @staticmethod
    @upsert_single_row
    async def handle_receive_funds_rejected(event: ReceiveFundsRejected):
        return q_bldr(TABLES.TRANSFER_REQUESTS) \
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id) \
            .upsert_if_greater(COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_REJECTED) \
            .done()

    @register_event_handler(NotifiedReceivedFundsRejected)
    @staticmethod
    @upsert_single_row
    async def handle_notified_received_funds_rejected(event: NotifiedReceivedFundsRejected):
        return q_bldr(TABLES.TRANSFER_REQUESTS) \
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT, event.funded_account_id) \
            .upsert_if_greater(COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REJECTION_RECEIVED) \
            .done()

    @register_event_handler(ReceiveFundsDebited)
    @staticmethod
    @upsert_multiple_rows
    async def handle_receive_funds_debited(event: ReceiveFundsDebited):
        transfer_request_q = q_bldr(TABLES.TRANSFER_REQUESTS) \
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount) \
            .upsert_if_greater(COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_DEBITED) \
            .done()
        bank_summary_q = make_update_balance_query(
            event.funding_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funding_account_id, TRANSACTION_TYPE.REQUEST_DEBIT)

        return [transfer_request_q, bank_summary_q, transactions_q]

    @register_event_handler(ReceiveFundsDebitedRolledBack)
    @staticmethod
    @upsert_multiple_rows
    async def handle_receive_funds_debited_rolled_back(event: ReceiveFundsDebitedRolledBack):
        transfer_request_q = q_bldr(TABLES.TRANSFER_REQUESTS) \
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount) \
            .upsert_if_greater(COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_DEBIT_ROLLBACK) \
            .done()
        bank_summary_q = make_update_balance_query(
            event.funding_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funding_account_id, TRANSACTION_TYPE.REQUEST_DEBIT_ROLLBACK)

        return [transfer_request_q, bank_summary_q, transactions_q]

    @register_event_handler(ReceiveFundsCredited)
    @staticmethod
    @upsert_multiple_rows
    async def handle_receive_funds_credited(event: ReceiveFundsCredited):
        transfer_request_q = q_bldr(TABLES.TRANSFER_REQUESTS) \
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT, event.funded_account_id) \
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount) \
            .upsert_if_greater(COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_CREDITED) \
            .done()
        bank_summary_q = make_update_balance_query(
            event.funded_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funded_account_id, TRANSACTION_TYPE.REQUEST_CREDIT)

        return [transfer_request_q, bank_summary_q, transactions_q]

    @register_event_handler(SendFundsCredited)
    @staticmethod
    @upsert_multiple_rows
    async def handle_send_funds_credited(event: SendFundsCredited):
        transfer_q = q_bldr(TABLES.TRANSFERS) \
            .at(COLUMNS.TRANSFERS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFERS.FUNDED_ACCOUNT, event.funded_account_id) \
            .upsert(COLUMNS.TRANSFERS.FUNDING_ACCOUNT, event.funding_account_id) \
            .upsert(COLUMNS.TRANSFERS.AMOUNT, event.amount) \
            .upsert_if_greater(COLUMNS.TRANSFERS.STATE, TRANSACTION_STATES.TRANSFER_CREDIT) \
            .done()
        bank_summary_q = make_update_balance_query(
            event.funded_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funded_account_id, TRANSACTION_TYPE.TRANSFER_CREDIT)

        return [transfer_q, bank_summary_q, transactions_q]

    @register_event_handler(SendFundsDebited)
    @staticmethod
    @upsert_multiple_rows
    async def handle_send_funds_debited(event: SendFundsDebited):
        transfer_q = q_bldr(TABLES.TRANSFERS) \
            .at(COLUMNS.TRANSFERS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFERS.FUNDED_ACCOUNT, event.funded_account_id) \
            .upsert(COLUMNS.TRANSFERS.FUNDING_ACCOUNT, event.funding_account_id) \
            .upsert(COLUMNS.TRANSFERS.AMOUNT, event.amount) \
            .upsert_if_greater(COLUMNS.TRANSFERS.STATE, TRANSACTION_STATES.TRANSFER_DEBIT) \
            .done()
        bank_summary_q = make_update_balance_query(
            event.funding_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funding_account_id, TRANSACTION_TYPE.TRANSFER_DEBIT, event)

        return [transfer_q, bank_summary_q, transactions_q]

    @register_event_handler(SendFundsDebitedRolledBack)
    @staticmethod
    @upsert_multiple_rows
    async def handle_send_funds_debited_rolled_back(event: SendFundsDebitedRolledBack):
        transfer_q = q_bldr(TABLES.TRANSFERS) \
            .at(COLUMNS.TRANSFERS.TRANSACTION_ID, event.transaction_id) \
            .upsert(COLUMNS.TRANSFERS.FUNDING_ACCOUNT, event.funding_account_id) \
            .upsert(COLUMNS.TRANSFERS.AMOUNT, event.amount) \
            .upsert_if_greater(COLUMNS.TRANSFERS.STATE, TRANSACTION_STATES.TRANSFER_DEBIT) \
            .done()
        bank_summary_q = make_update_balance_query(
            event.funding_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funding_account_id, TRANSACTION_TYPE.TRANSFER_DEBIT_ROLLBACK, event)

        return [transfer_q, bank_summary_q, transactions_q]
