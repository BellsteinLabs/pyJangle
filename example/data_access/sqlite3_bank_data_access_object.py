from functools import reduce
import sqlite3
from pyjangle.event.event_handler import register_event_handler
from pyjangle.query.handlers import register_query_handler
from data_access.bank_data_access_object import BankDataAccessObject
from events import (
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
from queries import (
    AccountLedger,
    AccountSummary,
    BankStats,
    AccountsList,
)
from query_responses import (
    AccountResponse,
    AccountSummaryResponse,
    BankStatsResponse,
    TransactionResponse,
    TransferResponse,
)
from pyjangle_sqlite3.event_handler_query_builder import Sqlite3QueryBuilder as q_bldr
from data_access.db_schema import (
    TABLES,
    COLUMNS,
    TRANSACTION_STATES,
    TRANSACTION_TYPE,
)
from data_access.db_settings import get_db_jangle_banking_path
from data_access.db_utility import (
    fetch_multiple_rows,
    upsert_multiple_rows,
    upsert_single_row,
    make_update_balance_query,
    make_update_transactions_query,
)

with open("example/data_access/create_tables.sql", "r") as create_tables_file:
    create_tables_sql_script = create_tables_file.read()
with open("example/data_access/clear_tables.sql", "r") as clear_tables_file:
    clear_tables_sql_script = clear_tables_file.read()


def create_database():
    with sqlite3.connect(
        get_db_jangle_banking_path(), detect_types=sqlite3.PARSE_DECLTYPES
    ) as conn:
        conn.executescript(create_tables_sql_script)
        conn.commit()
    conn.close()


class Sqlite3BankDataAccessObject(BankDataAccessObject):
    @staticmethod
    def clear():
        with sqlite3.connect(
            get_db_jangle_banking_path(), detect_types=sqlite3.PARSE_DECLTYPES
        ) as conn:
            conn.executescript(clear_tables_sql_script)
            conn.commit()
        conn.close()

    @staticmethod
    @register_query_handler(AccountsList)
    @fetch_multiple_rows
    async def bank_summary(_: AccountsList) -> list[AccountResponse]:
        return f"""
            SELECT
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.ACCOUNT_ID},
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.NAME},
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.BALANCE},
                (SELECT COUNT(*) FROM {TABLES.TRANSFER_REQUESTS} WHERE {TABLES.TRANSFER_REQUESTS}.{COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT} = {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.ACCOUNT_ID} AND {TABLES.TRANSFER_REQUESTS}.{COLUMNS.TRANSFER_REQUESTS.STATE} = 2) as \"pending_request_count\"
            FROM
                {TABLES.BANK_SUMMARY}
            WHERE
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.NAME} IS NOT NULL AND
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.BALANCE} IS NOT NULL AND
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.IS_DELETED} = 0
            """, lambda q_result: [
            AccountResponse(
                **{
                    k: v if k != COLUMNS.BANK_SUMMARY.BALANCE else str(v)
                    for k, v in account.items()
                }
            )
            for account in q_result
        ]

    @staticmethod
    @register_query_handler(BankStats)
    @fetch_multiple_rows
    async def bank_stats(_: BankStats) -> BankStats:
        return [
            f'SELECT COUNT(*) as "active_accounts_count" FROM {TABLES.BANK_SUMMARY} WHERE {COLUMNS.BANK_SUMMARY.IS_DELETED} <> 1',
            f'SELECT COUNT(*) as "deleted_accounts_count" FROM {TABLES.BANK_SUMMARY} WHERE {COLUMNS.BANK_SUMMARY.IS_DELETED} = 1',
            f'SELECT COALESCE(SUM({COLUMNS.BANK_SUMMARY.BALANCE}), 0) as "net_account_balance" FROM {TABLES.BANK_SUMMARY}',
            f'SELECT COUNT(*) as "transaction_count" from {TABLES.TRANSACTIONS}',
            f'SELECT COALESCE(AVG({COLUMNS.DEPOSITS.AMOUNT}), 0) as "average_deposit_amount" from {TABLES.DEPOSITS}',
            f'SELECT COALESCE(AVG({COLUMNS.WITHDRAWALS.AMOUNT}), 0) as "average_withdrawal_amount" from {TABLES.WITHDRAWALS}',
            f'SELECT COALESCE(SUM({COLUMNS.DEBTS_FORGIVEN.AMOUNT}), 0) as "debt_forgiven_amount" from {TABLES.DEBTS_FORGIVEN}',
        ], lambda q_result: BankStatsResponse(
            **reduce(lambda a, b: {**a, **b}, q_result)
        )

    @staticmethod
    @register_query_handler(AccountSummary)
    @fetch_multiple_rows
    async def account_summary(query: AccountSummary) -> AccountSummaryResponse:
        return (
            [
                f"""SELECT
                {COLUMNS.BANK_SUMMARY.ACCOUNT_ID},
                {COLUMNS.BANK_SUMMARY.NAME},
                {COLUMNS.BANK_SUMMARY.BALANCE}
            FROM {TABLES.BANK_SUMMARY}
            WHERE
                {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.ACCOUNT_ID} = '{query.account_id}'
            AND
                {COLUMNS.BANK_SUMMARY.IS_DELETED} <> 1""",
                f"""SELECT
                {COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT},
                {COLUMNS.TRANSFER_REQUESTS.AMOUNT},
                {TABLES.TRANSACTION_STATES}.{COLUMNS.TRANSACTION_STATES.DESCRIPTION} as 'state',
                {COLUMNS.TRANSFER_REQUESTS.TIMEOUT_AT},
                {COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID}
            FROM {TABLES.TRANSFER_REQUESTS}
            LEFT JOIN {TABLES.BANK_SUMMARY}
            ON {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.ACCOUNT_ID} = {TABLES.TRANSFER_REQUESTS}.{COLUMNS.TRANSFERS.FUNDING_ACCOUNT}
            LEFT JOIN {TABLES.TRANSACTION_STATES}
            ON {TABLES.TRANSACTION_STATES}.{COLUMNS.TRANSACTION_STATES.VALUE} = {TABLES.TRANSFER_REQUESTS}.{COLUMNS.TRANSFERS.STATE}
            WHERE {TABLES.TRANSFER_REQUESTS}.{COLUMNS.TRANSFERS.FUNDING_ACCOUNT} = '{query.account_id}'
            AND {TABLES.BANK_SUMMARY}.{COLUMNS.BANK_SUMMARY.IS_DELETED} <> 1
            AND {TABLES.TRANSFER_REQUESTS}.{COLUMNS.TRANSFERS.STATE} = 2
            """,
            ],
            lambda q_result: AccountSummaryResponse(
                account_id=q_result[0]["account_id"],
                name=q_result[0]["name"],
                balance=str(q_result[0]["balance"]),
                transfer_requests=[
                    TransferResponse(
                        funded_account=d["funded_account"],
                        amount=str(d["amount"]),
                        state=d["state"],
                        timeout_at=d["timeout_at"].isoformat(),
                        transaction_id=d["transaction_id"],
                    )
                    for d in q_result[1:]
                ],
            )
            if q_result
            else None,
        )

    @staticmethod
    @register_query_handler(AccountLedger)
    @fetch_multiple_rows
    async def account_ledger(query: AccountLedger) -> list[TransactionResponse]:
        return f"""SELECT
        {COLUMNS.TRANSACTIONS.ACCOUNT_ID},
        {COLUMNS.TRANSACTIONS.INITIATED_AT},
        {COLUMNS.TRANSACTIONS.AMOUNT},
        {TABLES.TRANSACTION_TYPES}.{COLUMNS.TRANSACTION_TYPES.DESCRIPTION} as {COLUMNS.TRANSACTIONS.TRANSACTION_TYPE}
        FROM {TABLES.TRANSACTIONS}
        LEFT JOIN {TABLES.TRANSACTION_TYPES}
        ON {TABLES.TRANSACTION_TYPES}.{COLUMNS.TRANSACTION_TYPES.VALUE} = {TABLES.TRANSACTIONS}.{COLUMNS.TRANSACTIONS.TRANSACTION_TYPE}
        WHERE {COLUMNS.TRANSACTIONS.ACCOUNT_ID} = '{query.account_id}'""", lambda q_result: [
            TransactionResponse(
                initiated_at=row_dict[COLUMNS.TRANSACTIONS.INITIATED_AT].isoformat(),
                amount=str(row_dict[COLUMNS.TRANSACTIONS.AMOUNT]),
                transaction_type=row_dict[COLUMNS.TRANSACTIONS.TRANSACTION_TYPE],
            )
            for row_dict in q_result
        ]

    @staticmethod
    @register_event_handler(AccountCreated)
    @upsert_single_row
    async def handle_account_created(event: AccountCreated):
        return (
            q_bldr(TABLES.BANK_SUMMARY)
            .at(COLUMNS.BANK_SUMMARY.ACCOUNT_ID, event.account_id)
            .upsert(COLUMNS.BANK_SUMMARY.NAME, event.name)
            .upsert(
                COLUMNS.BANK_SUMMARY.BALANCE,
                0,
                COLUMNS.BANK_SUMMARY.BALANCE_VERSION,
                event.version,
            )
            .upsert_if_greater(COLUMNS.BANK_SUMMARY.IS_DELETED, 0)
            .done()
        )

    @staticmethod
    @register_event_handler(AccountDeleted)
    @upsert_single_row
    async def handle_account_deleted(event: AccountDeleted):
        return (
            q_bldr(TABLES.BANK_SUMMARY)
            .at(COLUMNS.BANK_SUMMARY.ACCOUNT_ID, event.account_id)
            .upsert(COLUMNS.BANK_SUMMARY.IS_DELETED, 1)
            .done()
        )

    @staticmethod
    @register_event_handler(FundsDeposited)
    @upsert_multiple_rows
    async def handle_funds_deposited(event: FundsDeposited):
        bank_summary_q, bank_summary_p = make_update_balance_query(
            event.account_id, event
        )
        transactions_q, transactions_p = make_update_transactions_query(
            event.account_id, TRANSACTION_TYPE.DEPOSIT, event
        )
        deposit_q, deposit_p = (
            q_bldr(TABLES.DEPOSITS)
            .at(COLUMNS.DEPOSITS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.DEPOSITS.AMOUNT, event.amount)
            .done()
        )

        return [
            (bank_summary_q, bank_summary_p),
            (transactions_q, transactions_p),
            (deposit_q, deposit_p),
        ]

    @staticmethod
    @register_event_handler(FundsWithdrawn)
    @upsert_multiple_rows
    async def handle_funds_withdrawn(event: FundsWithdrawn):
        bank_summary_q, bank_summary_p = make_update_balance_query(
            event.account_id, event
        )
        transactions_q, transactions_p = make_update_transactions_query(
            event.account_id, TRANSACTION_TYPE.WITHDRAWAL, event
        )
        withdrawal_q, withdrawal_p = (
            q_bldr(TABLES.WITHDRAWALS)
            .at(COLUMNS.DEPOSITS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.WITHDRAWALS.AMOUNT, event.amount)
            .done()
        )

        return [
            (bank_summary_q, bank_summary_p),
            (transactions_q, transactions_p),
            (withdrawal_q, withdrawal_p),
        ]

    @staticmethod
    @register_event_handler(DebtForgiven)
    @upsert_multiple_rows
    async def handle_debt_forgiven(event: DebtForgiven):
        bank_summary_q, bank_summary_p = make_update_balance_query(
            event.account_id, event, lambda event: 0
        )
        transactions_q, transactions_p = (
            q_bldr(TABLES.TRANSACTIONS)
            .at(COLUMNS.TRANSACTIONS.EVENT_ID, event.id)
            .upsert(
                COLUMNS.TRANSACTIONS.TRANSACTION_ID,
                event.transaction_id,
            )
            .upsert(COLUMNS.TRANSACTIONS.ACCOUNT_ID, event.account_id)
            .upsert(COLUMNS.TRANSACTIONS.INITIATED_AT, event.created_at)
            .upsert(COLUMNS.TRANSACTIONS.AMOUNT, event.amount)
            .upsert(
                COLUMNS.TRANSACTIONS.TRANSACTION_TYPE, TRANSACTION_TYPE.DEBT_FORGIVENESS
            )
            .done()
        )
        debt_forgive_q, debt_forgive_p = (
            q_bldr(TABLES.DEBTS_FORGIVEN)
            .at(COLUMNS.DEPOSITS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.WITHDRAWALS.AMOUNT, event.amount)
            .done()
        )

        return [
            (bank_summary_q, bank_summary_p),
            (transactions_q, transactions_p),
            (debt_forgive_q, debt_forgive_p),
        ]

    @staticmethod
    @register_event_handler(RequestCreated)
    @upsert_single_row
    async def handle_receive_funds_requested(event: RequestCreated):
        return (
            q_bldr(TABLES.TRANSFER_REQUESTS)
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT, event.funded_account_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount)
            .upsert_if_greater(
                COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_SENT
            )
            .upsert(COLUMNS.TRANSFER_REQUESTS.TIMEOUT_AT, event.timeout_at)
            .done()
        )

    @staticmethod
    @register_event_handler(RequestReceived)
    @upsert_single_row
    async def handle_notified_receive_funds_requested(event: RequestReceived):
        return (
            q_bldr(TABLES.TRANSFER_REQUESTS)
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT, event.funded_account_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount)
            .upsert_if_greater(
                COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_RECEIVED
            )
            .upsert(COLUMNS.TRANSFER_REQUESTS.TIMEOUT_AT, event.timeout_at)
            .done()
        )

    @staticmethod
    @register_event_handler(RequestApproved)
    @upsert_single_row
    async def handle_receive_funds_approved(event: RequestApproved):
        return (
            q_bldr(TABLES.TRANSFER_REQUESTS)
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id)
            .upsert_if_greater(
                COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_ACCEPTED
            )
            .done()
        )

    @staticmethod
    @register_event_handler(RequestRejected)
    @upsert_single_row
    async def handle_receive_funds_rejected(event: RequestRejected):
        return (
            q_bldr(TABLES.TRANSFER_REQUESTS)
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id)
            .upsert_if_greater(
                COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_REJECTED
            )
            .done()
        )

    @staticmethod
    @register_event_handler(RequestRejectionReceived)
    @upsert_single_row
    async def handle_notified_received_funds_rejected(event: RequestRejectionReceived):
        return (
            q_bldr(TABLES.TRANSFER_REQUESTS)
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT, event.funded_account_id)
            .upsert_if_greater(
                COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REJECTION_RECEIVED
            )
            .done()
        )

    @staticmethod
    @register_event_handler(RequestDebited)
    @upsert_multiple_rows
    async def handle_receive_funds_debited(event: RequestDebited):
        transfer_request_q = (
            q_bldr(TABLES.TRANSFER_REQUESTS)
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount)
            .upsert_if_greater(
                COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_DEBITED
            )
            .done()
        )
        bank_summary_q = make_update_balance_query(event.funding_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funding_account_id, TRANSACTION_TYPE.REQUEST_DEBIT, event=event
        )

        return [transfer_request_q, bank_summary_q, transactions_q]

    @staticmethod
    @register_event_handler(RequestDebitRolledBack)
    @upsert_multiple_rows
    async def handle_receive_funds_debited_rolled_back(event: RequestDebitRolledBack):
        transfer_request_q = (
            q_bldr(TABLES.TRANSFER_REQUESTS)
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDING_ACCOUNT, event.funding_account_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount)
            .upsert_if_greater(
                COLUMNS.TRANSFER_REQUESTS.STATE,
                TRANSACTION_STATES.REQUEST_DEBIT_ROLLBACK,
            )
            .done()
        )
        bank_summary_q = make_update_balance_query(event.funding_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funding_account_id,
            TRANSACTION_TYPE.REQUEST_DEBIT_ROLLBACK,
            event=event,
        )

        return [transfer_request_q, bank_summary_q, transactions_q]

    @staticmethod
    @register_event_handler(RequestCredited)
    @upsert_multiple_rows
    async def handle_receive_funds_credited(event: RequestCredited):
        transfer_request_q = (
            q_bldr(TABLES.TRANSFER_REQUESTS)
            .at(COLUMNS.TRANSFER_REQUESTS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.FUNDED_ACCOUNT, event.funded_account_id)
            .upsert(COLUMNS.TRANSFER_REQUESTS.AMOUNT, event.amount)
            .upsert_if_greater(
                COLUMNS.TRANSFER_REQUESTS.STATE, TRANSACTION_STATES.REQUEST_CREDITED
            )
            .done()
        )
        bank_summary_q = make_update_balance_query(event.funded_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funded_account_id, TRANSACTION_TYPE.REQUEST_CREDIT, event=event
        )

        return [transfer_request_q, bank_summary_q, transactions_q]

    @staticmethod
    @register_event_handler(TransferCredited)
    @upsert_multiple_rows
    async def handle_send_funds_credited(event: TransferCredited):
        transfer_q = (
            q_bldr(TABLES.TRANSFERS)
            .at(COLUMNS.TRANSFERS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFERS.FUNDED_ACCOUNT, event.funded_account_id)
            .upsert(COLUMNS.TRANSFERS.FUNDING_ACCOUNT, event.funding_account_id)
            .upsert(COLUMNS.TRANSFERS.AMOUNT, event.amount)
            .upsert_if_greater(
                COLUMNS.TRANSFERS.STATE, TRANSACTION_STATES.TRANSFER_CREDIT
            )
            .done()
        )
        bank_summary_q = make_update_balance_query(event.funded_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funded_account_id, TRANSACTION_TYPE.TRANSFER_CREDIT, event=event
        )

        return [transfer_q, bank_summary_q, transactions_q]

    @staticmethod
    @register_event_handler(TransferDebited)
    @upsert_multiple_rows
    async def handle_send_funds_debited(event: TransferDebited):
        transfer_q = (
            q_bldr(TABLES.TRANSFERS)
            .at(COLUMNS.TRANSFERS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFERS.FUNDED_ACCOUNT, event.funded_account_id)
            .upsert(COLUMNS.TRANSFERS.FUNDING_ACCOUNT, event.funding_account_id)
            .upsert(COLUMNS.TRANSFERS.AMOUNT, event.amount)
            .upsert_if_greater(
                COLUMNS.TRANSFERS.STATE, TRANSACTION_STATES.TRANSFER_DEBIT
            )
            .done()
        )
        bank_summary_q = make_update_balance_query(event.funding_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funding_account_id, TRANSACTION_TYPE.TRANSFER_DEBIT, event
        )

        return [transfer_q, bank_summary_q, transactions_q]

    @staticmethod
    @register_event_handler(TransferDebitRolledBack)
    @upsert_multiple_rows
    async def handle_send_funds_debited_rolled_back(event: TransferDebitRolledBack):
        transfer_q = (
            q_bldr(TABLES.TRANSFERS)
            .at(COLUMNS.TRANSFERS.TRANSACTION_ID, event.transaction_id)
            .upsert(COLUMNS.TRANSFERS.FUNDING_ACCOUNT, event.funding_account_id)
            .upsert(COLUMNS.TRANSFERS.AMOUNT, event.amount)
            .upsert_if_greater(
                COLUMNS.TRANSFERS.STATE, TRANSACTION_STATES.TRANSFER_DEBIT
            )
            .done()
        )
        bank_summary_q = make_update_balance_query(event.funding_account_id, event)
        transactions_q = make_update_transactions_query(
            event.funding_account_id, TRANSACTION_TYPE.TRANSFER_DEBIT_ROLLBACK, event
        )

        return [transfer_q, bank_summary_q, transactions_q]
