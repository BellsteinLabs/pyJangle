class TRANSACTION_TYPE:
    DEPOSIT = 1
    WITHDRAWAL = 2
    TRANSFER_DEBIT = 3
    TRANSFER_DEBIT_ROLLBACK = 4
    TRANSFER_CREDIT = 5
    REQUEST_DEBIT = 6
    REQUEST_DEBIT_ROLLBACK = 7
    REQUEST_CREDIT = 8
    DEBT_FORGIVENESS = 9


class TRANSACTION_STATES:
    REQUEST_SENT = 1
    REQUEST_RECEIVED = 2
    REQUEST_REJECTED = 3
    REQUEST_ACCEPTED = 4
    REJECTION_RECEIVED = 5
    REQUEST_DEBITED = 6
    REQUEST_CREDITED = 7
    REQUEST_DEBIT_ROLLBACK = 8
    TRANSFER_DEBIT = 9
    TRANSFER_CREDIT = 10
    TRANSFER_DEBIT_ROLLBACK = 11


class TABLES:
    BANK_SUMMARY = "bank_summary"
    TRANSACTIONS = "transactions"
    DEPOSITS = "deposits"
    WITHDRAWALS = "withdrawals"
    TRANSFERS = "transfers"
    TRANSFER_REQUESTS = "transfer_requests"
    DEBTS_FORGIVEN = "debts_forgiven"
    TRANSACTION_TYPES = "transaction_types"
    TRANSACTION_STATES = "transaction_states"


class COLUMNS:
    class BANK_SUMMARY:
        ACCOUNT_ID = "account_id"
        NAME = "name"
        BALANCE = "balance"
        BALANCE_VERSION = "balance_version"
        IS_DELETED = "is_deleted"

    class TRANSACTIONS:
        EVENT_ID = "event_id"
        TRANSACTION_ID = "transaction_id"
        ACCOUNT_ID = "account_id"
        INITIATED_AT = "initiated_at"
        AMOUNT = "amount"
        TRANSACTION_TYPE = "transaction_type"

    class DEPOSITS:
        TRANSACTION_ID = "transaction_id"
        AMOUNT = "amount"

    class WITHDRAWALS:
        TRANSACTION_ID = "transaction_id"
        AMOUNT = "amount"

    class TRANSFERS:
        TRANSACTION_ID = "transaction_id"
        FUNDING_ACCOUNT = "funding_account"
        FUNDED_ACCOUNT = "funded_account"
        AMOUNT = "amount"
        STATE = "state"

    class TRANSFER_REQUESTS:
        TRANSACTION_ID = "transaction_id"
        FUNDED_ACCOUNT = "funded_account"
        FUNDING_ACCOUNT = "funding_account"
        AMOUNT = "amount"
        STATE = "state"
        TIMEOUT_AT = "timeout_at"

    class DEBTS_FORGIVEN:
        TRANSACTION_ID = "transaction_id"
        AMOUNT = "amount"

    class TRANSACTION_TYPES:
        VALUE = "value"
        DESCRIPTION = "description"

    class TRANSACTION_STATES:
        VALUE = "value"
        DESCRIPTION = "description"
