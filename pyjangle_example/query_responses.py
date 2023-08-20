from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class AccountResponse:
    account_id: str
    name: str
    balance: str
    pending_request_count: int


@dataclass(frozen=True, kw_only=True)
class BankStatsResponse:
    active_accounts_count: str
    deleted_accounts_count: str
    net_account_balance: str
    transaction_count: str
    average_deposit_amount: str
    average_withdrawal_amount: str
    debt_forgiven_amount: str


@dataclass(frozen=True, kw_only=True)
class TransferResponse:
    funded_account: str
    amount: str
    state: str


@dataclass(frozen=True, kw_only=True)
class AccountSummaryResponse:
    account_id: str
    name: str
    balance: str
    transfer_requests: list[TransferResponse]


@dataclass(frozen=True, kw_only=True)
class TransactionResponse:
    initiated_at: str
    amount: str
    transaction_type: str
