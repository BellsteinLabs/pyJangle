from dataclasses import dataclass


class BankSummary:
    pass


class BankStats:
    pass


@dataclass(frozen=True, kw_only=True)
class AccountSummary:
    account_id: str


@dataclass(frozen=True, kw_only=True)
class AccountLedger:
    account_id: str
