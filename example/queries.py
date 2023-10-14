from dataclasses import dataclass

from example.validation.descriptors import AccountId


class AccountsList:
    pass


class BankStats:
    pass


@dataclass(kw_only=True)
class AccountSummary:
    account_id: str = AccountId()


@dataclass(kw_only=True)
class AccountLedger:
    account_id: str = AccountId()
