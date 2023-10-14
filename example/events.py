from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pyjangle import VersionedEvent
from pyjangle.event.register_event import RegisterEvent
from validation.descriptors import (
    AccountId,
    AccountName,
    Amount,
    Balance,
    Timeout,
    TransactionId,
)


@dataclass(kw_only=True)
class JangleBankingEvent(VersionedEvent, metaclass=ABCMeta):
    @abstractmethod
    def convert_json_dict(data: dict):
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        return data


@RegisterEvent
@dataclass(kw_only=True)
class AccountIdProvisioned(VersionedEvent):
    """An id was provisioned for a new account on the AccountCreationAggregate."""

    pass


@RegisterEvent
@dataclass(kw_only=True)
class AccountCreated(VersionedEvent):
    """Account was created."""

    account_id: str = AccountId()
    name: str = AccountName()

    def deserialize(data: any) -> any:
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        return AccountCreated(**data)


@RegisterEvent
@dataclass(kw_only=True)
class AccountDeleted(VersionedEvent):
    """Account was deleted."""

    account_id: str = AccountId()


@RegisterEvent
@dataclass(kw_only=True)
class FundsDeposited(VersionedEvent):
    """Funds have been doposited into an account."""

    account_id: str = AccountId()
    amount: Decimal = Amount()
    balance: Decimal = Balance()
    transaction_id: str = TransactionId(create_id=True)


@RegisterEvent
@dataclass(kw_only=True)
class FundsWithdrawn(VersionedEvent):
    """Funds have been withdrawn from an account."""

    account_id: str = AccountId()
    amount: Decimal = Amount()
    balance: Decimal = Balance()
    transaction_id: str = TransactionId(create_id=True)


@RegisterEvent
@dataclass(kw_only=True)
class DebtForgiven(VersionedEvent):
    """Debt up to $100 has been forgiven in an account."""

    account_id: str = AccountId()
    amount: Decimal = Amount()
    transaction_id: str = TransactionId(create_id=True)


@RegisterEvent
@dataclass(kw_only=True)
class RequestCreated(VersionedEvent):
    """This account is requesting funds from another account."""

    funded_account_id: str = AccountId()
    funding_account_id: str = AccountId()
    amount: Decimal = Amount()
    transaction_id: str = TransactionId(create_id=True)
    timeout_at: datetime = Timeout()


@RegisterEvent
@dataclass(kw_only=True)
class RequestReceived(VersionedEvent):
    """This account was notified that another account requested ReceiveFunds."""

    funded_account_id: str = AccountId()
    funding_account_id: str = AccountId()
    amount: Decimal = Amount()
    transaction_id: str = TransactionId()
    timeout_at: datetime = Timeout()


@RegisterEvent
@dataclass(kw_only=True)
class RequestApproved(VersionedEvent):
    """Funding account has approved a transfer."""

    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()


@RegisterEvent
@dataclass(kw_only=True)
class RequestRejected(VersionedEvent):
    """Funding account has denied a transfer."""

    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()


@RegisterEvent
@dataclass(kw_only=True)
class RequestRejectionReceived(VersionedEvent):
    """This account was notified that another account rejected ReceiveFunds request."""

    funded_account_id: str = AccountId()
    transaction_id: str = TransactionId()


@RegisterEvent
@dataclass(kw_only=True)
class RequestDebited(VersionedEvent):
    """Funds sent to the account that requested them."""

    funding_account_id: str = AccountId()
    balance: Decimal = Balance()
    amount: Decimal = Amount()
    transaction_id: str = TransactionId()


@RegisterEvent
@dataclass(kw_only=True)
class RequestDebitRolledBack(VersionedEvent):
    """Sending funds to another account failed an rolled back."""

    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()
    balance: Decimal = Balance()
    amount: Decimal = Amount()


@RegisterEvent
@dataclass(kw_only=True)
class RequestCredited(VersionedEvent):
    """Received requested funds."""

    funded_account_id: str = AccountId()
    transaction_id: str = TransactionId()
    balance: Decimal = Balance()
    amount: Decimal = Amount()


@RegisterEvent
@dataclass(kw_only=True)
class TransferCredited(VersionedEvent):
    """Funded account confirms receipt of SendFunds request."""

    funded_account_id: str = AccountId()
    funding_account_id: str = AccountId()
    amount: Decimal = Amount()
    balance: Decimal = Balance()
    transaction_id: str = TransactionId()


@RegisterEvent
@dataclass(kw_only=True)
class TransferDebited(VersionedEvent):
    """Funds sent to another account."""

    funding_account_id: str = AccountId()
    funded_account_id: str = AccountId()
    amount: Decimal = Amount()
    balance: Decimal = Balance()
    transaction_id: str = TransactionId(create_id=True)


@RegisterEvent
@dataclass(kw_only=True)
class TransferDebitRolledBack(VersionedEvent):
    """Sending funds to another account failed an rolled back."""

    amount: Decimal = Amount()
    funding_account_id: str = AccountId()
    balance: Decimal = Balance()
    transaction_id: str = TransactionId()
