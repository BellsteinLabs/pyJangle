from dataclasses import *
import dataclasses
from datetime import datetime
from decimal import Decimal
import json
from typing import Mapping
import uuid
from json import dumps

from pyjangle import VersionedEvent
from pyjangle.event.register_event import RegisterEvent


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class AccountIdProvisioned(VersionedEvent):
    """An id was provisioned for a new account on the AccountCreationAggregate."""


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class AccountCreated(VersionedEvent):
    """Account was created."""
    account_id: str
    name: str


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class AccountDeleted(VersionedEvent):
    """Account was deleted."""
    account_id: str


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class FundsDeposited(VersionedEvent):
    """Funds have been doposited into an account."""
    account_id: str
    amount: Decimal
    balance: Decimal
    transaction_id: str = dataclasses.field(
        default_factory=lambda: str(uuid.uuid4()))


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class FundsWithdrawn(VersionedEvent):
    """Funds have been withdrawn from an account."""
    account_id: str
    amount: Decimal
    balance: Decimal
    transaction_id: str = dataclasses.field(
        default_factory=lambda: str(uuid.uuid4()))


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class DebtForgiven(VersionedEvent):
    """Debt up to $100 has been forgiven in an account."""
    account_id: str
    amount: Decimal
    transaction_id: str = dataclasses.field(
        default_factory=lambda: str(uuid.uuid4()))


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class ReceiveFundsRequested(VersionedEvent):
    """This account is requesting funds from another account."""
    funded_account_id: str
    funding_account_id: str
    amount: Decimal
    transaction_id: str
    timeout_at: datetime


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class NotifiedReceiveFundsRequested(VersionedEvent):
    """This account was notified that another account requested ReceiveFunds."""
    funded_account_id: str
    funding_account_id: str
    amount: Decimal
    transaction_id: str
    timeout_at: datetime


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class ReceiveFundsApproved(VersionedEvent):
    """Funding account has approved a transfer."""
    funding_account_id: str
    transaction_id: str


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class ReceiveFundsRejected(VersionedEvent):
    """Funding account has denied a transfer."""
    funding_account_id: str
    transaction_id: str


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class NotifiedReceivedFundsRejected(VersionedEvent):
    """This account was notified that another account rejected ReceiveFunds request."""
    funded_account_id: str
    transaction_id: str


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class ReceiveFundsDebited(VersionedEvent):
    """Funds sent to the account that requested them."""
    funding_account_id: str
    balance: Decimal
    amount: Decimal
    transaction_id: str


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class ReceiveFundsDebitedRolledBack(VersionedEvent):
    """Sending funds to another account failed an rolled back."""
    funding_account_id: str
    transaction_id: str
    balance: Decimal
    amount: Decimal


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class ReceiveFundsCredited(VersionedEvent):
    """Received requested funds."""
    funded_account_id: str
    transaction_id: str
    balance: Decimal
    amount: Decimal


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class SendFundsCredited(VersionedEvent):
    """Funded account confirms receipt of SendFunds request."""
    funded_account_id: str
    funding_account_id: str
    amount: Decimal
    balance: Decimal
    transaction_id: str


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class SendFundsDebited(VersionedEvent):
    """Funds sent to another account."""
    funding_account_id: str
    funded_account_id: str
    amount: Decimal
    balance: Decimal
    transaction_id: str = dataclasses.field(
        default_factory=lambda: str(uuid.uuid4()))


@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class SendFundsDebitedRolledBack(VersionedEvent):
    """Sending funds to another account failed an rolled back."""
    amount: Decimal
    funding_account_id: str
    balance: Decimal
    transaction_id: str
