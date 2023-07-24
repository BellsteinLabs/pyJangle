from dataclasses import *
import dataclasses
from datetime import datetime
from decimal import Decimal
import json
from typing import Mapping
import uuid
from json import dumps

from pyjangle import Event


@dataclass(frozen=True, kw_only=True)
class AccountIdProvisioned(Event):
    """An id was provisioned for a new account on the AccountCreationAggregate."""
    pass


@dataclass(frozen=True, kw_only=True)
class AccountCreated(Event):
    """Account was created."""
    account_id: str
    name: str


@dataclass(frozen=True, kw_only=True)
class AccountDeleted(Event):
    """Account was deleted."""
    account_id: str


@dataclass(frozen=True, kw_only=True)
class FundsDeposited(Event):
    """Funds have been doposited into an account."""
    account_id: str
    amount: Decimal
    transaction_id: uuid.UUID

    # @staticmethod
    # def from_json(d: dict[str, any]):
    #     return FundsDeposited(id=d["id"],
    #                             version=d["version"],
    #                             created_at=d["created_at"],
    #                             account_id=d["account_id"],
    #                             amount=d["amount"],
    #                             transaction_id=d["transaction_id"])


@dataclass(frozen=True, kw_only=True)
class FundsWithdrawn(Event):
    """Funds have been withdrawn from an account."""
    account_id: str
    amount: Decimal
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class DebtForgiven(Event):
    """Debt up to $100 has been forgiven in an account."""
    account_id: str
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class ReceiveFundsRequested(Event):
    """This account is requesting funds from another account."""
    funded_account_id: str
    funding_account_id: str
    amount: Decimal
    transaction_id: uuid.UUID
    timeout_at: datetime


@dataclass(frozen=True, kw_only=True)
class NotifiedReceiveFundsRequested(Event):
    """This account was notified that another account requested ReceiveFunds."""
    funded_account_id: str
    funding_account_id: str
    amount: Decimal
    transaction_id: uuid.UUID
    timeout_at: datetime


@dataclass(frozen=True, kw_only=True)
class ReceiveFundsApproved(Event):
    """Funding account has approved a transfer."""
    funding_account_id: str
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class ReceiveFundsRejected(Event):
    """Funding account has denied a transfer."""
    funding_account_id: str
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class NotifiedReceivedFundsRejected(Event):
    """This account was notified that another account rejected ReceiveFunds request."""
    funded_account_id: str
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class ReceiveFundsDebited(Event):
    """Funds sent to the account that requested them."""
    funding_account_id: str
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class ReceiveFundsDebitedRolledBack(Event):
    """Sending funds to another account failed an rolled back."""
    funding_account_id: str
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class ReceiveFundsCredited(Event):
    """Received requested funds."""
    funded_account_id: str
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class SendFundsCredited(Event):
    """Funded account confirms receipt of SendFunds request."""
    funded_account_id: str
    funding_account_id: str
    amount: Decimal
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class SendFundsDebited(Event):
    """Funds sent to another account."""
    funding_account_id: str
    funded_account_id: str
    amount: Decimal
    transaction_id: uuid.UUID


@dataclass(frozen=True, kw_only=True)
class SendFundsDebitedRolledBack(Event):
    """Sending funds to another account failed an rolled back."""
    amount: Decimal
    funding_account_id: str
    transaction_id: uuid.UUID

#     @staticmethod
#     def from_json(d: dict[str, any]):
#         return SendFundsDebitedRolledBack(id=d["id"],
#                                           version=d["version"],
#                                           created_at=d["created_at"],
#                                           amount=d["amount"],
#                                           funding_account_id=d["funding_account_id"],
#                                           transaction_id=d["transaction_id"])


# def _foo(type_name):
#     type(type_name).from_json()


# fd = FundsDeposited(version=42, account_id="4242", amount=420, transaction_id=uuid.uuid4())
# serialized = json.dumps(dataclasses.asdict(fd), cls=CustomEncoder)
# row = (str(FundsDeposited), serialized)
# #deserialized = json.loads(row[1], object_hook=lambda d : type(row[0]).from_json(d))
# deserialized_dict = json.loads(row[1])
# print(deserialized_dict)
# bar = FundsDeposited(**deserialized_dict)
# print(bar)
# print(fd)

# #print(deserialized)
