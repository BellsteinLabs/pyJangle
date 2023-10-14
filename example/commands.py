"Commands corresponding to all actions that can be taken on the application."

from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass
from pyjangle import Command
from .validation.descriptors import (
    AccountId,
    AccountName,
    Amount,
    Timeout,
    TransactionId,
)

# --------------
ACCOUNT_CREATION_AGGREGATE_ID = "ACCOUNT_CREATION_AGGREGATE"


@dataclass(kw_only=True)
class CreateAccount(Command):
    """Requests a new account to be created."""

    name: str = AccountName()
    initial_deposit: Decimal = Amount(can_be_none=True)

    def get_aggregate_id(self):
        return ACCOUNT_CREATION_AGGREGATE_ID


# --------------


@dataclass(kw_only=True)
class DepositFunds(Command):
    """Deposits funds into an account."""

    account_id: str = AccountId()
    amount: Decimal = Amount()

    def get_aggregate_id(self):
        return self.account_id


# --------------


@dataclass(kw_only=True)
class WithdrawFunds(Command):
    """Withdraws funds from an account."""

    account_id: str = AccountId()
    amount: Decimal = Amount()

    def get_aggregate_id(self):
        return self.account_id


# --------------


@dataclass(kw_only=True)
class Transfer(Command):
    """Transfer funds to another account."""

    funded_account_id: str = AccountId()
    funding_account_id: str = AccountId()
    amount: Decimal = Amount()

    def get_aggregate_id(self):
        return self.funding_account_id

    def __post_init__(self):
        if self.funded_account_id == self.funding_account_id:
            raise ValueError("Sending funds to self not allowed.")


@dataclass(kw_only=True)
class Request(Command):
    """Request funds from another account.

    This request is mediated by a saga.  The funding account can either accept or reject
    the request.
    """

    funded_account_id: str = AccountId()
    funding_account_id: str = AccountId()
    amount: Decimal = Amount()

    def get_aggregate_id(self):
        return self.funded_account_id

    def __post_init__(self):
        if self.funded_account_id == self.funding_account_id:
            raise ValueError("Receiving funds from self not allowed.")


# --------------


@dataclass(kw_only=True)
class ForgiveDebt(Command):
    """Forgives debt up to $100.

    This request will only be allowed twice during the lifetime of the account.
    """

    account_id: str = AccountId()

    def get_aggregate_id(self):
        return self.account_id


@dataclass(kw_only=True)
class DeleteAccount(Command):
    """Deletes an account.

    Deletes are soft meaning that the account will be marked as deleted, but it will
    remain in the system."""

    account_id: str = AccountId()

    def get_aggregate_id(self):
        return self.account_id


@dataclass(kw_only=True)
class GetRequestApproval(Command):
    """Ask the funding account for approval in a ReceiveFunds transfer.

    This command is only used by the ReceiveFundsTransfer saga.  It notifies the funding
    account that another account is requesting funds.  The funding account can then
    approve or deny the request.
    """

    funded_account_id: str = AccountId()
    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()
    timeout_at: datetime = Timeout()
    amount: Decimal = Amount()

    def get_aggregate_id(self):
        return self.funding_account_id


@dataclass(kw_only=True)
class RejectRequest(Command):
    """Reject a ReceiveFunds request from another account."""

    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()

    def get_aggregate_id(self):
        return self.funding_account_id


@dataclass(kw_only=True)
class AcceptRequest(Command):
    """Accept a ReceiveFunds request from another account."""

    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()

    def get_aggregate_id(self):
        return self.funding_account_id


@dataclass(kw_only=True)
class NotifyRequestRejected(Command):
    """Notifies funded account that a ReceiveFunds transfer was rejected.

    When the funding account rejects a request for a funds transfer, this command is
    used by the saga to notify the funded account that the transfer was rejected."""

    funded_account_id: str = AccountId()
    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()

    def get_aggregate_id(self):
        return self.funded_account_id


@dataclass(kw_only=True)
class DebitRequest(Command):
    """Deducts funds from funding account in a ReceiveFunds transfer.

    The saga handling a ReceiveFunds request uses this command to request that funds be
    deducted from the funding account after the transfer is approved by the funding
    account.
    """

    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()

    def get_aggregate_id(self):
        return self.funding_account_id


@dataclass(kw_only=True)
class CreditRequest(Command):
    """Adds funds to the funded account in a ReceiveFunds transfer.

    The saga handling a ReceiveFunds request uses this command to request that funds be
    credited to the funded account after the transfer is approved by the funding
    account.
    """

    funded_account_id: str = AccountId()
    transaction_id: str = TransactionId()

    def get_aggregate_id(self):
        return self.funded_account_id


@dataclass(kw_only=True)
class CreditTransfer(Command):
    """Credits funds for SendFunds request.

    A SendFunds request is mediated by an EventHandler which uses this command to add
    funds to the destination account."""

    funding_account_id: str = AccountId()
    funded_account_id: str = AccountId()
    amount: Decimal = Amount()
    transaction_id: str = TransactionId()

    def get_aggregate_id(self):
        return self.funded_account_id


@dataclass(kw_only=True)
class RollbackTransferDebit(Command):
    """Rollback a failed SendFunds transfer on funding account.

    In the event that a SendFunds command fails, the EventHandler handling the request
    will use this command to rollback the transaction on the funding account.
    """

    amount: Decimal = Amount()
    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()

    def get_aggregate_id(self):
        return self.funding_account_id


@dataclass(kw_only=True)
class RollbackRequestDebit(Command):
    """Rollback a failed ReceiveFunds transfer on funding account.

    In the event that a RecieveFunds transfer is approved on the funding account, and
    the saga subsequently fails to credit the funds on the funded account, maybe because
    it was deleted, this command rolls back the transaction on the funding account."""

    funding_account_id: str = AccountId()
    transaction_id: str = TransactionId()

    def get_aggregate_id(self):
        return self.funding_account_id
