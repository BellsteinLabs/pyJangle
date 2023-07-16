"""
Represents all actions that can be taken on the application.
"""

from datetime import datetime
from decimal import Decimal
import uuid
from dataclasses import dataclass
from pyjangle.command.command import Command

@dataclass(frozen=True, kw_only=True)
class CreateAccount(Command):
    """Requests a new account to be created."""
    account_id: uuid.uuid4
    name: str
    initial_deposit: Decimal = 0
    
    def get_aggregate_id(self):
        return self.account_id


@dataclass(frozen=True, kw_only=True)
class DepositFunds:
    """Deposits funds into an account."""
    account_id: uuid.UUID
    account_id: str
    amount: Decimal

@dataclass(frozen=True, kw_only=True)
class WithdrawFunds:
    """Withdraws funds from an account."""
    account_id: uuid.UUID
    amount: Decimal

@dataclass(frozen=True, kw_only=True)
class SendFunds:
    """Transfer funds to another account."""
    funded_account_id: str
    funding_account_id: str
    amount: Decimal
    transaction_id: uuid.UUID

    def __post_init__(self):
        if self.funded_account_id == self.funding_account_id:
            raise ValueError("Sending funds to self not allowed.")

@dataclass(frozen=True, kw_only=True)
class ReceiveFunds:
    """Request funds from another account.
    
    This request is mediated by a saga.  The funding account
    can either accept or reject the request.
    """
    funded_account_id: str
    funding_account_id: str
    amount: Decimal

    def __post_init__(self):
        if self.funded_account_id == self.funding_account_id:
            raise ValueError("Receiving funds from self not allowed.")
        
@dataclass(frozen=True, kw_only=True)
class RequestForgiveness:
    """Forgives debt up to $100.
    
    This request will only be allowed twice during the lifetime of the 
    account.
    """
    account_id: uuid.UUID

@dataclass(frozen=True, kw_only=True)
class DeleteAccount:
    """Deletes an account.
    
    Deletes are soft meaning that the account will be marked as deleted,
    but it will remain in the system."""
    account_id: uuid.UUID

@dataclass(frozen=True, kw_only=True)
class TryObtainReceiveFundsApproval:
    """Ask the funding account for approval in a ReceiveFunds transfer.
    
    This command is only used by the ReceiveFundsTransfer saga.
    It notifies the funding account that another account is requesting 
    funds.  The funding account can then approve or deny the request.
    """
    funded_account_id: str
    funding_account_id: str
    transaction_id: uuid.UUID
    timeout_at: datetime
    amount: Decimal

@dataclass(frozen=True, kw_only=True)
class RejectReceiveFundsRequest:
    """Reject a ReceiveFunds request from another account."""
    funding_account_id: uuid.UUID
    transaction_id: uuid.UUID

@dataclass(frozen=True, kw_only=True)
class AcceptReceiveFundsRequest:
    """Accept a ReceiveFunds request from another account."""
    funding_account_id: uuid.UUID
    transaction_id: uuid.UUID

@dataclass(frozen=True, kw_only=True)
class NotifyReceiveFundsRejected:
    """Notifies funded account that a ReceiveFunds transfer was rejected.
    
    When the funding account rejects a request for a funds transfer,
    this command is used by the saga to notify the funded account 
    that the transfer was rejected."""
    funded_account_id: str
    funding_account_id: str
    transaction_id: uuid

@dataclass(frozen=True, kw_only=True)
class DebitReceiveFunds:
    """Deducts funds from funding account in a ReceiveFunds transfer.
    
    The saga handling a ReceiveFunds request uses this command to 
    request that funds be deducted from the funding account after 
    the transfer is approved by the funding account.
    """
    funding_account_id: str
    transaction_id: uuid

@dataclass(frozen=True, kw_only=True)
class CreditReceiveFunds:
    """Adds funds to the funded account in a ReceiveFunds transfer.
    
    The saga handling a ReceiveFunds request uses this command to 
    request that funds be credited to the funded account after 
    the transfer is approved by the funding account.
    """
    funded_account_id: str
    transaction_id: uuid

@dataclass(frozen=True, kw_only=True)
class CreditSendFunds:
    """Credits funds for SendFunds request.
    
    A SendFunds request is mediated by an EventHandler
    which uses this command to add funds to the destination
    account."""
    funding_account_id: str
    funded_account_id: str
    transaction_id: str

@dataclass(frozen=True, kw_only=True)
class RollbackSendFundsDebit:
    """Rollback a failed SendFunds transfer on funding account.
    
    In the event that a SendFunds command fails,
    the EventHandler handling the request will use this 
    command to rollback the transaction on the funding 
    account.
    """
    funding_account_id: str
    transaction_id: str

@dataclass(frozen=True, kw_only=True)
class RollbackReceiveFundsDebit:
    """Rollback a failed ReceiveFunds transfer on funding account.
    
    In the event that a RecieveFunds transfer is 
    approved on the funding account, and the saga subsequently 
    fails to credit the funds on the funded account,
    maybe because it was deleted, this command rolls back
    the transaction on the funding account."""
    funding_account_id: str
    transaction_id: uuid