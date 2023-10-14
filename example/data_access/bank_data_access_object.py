import abc
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
from query_responses import (
    AccountResponse,
    AccountSummaryResponse,
    BankStatsResponse,
    TransactionResponse,
)


class BankDataAccessObject(metaclass=abc.ABCMeta):
    """Bank query interface.

    Methods prefixed with 'handle' add event data to the application-specific views, and
    all other methods are for responding to queries."""

    @staticmethod
    @abc.abstractmethod
    async def bank_summary() -> list[AccountResponse]:
        pass

    @staticmethod
    @abc.abstractmethod
    async def bank_stats() -> BankStatsResponse:
        pass

    @staticmethod
    @abc.abstractmethod
    async def account_summary(account_id: str) -> AccountSummaryResponse:
        pass

    @staticmethod
    @abc.abstractmethod
    async def account_ledger(account_id: str) -> list[TransactionResponse]:
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_account_created(event: AccountCreated):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_account_deleted(event: AccountDeleted):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_funds_deposited(event: FundsDeposited):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_funds_withdrawn(event: FundsWithdrawn):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_debt_forgiven(event: DebtForgiven):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_requested(event: RequestCreated):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_notified_receive_funds_requested(event: RequestReceived):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_approved(event: RequestApproved):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_rejected(event: RequestRejected):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_notified_received_funds_rejected(event: RequestRejectionReceived):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_debited(event: RequestDebited):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_debited_rolled_back(event: RequestDebitRolledBack):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_credited(event: RequestCredited):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_send_funds_credited(event: TransferCredited):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_send_funds_debited(event: TransferDebited):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_send_funds_debited_rolled_back(event: TransferDebitRolledBack):
        pass
