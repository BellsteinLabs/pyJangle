import abc
from pyjangle_example.events import AccountCreated, AccountDeleted, DebtForgiven, FundsDeposited, FundsWithdrawn, NotifiedReceiveFundsRequested, NotifiedReceivedFundsRejected, ReceiveFundsApproved, ReceiveFundsCredited, ReceiveFundsDebited, ReceiveFundsDebitedRolledBack, ReceiveFundsRejected, ReceiveFundsRequested, SendFundsCredited, SendFundsDebited, SendFundsDebitedRolledBack
from pyjangle_example.query_responses import AccountResponse, AccountSummaryResponse, BankStatsResponse, TransactionResponse


class BankDataAccessObject(metaclass=abc.ABCMeta):

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
    async def handle_receive_funds_requested(event: ReceiveFundsRequested):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_notified_receive_funds_requested(event: NotifiedReceiveFundsRequested):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_approved(event: ReceiveFundsApproved):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_rejected(event: ReceiveFundsRejected):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_notified_received_funds_rejected(event: NotifiedReceivedFundsRejected):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_debited(event: ReceiveFundsDebited):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_debited_rolled_back(event: ReceiveFundsDebitedRolledBack):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_receive_funds_credited(event: ReceiveFundsCredited):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_send_funds_credited(event: SendFundsCredited):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_send_funds_debited(event: SendFundsDebited):
        pass

    @staticmethod
    @abc.abstractmethod
    async def handle_send_funds_debited_rolled_back(event: SendFundsDebitedRolledBack):
        pass
