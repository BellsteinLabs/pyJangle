import functools

from pyjangle_example.example_commands import *
from pyjangle_example.example_events import *

from pyjangle import (Aggregate, CommandResponse, RegisterCommand,
                      reconstitute_aggregate_state, validate_command)

AMOUNT_INDEX = 0
TIMEOUT_INDEX = 1
IS_APPROVED_INDEX = 2
TIMEOUT_AT_ATTRIBUTE_NAME = "timeout_at"
TRANSACTION_TIMED_OUT_RESPONSE = CommandResponse(
    False, "Transaction timed out")


def fail_if_account_deleted(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.is_deleted:
            return CommandResponse(False, "Account deleted")
        return func(self, *args, **kwargs)
    return wrapper


def fail_if_transaction_timed_out(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        command = kwargs["command"]
        transaction_id = command.transaction_id
        if hasattr(command, TIMEOUT_AT_ATTRIBUTE_NAME) and getattr(command, TIMEOUT_AT_ATTRIBUTE_NAME) < datetime.now():
            return TRANSACTION_TIMED_OUT_RESPONSE
        if transaction_id in self.pending_receive_funds_approvals and self.pending_receive_funds_approvals[transaction_id][TIMEOUT_INDEX] < datetime.now():
            return TRANSACTION_TIMED_OUT_RESPONSE
        if transaction_id in self.pending_receive_funds_requests and self.pending_receive_funds_requests[transaction_id][TIMEOUT_INDEX] < datetime.now():
            return TRANSACTION_TIMED_OUT_RESPONSE
        return func(self, *args, **kwargs)
    return wrapper


@RegisterCommand(DepositFunds, WithdrawFunds, SendFunds, ReceiveFunds, RequestForgiveness, DeleteAccount, TryObtainReceiveFundsApproval, NotifyReceiveFundsRejected, CreditSendFunds)
class AccountAggregate(Aggregate):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.balance = 0
        self.is_deleted = False
        self.forgiveness_count = 0
        self.account_id = None
        self.name = None
        # (transaction_id-- tuple(amount, timeout_at))
        self.pending_receive_funds_requests = dict()
        # (transaction_id-- tuple(amount, timeout_at, is_approved))
        self.pending_receive_funds_approvals = dict()

    @validate_command(DepositFunds)
    @fail_if_account_deleted
    def deposit_funds(self, command: DepositFunds, next_version: int) -> CommandResponse:
        self.post_new_event(FundsDeposited(
            version=next_version, account_id=command.account_id, amount=command.amount))

    @validate_command(WithdrawFunds)
    @fail_if_account_deleted
    def withdraw_funds(self, command: WithdrawFunds, next_version: int) -> CommandResponse:
        if self.balance - command.amount < -100:
            return CommandResponse(False, "Insufficient funds")
        self.post_new_event(FundsWithdrawn(
            version=next_version, account_id=command.account_id, amount=command.amount))

    @validate_command(SendFunds)
    @fail_if_account_deleted
    def send_funds(self, command: SendFunds, next_version: int) -> CommandResponse:
        if self.balance - command.amount < -100:
            return CommandResponse(False, "Insufficient funds")
        self.post_new_event(SendFundsDebited(version=next_version, funding_account_id=command.funding_account_id,
                                             funded_account_id=command.funded_account_id, amount=command.amount))

    @validate_command(ReceiveFunds)
    @fail_if_account_deleted
    def receive_funds(self, command: ReceiveFunds, next_version: int) -> CommandResponse:
        self.post_new_event(ReceiveFundsRequested(version=next_version, funding_account_id=command.funding_account_id,
                                                  funded_account_id=command.funded_account_id, amount=command.amount, transaction_id=uuid.uuid4()))

    @validate_command(CreditSendFunds)
    @fail_if_account_deleted
    def credit_send_funds(self, command: CreditSendFunds, next_version: int) -> CommandResponse:
        self.post_new_event(SendFundsCredited(version=next_version, funded_account_id=command.funded_account_id,
                            funding_account_id=command.funding_account_id, amount=command.amount, transaction_id=command.transaction_id))

    @validate_command(RequestForgiveness)
    @fail_if_account_deleted
    def request_forgiveness(self, command: RequestForgiveness, next_version: int) -> CommandResponse:
        if self.forgiveness_count > 2:
            return CommandResponse(False, "Forgiveness quota exceeded")
        if self.balance > 0:
            return CommandResponse(False, "Forgiveness not applicable")
        self.post_new_event(DebtForgiven(
            version=next_version, account_id=command.account_id, amount=self.balance))

    @validate_command(DeleteAccount)
    @fail_if_account_deleted
    def delete_account(self, command: DeleteAccount, next_version: int) -> CommandResponse:
        if (self.is_deleted):
            return
        self.post_new_event(AccountDeleted(
            version=next_version, account_id=command.account_id))

    @validate_command(TryObtainReceiveFundsApproval)
    @fail_if_account_deleted
    @fail_if_transaction_timed_out
    def try_obtain_receive_funds_approval(self, command: TryObtainReceiveFundsApproval, next_version: int):
        if not command.transaction_id in self.pending_receive_funds_approvals:
            self.post_new_event(NotifiedReceiveFundsRequested(version=next_version, funded_account_id=command.funded_account_id,
                                                              funding_account_id=command.funding_account_id, amount=command.amount, transaction_id=command.transaction_id, timeout_at=command))

    @validate_command(RejectReceiveFundsRequest)
    @fail_if_account_deleted
    @fail_if_transaction_timed_out
    def reject_receive_funds_request(self, command: RejectReceiveFundsRequest, next_version: int):
        if not command.transaction_id in self.pending_receive_funds_approvals:
            return CommandResponse(False, "Transaction not found")
        self.post_new_event(ReceiveFundsRejected(
            version=next_version, funding_account_id=command.funding_account_id, transaction_id=command.transaction_id))

    @validate_command(AcceptReceiveFundsRequest)
    @fail_if_account_deleted
    @fail_if_transaction_timed_out
    def accept_receive_funds_request(self, command: AcceptReceiveFundsRequest, next_version: int):
        if not command.transaction_id in self.pending_receive_funds_approvals:
            return CommandResponse(False, "Transaction not found")
        self.post_new_event(ReceiveFundsApproved(
            version=next_version, funding_account_id=command.funding_account_id, transaction_id=command.transaction_id))

    @validate_command(NotifyReceiveFundsRejected)
    @fail_if_account_deleted
    @fail_if_transaction_timed_out
    def notify_receive_funds_rejected(self, command: NotifyReceiveFundsRejected, next_version: int):
        if not command.transaction_id in self.pending_receive_funds_requests:
            return CommandResponse(False, "Transaction not found")
        self.post_new_event(NotifiedReceivedFundsRejected(
            version=next_version, funded_account_id=command.funded_account_id, transaction_id=command.transaction_id))

    @validate_command(DebitReceiveFunds)
    @fail_if_account_deleted
    @fail_if_transaction_timed_out
    def debit_receive_funds(self, command: DebitReceiveFunds, next_version: int):
        if self.balance - self.pending_receive_funds_requests[command.transaction_id][AMOUNT_INDEX] < -100:
            return CommandResponse(False, "Insufficient funds")
        self.post_new_event(ReceiveFundsDebited(
            version=next_version, funding_account_id=command.funding_account_id, transaction_id=command.transaction_id))

    @validate_command(CreditReceiveFunds)
    @fail_if_account_deleted
    @fail_if_transaction_timed_out
    def credit_receive_funds(self, command: CreditReceiveFunds, next_version: int):
        self.post_new_event(ReceiveFundsCredited(
            version=next_version, funded_account_id=command.funded_account_id, transaction_id=command.transaction_id))

    @validate_command(CreditSendFunds)
    @fail_if_account_deleted
    def credit_send_funds(self, command: CreditSendFunds, next_version: int):
        self.post_new_event(SendFundsCredited(version=next_version, funded_account_id=command.funded_account_id,
                                              funding_account_id=command.funding_account_id, amount=command.amount, transaction_id=command.transaction_id))

    @validate_command(RollbackSendFundsDebit)
    @fail_if_account_deleted
    def rollback_send_funds_debit(self, command: RollbackSendFundsDebit, next_version: int):
        self.post_new_event(SendFundsDebitedRolledBack(
            version=next_version, funding_account_id=command.funding_account_id, transaction_id=command.transaction_id))

    @validate_command(RollbackReceiveFundsDebit)
    @fail_if_account_deleted
    def rollback_receive_funds_debit(self, command: RollbackReceiveFundsDebit, next_version: int):
        self.post_new_event(ReceiveFundsDebitedRolledBack(
            version=next_version, funding_account_id=command.funding_account_id, transaction_id=command.transaction_id))

    @reconstitute_aggregate_state(AccountCreated)
    def account_created(self, event: AccountCreated):
        pass

    @reconstitute_aggregate_state(AccountDeleted)
    def account_deleted(self, event: AccountDeleted):
        self.is_deleted = True

    @reconstitute_aggregate_state(FundsDeposited)
    def funds_deposited(self, event: FundsDeposited):
        self.balance += event.amount

    @reconstitute_aggregate_state(SendFundsCredited)
    def send_funds_credited(self, event: SendFundsCredited):
        self.balance += event.amount

    @reconstitute_aggregate_state(FundsWithdrawn)
    def funds_withdrawn(self, event: FundsWithdrawn):
        self.balance -= event.amount

    @reconstitute_aggregate_state(DebtForgiven)
    def debt_forgiven(self, event: DebtForgiven):
        self.balance = 0

    @reconstitute_aggregate_state(ReceiveFundsRequested)
    def receive_funds_requested(self, event: ReceiveFundsRequested):
        self.pending_receive_funds_requests[event.transaction_id] = (
            event.amount, event.timeout_at)

    @reconstitute_aggregate_state(NotifiedReceiveFundsRequested)
    def notified_receive_funds_requested(self, event: NotifiedReceiveFundsRequested):
        self.pending_receive_funds_approvals[event.transaction_id] = (
            event.amount, event.timeout_at, None)

    @reconstitute_aggregate_state(ReceiveFundsApproved)
    def receive_funds_approved(self, event: ReceiveFundsApproved):
        self.pending_receive_funds_approvals[event.transaction_id][IS_APPROVED_INDEX] = True

    @reconstitute_aggregate_state(ReceiveFundsRejected)
    def receive_funds_rejected(self, event: ReceiveFundsRejected):
        self.pending_receive_funds_approvals[event.transaction_id][IS_APPROVED_INDEX] = False

    @reconstitute_aggregate_state(NotifiedReceivedFundsRejected)
    def notified_receive_funds_rejected(self, event: NotifiedReceivedFundsRejected):
        del self.pending_receive_funds_requests[event.transaction_id]

    @reconstitute_aggregate_state(ReceiveFundsDebited)
    def receive_funds_debited(self, event: ReceiveFundsDebited):
        self.balance -= self.pending_receive_funds_requests[event.transaction_id][AMOUNT_INDEX]
        del self.pending_receive_funds_requests[event.transaction_id]

    @reconstitute_aggregate_state(ReceiveFundsCredited)
    def receive_funds_credited(self, event: ReceiveFundsCredited):
        self.balance += self.pending_receive_funds_requests[event.transaction_id][AMOUNT_INDEX]
        del self.pending_receive_funds_requests[event.transaction_id]

    @reconstitute_aggregate_state(SendFundsCredited)
    def send_funds_credited(self, event: SendFundsCredited):
        self.balance += event.amount

    @reconstitute_aggregate_state(SendFundsDebited)
    def send_funds_debited(self, event: SendFundsDebited):
        self.balance -= event.amount

    @reconstitute_aggregate_state(SendFundsDebitedRolledBack)
    def rollback_send_funds_debited(self, event: SendFundsDebitedRolledBack):
        self.balance += event.amount
