from datetime import timedelta, datetime
import functools
import uuid

from pyjangle_example.commands import AcceptRequest, CreditRequest, CreditTransfer, DebitRequest, DeleteAccount, DepositFunds, ForgiveDebt, GetRequestApproval, NotifyRequestRejected, RejectRequest, Request, RollbackRequestDebit, RollbackTransferDebit, Transfer, WithdrawFunds
from pyjangle_example.events import AccountCreated, AccountDeleted, DebtForgiven, FundsDeposited, FundsWithdrawn, RequestApproved, RequestCreated, RequestCredited, RequestDebitRolledBack, RequestDebited, RequestReceived, RequestRejected, RequestRejectionReceived, TransferCredited, TransferDebitRolledBack, TransferDebited

from pyjangle import (Aggregate, CommandResponse, RegisterAggregate,
                      reconstitute_aggregate_state, validate_command)

AMOUNT_INDEX = 0
TIMEOUT_INDEX = 1
IS_APPROVED_INDEX = 2
FUNDING_ACCOUNT_ID = 3
IS_FUNDS_TRANSFERRED_INDEX = 4
TIMEOUT_AT_ATTRIBUTE_NAME = "timeout_at"
TRANSACTION_TIMED_OUT_RESPONSE = CommandResponse(
    False, "Transaction timed out")


def fail_if_account_deleted_or_not_exists(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.is_exists:
            return CommandResponse(False, "Account does not exist")
        if self.is_deleted:
            return CommandResponse(False, "Account deleted")
        return func(self, *args, **kwargs)
    return wrapper


def fail_if_transaction_timed_out(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        command = args[0]
        transaction_id = command.transaction_id
        if hasattr(command, TIMEOUT_AT_ATTRIBUTE_NAME) and getattr(command, TIMEOUT_AT_ATTRIBUTE_NAME) < datetime.now():
            return TRANSACTION_TIMED_OUT_RESPONSE
        if transaction_id in self.pending_receive_funds_approvals and self.pending_receive_funds_approvals[transaction_id][TIMEOUT_INDEX] <= datetime.now():
            return TRANSACTION_TIMED_OUT_RESPONSE
        if transaction_id in self.pending_receive_funds_requests and self.pending_receive_funds_requests[transaction_id][TIMEOUT_INDEX] <= datetime.now():
            return TRANSACTION_TIMED_OUT_RESPONSE
        return func(self, *args, **kwargs)
    return wrapper


@RegisterAggregate
class AccountAggregate(Aggregate):

    def __init__(self, id: any):
        super().__init__(id)
        self.balance = 0
        self.is_deleted = False
        self.is_exists = False
        self.forgiveness_count = 0
        self.account_id = None
        self.name = None
        # (transaction_id-- tuple(amount, timeout_at, is_approved))
        self.pending_receive_funds_requests = dict()
        # (transaction_id-- tuple(amount, timeout_at, is_approved, funding_account_id, funds_transferred))
        self.pending_receive_funds_approvals = dict()
        # (transaction_id-- bool (set to true on debit, set to False on rollback))
        self.sent_funds = dict()
        # transaction_id
        self.received_funds = set()

    @validate_command(DepositFunds)
    @fail_if_account_deleted_or_not_exists
    def deposit_funds(self, command: DepositFunds, next_version: int) -> CommandResponse:
        self.post_new_event(FundsDeposited(
            version=next_version, account_id=command.account_id, amount=command.amount, balance=self.balance + command.amount))

    @validate_command(WithdrawFunds)
    @fail_if_account_deleted_or_not_exists
    def withdraw_funds(self, command: WithdrawFunds, next_version: int) -> CommandResponse:
        if self.balance - command.amount < -100:
            return CommandResponse(False, "Insufficient funds")
        self.post_new_event(FundsWithdrawn(
            version=next_version, account_id=command.account_id, amount=command.amount, balance=self.balance - command.amount))

    @validate_command(Transfer)
    @fail_if_account_deleted_or_not_exists
    def send_funds(self, command: Transfer, next_version: int) -> CommandResponse:
        if self.balance - command.amount < -100:
            return CommandResponse(False, "Insufficient funds")
        self.post_new_event(TransferDebited(version=next_version, funding_account_id=command.funding_account_id,
                                            funded_account_id=command.funded_account_id, amount=command.amount, balance=self.balance - command.amount))

    @validate_command(Request)
    @fail_if_account_deleted_or_not_exists
    def receive_funds(self, command: Request, next_version: int) -> CommandResponse:
        self.post_new_event(RequestCreated(version=next_version, funding_account_id=command.funding_account_id,
                                           funded_account_id=command.funded_account_id, amount=command.amount, timeout_at=(datetime.now() + timedelta(minutes=30))))

    @validate_command(CreditTransfer)
    @fail_if_account_deleted_or_not_exists
    def credit_send_funds(self, command: CreditTransfer, next_version: int) -> CommandResponse:
        self.post_new_event(TransferCredited(version=next_version, funded_account_id=command.funded_account_id,
                            funding_account_id=command.funding_account_id, amount=command.amount, transaction_id=command.transaction_id, balance=self.balance + command.amount))

    @validate_command(ForgiveDebt)
    @fail_if_account_deleted_or_not_exists
    def request_forgiveness(self, command: ForgiveDebt, next_version: int) -> CommandResponse:
        if self.forgiveness_count > 2:
            return CommandResponse(False, "Forgiveness quota exceeded")
        if self.balance >= 0:
            return CommandResponse(False, "Forgiveness not applicable")
        self.post_new_event(DebtForgiven(
            version=next_version, account_id=command.account_id, amount=-self.balance))

    @validate_command(DeleteAccount)
    def delete_account(self, command: DeleteAccount, next_version: int) -> CommandResponse:
        if (not self.is_exists):
            return CommandResponse(is_success=False, data="Account does not exist")
        if (self.is_deleted):
            return
        self.post_new_event(AccountDeleted(
            version=next_version, account_id=command.account_id))

    @validate_command(GetRequestApproval)
    @fail_if_account_deleted_or_not_exists
    @fail_if_transaction_timed_out
    def try_obtain_receive_funds_approval(self, command: GetRequestApproval, next_version: int):
        if not command.transaction_id in self.pending_receive_funds_approvals:
            self.post_new_event(RequestReceived(version=next_version, funded_account_id=command.funded_account_id,
                                                funding_account_id=command.funding_account_id, amount=command.amount, transaction_id=command.transaction_id, timeout_at=command.timeout_at))

    @validate_command(RejectRequest)
    @fail_if_account_deleted_or_not_exists
    @fail_if_transaction_timed_out
    def reject_receive_funds_request(self, command: RejectRequest, next_version: int):
        if not command.transaction_id in self.pending_receive_funds_approvals:
            return CommandResponse(False, f"Transaction '{str(command.transaction_id)}' not found")
        if self.pending_receive_funds_approvals[command.transaction_id][IS_APPROVED_INDEX] == False:
            return
        self.post_new_event(RequestRejected(
            version=next_version, funding_account_id=command.funding_account_id, transaction_id=command.transaction_id))

    @validate_command(AcceptRequest)
    @fail_if_account_deleted_or_not_exists
    @fail_if_transaction_timed_out
    def accept_receive_funds_request(self, command: AcceptRequest, next_version: int):
        if not command.transaction_id in self.pending_receive_funds_approvals:
            return CommandResponse(False, f"Transaction '{str(command.transaction_id)}' not found")
        if self.pending_receive_funds_approvals[command.transaction_id][IS_APPROVED_INDEX]:
            return
        self.post_new_event(RequestApproved(
            version=next_version, funding_account_id=self.pending_receive_funds_approvals[command.transaction_id][FUNDING_ACCOUNT_ID], transaction_id=command.transaction_id))

    @validate_command(NotifyRequestRejected)
    @fail_if_account_deleted_or_not_exists
    @fail_if_transaction_timed_out
    def notify_receive_funds_rejected(self, command: NotifyRequestRejected, next_version: int):
        if not command.transaction_id in self.pending_receive_funds_requests:
            return CommandResponse(False, f"Transaction '{str(command.transaction_id)}' not found")
        if self.pending_receive_funds_requests[command.transaction_id][IS_APPROVED_INDEX] == False:
            return
        self.post_new_event(RequestRejectionReceived(
            version=next_version, funded_account_id=command.funded_account_id, transaction_id=command.transaction_id))

    @validate_command(DebitRequest)
    @fail_if_account_deleted_or_not_exists
    @fail_if_transaction_timed_out
    def debit_receive_funds(self, command: DebitRequest, next_version: int):
        if command.transaction_id not in self.pending_receive_funds_approvals:
            return CommandResponse(False, f"Transaction '{str(command.transaction_id)}' not found")
        if self.pending_receive_funds_approvals[command.transaction_id][IS_FUNDS_TRANSFERRED_INDEX]:
            return
        if self.balance - self.pending_receive_funds_approvals[command.transaction_id][AMOUNT_INDEX] < -100:
            return CommandResponse(False, "Insufficient funds")
        self.post_new_event(RequestDebited(
            version=next_version, funding_account_id=command.funding_account_id, transaction_id=command.transaction_id, balance=self.balance - self.pending_receive_funds_approvals[command.transaction_id][AMOUNT_INDEX], amount=self.pending_receive_funds_approvals[command.transaction_id][AMOUNT_INDEX]))

    @validate_command(CreditRequest)
    @fail_if_account_deleted_or_not_exists
    @fail_if_transaction_timed_out
    def credit_receive_funds(self, command: CreditRequest, next_version: int):
        if not command.transaction_id in self.pending_receive_funds_requests:
            return CommandResponse(False, f"Transaction '{str(command.transaction_id)}' not found")
        if self.pending_receive_funds_requests[command.transaction_id][IS_APPROVED_INDEX]:
            return
        self.post_new_event(RequestCredited(
            version=next_version, funded_account_id=command.funded_account_id, transaction_id=command.transaction_id, balance=self.balance + self.pending_receive_funds_requests[command.transaction_id][AMOUNT_INDEX], amount=self.pending_receive_funds_requests[command.transaction_id][AMOUNT_INDEX]))

    @validate_command(CreditTransfer)
    @fail_if_account_deleted_or_not_exists
    def credit_send_funds(self, command: CreditTransfer, next_version: int):
        if command.transaction_id in self.received_funds:
            return
        self.post_new_event(TransferCredited(version=next_version, funded_account_id=command.funded_account_id,
                                             funding_account_id=command.funding_account_id, amount=command.amount, transaction_id=command.transaction_id, balance=self.balance + command.amount))

    @validate_command(RollbackTransferDebit)
    @fail_if_account_deleted_or_not_exists
    def rollback_send_funds_debit(self, command: RollbackTransferDebit, next_version: int):
        if not command.transaction_id in self.sent_funds:
            return CommandResponse(False, f"Transaction '{str(command.transaction_id)}' not found")
        if self.sent_funds[command.transaction_id] == False:
            return
        self.post_new_event(TransferDebitRolledBack(
            version=next_version, funding_account_id=command.funding_account_id, amount=command.amount, transaction_id=command.transaction_id, balance=self.balance + command.amount))

    @validate_command(RollbackRequestDebit)
    @fail_if_account_deleted_or_not_exists
    def rollback_receive_funds_debit(self, command: RollbackRequestDebit, next_version: int):
        if command.transaction_id not in self.pending_receive_funds_approvals:
            return CommandResponse(False, f"Transaction '{str(command.transaction_id)}' not found")
        if self.pending_receive_funds_approvals[command.transaction_id][IS_FUNDS_TRANSFERRED_INDEX] == None:
            return
        self.post_new_event(RequestDebitRolledBack(
            version=next_version, funding_account_id=command.funding_account_id, transaction_id=command.transaction_id, balance=self.balance + self.pending_receive_funds_approvals[command.transaction_id][AMOUNT_INDEX], amount=self.pending_receive_funds_approvals[command.transaction_id][AMOUNT_INDEX]))

    @reconstitute_aggregate_state(AccountCreated)
    def account_created(self, event: AccountCreated):
        self.is_exists = True

    @reconstitute_aggregate_state(AccountDeleted)
    def account_deleted(self, event: AccountDeleted):
        self.is_deleted = True

    @reconstitute_aggregate_state(FundsDeposited)
    def funds_deposited(self, event: FundsDeposited):
        self.balance += event.amount

    @reconstitute_aggregate_state(TransferCredited)
    def send_funds_credited(self, event: TransferCredited):
        self.balance += event.amount

    @reconstitute_aggregate_state(FundsWithdrawn)
    def funds_withdrawn(self, event: FundsWithdrawn):
        self.balance -= event.amount

    @reconstitute_aggregate_state(DebtForgiven)
    def debt_forgiven(self, event: DebtForgiven):
        self.balance = 0

    @reconstitute_aggregate_state(RequestCreated)
    def receive_funds_requested(self, event: RequestCreated):
        self.pending_receive_funds_requests[event.transaction_id] = (
            event.amount, event.timeout_at, None)

    @reconstitute_aggregate_state(RequestReceived)
    def notified_receive_funds_requested(self, event: RequestReceived):
        self.pending_receive_funds_approvals[event.transaction_id] = (
            event.amount, event.timeout_at, None, event.funding_account_id, False)

    @reconstitute_aggregate_state(RequestApproved)
    def receive_funds_approved(self, event: RequestApproved):
        t = self.pending_receive_funds_approvals[event.transaction_id]
        self.pending_receive_funds_approvals[event.transaction_id] = (
            t[0], t[1], True, t[3], t[4]
        )

    @reconstitute_aggregate_state(RequestRejected)
    def receive_funds_rejected(self, event: RequestRejected):
        t = self.pending_receive_funds_approvals[event.transaction_id]
        self.pending_receive_funds_approvals[event.transaction_id] = (
            t[0], t[1], False, t[3], t[4]
        )

    @reconstitute_aggregate_state(RequestRejectionReceived)
    def notified_receive_funds_rejected(self, event: RequestRejectionReceived):
        t = self.pending_receive_funds_requests[event.transaction_id]
        self.pending_receive_funds_requests[event.transaction_id] = (
            t[0], t[1], False)

    @reconstitute_aggregate_state(RequestDebited)
    def receive_funds_debited(self, event: RequestDebited):
        self.balance -= event.amount
        t = self.pending_receive_funds_approvals[event.transaction_id]
        self.pending_receive_funds_approvals[event.transaction_id] = (
            t[0], t[1], t[2], t[3], True)

    @reconstitute_aggregate_state(RequestCredited)
    def receive_funds_credited(self, event: RequestCredited):
        self.balance += event.amount
        t = self.pending_receive_funds_requests[event.transaction_id]
        self.pending_receive_funds_requests[event.transaction_id] = (
            t[0], t[1], True)

    @reconstitute_aggregate_state(TransferCredited)
    def send_funds_credited(self, event: TransferCredited):
        self.received_funds.add(event.transaction_id)
        self.balance += event.amount

    @reconstitute_aggregate_state(TransferDebited)
    def send_funds_debited(self, event: TransferDebited):
        self.sent_funds[event.transaction_id] = True
        self.balance -= event.amount

    @reconstitute_aggregate_state(TransferDebitRolledBack)
    def rollback_send_funds_debited(self, event: TransferDebitRolledBack):
        self.sent_funds[event.transaction_id] = False
        self.balance += event.amount

    @reconstitute_aggregate_state(RequestDebitRolledBack)
    def rollback_receive_funds_debited(self, event: RequestDebitRolledBack):
        self.balance += event.amount
        t = self.pending_receive_funds_approvals[event.transaction_id]
        self.pending_receive_funds_approvals[event.transaction_id] = (
            t[0], t[1], t[2], t[3], None)
