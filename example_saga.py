from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List
from pyjangle.command.command_dispatcher import command_dispatcher_instance
from pyjangle.command.command_response import CommandResponse
from pyjangle.event.event import *
from example_commands import *
from example_events import ReceiveFundsApproved, ReceiveFundsDebitedRolledBack, ReceiveFundsRejected, ReceiveFundsRequested
from pyjangle.event.event import Event
from pyjangle.saga.saga import Saga, reconstitute_saga_state

RETRY_TIMEOUT_LENGTH = timedelta(seconds=15)

EVENT_RECEIVE_FUNDS_TRANSFER_REQUESTED = 1
EVENT_RECEIVE_FUNDS_APPROVED = 4
EVENT_RECEIVE_FUNDS_REJECTED = 8

COMMAND_TRY_OBTAIN_RECEIVE_FUNDS_APPROVAL = 2
COMMAND_NOTIFY_RECEIEVE_FUNDS_REJECTED = 3
COMMAND_DEBIT_RECIEVE_FUNDS = 5
COMMAND_CREDIT_RECEIEVE_FUNDS = 6
COMMAND_ROLLBACK_RECEIVE_FUNDS_DEBIT = 7

FIELD_TRANSACTION_ID = "transaction_id"
FIELD_FUNDING_ACCOUNT_ID = "funding_account_id"
FIELD_FUNDED_ACCOUNT_ID = "funded_account_id"
FIELD_AMOUNT = "amount"
FIELD_TIMEOUT_AT = "timeout_at"

class TryObtainReceiveFundsApprovalCommandReceived(SagaEvent):
    pass

class NotifyReceiveFundsRejectedCommandReceived(SagaEvent):
    pass

class DebitReceiveFundsCommandReceived(SagaEvent):
    pass

class CreditReceiveFundsCommandReceived(SagaEvent):
    pass

class RollbackReceiveFundsDebitCommandReceived(SagaEvent):
    pass

class RequestFundsFromAnotherAccount(Saga):

    def __init__(self, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
        super().__init__(events, retry_at, timeout_at, is_complete)
        self.command_dispatcher = command_dispatcher_instance()
        
    def retry_saga_on_exception(self, callable: Callable[[CommandResponse], any], wait_time: timedelta = None):
        try:
            return callable()
        except:
            self.set_retry(datetime.now + (wait_time if wait_time != None else RETRY_TIMEOUT_LENGTH))
            

    def evaluate_hook(self):
        if not EVENT_RECEIVE_FUNDS_TRANSFER_REQUESTED in self.flags:
            return 
        if not COMMAND_TRY_OBTAIN_RECEIVE_FUNDS_APPROVAL in self.flags:
            request_transfer_approval_on_funding_account_command_response = None
            try:
                request_transfer_approval_on_funding_account_command_response = self.command_dispatcher(self._make_try_obtain_receive_funds_approval_command())
            except:
                self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                return
            if request_transfer_approval_on_funding_account_command_response.is_success:
                self._post_new_event(TryObtainReceiveFundsApprovalCommandReceived(saga_id=self.saga_id))
                return
            else:
                try:
                    self.command_dispatcher(self._make_notify_receive_funds_rejected_command())
                    self._post_new_event(NotifyReceiveFundsRejectedCommandReceived(saga_id=self.saga_id))
                    self.set_complete()
                except:
                    self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                    return
        if EVENT_RECEIVE_FUNDS_APPROVED in self.flags:   
            if not COMMAND_DEBIT_RECIEVE_FUNDS in self.flags:
                debit_transfer_funds_response = None
                try:
                    debit_transfer_funds_response = self.command_dispatcher(self._make_debit_receive_funds_command())
                except:
                    self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                    return
                if debit_transfer_funds_response.is_success:
                    self._post_new_event(DebitReceiveFundsCommandReceived(saga_id=self.saga_id))
                else:
                    try:
                        self.command_dispatcher(self._make_notify_receive_funds_rejected_command())
                        self._post_new_event(NotifyReceiveFundsRejectedCommandReceived(saga_id=self.saga_id))
                        self.set_complete()
                    except:
                        self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                        return
            if not COMMAND_CREDIT_RECEIEVE_FUNDS in self.flags:
                credit_receive_transfer_funds_response = None
                try:
                    credit_receive_transfer_funds_response = self.command_dispatcher(self._make_credit_receive_funds_command())
                except:
                    self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                    return
                if credit_receive_transfer_funds_response.is_success:
                    self._post_new_event(CreditReceiveFundsCommandReceived(saga_id=self.saga_id))
                    self.set_complete()
                else:
                    try:
                        self.command_dispatcher(self._make_rollback_receive_funds_debit_command())
                        self._post_new_event(RollbackReceiveFundsDebitCommandReceived(saga_id=self.saga_id))
                        self.set_complete()
                    except:
                        self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                        return
        if EVENT_RECEIVE_FUNDS_REJECTED in self.flags:
            if not COMMAND_NOTIFY_RECEIEVE_FUNDS_REJECTED in self.flags:
                try:
                    self.command_dispatcher(self._make_notify_receive_funds_rejected_command())
                    self._post_new_event(NotifyReceiveFundsRejectedCommandReceived(saga_id=self.saga_id))
                    self.set_complete()
                except:
                    self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                    return
                
    

    @reconstitute_saga_state(ReceiveFundsRequested)                
    def handle_receive_funds_transfer_requested(self, event: ReceiveFundsRequested):
        self.flags.add(EVENT_RECEIVE_FUNDS_TRANSFER_REQUESTED)
        setattr(self, FIELD_TRANSACTION_ID, event.transaction_id)
        setattr(self, FIELD_FUNDING_ACCOUNT_ID, event.funding_account_id)
        setattr(self, FIELD_FUNDED_ACCOUNT_ID, event.funded_account_id)
        setattr(self, FIELD_AMOUNT, event.amount)
        setattr(self, FIELD_TIMEOUT_AT, event.timeout_at)
        self.timeout_at = event.timeout_at

    @reconstitute_saga_state(ReceiveFundsApproved)
    def handle_receive_funds_transfer_approved(self, event: ReceiveFundsApproved):
        self.flags.add(EVENT_RECEIVE_FUNDS_APPROVED)

    @reconstitute_saga_state(ReceiveFundsRejected)
    def handler_receive_funds_transfer_rejected(self, event: ReceiveFundsRejected):
        self.flags.add(EVENT_RECEIVE_FUNDS_REJECTED)

    @reconstitute_saga_state(TryObtainReceiveFundsApprovalCommandReceived)
    def from_try_obtain_receive_funds_approval_command_received(self, event: TryObtainReceiveFundsApprovalCommandReceived):
        self.flags.add(COMMAND_TRY_OBTAIN_RECEIVE_FUNDS_APPROVAL)

    @reconstitute_saga_state(NotifyReceiveFundsRejectedCommandReceived)
    def from_notify_receive_funds_rejected_command_received(self, event: NotifyReceiveFundsRejectedCommandReceived):
        self.flags.add(COMMAND_NOTIFY_RECEIEVE_FUNDS_REJECTED)

    @reconstitute_saga_state(DebitReceiveFundsCommandReceived)
    def from_debit_receive_funds_command_received(self, event: DebitReceiveFundsCommandReceived):
        self.flags.add(COMMAND_DEBIT_RECIEVE_FUNDS)

    @reconstitute_saga_state(CreditReceiveFundsCommandReceived)
    def from_credit_receive_funds_command_received(self, event: CreditReceiveFundsCommandReceived):
        self.flags.add(COMMAND_CREDIT_RECEIEVE_FUNDS)

    @reconstitute_saga_state(RollbackReceiveFundsDebitCommandReceived)
    def from_rollback_receive_funds_debit_command_recived(self, event: RollbackReceiveFundsDebitCommandReceived):
        self.flags.add(COMMAND_ROLLBACK_RECEIVE_FUNDS_DEBIT)
        
    def _make_try_obtain_receive_funds_approval_command(self):
        return TryObtainReceiveFundsApproval(funding_account_id=getattr(self, FIELD_FUNDING_ACCOUNT_ID), funded_account_id=getattr(self, FIELD_FUNDED_ACCOUNT_ID), amount=getattr(self, FIELD_AMOUNT), transaction_id=getattr(self, FIELD_TRANSACTION_ID), timeout_at=getattr(self, FIELD_TIMEOUT_AT))
    
    def _make_notify_receive_funds_rejected_command(self):
        return NotifyReceiveFundsRejected(funding_account_id=getattr(self, FIELD_FUNDING_ACCOUNT_ID), funded_account_id=getattr(self, FIELD_FUNDED_ACCOUNT_ID), transaction_id=getattr(self, FIELD_TRANSACTION_ID))
    
    def _make_debit_receive_funds_command(self):
        return DebitReceiveFunds(funding_account_id=getattr(self, FIELD_FUNDING_ACCOUNT_ID), transaction_id=getattr(self, FIELD_TRANSACTION_ID))
    
    def _make_credit_receive_funds_command(self):
        return CreditReceiveFunds(funded_account_id=getattr(self, FIELD_FUNDED_ACCOUNT_ID), transaction_id=getattr(self, FIELD_TRANSACTION_ID))

    def _make_rollback_receive_funds_debit_command(self):
        return RollbackReceiveFundsDebit(funding_account_id=getattr(self, FIELD_FUNDING_ACCOUNT_ID), transaction_id=getattr(self, FIELD_TRANSACTION_ID))
    
