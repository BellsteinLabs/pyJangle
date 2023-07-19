from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List
from pyjangle.command.command_dispatcher import command_dispatcher_instance
from pyjangle.command.command_response import CommandResponse
from pyjangle.event.event import *
from example_commands import *
from example_events import ReceiveFundsApproved, ReceiveFundsDebitedRolledBack, ReceiveFundsRejected, ReceiveFundsRequested
from pyjangle.event.event import Event
from pyjangle.saga.saga import Saga, event_receiver, reconstitute_saga_state

RETRY_TIMEOUT_LENGTH = timedelta(seconds=15)

FIELD_TRANSACTION_ID = "transaction_id"
FIELD_FUNDING_ACCOUNT_ID = "funding_account_id"
FIELD_FUNDED_ACCOUNT_ID = "funded_account_id"
FIELD_AMOUNT = "amount"
FIELD_TIMEOUT_AT = "timeout_at"

class TryObtainReceiveFundsApprovalCommandSucceeded(Event):
    pass

class NotifyReceiveFundsRejectedCommandSucceeded(Event):
    pass

class DebitReceiveFundsCommandSucceeded(Event):
    pass

class CreditReceiveFundsCommandSucceeded(Event):
    pass

class RollbackReceiveFundsDebitCommandSucceeded(Event):
    pass

class RequestFundsFromAnotherAccount(Saga):

    def __init__(self, events: List[Event], retry_at: datetime = None, timeout_at: datetime = None, is_complete: bool = False):
        super().__init__(events, retry_at, timeout_at, is_complete)
        self.command_dispatcher = command_dispatcher_instance()
            
    @event_receiver(ReceiveFundsRequested, skip_if_any_flags_set=[TryObtainReceiveFundsApprovalCommandSucceeded, NotifyReceiveFundsRejectedCommandSucceeded])
    def on_receive_funds_requested(self, next_version: int):
        request_transfer_approval_on_funding_account_command_response = None
        try:
            request_transfer_approval_on_funding_account_command_response = self.command_dispatcher(self._make_try_obtain_receive_funds_approval_command())
        except:
            self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
            return
        if request_transfer_approval_on_funding_account_command_response.is_success:
            self._post_new_event(TryObtainReceiveFundsApprovalCommandSucceeded(saga_id=self.saga_id, version=next_version))
            return
        else:
            try:
                self.command_dispatcher(self._make_notify_receive_funds_rejected_command())
                self._post_new_event(NotifyReceiveFundsRejectedCommandSucceeded(saga_id=self.saga_id, version=next_version))
                self.set_complete()
            except:
                self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                return
        
    @event_receiver(ReceiveFundsApproved)
    def on_receive_funds_approved(self, next_version: int):
        if not DebitReceiveFunds in self.flags:
            debit_transfer_funds_response = None
            try:
                debit_transfer_funds_response = self.command_dispatcher(self._make_debit_receive_funds_command())
            except:
                self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                return
            if debit_transfer_funds_response.is_success:
                self._post_new_event(DebitReceiveFundsCommandSucceeded(saga_id=self.saga_id, version=next_version))
            else:
                try:
                    self.command_dispatcher(self._make_notify_receive_funds_rejected_command())
                    self._post_new_event(NotifyReceiveFundsRejectedCommandSucceeded(saga_id=self.saga_id, version=next_version))
                    self.set_complete()
                except:
                    self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                    return
        if not CreditReceiveFunds in self.flags:
            credit_receive_transfer_funds_response = None
            try:
                credit_receive_transfer_funds_response = self.command_dispatcher(self._make_credit_receive_funds_command())
            except:
                self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                return
            if credit_receive_transfer_funds_response.is_success:
                self._post_new_event(CreditReceiveFundsCommandSucceeded(saga_id=self.saga_id, version=next_version + 1 if DebitReceiveFunds in self.flags else next_version))
                self.set_complete()
            else:
                try:
                    self.command_dispatcher(self._make_rollback_receive_funds_debit_command())
                    self._post_new_event(RollbackReceiveFundsDebitCommandSucceeded(saga_id=self.saga_id, version=next_version + 1 if DebitReceiveFunds in self.flags else next_version))
                    self.set_complete()
                except:
                    self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                    return

    @event_receiver(ReceiveFundsRejected, skip_if_any_flags_set=[NotifyReceiveFundsRejectedCommandSucceeded])
    def on_receive_funds_rejected(self, next_version: int):
        if not NotifyReceiveFundsRejected in self.flags:
            try:
                self.command_dispatcher(self._make_notify_receive_funds_rejected_command())
                self._post_new_event(NotifyReceiveFundsRejectedCommandSucceeded(saga_id=self.saga_id, version=next_version))
                self.set_complete()
            except:
                self.set_retry(datetime.now + RETRY_TIMEOUT_LENGTH)
                return   

    @reconstitute_saga_state(ReceiveFundsRequested)                
    def handle_receive_funds_transfer_requested(self, event: ReceiveFundsRequested):
        setattr(self, FIELD_TRANSACTION_ID, event.transaction_id)
        setattr(self, FIELD_FUNDING_ACCOUNT_ID, event.funding_account_id)
        setattr(self, FIELD_FUNDED_ACCOUNT_ID, event.funded_account_id)
        setattr(self, FIELD_AMOUNT, event.amount)
        setattr(self, FIELD_TIMEOUT_AT, event.timeout_at)
        self.timeout_at = event.timeout_at

    @reconstitute_saga_state(ReceiveFundsApproved)
    def handle_receive_funds_transfer_approved(self, event: ReceiveFundsApproved):
        pass

    @reconstitute_saga_state(ReceiveFundsRejected)
    def handler_receive_funds_transfer_rejected(self, event: ReceiveFundsRejected):
        pass

    @reconstitute_saga_state(TryObtainReceiveFundsApprovalCommandSucceeded)
    def from_try_obtain_receive_funds_approval_command_received(self, event: TryObtainReceiveFundsApprovalCommandSucceeded):
        pass

    @reconstitute_saga_state(NotifyReceiveFundsRejectedCommandSucceeded)
    def from_notify_receive_funds_rejected_command_received(self, event: NotifyReceiveFundsRejectedCommandSucceeded):
        pass

    @reconstitute_saga_state(DebitReceiveFundsCommandSucceeded)
    def from_debit_receive_funds_command_received(self, event: DebitReceiveFundsCommandSucceeded):
        pass

    @reconstitute_saga_state(CreditReceiveFundsCommandSucceeded)
    def from_credit_receive_funds_command_received(self, event: CreditReceiveFundsCommandSucceeded):
        pass

    @reconstitute_saga_state(RollbackReceiveFundsDebitCommandSucceeded)
    def from_rollback_receive_funds_debit_command_recived(self, event: RollbackReceiveFundsDebitCommandSucceeded):
        pass
        
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
    
