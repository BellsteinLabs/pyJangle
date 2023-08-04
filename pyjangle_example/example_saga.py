from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import List
from pyjangle import command_dispatcher_instance
from pyjangle_example.example_commands import *
from pyjangle_example.example_events import ReceiveFundsApproved, ReceiveFundsDebitedRolledBack, ReceiveFundsRejected, ReceiveFundsRequested
from pyjangle import Event
from pyjangle import Saga, event_receiver, reconstitute_saga_state
from pyjangle.logging.logging import log
from pyjangle_example.example_events import SendFundsDebited

RETRY_TIMEOUT_LENGTH = timedelta(seconds=15)

FIELD_TRANSACTION_ID = "transaction_id"
FIELD_FUNDING_ACCOUNT_ID = "funding_account_id"
FIELD_FUNDED_ACCOUNT_ID = "funded_account_id"
FIELD_AMOUNT = "amount"
FIELD_TIMEOUT_AT = "timeout_at"


class TryObtainReceiveFundsApprovalCommandSucceeded(Event):
    def deserialize(data: any) -> any:
        pass


class TryObtainReceiveFundsApprovalCommandFailed(Event):
    def deserialize(data: any) -> any:
        pass


class NotifyReceiveFundsRejectedCommandAcknowledged(Event):
    def deserialize(data: any) -> any:
        pass


class DebitReceiveFundsCommandSucceeded(Event):
    def deserialize(data: any) -> any:
        pass


class DebitReceiveFundsCommandFailed(Event):
    def deserialize(data: any) -> any:
        pass


class CreditReceiveFundsCommandSucceeded(Event):
    def deserialize(data: any) -> any:
        pass


class CreditReceiveFundsCommandFailed(Event):
    def deserialize(data: any) -> any:
        pass


class RollbackReceiveFundsDebitCommandAcknowledged(Event):
    def deserialize(data: any) -> any:
        pass


class RequestFundsFromAnotherAccount(Saga):

    @event_receiver(ReceiveFundsRequested, skip_if_any_flags_set=[TryObtainReceiveFundsApprovalCommandSucceeded, NotifyReceiveFundsRejectedCommandAcknowledged])
    async def on_receive_funds_requested(self):
        await self.dispatch_command(
            command=self._make_try_obtain_receive_funds_approval_command,
            on_success_event=TryObtainReceiveFundsApprovalCommandSucceeded,
            on_failure_event=TryObtainReceiveFundsApprovalCommandFailed)

        if self.flags_has_any(TryObtainReceiveFundsApprovalCommandFailed):
            await self.dispatch_command(
                command=self._make_notify_receive_funds_rejected_command,
                on_success_event=NotifyReceiveFundsRejectedCommandAcknowledged,
                on_failure_event=NotifyReceiveFundsRejectedCommandAcknowledged)
            self.set_complete()
            return

    @event_receiver(ReceiveFundsApproved, skip_if_any_flags_set=[NotifyReceiveFundsRejectedCommandAcknowledged, CreditReceiveFundsCommandSucceeded, RollbackReceiveFundsDebitCommandAcknowledged])
    async def on_receive_funds_approved(self):
        await self.dispatch_command(
            command=self._make_debit_receive_funds_command,
            on_success_event=DebitReceiveFundsCommandSucceeded,
            on_failure_event=DebitReceiveFundsCommandFailed
        )

        if self.flags_has_any(DebitReceiveFundsCommandFailed):
            await self.dispatch_command(
                command=self._make_notify_receive_funds_rejected_command,
                on_success_event=NotifyReceiveFundsRejectedCommandAcknowledged,
                on_failure_event=NotifyReceiveFundsRejectedCommandAcknowledged
            )
            self.set_complete()
            return

        if self.flags_has_any(DebitReceiveFundsCommandSucceeded):
            await self.dispatch_command(
                command=self._make_credit_receive_funds_command,
                on_success_event=CreditReceiveFundsCommandSucceeded,
                on_failure_event=CreditReceiveFundsCommandFailed,
            )

        if self.flags_has_any(CreditReceiveFundsCommandSucceeded):
            self.set_complete()

        if self.flags_has_any(CreditReceiveFundsCommandFailed):
            await self.dispatch_command(
                command=self._make_rollback_receive_funds_debit_command,
                on_success_event=RollbackReceiveFundsDebitCommandAcknowledged,
                on_failure_event=RollbackReceiveFundsDebitCommandAcknowledged
            )

        if self.flags_has_any(RollbackReceiveFundsDebitCommandAcknowledged):
            self.set_complete()

    @event_receiver(ReceiveFundsRejected, skip_if_any_flags_set=[NotifyReceiveFundsRejectedCommandAcknowledged])
    async def on_receive_funds_rejected(self):
        await self.dispatch_command(
            command=self._make_notify_receive_funds_rejected_command,
            on_failure_event=NotifyReceiveFundsRejectedCommandAcknowledged,
            on_success_event=NotifyReceiveFundsRejectedCommandAcknowledged
        )
        self.set_complete()

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
    def from_try_obtain_receive_funds_approval_command_succeeded(self, event: TryObtainReceiveFundsApprovalCommandSucceeded):
        pass

    @reconstitute_saga_state(TryObtainReceiveFundsApprovalCommandFailed)
    def from_try_obtain_receive_funds_approval_command_failed(self, event: TryObtainReceiveFundsApprovalCommandFailed):
        pass

    @reconstitute_saga_state(CreditReceiveFundsCommandFailed)
    def from_try_obtain_receive_funds_approval_command_received(self, event: CreditReceiveFundsCommandFailed):
        pass

    @reconstitute_saga_state(NotifyReceiveFundsRejectedCommandAcknowledged)
    def from_notify_receive_funds_rejected_command_received(self, event: NotifyReceiveFundsRejectedCommandAcknowledged):
        pass

    @reconstitute_saga_state(DebitReceiveFundsCommandSucceeded)
    def from_debit_receive_funds_command_received(self, event: DebitReceiveFundsCommandSucceeded):
        pass

    @reconstitute_saga_state(CreditReceiveFundsCommandSucceeded)
    def from_credit_receive_funds_command_received(self, event: CreditReceiveFundsCommandSucceeded):
        pass

    @reconstitute_saga_state(DebitReceiveFundsCommandFailed)
    def from_debit_receive_funds_command_failed(self, event: DebitReceiveFundsCommandFailed):
        pass

    @reconstitute_saga_state(RollbackReceiveFundsDebitCommandAcknowledged)
    def from_rollback_receive_funds_debit_command_recived(self, event: RollbackReceiveFundsDebitCommandAcknowledged):
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


class CreditSendFundsSucceeded(Event):
    def deserialize(data: any) -> any:
        pass


class CreditSendFundsFailed(Event):
    def deserialize(data: any) -> any:
        pass


class SendFundsToAnotherAccountSaga(Saga):

    @event_receiver(SendFundsDebited)
    async def on_send_funds_debited(self):
        await self.dispatch_command(
            command=self._make_credit_send_funds_command,
            on_success_event=CreditSendFundsSucceeded,
            on_failure_event=CreditSendFundsFailed)
        if self.flags_has_any(CreditSendFundsFailed):
            await self.dispatch_command(self._make_rollback_send_funds_debit_command)
        self.set_complete()

    @reconstitute_saga_state(SendFundsDebited)
    def from_send_funds_debited(self, event: SendFundsDebited):
        self.amount = event.amount
        self.funded_account_id = event.funded_account_id
        self.funding_account_id = event.funding_account_id
        self.transaction_id = event.transaction_id

    @reconstitute_saga_state(CreditSendFundsSucceeded)
    def from_credit_send_funds_succeeded(self, event: CreditSendFundsSucceeded):
        pass

    @reconstitute_saga_state(CreditSendFundsFailed)
    def from_credit_send_funds_failed(self, event: CreditSendFundsFailed):
        pass

    def _make_credit_send_funds_command(self):
        return CreditSendFunds(funding_account_id=self.funding_account_id,
                               funded_account_id=self.funded_account_id, amount=self.amount, transaction_id=self.transaction_id)

    def _make_rollback_send_funds_debit_command(self):
        return RollbackSendFundsDebit(
            funding_account_id=self.funding_account_id, amount=self.amount, transaction_id=self.transaction_id)
