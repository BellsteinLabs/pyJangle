from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import List
from pyjangle import command_dispatcher_instance
from pyjangle import RegisterSaga
from pyjangle.event.register_event import RegisterEvent
from pyjangle_example.commands import *
from pyjangle_example.events import (
    RequestApproved,
    RequestDebitRolledBack,
    RequestRejected,
    RequestCreated,
)
from pyjangle import Event
from pyjangle import Saga, event_receiver, reconstitute_saga_state
from pyjangle.logging.logging import log
from pyjangle_example.events import TransferDebited

RETRY_TIMEOUT_LENGTH = timedelta(seconds=15)

FIELD_TRANSACTION_ID = "transaction_id"
FIELD_FUNDING_ACCOUNT_ID = "funding_account_id"
FIELD_FUNDED_ACCOUNT_ID = "funded_account_id"
FIELD_AMOUNT = "amount"
FIELD_TIMEOUT_AT = "timeout_at"


@RegisterEvent
class GetRequestApprovalCommandSucceeded(Event):
    pass


@RegisterEvent
class GetRequestApprovalCommandFailed(Event):
    pass


@RegisterEvent
class NotifyRequestRejectedCommandAcknowledged(Event):
    pass


@RegisterEvent
class DebitRequestCommandSucceeded(Event):
    pass


@RegisterEvent
class DebitRequestCommandFailed(Event):
    pass


@RegisterEvent
class CreditRequestCommandSucceeded(Event):
    pass


@RegisterEvent
class CreditRequestCommandFailed(Event):
    pass


@RegisterEvent
class RollbackRequestCommandAcknowledged(Event):
    pass


@RegisterSaga
class RequestSaga(Saga):
    @event_receiver(
        RequestCreated,
        skip_if_any_flags_set=[
            GetRequestApprovalCommandSucceeded,
            NotifyRequestRejectedCommandAcknowledged,
        ],
    )
    async def on_receive_funds_requested(self):
        await self._dispatch_command(
            command=self._make_try_obtain_receive_funds_approval_command,
            on_success_event=GetRequestApprovalCommandSucceeded,
            on_failure_event=GetRequestApprovalCommandFailed,
        )

        if self.flags_has_any(GetRequestApprovalCommandFailed):
            await self._dispatch_command(
                command=self._make_notify_receive_funds_rejected_command,
                on_success_event=NotifyRequestRejectedCommandAcknowledged,
                on_failure_event=NotifyRequestRejectedCommandAcknowledged,
            )
            self.set_complete()
            return

    @event_receiver(
        RequestApproved,
        skip_if_any_flags_set=[
            NotifyRequestRejectedCommandAcknowledged,
            CreditRequestCommandSucceeded,
            RollbackRequestCommandAcknowledged,
        ],
    )
    async def on_receive_funds_approved(self):
        await self._dispatch_command(
            command=self._make_debit_receive_funds_command,
            on_success_event=DebitRequestCommandSucceeded,
            on_failure_event=DebitRequestCommandFailed,
        )

        if self.flags_has_any(DebitRequestCommandFailed):
            await self._dispatch_command(
                command=self._make_notify_receive_funds_rejected_command,
                on_success_event=NotifyRequestRejectedCommandAcknowledged,
                on_failure_event=NotifyRequestRejectedCommandAcknowledged,
            )
            self.set_complete()
            return

        if self.flags_has_any(DebitRequestCommandSucceeded):
            await self._dispatch_command(
                command=self._make_credit_receive_funds_command,
                on_success_event=CreditRequestCommandSucceeded,
                on_failure_event=CreditRequestCommandFailed,
            )

        if self.flags_has_any(CreditRequestCommandSucceeded):
            self.set_complete()

        if self.flags_has_any(CreditRequestCommandFailed):
            await self._dispatch_command(
                command=self._make_rollback_receive_funds_debit_command,
                on_success_event=RollbackRequestCommandAcknowledged,
                on_failure_event=RollbackRequestCommandAcknowledged,
            )

        if self.flags_has_any(RollbackRequestCommandAcknowledged):
            self.set_complete()

    @event_receiver(
        RequestRejected,
        skip_if_any_flags_set=[NotifyRequestRejectedCommandAcknowledged],
    )
    async def on_receive_funds_rejected(self):
        await self._dispatch_command(
            command=self._make_notify_receive_funds_rejected_command,
            on_failure_event=NotifyRequestRejectedCommandAcknowledged,
            on_success_event=NotifyRequestRejectedCommandAcknowledged,
        )
        self.set_complete()

    @reconstitute_saga_state(RequestCreated)
    def handle_receive_funds_transfer_requested(self, event: RequestCreated):
        setattr(self, FIELD_TRANSACTION_ID, event.transaction_id)
        setattr(self, FIELD_FUNDING_ACCOUNT_ID, event.funding_account_id)
        setattr(self, FIELD_FUNDED_ACCOUNT_ID, event.funded_account_id)
        setattr(self, FIELD_AMOUNT, event.amount)
        setattr(self, FIELD_TIMEOUT_AT, event.timeout_at)
        self.set_timeout(event.timeout_at)

    @reconstitute_saga_state(RequestApproved)
    def handle_request_approved(self, event: RequestApproved):
        pass

    @reconstitute_saga_state(RequestRejected)
    def handler_receive_funds_transfer_rejected(self, event: RequestRejected):
        pass

    @reconstitute_saga_state(GetRequestApprovalCommandSucceeded)
    def from_try_obtain_receive_funds_approval_command_succeeded(
        self, event: GetRequestApprovalCommandSucceeded
    ):
        pass

    @reconstitute_saga_state(GetRequestApprovalCommandFailed)
    def from_try_obtain_receive_funds_approval_command_failed(
        self, event: GetRequestApprovalCommandFailed
    ):
        pass

    @reconstitute_saga_state(CreditRequestCommandFailed)
    def from_try_obtain_receive_funds_approval_command_received(
        self, event: CreditRequestCommandFailed
    ):
        pass

    @reconstitute_saga_state(NotifyRequestRejectedCommandAcknowledged)
    def from_notify_receive_funds_rejected_command_received(
        self, event: NotifyRequestRejectedCommandAcknowledged
    ):
        pass

    @reconstitute_saga_state(DebitRequestCommandSucceeded)
    def from_debit_receive_funds_command_received(
        self, event: DebitRequestCommandSucceeded
    ):
        pass

    @reconstitute_saga_state(CreditRequestCommandSucceeded)
    def from_credit_receive_funds_command_received(
        self, event: CreditRequestCommandSucceeded
    ):
        pass

    @reconstitute_saga_state(DebitRequestCommandFailed)
    def from_debit_receive_funds_command_failed(self, event: DebitRequestCommandFailed):
        pass

    @reconstitute_saga_state(RollbackRequestCommandAcknowledged)
    def from_rollback_receive_funds_debit_command_recived(
        self, event: RollbackRequestCommandAcknowledged
    ):
        pass

    def _make_try_obtain_receive_funds_approval_command(self):
        return GetRequestApproval(
            funding_account_id=getattr(self, FIELD_FUNDING_ACCOUNT_ID),
            funded_account_id=getattr(self, FIELD_FUNDED_ACCOUNT_ID),
            amount=getattr(self, FIELD_AMOUNT),
            transaction_id=getattr(self, FIELD_TRANSACTION_ID),
            timeout_at=getattr(self, FIELD_TIMEOUT_AT),
        )

    def _make_notify_receive_funds_rejected_command(self):
        return NotifyRequestRejected(
            funding_account_id=getattr(self, FIELD_FUNDING_ACCOUNT_ID),
            funded_account_id=getattr(self, FIELD_FUNDED_ACCOUNT_ID),
            transaction_id=getattr(self, FIELD_TRANSACTION_ID),
        )

    def _make_debit_receive_funds_command(self):
        return DebitRequest(
            funding_account_id=getattr(self, FIELD_FUNDING_ACCOUNT_ID),
            transaction_id=getattr(self, FIELD_TRANSACTION_ID),
        )

    def _make_credit_receive_funds_command(self):
        return CreditRequest(
            funded_account_id=getattr(self, FIELD_FUNDED_ACCOUNT_ID),
            transaction_id=getattr(self, FIELD_TRANSACTION_ID),
        )

    def _make_rollback_receive_funds_debit_command(self):
        return RollbackRequestDebit(
            funding_account_id=getattr(self, FIELD_FUNDING_ACCOUNT_ID),
            transaction_id=getattr(self, FIELD_TRANSACTION_ID),
        )


class CreditSendFundsSucceeded(Event):
    def deserialize(data: any) -> any:
        pass


class CreditSendFundsFailed(Event):
    def deserialize(data: any) -> any:
        pass
