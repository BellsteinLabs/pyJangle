from decimal import Decimal
import os
import sys

PROJECT_PATH = os.getcwd()
SOURCE_PATH = os.path.join(PROJECT_PATH, "src")
sys.path.append(SOURCE_PATH)
print(PROJECT_PATH + SOURCE_PATH)

from asyncio import Queue, wait, wait_for, run
from pyjangle.command.command_dispatcher import (
    register_command_dispatcher,
    command_dispatcher_instance,
)
from pyjangle.command.command_response import CommandResponse
from pyjangle import background_tasks
from pyjangle.saga.saga_daemon import begin_retry_sagas_loop

import aggregates.account_aggregate
import aggregates.account_creation_aggregate
from pyjangle import initialize_pyjangle, init_background_tasks
from pyjangle import begin_processing_committed_events
from pyjangle import default_event_dispatcher, has_registered_event_handler
from pyjangle import RegisterEventRepository
from pyjangle.logging import MESSAGE
from pyjangle import RegisterSagaRepository
from pyjangle.saga.in_memory_transient_saga_repository import InMemorySagaRepository
from commands import (
    AcceptRequest,
    CreateAccount,
    DeleteAccount,
    Request,
    RejectRequest,
    ForgiveDebt,
    Transfer,
    WithdrawFunds,
)

from pyjangle import VersionedEvent, register_event_dispatcher, handle_command
from pyjangle.event.in_memory_event_repository import InMemoryEventRepository
from commands import DepositFunds
from events import (
    AccountCreated,
    AccountDeleted,
    AccountIdProvisioned,
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
import event_handlers

from pyjangle_json_logging import initialize_logging

event_queue = Queue(maxsize=1)


async def main():
    init_background_tasks(saga_retry_interval_seconds=3)

    global event_queue
    account_ids = []
    account_names = ["HSPBC", "Natalie"]
    command_dispatcher = command_dispatcher_instance()

    response = await command_dispatcher(CreateAccount(name=account_names[0]))
    assert response.is_success
    assert int(response.data) == 1
    account_ids.append(response.data)
    event: AccountIdProvisioned = await _dequeue_event()
    assert isinstance(event, AccountIdProvisioned)
    event: AccountCreated = await _dequeue_event()
    assert isinstance(event, AccountCreated)
    assert int(event.account_id) == 1
    assert event.name == account_names[0]

    response = await command_dispatcher(
        CreateAccount(name=account_names[1], initial_deposit=Decimal(100))
    )
    assert response.is_success
    assert int(response.data) == 2
    account_ids.append(response.data)
    event: AccountIdProvisioned = await _dequeue_event()
    assert isinstance(event, AccountIdProvisioned)
    event: AccountCreated = await _dequeue_event()
    assert isinstance(event, AccountCreated)
    assert int(event.account_id) == 2
    assert event.name == account_names[1]
    event: FundsDeposited = await _dequeue_event()
    assert isinstance(event, FundsDeposited)
    assert int(event.account_id) == 2
    assert event.amount == 100

    response = await command_dispatcher(
        DepositFunds(account_id=account_ids[0], amount=Decimal(50.50))
    )
    event: FundsDeposited = await _dequeue_event()
    assert isinstance(event, FundsDeposited)
    assert int(event.account_id) == 1
    assert event.amount == 50.50

    response = await command_dispatcher(
        WithdrawFunds(account_id=account_ids[1], amount=Decimal(250))
    )
    assert not response.is_success
    assert response.data == "Insufficient funds"

    response = await command_dispatcher(
        WithdrawFunds(account_id=account_ids[1], amount=Decimal(150))
    )
    assert response.is_success
    event: FundsWithdrawn = await _dequeue_event()
    assert isinstance(event, FundsWithdrawn)
    assert int(event.amount) == 150
    assert event.account_id == account_ids[1]

    response = await command_dispatcher(ForgiveDebt(account_id=account_ids[1]))
    assert response.is_success
    event: DebtForgiven = await _dequeue_event()
    assert isinstance(event, DebtForgiven)
    assert event.account_id == account_ids[1]

    response = await command_dispatcher(
        Transfer(
            funded_account_id="SOMEID",
            funding_account_id=account_ids[0],
            amount=Decimal(20),
        )
    )
    assert response.is_success
    event: TransferDebited = await _dequeue_event()
    assert isinstance(event, TransferDebited)
    assert event.amount == 20
    assert event.funding_account_id == account_ids[0]
    assert event.funded_account_id == "SOMEID"
    event: TransferDebitRolledBack = await _dequeue_event()
    assert isinstance(event, TransferDebitRolledBack)
    assert event.amount == 20
    assert event.funding_account_id == account_ids[0]

    response = await command_dispatcher(
        Transfer(
            funded_account_id=account_ids[1],
            funding_account_id=account_ids[0],
            amount=Decimal(19),
        )
    )
    assert response.is_success
    event: TransferDebited = await _dequeue_event()
    assert isinstance(event, TransferDebited)
    assert event.amount == 19
    assert event.funding_account_id == account_ids[0]
    assert event.funded_account_id == account_ids[1]
    event: TransferCredited = await _dequeue_event()
    assert isinstance(event, TransferCredited)
    assert event.amount == 19
    assert event.funding_account_id == account_ids[0]
    assert event.funded_account_id == account_ids[1]

    response = await command_dispatcher(
        Request(
            funded_account_id=account_ids[0],
            funding_account_id=account_ids[1],
            amount=Decimal(15.43),
        )
    )
    assert response.is_success
    event: RequestCreated = await _dequeue_event()
    assert isinstance(event, RequestCreated)
    assert event.amount == 15.43
    assert event.funded_account_id == account_ids[0]
    assert event.funding_account_id == account_ids[1]
    event: RequestReceived = await _dequeue_event()
    assert isinstance(event, RequestReceived)
    assert event.amount == 15.43
    assert event.funded_account_id == account_ids[0]
    assert event.funding_account_id == account_ids[1]

    response = await command_dispatcher(
        AcceptRequest(
            funding_account_id=event.funding_account_id, transaction_id="FAKE_ID"
        )
    )
    assert not response.is_success
    response = await command_dispatcher(
        AcceptRequest(
            funding_account_id=event.funding_account_id,
            transaction_id=event.transaction_id,
        )
    )
    assert response.is_success
    event: RequestApproved = await _dequeue_event()
    assert isinstance(event, RequestApproved)
    assert event.funding_account_id == account_ids[1]
    event: RequestDebited = await _dequeue_event()
    assert isinstance(event, RequestDebited)
    assert event.funding_account_id == account_ids[1]
    assert event.funding_account_id == account_ids[1]
    event: RequestCredited = await _dequeue_event()
    assert isinstance(event, RequestCredited)
    assert event.funded_account_id == account_ids[0]

    response = await command_dispatcher(
        Request(
            funded_account_id=account_ids[1],
            funding_account_id=account_ids[0],
            amount=Decimal(1000),
        )
    )
    assert response.is_success
    event: RequestCreated = await _dequeue_event()
    assert isinstance(event, RequestCreated)
    assert event.amount == 1000
    assert event.funded_account_id == account_ids[1]
    assert event.funding_account_id == account_ids[0]
    event: RequestReceived = await _dequeue_event()
    assert isinstance(event, RequestReceived)
    assert event.amount == 1000
    assert event.funded_account_id == account_ids[1]
    assert event.funding_account_id == account_ids[0]

    response = await command_dispatcher(
        AcceptRequest(
            funding_account_id=event.funding_account_id,
            transaction_id=event.transaction_id,
        )
    )
    assert response.is_success
    event: RequestApproved = await _dequeue_event()
    assert isinstance(event, RequestApproved)
    assert event.funding_account_id == account_ids[0]
    event: RequestRejectionReceived = await _dequeue_event()
    assert isinstance(event, RequestRejectionReceived)
    assert event.funded_account_id == account_ids[1]

    response = await command_dispatcher(
        Request(
            funded_account_id=account_ids[1],
            funding_account_id=account_ids[0],
            amount=Decimal(1000),
        )
    )
    assert response.is_success
    event: RequestCreated = await _dequeue_event()
    assert isinstance(event, RequestCreated)
    assert event.amount == 1000
    assert event.funded_account_id == account_ids[1]
    assert event.funding_account_id == account_ids[0]
    event: RequestReceived = await _dequeue_event()
    assert isinstance(event, RequestReceived)
    assert event.amount == 1000
    assert event.funded_account_id == account_ids[1]
    assert event.funding_account_id == account_ids[0]

    response = await command_dispatcher(
        RejectRequest(
            funding_account_id=event.funding_account_id,
            transaction_id=event.transaction_id,
        )
    )
    assert response.is_success
    event: RequestRejected = await _dequeue_event()
    assert isinstance(event, RequestRejected)
    assert event.funding_account_id == account_ids[0]
    event: RequestRejectionReceived = await _dequeue_event()
    assert isinstance(event, RequestRejectionReceived)
    assert event.funded_account_id == account_ids[1]

    # Receive funds but the credit fails
    response = await command_dispatcher(
        Request(
            funded_account_id=account_ids[0],
            funding_account_id=account_ids[1],
            amount=Decimal(15.43),
        )
    )
    assert response.is_success
    event: RequestCreated = await _dequeue_event()
    assert isinstance(event, RequestCreated)
    assert event.amount == 15.43
    assert event.funded_account_id == account_ids[0]
    assert event.funding_account_id == account_ids[1]
    event: RequestReceived = await _dequeue_event()
    assert isinstance(event, RequestReceived)
    assert event.amount == 15.43
    assert event.funded_account_id == account_ids[0]
    assert event.funding_account_id == account_ids[1]

    response: CommandResponse = await command_dispatcher(
        DeleteAccount(account_id=account_ids[0])
    )
    assert response.is_success
    deleted_event: AccountDeleted = await _dequeue_event()
    assert isinstance(deleted_event, AccountDeleted)
    assert deleted_event.account_id == account_ids[0]

    response = await command_dispatcher(
        AcceptRequest(
            funding_account_id=account_ids[1], transaction_id=event.transaction_id
        )
    )
    assert response.is_success
    event: RequestApproved = await _dequeue_event()
    assert isinstance(event, RequestApproved)
    assert event.funding_account_id == account_ids[1]
    event: RequestDebited = await _dequeue_event()
    assert isinstance(event, RequestDebited)
    assert event.funding_account_id == account_ids[1]
    assert event.funding_account_id == account_ids[1]
    event: RequestDebitRolledBack = await _dequeue_event()
    assert isinstance(event, RequestDebitRolledBack)
    assert event.funding_account_id == account_ids[1]

    print("Done!")


async def _dequeue_event():
    global event_queue
    return await wait_for(event_queue.get(), 1)


async def dispatch_event(event: VersionedEvent, completed_callback):
    global event_queue
    await event_queue.put(event)

    if has_registered_event_handler(type(event)):
        await default_event_dispatcher(event, completed_callback)


initialize_pyjangle(event_dispatcher_func=dispatch_event)
initialize_logging(MESSAGE)
run(main())
