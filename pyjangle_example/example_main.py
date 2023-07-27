import logging
import sqlite3
import uuid
from typing import Callable
from asyncio import Queue, create_task, wait, wait_for, run
from pyjangle.command.command_dispatcher import RegisterCommandDispatcher, command_dispatcher_instance
from pyjangle.saga.saga_daemon import begin_retry_sagas_loop

import pyjangle_example.example_account_aggregate
import pyjangle_example.example_account_creation_aggregate
from pyjangle.event.event_dispatcher import begin_processing_committed_events
from pyjangle.event.event_handler import handle_event, has_registered_event_handler
from pyjangle.event.event_repository import RegisterEventRepository
from pyjangle.logging import MESSAGE
from pyjangle.saga.saga_repository import RegisterSagaRepository
from pyjangle.test.transient_saga_repository import TransientSagaRepository
from pyjangle_example.example_commands import CreateAccount, RequestForgiveness, SendFunds, WithdrawFunds

from pyjangle import VersionedEvent, RegisterEventDispatcher, handle_command, tasks
from pyjangle.test.transient_event_repository import TransientEventRepository
from pyjangle_example.example_commands import DepositFunds
from pyjangle_example.example_events import AccountCreated, AccountIdProvisioned, DebtForgiven, FundsDeposited, FundsWithdrawn, SendFundsDebited
import pyjangle_example.example_event_handlers
from pyjangle_json.logging import initialize_jangle_logging

event_queue = Queue(maxsize=1)


async def main():
    tasks.background_tasks.append(create_task(
        begin_processing_committed_events()))
    tasks.background_tasks.append(create_task(begin_retry_sagas_loop(3)))

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

    response = await command_dispatcher(CreateAccount(name=account_names[1], initial_deposit=100))
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

    response = await command_dispatcher(DepositFunds(account_id=account_ids[0], amount=50.50))
    event: FundsDeposited = await _dequeue_event()
    assert isinstance(event, FundsDeposited)
    assert int(event.account_id) == 1
    assert event.amount == 50.50

    response = await command_dispatcher(WithdrawFunds(account_id=account_ids[1], amount=250))
    assert not response.is_success
    assert response.data == "Insufficient funds"

    response = await command_dispatcher(WithdrawFunds(account_id=account_ids[1], amount=150))
    assert response.is_success
    event: FundsWithdrawn = await _dequeue_event()
    assert isinstance(event, FundsWithdrawn)
    assert int(event.amount) == 150
    assert event.account_id == account_ids[1]

    response = await command_dispatcher(RequestForgiveness(account_id=account_ids[1]))
    assert response.is_success
    event: DebtForgiven = await _dequeue_event()
    assert isinstance(event, DebtForgiven)
    assert event.account_id == account_ids[1]

    response = await command_dispatcher(SendFunds(funded_account_id="None", funding_account_id=account_ids[0], amount=20))
    assert response.is_success
    event: SendFundsDebited = await _dequeue_event()
    assert isinstance(event, SendFundsDebited)
    assert event.amount == 20
    assert event.funding_account_id == account_ids[0]
    assert event.funded_account_id == "None"

    # response = await command_dispatcher()

    # TODO Receive Funds Request

    # TODO Account deletion

    await wait(tasks.background_tasks)

    print("Done!")


async def _dequeue_event():
    global event_queue
    return await wait_for(event_queue.get(), 1)


async def receive_event(event: VersionedEvent):
    if has_registered_event_handler(type(event)):
        await handle_event(event)

    global event_queue
    await event_queue.put(event)

initialize_jangle_logging(MESSAGE)
RegisterEventRepository(TransientEventRepository)
RegisterSagaRepository(TransientSagaRepository)
RegisterEventDispatcher(receive_event)
RegisterCommandDispatcher(handle_command)
# TODO: Register defaults
run(main())
