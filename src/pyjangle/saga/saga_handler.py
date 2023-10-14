from pyjangle import (
    DuplicateKeyError,
    VersionedEvent,
    LogToggles,
    log,
    Saga,
    saga_repository_instance,
    SagaNotFoundError,
)


async def handle_saga_event(
    saga_id: any, event: VersionedEvent, saga_type: type[Saga] | None
):
    """Updates a saga's state with an event.

    This function does the following:
    - Retrieve the saga with id `saga_id` from the registered saga repository.
    - Returns if the saga is completed or timed out.
    - Evaluates the event against the saga.
    - If the saga is updated, commit the changes.

    Args:
        saga_id:
            ID of the saga to evaluate `event` against.
        event:
            The event that is updating the saga's state.
        saga_type:
            The type of the saga with `saga_id`.

    Raises:
        SagaNotFoundError:
            Saga with specified id not found.
    """
    saga_repository = saga_repository_instance()
    saga = await saga_repository.get_saga(saga_id)
    if not saga and not event:
        raise SagaNotFoundError(
            f"Tried to restore non-existant saga with id '{saga_id}' and apply no events to it."
        )
    if saga:
        log(
            LogToggles.saga_retrieved,
            "Retrieved saga",
            {"saga_id": saga_id, "saga": vars(saga)},
        )
    else:
        log(
            LogToggles.saga_new,
            "Received first event in a new saga",
            {"saga_id": saga_id},
        )
        saga = saga_type(saga_id=saga_id)
    if saga and (saga.is_complete or saga.is_timed_out):
        return
    await saga.evaluate(event)
    log(
        LogToggles.apply_event_to_saga,
        "Applied event to saga",
        {
            "saga_id": saga_id,
            "saga_type": str(type(saga)),
            "saga": vars(saga),
            "event": vars(event),
        },
    )
    if saga.is_dirty:
        try:
            await saga_repository.commit_saga(saga)
        except DuplicateKeyError as e:
            log(
                LogToggles.saga_duplicate_key,
                "Concurrent saga execution detected.  This is unlikely and could indicate an issue.",
                {
                    "saga_id": saga_id,
                    "saga_type": str(type(saga)),
                    "saga": vars(saga),
                    "event": vars(event),
                },
            )
            return
        log(
            LogToggles.saga_committed,
            "Committed saga to saga store.",
            {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": vars(saga)},
        )
    else:
        log(
            LogToggles.saga_nothing_happened,
            "Saga state was not changed.",
            {"saga_id": saga_id, "saga_type": str(type(saga)), "saga": vars(saga)},
        )
