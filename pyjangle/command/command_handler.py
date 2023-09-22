from pyjangle import (
    Command,
    CommandResponse,
    DuplicateKeyError,
    VersionedEvent,
    LogToggles,
    Snapshottable,
    Aggregate,
    JangleError,
    log,
    snapshot_repository_instance,
    command_to_aggregate_map_instance,
    event_repository_instance,
    event_dispatcher_instance,
    enqueue_committed_event_for_dispatch,
    ERROR,
)


class CommandHandlerError(JangleError):
    "Unexpected error while handling command."
    pass


async def handle_command(command: Command) -> CommandResponse:
    """Orchestrates command processing.

    Processing a command involves the following steps:
    - Map the command to an aggregate
    - Instantiate a blank aggregate
    - Retrieve and apply aggregate snapshot, if applicable
    - Retrieve and apply events from event store to reconstitute the aggregate state
    - Validate the command
    - If command validation succeeds, commit new events to the event store
    - Create an updated snapshot, if applicable
    - If an event dispatcher is registered, dispatch the new events

    This method also handles the optimistic concurrency mechanism that handles the case
    where two aggregates are instantiated at roughly the same time resulting in events
    with identical aggregate IDs and version numbers being committed at the same time.
    When a primary key violation is detected, this method will re-apply the command to
    the aggregate based on the new events from the competing aggregate instance.

    Args:
        command:
            The command to process.

    Returns:
        A `CommandResponse` containing the result of the command processing.

    Raises:
        CommandHandlerError:
            Unexpected error while handling command.
    """

    try:
        aggregate_id = command.get_aggregate_id()
        log(
            LogToggles.command_received,
            "Command received",
            {
                "aggregate_id": aggregate_id,
                "command_type": str(type(command)),
                "command_data": vars(command),
            },
        )
        while True:
            # Instantiate blank aggregate
            aggregate = command_to_aggregate_map_instance()[type(command)](
                id=aggregate_id
            )
            log(
                LogToggles.aggregate_created,
                "Blank aggregate created",
                {"aggregate_id": aggregate_id, "aggregate_type": str(type(aggregate))},
            )
            aggregate = await _apply_snapshotting_to_aggregate(aggregate, command)
            event_repository = event_repository_instance()
            # Get events between snapshot and current
            events = list(
                await event_repository.get_events(aggregate_id, aggregate.version)
            )
            log(
                LogToggles.retrieved_aggregate_events,
                "Retrieved aggregate events",
                {
                    "aggregate_id": aggregate_id,
                    "aggregate_type": str(type(aggregate)),
                    "event_count": len(events),
                },
            )
            aggregate.apply_events(events)
            command_response = aggregate.validate(command)
            if command_response.is_success:
                try:
                    await event_repository.commit_events(aggregate.new_events)
                    for id, event in aggregate.new_events:
                        log(
                            LogToggles.committed_event,
                            "Event committed",
                            {
                                "aggregate_type": str(type(aggregate)),
                                "aggregate_id": id,
                                "event_type": str(type(event)),
                                "event": vars(event),
                            },
                        )
                    await _record_new_snapshot_if_applicable(aggregate_id, aggregate)
                    await _dispatch_events_locally(
                        [event for (_, event) in aggregate.new_events]
                    )
                except DuplicateKeyError:
                    continue
            return command_response
    except Exception as e:
        raise CommandHandlerError("Error while handling command") from e


async def _apply_snapshotting_to_aggregate(
    aggregate: Snapshottable, command: Command
) -> Aggregate:
    """Applies a snapshot to an aggregate if applicable.

    If a snapshot is applicable, found, and successfully applied, the aggregate will be
    updated with the state from the snapshot, and its version will be updated.  If an
    error is encountered while applying the snapshot, the aggregate will be
    recreated to remove any erroneous state from a partially applied snapshot, and the
    snapshot will be deleted.

    Args:
        aggregate:
            The aggregate to which the snapshot may be applied.
        command:
            The command currently being applied to the aggregate.
    """

    is_snapshotting = _is_snapshotting(aggregate)

    log(
        LogToggles.is_snapshotting,
        f"Snapshotting status",
        {
            "enabled": is_snapshotting,
            "aggregate_type": str(type(aggregate)),
            "aggregate_id": aggregate.id,
        },
    )

    if not is_snapshotting:
        return aggregate
    snapshot_repo = snapshot_repository_instance()
    snapshot_tuple = await snapshot_repo.get_snapshot(command.get_aggregate_id())
    version = snapshot_tuple[0] if snapshot_tuple else None
    snapshot = snapshot_tuple[1] if snapshot_tuple else None
    log(
        LogToggles.is_snapshot_found,
        f"Snapshot was {'found' if version else 'not found'}.",
        {
            "aggregate_id": aggregate.id,
            "aggregate_type": str(type(aggregate)),
            "version": version,
        },
    )
    if version and snapshot:
        # Found a snapshot
        try:
            aggregate.apply_snapshot(version, snapshot)
            log(
                LogToggles.snapshot_applied,
                f"Snapshot applied",
                {
                    "aggregate_id": aggregate.id,
                    "aggregate_type": str(type(aggregate)),
                    "version": version,
                },
            )
        except Exception as e:
            # A code change in the aggregate probably caused this snapshot to become
            # outdated, most likely.  The snapshot will be deleted.
            log(
                LogToggles.snapshot_application_failed,
                msg="Snapshot application failed",
                exc_info=e,
            )
            # Reset the aggregate to a pristine state just in case the snapshot was
            # partially applied.
            aggregate = command_to_aggregate_map_instance()[type(command)](aggregate.id)
            await snapshot_repo.delete_snapshot(command.get_aggregate_id())
            log(
                LogToggles.snapshot_deleted,
                "Deleted snapshot",
                {"aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate))},
            )
            return aggregate
    return aggregate


async def _record_new_snapshot_if_applicable(aggregate_id: any, aggregate: Aggregate):
    """Periodically creates and stores a new snapshot.

    A `Snapshottable` aggregate specifies an interval at which a snapshot should be
    created.  For example, if a call to
    `snapshottable_aggregate.get_snapshot_frequency()` yields 5, a new snapshot of the
    aggregate will be stored every 5 events.

    Args:
        aggregate_id:
            Aggregate ID of the `aggregate` arg.
        aggregate:
            The aggregate to snapshot.
    """

    if not _is_snapshotting(aggregate):
        return

    updated_version = aggregate.version + len(aggregate.new_events)
    snapshotable: Snapshottable = aggregate

    if updated_version % snapshotable.get_snapshot_frequency() == 0:
        # BEFORE a snapshot is created, it's important to apply the new
        # events that were created from the command validators.  Normally
        # these events are NOT applied until the next time the aggregate
        # is instantiated!
        aggregate.apply_events(
            [
                event
                for (current_aggregate_id, event) in aggregate.new_events
                if current_aggregate_id == aggregate_id
            ]
        )
        await snapshot_repository_instance().store_snapshot(
            aggregate_id, aggregate.version, snapshotable.get_snapshot()
        )
        log(
            LogToggles.snapshot_taken,
            "Snapshot recorded",
            {
                "aggregate_id": aggregate.id,
                "aggregate_type": str(type(aggregate)),
                "version": updated_version,
            },
        )
    else:
        log(
            LogToggles.snapshot_not_needed,
            "Snapshot not needed",
            {
                "aggregate_id": aggregate.id,
                "aggregate_type": str(type(aggregate)),
                "version": updated_version,
            },
        )


def _is_snapshotting(aggregate: Aggregate) -> bool:
    """Determines if snapshotting is turned on for `aggregate`."""
    return (
        isinstance(aggregate, Snapshottable) and aggregate.get_snapshot_frequency() > 0
    )


async def _dispatch_events_locally(events: list[VersionedEvent]):
    "Dispatches events to a queue that is monitored by the registered event dispatcher."

    if not event_dispatcher_instance():
        return

    for event in events:
        await enqueue_committed_event_for_dispatch(event)
        log(
            LogToggles.queued_event_for_local_dispatch,
            "Events queued for local dispatch.",
            {
                "events": [
                    {"event_type": str(type(e)), "event_data": vars(e)}
                    for e in events
                ]
            },
        )
