from __future__ import annotations
from pyjangle.aggregate.aggregate import Aggregate
from pyjangle import Command
from pyjangle import CommandResponse


from pyjangle import command_to_aggregate_map_instance
from pyjangle.event.event import Event
from pyjangle.event.event_dispatcher import (
    enqueue_committed_event_for_dispatch, event_dispatcher_instance)
from pyjangle.event.event_repository import (DuplicateKeyError,
                                             event_repository_instance)
from pyjangle.logging.logging import LogToggles, log
from pyjangle.snapshot.snapshot_repository import snapshot_repository_instance
from pyjangle.snapshot.snapshottable import Snapshottable


async def handle_command(command: Command) -> CommandResponse:
    """Orchestrates interactions b/w aggregate, event storage, and snapshots.

    This method is very important.  It instantiates aggregates and replays 
    corresponding events that are sourced from the event repository.  It also
    applies snapshots when one is available to reduce the overhead of 
    retrieving events for aggregates with long histories.  It connects the 
    command to the aggregate and commits any events that result from the 
    command validation.  It also creates a new snapshot if it's needed.  In 
    the event that committing events fails because of a duplicate key (means
    someone else got there first), this method will retry command validation
    until it succeeds (optimistic concurrency at its finest)"""

    aggregate_id = command.get_aggregate_id()
    log(LogToggles.command_received, "Command received", {
        "aggregate_id": aggregate_id, "command_data": command.__dict__})
    while True:
        # create blank instance of relevant aggregate based on command type
        aggregate = command_to_aggregate_map_instance()[
            type(command)](id=aggregate_id)
        log(LogToggles.aggregate_created, "Blank aggregate created", {
            "aggregate_id": aggregate_id, "aggregate_type": str(type(aggregate))})
        # this will apply any snapshotting to the aggregate AND update its verison
        aggregate = await _apply_snapshotting_to_aggregate(aggregate, command)
        event_repository = event_repository_instance()
        # get post-snapshot events for aggregate (it's why the version is passed in as an argument)
        events = await event_repository.get_events(aggregate_id, aggregate.version)
        log(LogToggles.retrieved_aggregate_events, "Retrieved aggregate events", {
            "aggregate_id": aggregate_id, "aggregate_type": str(type(aggregate)), "event_count": len(list(events))})
        aggregate.apply_events(events)
        command_response = aggregate.validate(command)
        if command_response.is_success:
            try:
                await event_repository.commit_events(aggregate_id, aggregate.new_events)
                for e in aggregate.new_events:
                    log(LogToggles.committed_event, "Event committed", {
                        "aggregate_id": aggregate_id, "event": e.__dict__})
                await _record_new_snapshot_if_applicable(aggregate_id, aggregate)
                await _dispatch_events_locally(aggregate.new_events)
            except DuplicateKeyError:
                continue
        return command_response


async def _apply_snapshotting_to_aggregate(aggregate: Snapshottable, command: Command) -> Aggregate:
    """Applies snapshot to an aggregate.

    When an aggregate has a snapshot, applying it 
    can reduce the need to retrieve events from 
    the event store."""
    is_snapshotting = _is_snapshotting(aggregate)
    if not is_snapshotting:
        return aggregate
    log(LogToggles.is_snapshotting, f"Snapshotting status", {
        "enabled": is_snapshotting, "aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate))})
    snapshot_repo = snapshot_repository_instance() if is_snapshotting else None
    version_and_snapshot_tuple = await snapshot_repo.get_snapshot(command.get_aggregate_id()) if is_snapshotting else None
    log(LogToggles.is_snapshot_found, f"Snapshot was {'found' if version_and_snapshot_tuple != None else 'not found'}.", {
        "aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate)), "version": version_and_snapshot_tuple[0] if version_and_snapshot_tuple != None else None})
    if version_and_snapshot_tuple != None:
        # Found a snapshot
        try:
            version = version_and_snapshot_tuple[0]
            snapshot = version_and_snapshot_tuple[1]
            aggregate.apply_snapshot(version, snapshot)
            log(LogToggles.snapshot_applied, f"Snapshot applied", {"aggregate_id": aggregate.id, "aggregate_type": str(
                type(aggregate)), "version": version_and_snapshot_tuple[0]})
        except Exception as e:
            # Code change in the aggregate probably caused this snapshot
            # to become outdated, most likely.  The snapshot will be
            # deleted.
            log(LogToggles.snapshot_application_failed,
                "Snapshot application failed", exc_info=e)
            # Reset the aggregate to a pristine state just in case the
            # snapshot was partially applied.
            aggregate = command_to_aggregate_map_instance()[
                type(command)](aggregate.id)
            await snapshot_repo.delete_snapshot(command.get_aggregate_id())
            log(LogToggles.snapshot_deleted, "Deleted snapshot", {
                "aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate))})
            return aggregate
    return aggregate


async def _record_new_snapshot_if_applicable(aggregate_id: any, aggregate: Aggregate):
    """Creates a snapshot at regular intervals.

    If an aggregate has a snapshot frequency of 20,
    no more that 19 events will ever need to be retrieved from the event store.
    """

    is_snapshotting = _is_snapshotting(aggregate)
    if not is_snapshotting:
        return

    updated_version = aggregate.version + len(aggregate.new_events)
    snapshotable: Snapshottable = aggregate

    if updated_version % snapshotable.get_snapshot_frequency() == 0:
        # BEFORE a snapshot is created, it's important to apply the new
        # events that were created from the command validators.  Normally
        # these events are NOT applied until the next time the aggregate
        # is instantiated!
        aggregate.apply_events(aggregate.new_events)
        await snapshot_repository_instance().store_snapshot(aggregate_id, aggregate.version, snapshotable.get_snapshot())
        log(LogToggles.snapshot_taken, "Snapshot recorded", {
            "aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate)), "version": updated_version})
    else:
        log(LogToggles.snapshot_not_needed, "Snapshot not needed", {
            "aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate)), "version": updated_version})


def _is_snapshotting(aggregate: Aggregate) -> bool:
    """Determines if snapshotting is turned on for this aggregate."""
    return isinstance(aggregate, Snapshottable) and aggregate.get_snapshot_frequency() > 0


async def _dispatch_events_locally(events: list[Event]):
    """Does something with events after they're committed to the event repository.

    The something that this does is determined by the event dispatcher that's 
    registered with the framework via @RegisterEventDispatcher

    THROWS
    ------
    EventDispatcherError when there is no event dispatcher registered."""
    if not event_dispatcher_instance():
        return

    for event in events:
        await enqueue_committed_event_for_dispatch(event)
        log(LogToggles.dispatched_event_locally, "Events dispatched locally", {
            "events": [{"event_type": str(type(e)), "event_data": e.__dict__} for e in events]})

__all__ = [handle_command.__name__]
