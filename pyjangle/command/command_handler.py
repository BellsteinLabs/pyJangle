import logging
from typing import Callable, List
from pyjangle.aggregate.aggregate import Aggregate
from pyjangle.command.command import Command
from pyjangle.command.command_response import CommandResponse
from pyjangle.event.event import Event
from pyjangle.event.event_dispatcher import EventDispatcherError, event_dispatcher_instance
from pyjangle.event.event_repository import DuplicateKeyError, event_repository_instance
from pyjangle.log_tools.log_tools import Toggles
from pyjangle.snapshot.snapshot_repository import snapshot_repository_instance
from pyjangle.command.register import command_to_aggregate_map_instance
from pyjangle.snapshot.snapshottable import Snapshottable

logger = logging.getLogger(__name__)

def handle_command(command: Command, dispatch_new_events_locally: bool = True) -> CommandResponse:
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
    if Toggles.Info.log_command_received:
        logger.debug("Command received", {"aggregate_id": aggregate_id, "command_data": command.__dict__})
    while True:
        #create blank instance of relevant aggregate based on command type
        aggregate = command_to_aggregate_map_instance()[type(command)](id=aggregate_id)
        if Toggles.Debug.log_aggregate_created:
            logger.debug("Blank aggregate created", {"aggregate_id": aggregate_id, "aggregate_type": str(type(aggregate))})
        #this will apply any snapshotting to the aggregate AND update its verison
        aggregate = _apply_snapshotting_to_aggregate(aggregate, command)
        event_repository = event_repository_instance()
        #get post-snapshot events for aggregate (it's why the version is passed in as an argument)
        events = event_repository.get_events(aggregate_id, aggregate.version)
        if Toggles.Debug.log_retrieved_aggregate_events:
            logger.debug("Retrieved aggregate events", {"aggregate_id": aggregate_id, "aggregate_type": str(type(aggregate)), "event_count": len(events)})
        aggregate.apply_events(events)

        command_response = aggregate.validate(command)
        if command_response == None: return CommandResponse(True, data=None)
        if command_response.is_success:
            try:
                event_repository.commit_events(aggregate_id, aggregate.new_events)
                if Toggles.Info.log_committed_event:
                    for e in aggregate.new_events:
                        logger.info("Event committed", {"aggregate_id": aggregate_id, "event": e.__dict__})
                _record_new_snapshot_if_applicable(aggregate_id, aggregate)
                #this is a workaround for not using a message bus.  Once events are committed, let an in-memory event-handler 
                #decide what to do with them (update database tables, or spin up a saga, etc.)
                #dispatching events needs to be completed asynchronously to prevent holding up the command response
                #TODO: Make me async, please
                if dispatch_new_events_locally: _dispatch_events_locally(aggregate.new_events, event_repository.mark_event_handled)
            except DuplicateKeyError:
                continue
        return command_response
    
def _apply_snapshotting_to_aggregate(aggregate: Snapshottable, command: Command) -> Aggregate:
    """Applies snapshot to an aggregate.
    
    When an aggregate has a snapshot, applying it 
    can reduce the need to retrieve events from 
    the event store."""
    is_snapshotting = _is_snapshotting(aggregate)
    if not is_snapshotting:
        return aggregate
    if Toggles.Debug.log_is_snapshotting:
        logger.debug(f"Snapshotting status", {"enabled": is_snapshotting, "aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate))})
    snapshot_repo = snapshot_repository_instance() if is_snapshotting else None
    version_and_snapshot_tuple = snapshot_repo.get_snapshot(command.get_aggregate_id()) if is_snapshotting else None
    if Toggles.Debug.log_is_snapshot_found:
        logger.debug(f"Snapshot was {'found' if version_and_snapshot_tuple != None else 'not found'}.", {"aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate)), "version": version_and_snapshot_tuple[0] if version_and_snapshot_tuple != None else None})
    if version_and_snapshot_tuple != None:
        #Found a snapshot
        try:
            version = version_and_snapshot_tuple[0]
            snapshot = version_and_snapshot_tuple[1]
            aggregate.apply_snapshot(version, snapshot)
            if Toggles.Debug.log_snapshot_applied:
                logger.debug(f"Snapshot applied", {"aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate)), "version": version_and_snapshot_tuple[0]})
        except Exception as e:
            #Code change in the aggregate probably caused this snapshot 
            #to become outdated, most likely.  The snapshot will be 
            #deleted.
            if Toggles.Warning.log_snapshot_application_failed:
                logger.warning("Snapshot application failed", exc_info=e)
            #Reset the aggregate to a pristine state just in case the 
            #snapshot was partially applied.
            aggregate = command_to_aggregate_map_instance()[type(command)](aggregate.id)
            snapshot_repo.delete_snapshot(command.get_aggregate_id())
            if Toggles.Info.log_snapshot_deleted:
                logger.info("Deleted snapshot", {"aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate))})
            return aggregate
    return aggregate

def _record_new_snapshot_if_applicable(aggregate_id: any, aggregate: Aggregate):
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
        #BEFORE a snapshot is created, it's important to apply the new 
        #events that were created from the command validators.  Normally
        #these events are NOT applied until the next time the aggregate 
        #is instantiated!
        aggregate.apply_events(aggregate.new_events)
        snapshot_repository_instance().store_snapshot(aggregate_id, aggregate.version, snapshotable.get_snapshot())
        if Toggles.Info.log_snapshot_taken:
            logger.info("Snapshot recorded", {"aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate)), "version": updated_version})
    else: 
        if Toggles.Debug.log_snapshot_not_needed:
            logger.debug("Snapshot not needed",{"aggregate_id": aggregate.id, "aggregate_type": str(type(aggregate)), "version": updated_version})

def _is_snapshotting(aggregate: Aggregate) -> bool:
    """Determines if snapshotting is turned on for this aggregate."""
    return isinstance(aggregate, Snapshottable) and aggregate.get_snapshot_frequency() > 0

def _dispatch_events_locally(events: List[Event], event_handled_callback: Callable[[Event], None]):
    """Does something with events after they're commited to the event repository.
    
    The something that this does is determined by the event dispatcher that's 
    registered with the framework via @RegisterEventDispatcher
    
    THROWS
    ------
    EventDispatcherError when there is no event dispatcher registered."""
    event_dispatcher = event_dispatcher_instance()
    if event_dispatcher == None:
        raise EventDispatcherError("Local event dispatch failed: no event_dispatcher_instance registered")
    event_dispatcher(events, event_handled_callback)
    if Toggles.Debug.log_dispatched_event_locally:
        logger.debug("Events dispatched locally", {"events": [{"event_type": str(type(e)), "event_data": e.__dict__} for e in events]})


        