from asyncio import Queue, sleep
from datetime import timedelta
from pyjangle.event.event_dispatcher import EventDispatcherError, enqueue_committed_event_for_dispatch, event_dispatcher_instance
from pyjangle.event.event_repository import event_repository_instance
from pyjangle.event.event_handler import handle_event
from pyjangle.logging.logging import LogToggles, log

async def retry_failed_events(batch_size: int = 100, max_age_in_seconds: timedelta = timedelta(seconds=30)):
    repo = event_repository_instance()
    unhandled_events = [event async for event in repo.get_unhandled_events(batch_size=batch_size, time_delta=timedelta(seconds=0))]
    log(LogToggles.retrying_failed_events, f"Retrying {len(unhandled_events)} failed events...")
    for event in unhandled_events:
        event_repo = event_repository_instance()
        await event_dispatcher_instance()(event)
        await event_repo.mark_event_handled(event.id)
    log(LogToggles.retrying_failed_events, f"Finished retrying {len(unhandled_events)} failed events")

#TODO:  Need a better solution that this.  Ideally, events are streamed, 
#and the caller is notified when the operation is completed so that
#subsequent calls don't overlap.
async def begin_retry_failed_events_loop(frequency_in_seconds: float, batch_size: int = 100, max_age_in_seconds: timedelta = timedelta(seconds=30)):
    """Called by daemon to preiodically retry failed events.
    
    It happens... the network goes down, a stray cosmic particle
    accidentally flips a bit.  It's okay if an event handler fails 
    the first time.  It will not be marked as handled, and you 
    can create a CRON job, for example, that periodically calls 
    this method every 30 seconds, 5 minutes, whatever fits
    your application.
    
    Be careful of how many events you retry at a time.  The more 
    events, the more memory you could potentially use."""

    if not event_dispatcher_instance():
        raise EventDispatcherError("No event dispatcher registered.")
    event_repository_instance()
    while True:
        await sleep(frequency_in_seconds)
        await retry_failed_events(batch_size=batch_size, max_age_in_seconds=max_age_in_seconds)

    