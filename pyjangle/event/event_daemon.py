import logging
from pyjangle.event.event_repository import event_repository_instance
from pyjangle.event.event_handler import handle_event
from pyjangle.log_tools.log_tools import Toggles

logger = logging.getLogger(__name__)

#TODO:  Need a better solution that this.  Ideally, events are streamed, 
#and the caller is notified when the operation is completed so that
#subsequent calls don't overlap.
def retry_failed_events(count: int, raise_on_missing_event_handler = True):
    """Called by daemon to preiodically retry failed events.
    
    It happens... the network goes down, a stray cosmic particle
    accidentally flips a bit.  It's okay if an event handler fails 
    the first time.  It will not be marked as handled, and you 
    can create a CRON job, for example, that periodically calls 
    this method every 30 seconds, 5 minutes, whatever fits
    your application.
    
    Be careful of how many events you retry at a time.  The more 
    events, the more memory you could potentially use."""

    repo = event_repository_instance()
    events = repo.get_failed_events(count)
    for event in events:
        handle_event(event, raise_on_missing_event_handler=True)
    if Toggles.Info.log_retrying_failed_events:
        logger.info(f"Retrying {len(events)} failed events...")