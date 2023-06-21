from pyjangle.event.event_repository import event_repository_instance
from pyjangle.event.event_handler import handle_event


def retry_failed_events(max_batch_size: int, raise_on_missing_event_handler = True):
    """Called by daemon to preiodically retry failed events.
    
    It happens... the network goes down, a stray cosmic particle
    accidentally flips a bit.  It's okay if an event handler fails 
    the first time.  It will not be marked as handled, and you 
    can create a CRON job, for example, that periodically calls 
    this method every 30 seconds, 5 minutes, whatever fits
    your application."""
    repo = event_repository_instance()
    events = repo.get_failed_events(max_batch_size)
    for event in events:
        handle_event(event, raise_on_missing_event_handler=True)