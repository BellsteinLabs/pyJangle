import logging
from pyjangle.event.event_repository import event_repository_instance
from pyjangle.event.event_handler import handle_event
from pyjangle.log_tools.log_tools import Toggles
from pyjangle.saga.saga_handler import handle_saga_event
from pyjangle.saga.saga_metadata import SagaMetadata
from pyjangle.saga.saga_repository import saga_repository_instance

logger = logging.getLogger(__name__)

#TODO: Need a solution that can retry all sagas
#by streaming them from the db and letting 
#the caller know when the operation is done
#to avoid overlapping calls
def retry_sagas(max_batch_size: int):
    """Entrypoint for saga retry daemon
    
    This is an entrypiont for a daemon (not supplied) 
    that periodically queries for sagas that require
    retry, likely because of a network outage or 
    similar issue.  Because memory is limited,
    use the max_batch_size parameter to process only 
    a few at a time."""
    repo = saga_repository_instance()
    metadatas: list[SagaMetadata] = repo.get_retry_saga_metadata(max_batch_size)
    if Toggles.Info.log_retrying_sagas:
        logger.log(f"Retrying {max_batch_size} sagas.")
    for metadata in metadatas:
        handle_saga_event(saga_id=metadata.id, event=None, saga_type=metadata.type)
