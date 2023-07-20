from dataclasses import dataclass
from datetime import datetime
from pyjangle.saga.register_saga import get_saga_name

from pyjangle.saga.saga import Saga


@dataclass(frozen=True, kw_only=True)
class SagaMetadata:
    """Metadata of a saga.
    
    This is retrieved from the saga repository
    along with the saga's events.  The metadata
    and the events are required to reconstitute 
    the saga."""
    id: any
    type: str
    retry_at: datetime
    timeout_at: datetime
    is_complete: bool
    is_timed_out: bool

def from_saga(saga: Saga) -> SagaMetadata:
    return SagaMetadata(saga.saga_id, get_saga_name(saga), saga.retry_at, saga.timeout_at, saga.is_complete)