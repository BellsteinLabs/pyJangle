from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, kw_only=True)
class SagaMetadata:
    """Metadata of a saga.
    
    This is retrieved from the saga repository
    along with the saga's events.  The metadata
    and the events are required to reconstitute 
    the saga."""
    id: any
    type: type
    retry_at: datetime
    timeout_at: datetime
    is_complete: bool