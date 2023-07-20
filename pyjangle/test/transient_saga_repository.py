from datetime import datetime
from typing import List
from pyjangle.event.event import Event
from pyjangle.event.event_repository import DuplicateKeyError
from pyjangle.saga.saga_metadata import SagaMetadata
from pyjangle.saga.saga_repository import SagaRepository

class TransientSagaRepository(SagaRepository):

    def __init__(self) -> None:
        self.metadata = dict()
        self.events = dict()

    async def get_saga(self, saga_id: any) -> tuple[SagaMetadata, List[Event]]:
        return (self.metadata.get(saga_id, None), self.events.get(saga_id, None))
    
    def _has_duplicate_ids(self, events: list[Event]):
        return len([event.id for event in events]) != len(set([event.id for event in events]))

    async def commit_saga(self, metadata: SagaMetadata, events: list[Event]):
        if not metadata.id in self.events:
            self.events[metadata.id] = []
        existing_events = self.events[metadata.id]
        if set([event.id for event in events]).intersection(set([event.id for event in existing_events])):
            raise DuplicateKeyError()
        if self._has_duplicate_ids(events):
            raise DuplicateKeyError()
        self.metadata[metadata.id] = metadata
        self.events[metadata.id] += events

    async def get_retry_saga_metadata(self, max_count: int) -> list[SagaMetadata]:
        current_time = datetime.now()
        return [metadata for metadata in self.metadata.values() if not metadata.is_complete and not metadata.is_timed_out and (not metadata.timeout_at or datetime.fromisoformat(metadata.timeout_at) > current_time) and (metadata.retry_at and datetime.fromisoformat(metadata.retry_at) < current_time)]