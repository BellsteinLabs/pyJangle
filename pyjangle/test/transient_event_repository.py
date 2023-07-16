from datetime import datetime, timedelta
from typing import Iterator, List
from pyjangle.event.event import Event
from pyjangle.event.event_repository import DuplicateKeyError, EventRepository, RegisterEventRepository

class TransientEventRepository(EventRepository):

    def __init__(self) -> None:
        super().__init__()
        self._events_by_aggregate_id: dict[any, list[Event]] = dict()
        self._events_by_event_id: dict[any, Event] = dict()
        self._unhandled_events = set()

    async def get_events(self, aggregate_id: any, current_version = 0) -> List[Event]:
        return sorted([event for event in self._events_by_aggregate_id.get(aggregate_id, []) if event.version > current_version], key=lambda event : event.version)

    async def commit_events(self, aggregate_id: any, events: List[Event]):
        if not aggregate_id in self._events_by_aggregate_id:
            self._events_by_aggregate_id[aggregate_id] = []

        duplicate_events_found = set([event.version for event in events]).intersection(set([event.version for event in self._events_by_aggregate_id[aggregate_id]]))

        if duplicate_events_found:
            raise DuplicateKeyError()
        
        self._events_by_aggregate_id[aggregate_id] += events
        self._events_by_event_id.update([(event.id, event) for event in events])
        [self._unhandled_events.add(event.id) for event in events]

    async def mark_event_handled(self, id: str):
        if id in self._unhandled_events: #pragma no cover
            self._unhandled_events.remove(id)

    async def get_unhandled_events(self, batch_size: int = 100, time_delta: timedelta = timedelta(seconds=30)) -> Iterator[Event]:
        for id in self._unhandled_events:
            cutoff_time = datetime.now() - time_delta
            event = self._events_by_event_id[id]
            if event.created_at < cutoff_time: #pragma no cover
                yield event