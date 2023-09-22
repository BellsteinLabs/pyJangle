from datetime import datetime, timedelta
from typing import Iterator, List
from pyjangle import VersionedEvent
from pyjangle import DuplicateKeyError, EventRepository, RegisterEventRepository


class TransientEventRepository(EventRepository):
    def __init__(self) -> None:
        super().__init__()
        self._events_by_aggregate_id: dict[any, list[VersionedEvent]] = dict()
        self._events_by_event_id: dict[any, VersionedEvent] = dict()
        self._unhandled_events = set()

    async def get_events(
        self, aggregate_id: any, current_version=0
    ) -> List[VersionedEvent]:
        return sorted(
            [
                event
                for event in self._events_by_aggregate_id.get(aggregate_id, [])
                if event.version > current_version
            ],
            key=lambda event: event.version,
        )

    async def commit_events(
        self, aggregate_id_and_event_tuples: list[tuple[any, VersionedEvent]]
    ):
        duplicate_detector: dict[any, set[int]] = dict()
        duplicate_events_found = False

        existing_events = list()
        for kvp in self._events_by_aggregate_id.items():
            for e in kvp[1]:
                existing_events.append((kvp[0], e))

        for aggregate_id, event in aggregate_id_and_event_tuples + existing_events:
            if not aggregate_id in duplicate_detector:
                duplicate_detector[aggregate_id] = set()
            if event.version in duplicate_detector[aggregate_id]:
                duplicate_events_found = True
                break
            duplicate_detector[aggregate_id].add(event.version)

        if duplicate_events_found:
            raise DuplicateKeyError()

        for aggregate_id, event in aggregate_id_and_event_tuples:
            if aggregate_id not in self._events_by_aggregate_id:
                self._events_by_aggregate_id[aggregate_id] = []
            self._events_by_aggregate_id[aggregate_id].append(event)
            self._events_by_event_id[event.id] = event
            self._unhandled_events.add(event.id)

    async def mark_event_handled(self, id: str):
        if id in self._unhandled_events:  # pragma no cover
            self._unhandled_events.remove(id)

    async def get_unhandled_events(
        self, batch_size: int = 100, time_delta: timedelta = timedelta(seconds=30)
    ) -> Iterator[VersionedEvent]:
        for id in self._unhandled_events:
            cutoff_time = datetime.now() - time_delta
            event = self._events_by_event_id[id]
            if event.created_at < cutoff_time:  # pragma no cover
                yield event
