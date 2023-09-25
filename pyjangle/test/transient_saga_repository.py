from datetime import datetime
from pyjangle.event.event import VersionedEvent
from pyjangle import DuplicateKeyError
from pyjangle.saga.saga import Saga
from pyjangle.saga.saga_repository import SagaRepository


class TransientSagaRepository(SagaRepository):
    def __init__(self) -> None:
        self.types = dict()
        self.sagas = dict()
        self.events = dict()

    async def get_saga(self, saga_id: any) -> Saga:
        if saga_id not in self.types:
            return None
        return self.types[saga_id](
            saga_id,
            [self.events[saga_id][event_id] for event_id in self.events[saga_id]],
            self.sagas[saga_id].retry_at,
            self.sagas[saga_id].timeout_at,
            self.sagas[saga_id].is_complete,
        )

    async def commit_saga(self, saga: Saga):
        if not saga.saga_id in self.events:
            self.events[saga.saga_id] = dict()
        if _has_duplicate_ids(
            saga.new_events
            + [
                self.events[saga.saga_id][key]
                for key in self.events[saga.saga_id].keys()
            ]
        ):
            raise DuplicateKeyError()
        self.sagas[saga.saga_id] = saga
        self.types[saga.saga_id] = type(saga)
        for event in saga.new_events:
            self.events[saga.saga_id][event.id] = event

    async def get_retry_saga_ids(self, batch_size: int) -> list[any]:
        current_time = datetime.now()
        return [
            saga_id
            for saga_id in self.sagas
            if not self.sagas[saga_id].is_complete
            and not self.sagas[saga_id].is_timed_out
            and (
                not self.sagas[saga_id].timeout_at
                or self.sagas[saga_id].timeout_at > current_time
            )
            and (
                self.sagas[saga_id].retry_at
                and self.sagas[saga_id].retry_at < current_time
            )
        ]


def _has_duplicate_ids(events: list[VersionedEvent]):
    return len([event.id for event in events]) != len(
        set([event.id for event in events])
    )
