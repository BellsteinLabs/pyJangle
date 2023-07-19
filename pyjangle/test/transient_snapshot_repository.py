from pyjangle.snapshot.snapshot_repository import RegisterSnapshotRepository, SnapshotRepository


@RegisterSnapshotRepository
class TransientSnapshotRepository(SnapshotRepository):
    def __init__(self) -> None:
        super().__init__()
        self._snapshots: dict[any, any] = dict()

    async def get_snapshot(self, aggregate_id: str) -> tuple[int, any] | None:
        value = self._snapshots[aggregate_id] if aggregate_id in self._snapshots else None
        return value
    
    async def store_snapshot(self, aggregate_id: any, version: int, snapshot: any):
        self._snapshots[aggregate_id] = (version, snapshot)

    async def delete_snapshot(self, aggregate_id: str):
        if aggregate_id in self._snapshots: #pragma no cover
            del self._snapshots[aggregate_id]