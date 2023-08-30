from dataclasses import dataclass
from datetime import datetime
import uuid
from pyjangle.event.event import VersionedEvent
from pyjangle.event.register_event import RegisterEvent


@RegisterEvent()
@dataclass(kw_only=True)
class EventA(VersionedEvent):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]
        data["created_at"] = data["created_at"]
        return EventA(**data)


@RegisterEvent()
@dataclass(kw_only=True)
class EventB(VersionedEvent):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]  # pragma no cover
        data["created_at"] = data["created_at"]  # pragma no cover
        return EventA(**data)  # pragma no cover


@RegisterEvent()
@dataclass(kw_only=True)
class EventThatCompletesSaga(VersionedEvent):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]  # pragma no cover
        data["created_at"] = data["created_at"]  # pragma no cover
        return EventThatCompletesSaga(**data)  # pragma no cover


@RegisterEvent
@dataclass(kw_only=True)
class EventThatContinuesSaga(VersionedEvent):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]  # pragma no cover
        data["created_at"] = data["created_at"]  # pragma no cover
        return EventThatContinuesSaga(**data)  # pragma no cover


@RegisterEvent
@dataclass(kw_only=True)
class EventThatTimesOutSaga(VersionedEvent):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]  # pragma no cover
        data["created_at"] = data["created_at"]  # pragma no cover
        return EventThatTimesOutSaga(**data)  # pragma no cover


@RegisterEvent
@dataclass(kw_only=True)
class EventThatCompletesACommand(VersionedEvent):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]  # pragma no cover
        data["created_at"] = data["created_at"]  # pragma no cover
        return EventThatTimesOutSaga(**data)  # pragma no cover


@RegisterEvent
@dataclass(kw_only=True)
class EventThatSetsSagaToTimedOut(VersionedEvent):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]  # pragma no cover
        data["created_at"] = data["created_at"]  # pragma no cover
        return EventThatSetsSagaToTimedOut(**data)  # pragma no cover


@RegisterEvent
@dataclass(kw_only=True)
class EventThatCausesDuplicateKeyError(VersionedEvent):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]  # pragma no cover
        data["created_at"] = data["created_at"]  # pragma no cover
        return EventThatCausesDuplicateKeyError(**data)  # pragma no cover


@RegisterEvent
@dataclass(kw_only=True)
class EventThatCausesSagaToRetry(VersionedEvent):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]  # pragma no cover
        data["created_at"] = data["created_at"]  # pragma no cover
        return EventThatCausesSagaToRetry(**data)  # pragma no cover


@RegisterEvent
@dataclass(kw_only=True)
class TestSagaEvent(VersionedEvent):
    version: int = 0

    def deserialize(data: any) -> any:
        return TestSagaEvent(**data)
