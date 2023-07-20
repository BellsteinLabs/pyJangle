from dataclasses import dataclass
from datetime import datetime
import uuid
from pyjangle.event.event import Event
from pyjangle.event.register import RegisterEvent

@RegisterEvent()
@dataclass(frozen=True, kw_only=True)
class EventA(Event):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]
        data["created_at"] = data["created_at"]
        return EventA(**data)

@RegisterEvent()
@dataclass(frozen=True, kw_only=True)
class EventB(Event):
    def deserialize(data: any) -> any:
        data["id"] = data["id"] #pragma no cover
        data["created_at"] = data["created_at"]#pragma no cover
        return EventA(**data)#pragma no cover
    
@RegisterEvent()
@dataclass(frozen=True, kw_only=True)
class EventThatCompletesSaga(Event):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]#pragma no cover
        data["created_at"] = data["created_at"]#pragma no cover
        return EventThatCompletesSaga(**data)#pragma no cover
    
@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class EventThatContinuesSaga(Event):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]#pragma no cover
        data["created_at"] = data["created_at"]#pragma no cover
        return EventThatContinuesSaga(**data)#pragma no cover
    
@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class EventThatTimesOutSaga(Event):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]#pragma no cover
        data["created_at"] = data["created_at"]#pragma no cover
        return EventThatTimesOutSaga(**data)#pragma no cover
    
@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class EventThatCompletesACommand(Event):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]#pragma no cover
        data["created_at"] = data["created_at"]#pragma no cover
        return EventThatTimesOutSaga(**data)#pragma no cover
    
@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class EventThatSetsSagaToTimedOut(Event):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]#pragma no cover
        data["created_at"] = data["created_at"]#pragma no cover
        return EventThatSetsSagaToTimedOut(**data)#pragma no cover
    
@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class EventThatCausesDuplicateKeyError(Event):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]#pragma no cover
        data["created_at"] = data["created_at"]#pragma no cover
        return EventThatCausesDuplicateKeyError(**data)#pragma no cover
    
@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class EventThatCausesSagaToRetry(Event):
    def deserialize(data: any) -> any:
        data["id"] = data["id"]#pragma no cover
        data["created_at"] = data["created_at"]#pragma no cover
        return EventThatCausesSagaToRetry(**data)#pragma no cover
    
@RegisterEvent
@dataclass(frozen=True, kw_only=True)
class TestSagaEvent(Event):
    version: int = 0
    def deserialize(data: any) -> any:
        return TestSagaEvent(**data)
    
