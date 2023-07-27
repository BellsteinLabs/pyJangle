import dataclasses
from datetime import datetime
import json
import sqlite3
from pyjangle.event.event import VersionedEvent
from pyjangle.event.register_event import get_event_name, get_event_type
from pyjangle.saga.register_saga import get_saga_name, get_saga_type
from pyjangle.saga.saga import Saga
from pyjangle_sqllite3.symbols import FIELDS


def serialize_event(event: VersionedEvent) -> any:
    as_dict = dataclasses.asdict(event)
    return {
        FIELDS.EVENT_STORE.EVENT_ID: as_dict.pop("id"),
        FIELDS.EVENT_STORE.AGGREGATE_VERSION: as_dict.pop("version"),
        FIELDS.EVENT_STORE.DATA: json.dumps(as_dict),
        FIELDS.EVENT_STORE.CREATED_AT: as_dict.pop("created_at"),
        FIELDS.EVENT_STORE.TYPE: get_event_name(type(event))
    }


def deserialize_event(fields: sqlite3.Row) -> VersionedEvent:
    data_dict = json.loads(fields[FIELDS.EVENT_STORE.DATA])
    data_dict["id"] = fields[FIELDS.EVENT_STORE.EVENT_ID]
    data_dict["created_at"] = fields[FIELDS.EVENT_STORE.CREATED_AT]
    data_dict["version"] = fields[FIELDS.EVENT_STORE.AGGREGATE_VERSION]
    event_class_instance: type[VersionedEvent] = get_event_type(
        fields[FIELDS.EVENT_STORE.TYPE])
    return event_class_instance.deserialize(data_dict)


def serialize_saga(saga: Saga) -> tuple[dict, list[dict]]:
    saga_metadata = {
        FIELDS.SAGA_METADATA.SAGA_ID: saga.saga_id,
        FIELDS.SAGA_METADATA.SAGA_TYPE: get_saga_name(type(saga)),
        FIELDS.SAGA_METADATA.RETRY_AT: saga.retry_at.isoformat() if saga.retry_at else None,
        FIELDS.SAGA_METADATA.TIMEOUT_AT: saga.timeout_at.isoformat() if saga.timeout_at else None,
        FIELDS.SAGA_METADATA.IS_COMPLETE: 1 if saga.is_complete else 0,
        FIELDS.SAGA_METADATA.IS_TIMED_OUT: 1 if saga.is_timed_out else 0,
    }
    saga_events = [{
        FIELDS.SAGA_EVENTS.SAGA_ID: saga.saga_id,
        FIELDS.SAGA_EVENTS.EVENT_ID: event.id,
        FIELDS.SAGA_EVENTS.DATA: json.dumps(dataclasses.asdict(event)),
        FIELDS.SAGA_EVENTS.CREATED_AT: event.created_at,
        FIELDS.SAGA_EVENTS.TYPE: get_event_name(type(event)),
    } for event in saga.new_events]
    return (saga_metadata, saga_events)


def deserialize_saga(serialized: tuple[dict, list[dict]]) -> Saga:
    metadata_dict = serialized[0]
    saga_type = get_saga_type(metadata_dict[FIELDS.SAGA_METADATA.SAGA_TYPE])
    saga_id = metadata_dict[FIELDS.SAGA_METADATA.SAGA_ID]
    saga_retry_at = datetime.fromisoformat(
        metadata_dict[FIELDS.SAGA_METADATA.RETRY_AT]) if metadata_dict[FIELDS.SAGA_METADATA.RETRY_AT] else None
    saga_timeout_at = datetime.fromisoformat(
        metadata_dict[FIELDS.SAGA_METADATA.TIMEOUT_AT]) if metadata_dict[FIELDS.SAGA_METADATA.TIMEOUT_AT] else None
    saga_is_complete = bool(metadata_dict[FIELDS.SAGA_METADATA.IS_COMPLETE])
    saga_is_timed_out = bool(metadata_dict[FIELDS.SAGA_METADATA.IS_TIMED_OUT])
    events = list()
    for serialized_event in serialized[1]:
        data_dict = json.loads(serialized_event[FIELDS.EVENT_STORE.DATA])
        data_dict["id"] = serialized_event[FIELDS.EVENT_STORE.EVENT_ID]
        data_dict["created_at"] = serialized_event[FIELDS.EVENT_STORE.CREATED_AT]
        event_class_instance: type[VersionedEvent] = get_event_type(
            serialized_event[FIELDS.EVENT_STORE.TYPE])
        events.append(event_class_instance.deserialize(data_dict))
    return saga_type(saga_id, events, saga_retry_at, saga_timeout_at, saga_is_complete, saga_is_timed_out)


def serialize_snapshot(snapshot: any) -> any:
    return json.dumps(snapshot)


def deserialize_snapshot(fields: sqlite3.Row) -> any:
    return (fields[FIELDS.SNAPSHOTS.VERSION], json.loads(fields[FIELDS.SNAPSHOTS.DATA]))
