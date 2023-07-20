import dataclasses
import json
import sqlite3
from pyjangle.event.event import Event
from pyjangle.event.register import get_event_name, get_event_type
from pyjangle_sqllite3.symbols import FIELDS

def serialize_event(event: Event) -> any:
    as_dict = dataclasses.asdict(event)   
    return {
        FIELDS.EVENT_STORE.EVENT_ID: as_dict.pop("id") ,
        FIELDS.EVENT_STORE.AGGREGATE_VERSION: as_dict.pop("version"),
        FIELDS.EVENT_STORE.DATA: json.dumps(as_dict),
        FIELDS.EVENT_STORE.CREATED_AT: as_dict.pop("created_at"),
        FIELDS.EVENT_STORE.TYPE: get_event_name(type(event))
    }

def deserialize_event(fields: sqlite3.Row) -> Event:
    data_dict = json.loads(fields[FIELDS.EVENT_STORE.DATA])
    data_dict["id"] = fields[FIELDS.EVENT_STORE.EVENT_ID]
    data_dict["created_at"] = fields[FIELDS.EVENT_STORE.CREATED_AT]
    data_dict["version"] = fields[FIELDS.EVENT_STORE.AGGREGATE_VERSION]
    event_class_instance: type[Event] = get_event_type(fields[FIELDS.EVENT_STORE.TYPE])
    return event_class_instance.deserialize(data_dict)

def serialize_saga_event(event: Event) -> any:
    as_dict = dataclasses.asdict(event)
    return {
        FIELDS.SAGA_EVENTS.EVENT_ID: as_dict.pop("id"),
        FIELDS.SAGA_EVENTS.DATA: json.dumps(as_dict),
        FIELDS.SAGA_EVENTS.CREATED_AT: as_dict.pop("created_at"),
        FIELDS.SAGA_EVENTS.TYPE: get_event_name(type(event))
    }

def deserialize_saga_event(fields: sqlite3.Row) -> Event:
    data_dict = json.loads(fields[FIELDS.SAGA_EVENTS.DATA])
    data_dict["id"] = fields[FIELDS.SAGA_EVENTS.EVENT_ID]
    data_dict["created_at"] = fields[FIELDS.SAGA_EVENTS.CREATED_AT]
    event_class_instance: type[Event] = get_event_type(fields[FIELDS.EVENT_STORE.TYPE])
    return event_class_instance.deserialize(data_dict)

def serialize_snapshot(snapshot: any) -> any:
    return json.dumps(snapshot)

def deserialize_snapshot(fields: sqlite3.Row) -> any:
    return (fields[FIELDS.SNAPSHOTS.VERSION], json.loads(fields[FIELDS.SNAPSHOTS.DATA]))