import dataclasses
from datetime import datetime
import json
import sqlite3
from pyjangle.event.event import VersionedEvent
from pyjangle.event.register_event import get_event_name, get_event_type
from pyjangle.saga.register_saga import get_saga_name, get_saga_type
from pyjangle.saga.saga import Saga
from pyjangle_sqllite3.symbols import FIELDS



def serialize_snapshot(snapshot: any) -> any:
    return json.dumps(snapshot)


def deserialize_snapshot(fields: sqlite3.Row) -> any:
    return (fields[FIELDS.SNAPSHOTS.VERSION], json.loads(fields[FIELDS.SNAPSHOTS.DATA]))
