import sqlite3
from typing import List
from pyjangle.event.event import Event

from pyjangle.event.event_repository import EventRepository, RegisterEventRepository

@RegisterEventRepository
class SqlLiteEventRepository(EventRepository):
    def __init__(self):
        #Create event store table if it's not already there
        with open('pyjangle/sqllite/create_event_store.sql', 'r') as create_event_store_sql_file:
            create_event_store_sql_script = create_event_store_sql_file.read()

        self.conn = sqlite3.connect("event_store.db")
        self.cursor = self.conn.cursor()
        self.cursor.executescript(create_event_store_sql_script)

    def get_events(self, aggregate_id: any, current_version = 0) -> List[Event]:
        # TODO: How to avoid injection with postgres? This is the sqllite way
        sql_script = f"""
            SELECT aggregate_id, version_id 
            FROM event_store
            WHERE aggregate_id = ?
            AND version_id >= ?
        """
        # Do I need to do: ORDER BY VERSION_ID asc?
        response = self.cursor.execute(sql_script, (aggregate_id, current_version))

        return []

    def commit_events(self, events: List[Event]):
        pass


def adapt_event(event: Event):
    return f"{event.id};{event.version}"

def convert_event(s):
    columns = s.split(b";")
    id = columns[0]
    version = columns[1]
    return Event(id, version)
    
sqlite3.register_adapter(Event, adapt_event)