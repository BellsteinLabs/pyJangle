from dataclasses import dataclass
import functools
from pyjangle.command.command import Command
from pyjangle.event.event import Event


class CommandA(Command):
    def get_aggregate_id(self):
        return 1
    
class CommandB(Command):
    def get_aggregate_id(self):
        return 1
    
@dataclass(frozen=True, kw_only=True)
class EventA(Event):
    pass

@dataclass(frozen=True, kw_only=True)
class EventB(Event):
    pass

def Counter(wrapped):
    wrapped._calls = 0
    @functools.wraps(wrapped)
    def wrapper(*args, **kwargs):
        wrapped._calls += 1
        wrapped(*args, **kwargs)
    return wrapper
