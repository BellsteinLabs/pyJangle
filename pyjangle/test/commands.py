from pyjangle.command.command import Command


class CommandThatAlwaysSucceeds(Command):
    def get_aggregate_id(self):
        return 1
    
class CommandThatFails(Command):
    def get_aggregate_id(self):
        return 1
    
class AnotherCommandThatAlwaysSucceeds(Command):
    def get_aggregate_id(self):
        return 1
    
class CommandB(Command):
    def get_aggregate_id(self):
        pass

class CommandThatFailsTheFirstTime(Command):
    def get_aggregate_id(self):
        return 1