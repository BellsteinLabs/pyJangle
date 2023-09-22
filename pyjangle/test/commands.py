from pyjangle import Command


class CommandThatShouldSucceedA(Command):
    def get_aggregate_id(self):
        return 1


class CommandThatShouldFail(Command):
    def get_aggregate_id(self):
        return 1


class CommandThatShouldSucceedB(Command):
    def get_aggregate_id(self):
        return 1


class CommandThatShouldSucceedC(Command):
    def get_aggregate_id(self):
        return 1


class CommandThatShouldErrorOnlyFirstTime(Command):
    def get_aggregate_id(self):
        return 1
