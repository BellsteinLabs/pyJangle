import logging
from pyjangle.command.command import Command

from pyjangle.command.register import RegisterCommand
from pyjangle.log_tools.log_tools import JangleJSONFormatter

formatter = JangleJSONFormatter()
formatter.set_included_fields(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
logging.getLogger().setLevel(logging.DEBUG)
logging.info("Initializing root logger")
root_logger = logging.getLogger()
for h in root_logger.handlers:
    h.setFormatter(formatter)
logger = logging.getLogger(__name__)



class ExampleCommand(Command):
    def __init__(self) -> None:
        self.name = "Bob"
        self.age = 42
        self.likes = ["sailing", "beach walks", "frogs"]

    def get_aggregate_id(self):
        return self.age

logging.getLogger(__name__).info("Test %(type)s", {"type": "Command", "value": ExampleCommand().__dict__})

try:
    raise Exception("foo")
except Exception as e:
    logging.getLogger(__name__).exception(e, {"type": "Command", "value": ExampleCommand().__dict__})

exit()
