"""JSON logging capabilities.

Call `initialize_jangle_logging` in order to register a log formatter that emits logs to
STDOUT in JSON format.  This may make it easier for some logging tools to parse and 
index logging messages."""

from .logging import LogFormatter, initialize_logging
