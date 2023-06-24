import json
from logging import Formatter, LogRecord
import logging
from typing import List, Mapping

from pyjangle.serialization.dumps_encoders import UUIDEncoder

NAME = 1
LEVELNO = 2
LEVELNAME = 3
PATHNAME = 4
FILENAME = 5
MODULE = 6
LINENO = 7
FUNCNAME = 8
CREATED = 9
ASCTIME = 10
MSECS = 11
RELATIVE_CREATED = 12
THREAD = 13
THREADNAME = 14
PROCESS = 15
MESSAGE = 16

class JangleJSONFormatter(Formatter):

    _field_mappings = {
        NAME: "name",
        LEVELNO: "levelno",
        LEVELNAME: "levelname",
        PATHNAME: "pathname",
        FILENAME: "filename",
        MODULE: "module",
        LINENO: "lineno",
        FUNCNAME: "funcName",
        CREATED: "created",
        ASCTIME: "asctime",
        MSECS: "msecs",
        RELATIVE_CREATED: "relativeCreated",
        THREAD: "thread",
        THREADNAME: "threadName",
        PROCESS: "process",
        MESSAGE: "message"
    }

    def set_included_fields(self, *fields: List[int]):
        """Specify which fields are included in log messages.
        
        Possible options are:
            NAME = 1
            LEVELNO = 2
            LEVELNAME = 3
            PATHNAME = 4
            FILENAME = 5
            MODULE = 6
            LINENO = 7
            FUNCNAME = 8
            CREATED = 9
            ASCTIME = 10
            MSECS = 11
            RELATIVE_CREATED = 12
            THREAD = 13
            THREADNAME = 14
            PROCESS = 15
            MESSAGE = 16

        The fields will show in the order you provide them.  A 
        special 'detail' field will include args[0] from your log
        message assuming that args[0] is a dict().  If an 
        exception is logged or a stacktract is included, they will
        use the special fields 'exception' and 'stack' respectively.
        'detail', 'exception', and 'stack' are always tacked on the 
        the end of the log entry.
        """
        self.fields = list(fields)
        self.include_asc_time = ASCTIME in self.fields

    def formatMessage(self, record: LogRecord):

        #only add asctime if it's been included
        if hasattr(self, "include_asc_time") and self.include_asc_time:
            record.asctime = self.formatTime(record, self.datefmt)
        log_dict = dict()
        for x in self.fields:
            label = JangleJSONFormatter._field_mappings[x]
            log_dict[label] = getattr(record, label)
        if (isinstance(record.args, Mapping)):
            log_dict["detail"] = record.args
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            log_dict["exception"] = record.exc_text
        if record.stack_info:
            log_dict["stack"] = self.formatStack(record.stack_info)

        record.exc_info = None
        record.exc_text = None
        record.stack_info = None

        return json.dumps(log_dict, indent=4, cls=UUIDEncoder)
    


def initialize_jangle_logging(*included_fields: int, logging_module:str|None = None, logging_level: int = logging.DEBUG): 
    """Initializes pyJangle JSON logging.
    
    PARAMETERS
    ----------
    logging_module
        Set the logging module that you'd like to configure.  
        Typically, you'll just configure the root module 
        which is the default.
    
    logging_level
        Specify the logging level as defined in the logging 
        package.

    included_fields
        These will be logged in the order you specify them.

        Possible options are:
            NAME = 1
            LEVELNO = 2
            LEVELNAME = 3
            PATHNAME = 4
            FILENAME = 5
            MODULE = 6
            LINENO = 7
            FUNCNAME = 8
            CREATED = 9
            ASCTIME = 10
            MSECS = 11
            RELATIVE_CREATED = 12
            THREAD = 13
            THREADNAME = 14
            PROCESS = 15
            MESSAGE = 16

        The fields will show in the order you provide them.  A 
        special 'detail' field will include args[0] from your log
        message assuming that args[0] is a dict().  If an 
        exception is logged or a stacktract is included, they will
        use the special fields 'exception' and 'stack' respectively.
        'detail', 'exception', and 'stack' are always tacked on the 
        the end of the log entry.
    """
    if not included_fields:
        included_fields = (NAME, LEVELNO, LEVELNAME, PATHNAME, FILENAME, MODULE, LINENO, FUNCNAME, CREATED, ASCTIME, MSECS, RELATIVE_CREATED, THREAD, THREADNAME, PROCESS, MESSAGE)
    formatter = JangleJSONFormatter()
    formatter.set_included_fields(*included_fields)
    logger = logging.getLogger(logging_module)
    logger.setLevel(logging_level)
    logging.info("--pyJangle logging initialized--")
    for handler in logger.handlers:
        handler.setFormatter(formatter)

class Toggles:
    class Debug:
        log_post_new_event = True
        log_event_applied_to_aggregate = True
        log_saga_retrieved = True
        log_saga_committed = True
        log_apply_event_to_saga = True
        log_is_snapshotting = True
        log_is_snapshot_found = True
        log_snapshot_applied = True
        log_snapshot_not_needed = True
        log_dispatched_event_locally = True
        log_retrieved_aggregate_events = True
        log_aggregate_created = True
        
    class Info:
        log_command_validator_method_name_caching = True
        log_state_reconstitutor_method_name_caching = True
        log_command_dispatcher_registration = True
        log_event_dispatcher_registration = True
        log_event_handler_registration = True
        log_event_repository_registration = True
        log_query_handler_registration = True
        log_snapshot_repository_registration = True
        log_saga_repository_registration = True
        log_command_validation_succeeded = True
        log_command_validation_failed = True
        log_command_registered_to_aggregate = True
        log_retrying_sagas = True
        log_retrying_failed_events = True
        log_snapshot_deleted = True
        log_snapshot_taken = True
        log_committed_event = True
        log_command_received = True

    class Warning:
        log_snapshot_application_failed = True