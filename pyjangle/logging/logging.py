
import logging


DEBUG = "debug"
INFO = "info"
WARNING = "warning"
ERROR = "error"
FATAL = "fatal"

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

def log(log_key, *args, **kwargs):
    logger = getattr(logging, log_key)
    logger(*args, **kwargs)

class LogToggles:

    post_new_event = DEBUG
    event_applied_to_aggregate = DEBUG
    saga_new = DEBUG
    saga_retrieved = DEBUG
    saga_committed = DEBUG
    saga_nothing_happened = WARNING
    saga_duplicate_key = WARNING
    apply_event_to_saga = DEBUG
    is_snapshotting = DEBUG
    is_snapshot_found = DEBUG
    snapshot_applied = DEBUG
    snapshot_not_needed = DEBUG
    dispatched_event_locally = DEBUG
    retrieved_aggregate_events = DEBUG
    aggregate_created = DEBUG
    command_validator_method_name_caching = INFO
    state_reconstitutor_method_name_caching = INFO
    command_dispatcher_registration = INFO
    event_handler_failed = ERROR
    event_dispatching_error = ERROR
    event_registered = INFO
    event_dispatcher_registration = INFO
    event_handler_registration = INFO
    event_repository_registration = INFO
    event_dispatcher_ready = INFO
    query_handler_registration = INFO
    snapshot_repository_registration = INFO
    saga_repository_registration = INFO
    command_validation_succeeded = INFO
    command_validation_failed = INFO
    command_registered_to_aggregate = INFO
    retrying_sagas = INFO
    retrying_failed_events = INFO
    snapshot_deleted = INFO
    snapshot_taken = INFO
    committed_event = INFO
    command_received = INFO
    snapshot_application_failed = WARNING
    serializer_registered = INFO
    deserializer_registered = INFO