import logging
import os
from pyjangle import log


def _get_integer_env_var(name, default):
    try:
        raw = os.getenv(name, default)
        return int(raw)
    except ValueError as e:
        log(
            logging.ERROR,
            f"Specify an integer for Environment variable {name}. '{raw}' is invalid",
        )
        raise


# Number of db records to buffer in memory concurrently.
_BATCH_SIZE = _get_integer_env_var("BATCH_SIZE", "100")


def get_batch_size():
    "Get the number of database records to concurrently buffer in memory per query."
    return _BATCH_SIZE


def set_batch_size(batch_size: int):
    "Set the number of database records to concurrently buffer in memory per query."
    global _BATCH_SIZE
    _BATCH_SIZE = batch_size


# When events are committed, and if there is a registered event dispatcher, events are
# put onto an in-memory queue pending an eventual dequeue prior to being dispatched.
# This value represents the maximum size of the queue.
_EVENTS_READY_FOR_DISPATCH_QUEUE_SIZE = _get_integer_env_var(
    "EVENTS_READY_FOR_DISPATCH_QUEUE_SIZE", "200"
)


def get_events_ready_for_dispatch_queue_size():
    "Gets the size of the queue holding events that are ready to be dispatched."
    return _EVENTS_READY_FOR_DISPATCH_QUEUE_SIZE


def set_events_ready_for_dispatch_queue_size(size: int):
    "Sets the size of the queue holding events that are ready to be dispatched."
    global _EVENTS_READY_FOR_DISPATCH_QUEUE_SIZE
    _EVENTS_READY_FOR_DISPATCH_QUEUE_SIZE = size


# Name of the environment variable used to specify the saga retry interval.
_saga_retry_interval = _get_integer_env_var("SAGA_RETRY_INTERVAL", "30")


def get_saga_retry_interval():
    "Gets the saga retry interval."
    return _saga_retry_interval


def set_saga_retry_interval(seconds: int):
    "Sets the saga retry interval."
    global _saga_retry_interval
    _saga_retry_interval = seconds


# Name of the environment variable used to specify the failed events retry interval.
_failed_events_retry_interval = _get_integer_env_var(
    "FAILED_EVENTS_RETRY_INTERVAL", "30"
)


def get_failed_events_retry_interval():
    "Gets the failed events retry interval."
    return _failed_events_retry_interval


def set_failed_events_retry_interval(seconds: int):
    "Sets the failed events retry interval."
    global _failed_events_retry_interval
    _failed_events_retry_interval = seconds


# Events older than this value that haven't been marked as handled are considered to be
# failed events.
_failed_events_max_age = _get_integer_env_var("FAILED_EVENTS_MAX_AGE", "30")


def get_failed_events_max_age():
    "Gets the maximum age of an unhandeled event that isn't considered failed."
    return _failed_events_max_age


def set_failed_events_max_age(max_age: int):
    "Sets the maximum age of an unhandeled event that isn't considered failed."
    global _failed_events_max_age
    _failed_events_max_age = max_age
