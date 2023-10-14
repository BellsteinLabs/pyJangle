"""Simple concrete implementations that are used in many test scenarios."""

import os
import sys

PROJECT_PATH = os.getcwd()
SOURCE_PATH = os.path.join(PROJECT_PATH, "src")
sys.path.append(SOURCE_PATH)


from .aggregates import SnapshottableTestAggregate, NotSnapshottableTestAggregate
from .commands import (
    CommandThatShouldSucceedA,
    CommandThatShouldFail,
    CommandThatShouldSucceedB,
    CommandThatShouldSucceedC,
    CommandThatShouldErrorOnlyFirstTime,
)
from .json_encode_decode import CustomJSONDecoder, CustomJSONEncoder
from .events import (
    EventA,
    EventB,
    EventThatCompletesSaga,
    EventThatContinuesSaga,
    EventThatTimesOutSaga,
    EventThatCompletesACommand,
    EventThatSetsSagaToTimedOut,
    EventThatCausesDuplicateKeyError,
    EventThatCausesSagaToRetry,
    TestSagaEvent,
)
from .registration_paths import (
    SNAPSHOT_REPO,
    EVENT_REPO,
    EVENT_DISPATCHER,
    COMMAND_DISPATCHER,
    COMMAND_TO_AGGREGATE_MAP,
    COMMITTED_EVENT_QUEUE,
    NAME_TO_EVENT_TYPE_MAP,
    EVENT_TYPE_TO_NAME_MAP,
    QUERY_TYPE_TO_QUERY_HANDLER_MAP,
    SERIALIZER,
    DESERIALIZER,
    SAGA_REPO,
    NAME_TO_SAGA_TYPE_MAP,
    SAGA_TYPE_TO_NAME_MAP,
    EVENT_TYPE_TO_EVENT_HANDLER_MAP,
    EVENT_ID_FACTORY,
)
from .reset import ResetPyJangleState
from .sagas import SagaForTestingRetryLogic, SagaForTesting
