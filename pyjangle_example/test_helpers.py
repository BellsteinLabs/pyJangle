import datetime
from decimal import Decimal
from unittest import TestCase
from unittest.mock import Mock
import uuid

from pyjangle.event.event import VersionedEvent

AGGREGATE_ID = "42"
ACCOUNT_ID = "000042"
OTHER_ACCOUNT_ID = "000043"
ACCOUNT_NAME = "TEST_NAME"
INITIAL_DEPOSIT = Decimal(5)
SMALL_AMOUNT = Decimal(20)
LARGE_AMOUNT = Decimal(150)
KNOWN_UUID = str(uuid.uuid4())
ACCOUNT_ID_FLD = "account_id"
AMOUNT_FLD = "amount"
BALANCE_FLD = "balance"
TRANSACTION_ID_FLD = "transaction_id"
CREATED_AT_FLD = "created_at"
TIMEOUT_AT_FLD = "timeout_at"
FUNDED_ACCOUNT_ID_FLD = "funded_account_id"
FUNDING_ACCOUNT_ID_FLD = "funding_account_id"
FAKE_CURRENT_TIME = datetime.datetime.min
THIRTY_MINUTES_FROM_FAKE_NOW = FAKE_CURRENT_TIME + \
    datetime.timedelta(minutes=30)
NAME_FLD = "name"

datetime_mock = Mock(wraps=datetime)
datetime_mock.now = Mock(return_value=FAKE_CURRENT_TIME)


def get_account_id(number: int):
    return "{:06d}".format(number)


class ExpectedEvent:
    def __init__(self, event_type: type, agg_id: any, version: int, attributes: dict[str, any] = {}, index: int = 0) -> None:
        self.index = index
        self.event_type = event_type
        self.agg_id = agg_id
        self.version = version
        self.attributes = attributes


def verify_events(test_case: TestCase, events: list[tuple[any, VersionedEvent]], expected_events: list[ExpectedEvent] = [], expected_length: int = 1):
    test_case.assertEqual(len(events), expected_length)
    if len(expected_events) != expected_length:
        raise Exception(
            f"Expected_length '{expected_length}' and len(expected) '{str(len(expected_events))}' must be equivalent")
    for i, expected_type, expected_aggregate_id, expected_version, expected_values in [(e.index, e.event_type, e.agg_id, e.version, e.attributes) for e in expected_events]:
        actual_aggregate_id = events[i][0]
        actual_event: VersionedEvent = events[i][1]
        if expected_type:
            test_case.assertIsInstance(
                actual_event, expected_type, f"Expected type '{str(expected_type)}({str(i)})', but got type '{str(type(actual_event))}'")
        if expected_aggregate_id:
            test_case.assertEqual(expected_aggregate_id, actual_aggregate_id,
                                  f"Aggregate ID mismatch on event typed '{str(expected_type)}({str(i)})': Expected '{expected_aggregate_id}', got '{actual_aggregate_id}'")
        if expected_version:
            test_case.assertEqual(expected_version, actual_event.version,
                                  f"Version mismatch on event typed '{str(expected_type)}({str(i)})': Expected '{expected_version}', got '{actual_event.version}'")
        if expected_values:
            for attribute_name, expected_value in expected_values.items():
                if not hasattr(actual_event, attribute_name):
                    raise Exception(
                        f"Event of type '{str(expected_type)}({str(i)})' does not have an attribute named {attribute_name}")
                actual_value = getattr(actual_event, attribute_name)
                test_case.assertEqual(
                    actual_value, expected_value, f"Event typed '{str(expected_type)}({str(i)})': Attribute '{attribute_name}': Expected '{str(expected_value)}', got '{str(actual_value)}'")
