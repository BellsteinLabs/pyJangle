"""Descriptors for query, event, and command fields.

Queries, events, and commands must be immutable, and these descriptors ensure that each
field is both validated and read-only to ensure that the aforementioned classes are 
always consistent and valid."""

from datetime import datetime
from decimal import Decimal
from uuid import uuid4
from pyjangle import ImmutableAttributeDescriptor


def validate_type(value: any, t: type):
    if not isinstance(value, t):
        raise TypeError(f"Expected {str(value)} to be a {str(t)}")


def validate_min(value: any, min, inclusive=True):
    if value < min or (not inclusive and value == min):
        raise ValueError(
            f"Expected {str(value)} to be no less than {str(min)}" + ", inclusive"
            if inclusive
            else ""
        )


def validate_min_length(value: any, min_length):
    if len(value) < min_length:
        raise ValueError(
            f"Expected length of {str(value)} to be more than {str(min_length)}"
        )


def validate_max_length(value: any, max_length):
    if len(value) > max_length:
        raise ValueError(
            f"Expected length of {str(value)} to be less than {str(max_length)}"
        )


def validate_max(value: any, max, inclusive=True):
    if value > max or (not inclusive and value == max):
        raise ValueError(
            f"Expected {str(value)} to be no more than {str(max)}" + ", inclusive"
            if inclusive
            else ""
        )


def validate_not_None(value):
    if value == None:
        raise ValueError(f"Expected {str(value)} to be set to a value")


class AccountName(ImmutableAttributeDescriptor):
    def validate(self, value):
        validate_type(value, str)
        validate_not_None(value)
        validate_min_length(value, 5)
        validate_max_length(value, 15)


class Amount(ImmutableAttributeDescriptor):
    def __init__(self, can_be_none=False):
        self.can_be_none = can_be_none

    def validate(self, value):
        if self.can_be_none and value == None:
            return
        validate_not_None(value)
        validate_type(value, Decimal)
        validate_min(value, Decimal(0), inclusive=False)
        validate_max(value, Decimal(1000000))


class Balance(ImmutableAttributeDescriptor):
    def validate(self, value):
        validate_not_None(value)
        validate_type(value, Decimal)


class AccountId(ImmutableAttributeDescriptor):
    def validate(self, value):
        validate_type(value, str)
        validate_not_None(value)
        validate_min_length(value, 6)
        validate_max_length(value, 6)


class TransactionId(ImmutableAttributeDescriptor):
    def __init__(self, create_id=False):
        self.create_id = create_id

    def __get__(self, instance, instance_type=None):
        if instance is None:
            return None
        if not hasattr(instance, self.private_name) or (
            not getattr(instance, self.private_name) and self.create_id
        ):
            setattr(instance, self.private_name, str(uuid4()))
        return getattr(instance, self.private_name)

    def validate(self, value):
        if value is None and self.create_id:
            return
        validate_type(value, str)
        validate_not_None(value)
        validate_min_length(value, 1)
        validate_max_length(value, 50)


class Timeout(ImmutableAttributeDescriptor):
    def validate(self, value):
        validate_type(value, (datetime, type(None)))
