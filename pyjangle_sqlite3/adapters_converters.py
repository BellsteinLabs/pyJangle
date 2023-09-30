"""Adapters and converters for datetime and Decimal."""

from datetime import datetime
from decimal import Decimal
from sqlite3 import register_adapter, register_converter


def adapt_datetime(datetime: datetime):
    "Adapt datetime to str."
    return datetime.isoformat()


def convert_datetime(bytes: bytes):
    "Convert bytes to datetime."
    return datetime.fromisoformat(bytes.decode())


def adapt_decimal(value: Decimal):
    "Adapt decimal to str."
    return str(value)


def convert_decimal(bytes: bytes):
    "Convert bytes to decimal."
    return Decimal(bytes.decode())