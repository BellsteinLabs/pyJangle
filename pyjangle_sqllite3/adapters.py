from datetime import datetime
from decimal import Decimal
from sqlite3 import register_adapter, register_converter


def adapt_datetime(datetime: datetime):
    return datetime.isoformat()


def convert_datetime(bytes: bytes):
    return datetime.fromisoformat(bytes.decode())


def adapt_decimal(value: Decimal):
    return str(value)


def convert_decimal(bytes: bytes):
    return Decimal(bytes.decode())


def register_all():
    register_adapter(datetime, adapt_datetime)
    register_converter(datetime.__name__, convert_datetime)
    register_adapter(Decimal, adapt_decimal)
    register_converter(Decimal.__name__, convert_decimal)
