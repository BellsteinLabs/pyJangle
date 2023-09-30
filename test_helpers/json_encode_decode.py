"""JSON Encoders and decoders that map python types to/from serializable representations

`CustomJSONEncoder` encodes `datetime` and `Decimal` to string format to be compatible
with the sqlite3 persistence mechanisms.

`CustomJSONDecoder` decodes from string format, used by sqlite3, to `Decimal` and 
`datetime`.

Usage Example:

    register_serializer(lambda event: dumps(asdict(event), cls=CustomJSONEncoder))
    register_deserializer(lambda encoded: loads(encoded, cls=CustomJSONDecoder))
"""

from datetime import datetime
from decimal import Decimal
import json


class CustomJSONEncoder(json.JSONEncoder):
    "Converts `datetime` and `Decimal` strings (ISO format in the case of `Decimal`)."

    def default(self, data):
        if isinstance(data, Decimal):
            return str(data)
        if isinstance(data, datetime):
            return data.isoformat()
        return super(CustomJSONEncoder, self).default(data)


class CustomJSONDecoder(json.JSONDecoder):
    """Converts json to python types.

    If a field name is balance or amount, the field is assumed to be a `Decimal`.  If
    a field name ends with '_at', the field is assumed to be a `datetime`.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        for k in obj:
            if k == "balance":
                obj[k] = Decimal(obj[k])
            if k == "ammount":
                obj[k] = Decimal(obj[k])
            if k.endswith("_at"):
                obj[k] = datetime.fromisoformat(obj[k])
        return obj
