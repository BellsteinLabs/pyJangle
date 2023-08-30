from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
import json
from typing import Any


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, data):
        if isinstance(data, Decimal):
            return str(data)
        if isinstance(data, datetime):
            return data.isoformat()
        return super(CustomJSONEncoder, self).default(data)


class CustomJSONDecoder(json.JSONDecoder):
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
