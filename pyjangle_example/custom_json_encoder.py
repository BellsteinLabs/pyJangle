from decimal import Decimal
import json


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, data):
        if isinstance(data, Decimal):
            return str(data)
        return super(CustomJSONEncoder, self).default(data)
