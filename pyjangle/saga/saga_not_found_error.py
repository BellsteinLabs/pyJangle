from pyjangle import JangleError


class SagaNotFoundError(JangleError):
    "Saga with specified id not found."
    pass
