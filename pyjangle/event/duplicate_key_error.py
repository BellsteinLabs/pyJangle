from pyjangle import JangleError


class DuplicateKeyError(JangleError):
    "Primary key constraint was violated."
    pass
