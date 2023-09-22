class CommandResponse:
    """The result of validating a command on an aggregate."""

    def __init__(self, is_success: bool, data: object = None) -> None:
        self.is_success = is_success
        self.data = data
