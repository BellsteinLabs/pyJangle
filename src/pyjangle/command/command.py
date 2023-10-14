import abc


class Command(metaclass=abc.ABCMeta):
    """An intent to change the state of a domain.

    A command is a request to change the state of the domain, and consequently, it can
    either be accepted or denied.  This determination is typically made by an aggregate.
    Commands should use imperative naming (ex: TurnRight, DepositFunds, CancelRequest).
    Because commands are mapped to aggregates, they require an implementation of
    `get_aggregate_id` to map to a specific aggregate instance.

    Extending this class provides a natural place for tacking on correlation IDs and
    user IDs since the command is a natural entry-point into the system, and those IDs
    can be easily trickled down to all derivative events and commands.

    While the aggregate ID maps the command to a specific type, the `RegisterAggregate`
    class decorator on the corresponding aggregate class ensures the command is mapped
    to the correct *type* of aggregate.
    """

    @abc.abstractmethod
    def get_aggregate_id(self):
        """An id used to associate the command to an aggregate instance."""
        pass
