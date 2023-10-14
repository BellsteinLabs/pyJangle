from abc import ABC, abstractmethod


class ImmutableAttributeDescriptor(ABC):
    """Descriptor representing a readonly attribute that meets some criteria.

    This descriptor is useful for enforcing the immutability constraint on Commands,
    Queries, and Events.  It can also be used to implement input validation by
    overriding `validate`."""

    def __set_name__(self, owner, name):
        self.private_name = "_" + name

    def __get__(self, instance, instance_type=None):
        return getattr(instance, self.private_name, None)

    def __set__(self, instance, value):
        self.validate(value)
        if hasattr(instance, self.private_name):
            raise AttributeError("Immutable attribute is already set")
        setattr(instance, self.private_name, value)

    @abstractmethod
    def validate(self, value):
        pass
