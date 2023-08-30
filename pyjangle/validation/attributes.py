from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal


class ImmutableAttributeValidator(ABC):

    def __set_name__(self, owner, name):
        self.private_name = '_' + name

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
