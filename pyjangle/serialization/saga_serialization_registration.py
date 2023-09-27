# import inspect
# from typing import Callable
# from pyjangle import JangleError, LogToggles, log

# _saga_serializer = None
# _saga_deserializer = None


# class SagaSerializerBadSignatureError(JangleError):
#     "Saga serializer signature is invalid."
#     pass


# class SagaSerializerMissingError(JangleError):
#     "Saga serializer not registered."
#     pass


# class SagaDeserializerBadSignatureError(JangleError):
#     "Saga deserializer signature is invalid."
#     pass


# class SagaDeserializerMissingError(JangleError):
#     "Saga deserializer not registered."
#     pass


# def register_saga_serializer(wrapped: Callable[[any], None]):
#     """Registers an saga serializer.

#     Wraps a function that can serialize sagas.  The wrapped function's output should be
#     in the format expected by the registered saga repository.

#     Signature:
#         def func_name(saga: Saga) -> any:

#     Raises:
#         SagaSerializerBadSignatureError:
#             Saga serializer signature is invalid.
#     """

#     global _saga_serializer
#     if not inspect.isfunction(wrapped):
#         raise SagaSerializerBadSignatureError("Decorated member is not a function")
#     if len(inspect.signature(wrapped).parameters) != 1:
#         raise SagaSerializerBadSignatureError(
#             "@register_saga_serializer must decorate a method with 1 parameters: saga: Saga"
#         )
#     if _saga_serializer:
#         raise SagaSerializerBadSignatureError(
#             f"A serializer is already registered: {str(_saga_serializer)}"
#         )
#     _saga_serializer = wrapped
#     log(
#         LogToggles.serializer_registered,
#         "Serializer registered",
#         {"serializer", wrapped.__module__ + "." + wrapped.__name__},
#     )
#     return wrapped


# def register_saga_deserializer(wrapped: Callable[[any], None]):
#     """Registers an saga deserializer.

#     Wraps a function that can deserialize sagas.  This will typically be used by the
#     saga repository.  The `serialized_saga` parameter will contain whatever is
#     provided from the saga repository.  See the saga repository documentation for details.

#     Signature:
#         def func_name(serialized_saga: any) -> Saga:

#     Raises:
#         SagaDeserializerBadSignatureError:
#             Saga deserializer signature is invalid.
#     """

#     global _saga_deserializer
#     if not inspect.isfunction(wrapped):
#         raise SagaDeserializerBadSignatureError("Decorated member is not a function")
#     if len(inspect.signature(wrapped).parameters) != 1:
#         raise SagaDeserializerBadSignatureError(
#             "@register_saga_deserializer must decorate a method with 1 parameters: serialized_saga: any"
#         )
#     if _saga_deserializer:
#         raise SagaDeserializerBadSignatureError(
#             f"A deserializer is already registered: {str(type(_saga_deserializer))}"
#         )
#     _saga_deserializer = wrapped
#     log(
#         LogToggles.deserializer_registered,
#         "Deserializer registered",
#         {"deserializer", wrapped.__module__ + "." + wrapped.__name__},
#     )
#     return wrapped


# def get_saga_serializer():
#     """Returns saga serializer that was registered with @register_saga_serializer.

#     Raises:
#         SagaSerializerMissingError:
#             Saga serializer not registered.
#     """

#     if not _saga_serializer:
#         raise SagaSerializerMissingError(
#             "Saga serializer has not been registered with @register_saga_serializer"
#         )
#     return _saga_serializer


# def get_saga_deserializer():
#     """Returns saga deserializer that was registered with @register_saga_deserializer.

#     Raises:
#         SagaDeserializerMissingError:
#             Saga deserializer not registered.
#     """

#     if not _saga_deserializer:
#         raise SagaDeserializerMissingError(
#             "Saga deserializer has not been registered with @register_saga_deserializer"
#         )
#     return _saga_deserializer
