# import inspect
# from typing import Callable
# from pyjangle import JangleError
# from pyjangle.logging.logging import LogToggles, log

# _snapshot_serializer = None
# _snapshot_deserializer = None


# class SnapshotSerializerBadSignatureError(JangleError):
#     "Snapshot serializer signature is invalid."
#     pass


# class SnapshotSerializerMissingError(JangleError):
#     "Snapshot serializer not registered."
#     pass


# class SnapshotDeserializerBadSignatureError(JangleError):
#     "Snapshot deserializer signature is invalid."
#     pass


# class SnapshotDeserializerMissingError(JangleError):
#     "Snapshot deserializer not registered."
#     pass


# def register_snapshot_serializer(wrapped: Callable[[any], None]):
#     """Registers a snapshot serializer.

#     Wraps a function that can serialize snapshots.  The wrapped function's output should
#     be in the format expected by the registered snapshot repository.

#     Signature:
#         def func_name(snapshot: Snapshot) -> any:

#     Raises:
#         SnapshotSerializerBadSignatureError:
#             Snapshot serializer signature is invalid.
#     """

#     global _snapshot_serializer
#     if not inspect.isfunction(wrapped):
#         raise SnapshotSerializerBadSignatureError("Decorated member is not a function")
#     if len(inspect.signature(wrapped).parameters) != 1:
#         raise SnapshotSerializerBadSignatureError(
#             "@register_snapshot_serializer must decorate a method with 1 parameters: snapshot: Snapshot"
#         )
#     if _snapshot_serializer:
#         raise SnapshotSerializerBadSignatureError(
#             f"A serializer is already registered: {str(_snapshot_serializer)}"
#         )
#     _snapshot_serializer = wrapped
#     log(
#         LogToggles.serializer_registered,
#         "Serializer registered",
#         {"serializer", wrapped.__module__ + "." + wrapped.__name__},
#     )
#     return wrapped


# def register_snapshot_deserializer(wrapped: Callable[[any], None]):
#     """Registers a snapshot deserializer.

#     Wraps a function that can deserialize snapshots.  This will typically be used by the
#     snapshot repository.  The `serialized_snapshot` parameter will contain whatever is
#     provided from the snapshot repository.  See the snapshot repository documentation
#     for details.

#     Signature:
#         def func_name(serialized_snapshot: any) -> Snapshot:

#     Raises:
#         SnapshotDeserializerBadSignatureError:
#             Snapshot deserializer signature is invalid.
#     """

#     global _snapshot_deserializer
#     if not inspect.isfunction(wrapped):
#         raise SnapshotDeserializerBadSignatureError(
#             "Decorated member is not a function"
#         )
#     if len(inspect.signature(wrapped).parameters) != 1:
#         raise SnapshotDeserializerBadSignatureError(
#             "@register_snapshot_deserializer must decorate a method with 1 parameters: serialized_snapshot: any"
#         )
#     if _snapshot_deserializer:
#         raise SnapshotDeserializerBadSignatureError(
#             f"A deserializer is already registered: {str(type(_snapshot_deserializer))}"
#         )
#     _snapshot_deserializer = wrapped
#     log(
#         LogToggles.deserializer_registered,
#         "Deserializer registered",
#         {"deserializer", wrapped.__module__ + "." + wrapped.__name__},
#     )
#     return wrapped


# def get_snapshot_serializer():
#     """Returns snapshot serializer registered with @register_snapshot_serializer.

#     Raises:
#         SnapshotSerializerMissingError:
#             Snapshot serializer not registered.
#     """

#     if not _snapshot_serializer:
#         raise SnapshotSerializerMissingError(
#             "Snapshot serializer not registered with @register_snapshot_serializer"
#         )
#     return _snapshot_serializer


# def get_snapshot_deserializer():
#     """Returns snapshot deserializer registered with @register_snapshot_deserializer.

#     Raises:
#         SnapshotDeserializerMissingError:
#             Snapshot deserializer not registered.
#     """

#     if not _snapshot_deserializer:
#         raise SnapshotDeserializerMissingError(
#             "Snapshot deserializer has not been registered with @register_snapshot_deserializer"
#         )
#     return _snapshot_deserializer
