SNAPSHOT_REPO = "pyjangle.snapshot.snapshot_repository._registered_snapshot_repository"
EVENT_REPO = "pyjangle.event.event_repository._event_repository_instance"
EVENT_DISPATCHER = "pyjangle.event.event_dispatcher._event_dispatcher"
COMMAND_DISPATCHER = "pyjangle.command.command_dispatcher._command_dispatcher_instance"
COMMAND_TO_AGGREGATE_MAP = (
    "pyjangle.aggregate.register_aggregate._command_to_aggregate_map"
)
COMMITTED_EVENT_QUEUE = "pyjangle.event.event_dispatcher._committed_event_queue"
NAME_TO_EVENT_TYPE_MAP = "pyjangle.event.register_event._name_to_event_type_map"
EVENT_TYPE_TO_NAME_MAP = "pyjangle.event.register_event.__event_type_to_name_map"
QUERY_TYPE_TO_QUERY_HANDLER_MAP = (
    "pyjangle.query.handlers._query_type_to_query_handler_map"
)
EVENT_SERIALIZER = (
    "pyjangle.serialization.event_serialization_registration._event_serializer"
)
EVENT_DESERIALIZER = (
    "pyjangle.serialization.event_serialization_registration._event_deserializer"
)
SNAPSHOT_SERIALIZER = (
    "pyjangle.serialization.snapshot_serialization_registration._snapshot_serializer"
)
SNAPSHOT_DESERIALIZER = (
    "pyjangle.serialization.snapshot_serialization_registration._snapshot_deserializer"
)
SAGA_SERIALIZER = (
    "pyjangle.serialization.saga_serialization_registration._saga_serializer"
)
SAGA_DESERIALIZER = (
    "pyjangle.serialization.saga_serialization_registration._saga_deserializer"
)
SAGA_REPO = "pyjangle.saga.saga_repository._registered_saga_repository"
NAME_TO_SAGA_TYPE_MAP = "pyjangle.saga.register_saga.__name_to_saga_type_map"
SAGA_TYPE_TO_NAME_MAP = "pyjangle.saga.register_saga.__saga_type_to_name_map"
EVENT_TYPE_TO_EVENT_HANDLER_MAP = (
    "pyjangle.event.event_handler._event_type_to_event_handler_handler_map"
)
EVENT_ID_FACTORY = "pyjangle.event.register_event_id_factory._event_id_factory"
