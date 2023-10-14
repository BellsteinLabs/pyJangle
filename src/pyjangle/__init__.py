"""Components for developing event-based applications.

This package includes components that facilitate event-based applications through 
established designed patterns and architectural styles such as Sagas, CQRS, DDD, and 
immutable DTOs.  Familiarity with those concepts may be beneficial.  All of the 
important components in this library have detailed docstrings on their usage.  All 
decorators, there are several of them, provide detailed descriptions of the function 
signatures of the functions they should decorate.

Quickstart:
- This library requires several different components to be registered to successfully 
  process data.  The `initialize` module is the quickest and most concise way to get 
  started.  See the `example` package for a reference implementation.

- It is important to import all modules containing events, sagas, aggregates, etc are
  imported at some point before command/event processing begins.  Importing these 
  modules is what registers components, assuming they are decorated correctly.*

- The `pyjangle_json_logging` package (or an equivalent) is very strongly recommended.  
  It provides a very detailed look into the innerworkings of pyjangle at runtime and 
  post-runtime in an easily readable format that is also understood by the many logging 
  tools that can parse and index json logs.  This step will trivialize debugging and 
  troubleshooting.

- To create an aggregate, see the docstring on the `Aggregate` class.

- If snapshotting is desired, your aggregate should also implement `Snapshottable`, and 
  there should be a registered `SnapshotRepository`.

- Use the `handle_command` function as your registered `command_dispatcher` (this is the 
  default value in the `initialize` module).  The command handler does the lionshare of 
  the orchestration of application components, and you will generally not want to 
  implement this yourself.

- Defining a command requires extending the `Command` class.

- The `begin_retry_failed_events_loop` task is a convenient way to ensure that failed 
  events are eventually retried.

- You may opt to have a separate process handle committed events to update database 
  tables with queryable data.  Regardless of where in your architecture this activity 
  takes place, the `default_event_dispatcher` is most likely the component that you will
  want to register via `register_event_dispatcher` which is the default option in the 
  `initialize` module.

- Defining events requires that you extend the `Event` class and decorate the event with
  `register_event`.  

- Use `register_event_handler` to specify what should happen once an event is committed 
  to storage.

- Your registered event repository, see `RegisterEventRepository`, is the mechanism that 
  persists your events.  The interface is relatively straightforward to implement, and 
  there is a reference implementation in the `pyjangle_seqlite3` package.

- Your registered saga repository, see `RegisterSagaRepository`, is the mechanism that 
  persists your sagas.  The interface is relatively straightforward to implement, and 
  there is a reference implementation in the `pyjangle_seqlite3` package.

- Your registered snapshot repository, see `RegisterSnapshotRepository`, is the 
  mechanism that persists your snapshots.  The interface is relatively straightforward 
  to implement, and there is a reference implementation in the `pyjangle_seqlite3` 
  package.

- Use `register_query_handler` to define how each query should be handled.

- Defining a saga requires extending from the `Saga` class and decorating the saga using 
  `RegisterSaga`.  Event handlers for events that should be routed to a saga can call 
  the `handle_saga_event` method for easy orchestration.

- Provide the necessary serializer and deserializer for events and snapshots using the 
  `register_serializer` and `register_deserializer` decorators.

- To enforce the immutability of queries, commands, and events, it is highly recommended 
  to take advantage of `ImmutableAttributeDescriptor` when creating instance fields to 
  ensure that they are both read-only and valid.  It should never be the case that a 
  query, command, or event is instantiated in an inconsistent state.  See the reference 
  implementation, `example`, for an example.

- Settings are defined in the settings module or easily specified using the `initialize`
  module.  Settings can also be specified using environment variables.  The specific 
  variables are:
  
    BATCH_SIZE
    EVENTS_READY_FOR_DISPATCH_QUEUE_SIZE
    FAILED_EVENTS_RETRY_INTERVAL
    FAILED_EVENTS_MAX_AGE
"""

from .error.error import JangleError
from .logging.logging import (
    log,
    LogToggles,
    ERROR,
    FATAL,
    WARNING,
    INFO,
    DEBUG,
    NAME,
    LEVELNO,
    LEVELNAME,
    PATHNAME,
    FILENAME,
    MODULE,
    LINENO,
    FUNCNAME,
    CREATED,
    ASCTIME,
    MSECS,
    RELATIVE_CREATED,
    THREAD,
    THREADNAME,
    PROCESS,
    MESSAGE,
)
from .settings import (
    get_batch_size,
    set_batch_size,
    set_events_ready_for_dispatch_queue_size,
    get_events_ready_for_dispatch_queue_size,
    set_saga_retry_interval,
    get_saga_retry_interval,
    get_failed_events_retry_interval,
    set_failed_events_retry_interval,
    get_failed_events_max_age,
    set_failed_events_max_age,
)
from .registration.utility import find_decorated_method_names, register_instance_methods
from .registration.background_tasks import background_tasks

from .snapshot.snapshot_repository import (
    DuplicateSnapshotRepositoryError,
    SnapshotRepositoryMissingError,
    RegisterSnapshotRepository,
    SnapshotRepository,
    snapshot_repository_instance,
)

from .snapshot.snapshottable import SnapshotError, Snapshottable

from .snapshot.in_memory_snapshot_repository import InMemorySnapshotRepository

from .event.register_event_id_factory import (
    DuplicateEventIdFactoryRegistrationError,
    EventIdRegistrationFactoryBadSignatureError,
    default_event_id_factory,
    event_id_factory_instance,
    register_event_id_factory,
)
from .event.event import Event, VersionedEvent
from .event.duplicate_key_error import DuplicateKeyError

from .command.command_response import CommandResponse
from .command.command import Command
from .command.command_dispatcher import (
    register_command_dispatcher,
    command_dispatcher_instance,
    CommandDispatcherBadSignatureError,
    DuplicateCommandDispatcherError,
    CommandDispatcherNotRegisteredError,
)


from .event.event_repository import (
    RegisterEventRepository,
    EventRepository,
    event_repository_instance,
    DuplicateEventRepositoryError,
    EventRepositoryMissingError,
)

from .aggregate.aggregate import (
    COMMAND_TYPE_ATTRIBUTE_NAME,
    EVENT_TYPE_ATTRIBUTE_NAME,
    Aggregate,
    ValidateCommandMethodMissingError,
    CommandValidatorBadSignatureError,
    ReconstituteStateMethodMissingError,
    ReconstituteStateError,
    CommandValidationError,
    register_instance_methods,
    reconstitute_aggregate_state,
    validate_command,
)

from .aggregate.register_aggregate import (
    command_to_aggregate_map_instance,
    DuplicateCommandRegistrationError,
    AggregateRegistrationError,
    RegisterAggregate,
)

from .event.register_event import (
    EventRegistrationError,
    DuplicateEventNameRegistrationError,
    RegisterEvent,
    get_event_name,
    get_event_type,
)

from .event.event_handler import (
    EventHandlerError,
    EventHandlerMissingError,
    EventHandlerBadSignatureError,
    register_event_handler,
    event_type_to_handler_instance,
    has_registered_event_handler,
)
from .event.event_dispatcher import (
    begin_processing_committed_events,
    enqueue_committed_event_for_dispatch,
    EventDispatcherMissingError,
    DuplicateEventDispatcherError,
    EventDispatcherBadSignatureError,
    register_event_dispatcher,
    event_dispatcher_instance,
    default_event_dispatcher,
    default_event_dispatcher_with_blacklist,
)

from .command.command_handler import handle_command

from .event.event_daemon import begin_retry_failed_events_loop, retry_failed_events
from .event.in_memory_event_repository import InMemoryEventRepository

from .query.handlers import (
    QueryHandlerRegistrationBadSignatureError,
    DuplicateQueryRegistrationError,
    QueryHandlerMissingError,
    register_query_handler,
    handle_query,
)

from .saga.saga_not_found_error import SagaNotFoundError
from .saga.saga import (
    ReconstituteSagaStateBadSignatureError,
    EventReceiverBadSignatureError,
    EventRceiverMissingError,
    ReconstituteSagaStateMissingError,
    reconstitute_saga_state,
    event_receiver,
    Saga,
)
from .saga.register_saga import (
    SagaRegistrationError,
    DuplicateSagaNameError,
    RegisterSaga,
    get_saga_name,
    get_saga_type,
)
from .saga.saga_repository import (
    SagaRepositoryMissingError,
    DuplicateSagaRepositoryError,
    RegisterSagaRepository,
    SagaRepository,
    saga_repository_instance,
)
from .saga.in_memory_transient_saga_repository import InMemorySagaRepository
from .saga.saga_handler import handle_saga_event
from .saga.saga_daemon import (
    retry_sagas,
    begin_retry_sagas_loop,
    retry_saga,
    SagaRetryError,
)

from .serialization.serialization_registration import (
    SerializerBadSignatureError,
    SerializerMissingError,
    DeserializerBadSignatureError,
    DeserializerMissingError,
    register_serializer,
    register_deserializer,
    get_serializer,
    get_deserializer,
)


from .validation.attributes import ImmutableAttributeDescriptor

from .initialize import initialize_pyjangle, init_background_tasks
