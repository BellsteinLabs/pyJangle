from asyncio import Queue
from unittest.mock import patch
from pyjangle import default_event_id_factory

from pyjangle.test.registration_paths import (
    COMMAND_DISPATCHER,
    COMMAND_TO_AGGREGATE_MAP,
    COMMITTED_EVENT_QUEUE,
    EVENT_DISPATCHER,
    EVENT_ID_FACTORY,
    EVENT_REPO,
    SNAPSHOT_REPO,
    EVENT_TYPE_TO_NAME_MAP,
    NAME_TO_EVENT_TYPE_MAP,
    EVENT_TYPE_TO_EVENT_HANDLER_MAP,
    QUERY_TYPE_TO_QUERY_HANDLER_MAP,
    NAME_TO_SAGA_TYPE_MAP,
    SAGA_TYPE_TO_NAME_MAP,
    SAGA_REPO,
)
from pyjangle.test.transient_event_repository import TransientEventRepository
from pyjangle.test.transient_saga_repository import TransientSagaRepository
from pyjangle.test.transient_snapshot_repository import TransientSnapshotRepository


def ResetPyJangleState(cls):
    return patch(SAGA_REPO, new_callable=lambda: TransientSagaRepository())( patch.dict(SAGA_TYPE_TO_NAME_MAP)(
        patch.dict(NAME_TO_SAGA_TYPE_MAP)(
            patch.dict(QUERY_TYPE_TO_QUERY_HANDLER_MAP)(
                patch.dict(EVENT_TYPE_TO_EVENT_HANDLER_MAP)(
                    patch.dict(EVENT_TYPE_TO_NAME_MAP)(
                        patch.dict(NAME_TO_EVENT_TYPE_MAP)(
                            patch(EVENT_ID_FACTORY, new=default_event_id_factory)(
                                patch(COMMAND_DISPATCHER, None)(
                                    patch(
                                        COMMITTED_EVENT_QUEUE,
                                        new_callable=lambda: Queue(),
                                    )(
                                        patch(EVENT_DISPATCHER, None)(
                                            patch(
                                                EVENT_REPO,
                                                new_callable=lambda: TransientEventRepository(),
                                            )(
                                                patch(
                                                    SNAPSHOT_REPO,
                                                    new_callable=lambda: TransientSnapshotRepository(),
                                                )(
                                                    patch.dict(
                                                        COMMAND_TO_AGGREGATE_MAP
                                                    )(cls)
                                                )
                                            )
                                        )
                                    )
                                )
                            ))
                        )
                    )
                )
            )
        )
    )
