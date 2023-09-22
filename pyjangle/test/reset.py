from asyncio import Queue
from unittest.mock import patch

from pyjangle.test.registration_paths import (
    COMMAND_DISPATCHER,
    COMMAND_TO_AGGREGATE_MAP,
    COMMITTED_EVENT_QUEUE,
    EVENT_DISPATCHER,
    EVENT_REPO,
    SNAPSHOT_REPO,
)
from pyjangle.test.transient_event_repository import TransientEventRepository
from pyjangle.test.transient_snapshot_repository import TransientSnapshotRepository


def ResetPyJangleState(cls):
    return patch(COMMAND_DISPATCHER, None)(
        patch(COMMITTED_EVENT_QUEUE, new_callable=lambda: Queue())(
            patch(EVENT_DISPATCHER, None)(
                patch(EVENT_REPO, new_callable=lambda: TransientEventRepository())(
                    patch(
                        SNAPSHOT_REPO,
                        new_callable=lambda: TransientSnapshotRepository(),
                    )(patch.dict(COMMAND_TO_AGGREGATE_MAP)(cls))
                )
            )
        )
    )
