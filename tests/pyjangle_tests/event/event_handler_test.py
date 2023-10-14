from unittest import IsolatedAsyncioTestCase

from pyjangle import (
    VersionedEvent,
    EventHandlerMissingError,
    EventHandlerBadSignatureError,
    default_event_dispatcher,
    register_event_handler,
)
from test_helpers.events import EventA
from test_helpers.reset import ResetPyJangleState


@ResetPyJangleState
class TestEventHandler(IsolatedAsyncioTestCase):
    async def test_can_register_multiple_handlers_for_same_event(self, *_):
        self.calls = 0

        @register_event_handler(EventA)
        async def foo(event: VersionedEvent):
            self.calls += 1

        @register_event_handler(EventA)
        async def bar(event: EventA):
            self.calls += 1

        await default_event_dispatcher(
            EventA(id=1, version=1, created_at=None), empty_event_completer
        )
        self.assertEqual(self.calls, 2)

    async def test_no_handler_registered(self, *_):
        with self.assertRaises(EventHandlerMissingError):
            await default_event_dispatcher(
                EventA(id=1, version=1, created_at=None), empty_event_completer
            )

    async def test_event_handler_wrong_parameter_count(self, *_):
        with self.assertRaises(EventHandlerBadSignatureError):

            @register_event_handler(EventA)
            async def foo(_, __):
                pass

    async def test_event_handler_is_not_coroutine(self, *_):
        with self.assertRaises(EventHandlerBadSignatureError):

            @register_event_handler(EventA)
            def foo(_, __):
                pass

    async def test_event_handler_is_not_callable(self, *_):
        with self.assertRaises(EventHandlerBadSignatureError):

            @register_event_handler(EventA)
            class Foo:
                pass

    async def test_failed_event_handler_reraises_exception(self, *_):
        @register_event_handler(EventA)
        async def foo(event):
            raise Exception

        with self.assertRaises(Exception):
            await default_event_dispatcher(EventA(id=1, version=1, created_at=None))


async def empty_event_completer(_):
    pass
