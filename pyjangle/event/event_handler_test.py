from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch
from pyjangle.event.event import Event

from pyjangle.event.event_handler import EventHandlerError, EventHandlerRegistrationError, handle_event, register_event_handler
from pyjangle.test.test_types import EventA

@patch.dict("pyjangle.event.event_handler.__event_type_to_event_handler_handler_map")
class TestEventHandler(IsolatedAsyncioTestCase):
    async def test_can_register_multiple_handlers_for_same_event(self, *_):
        self.calls = 0
        @register_event_handler(EventA)
        async def foo(event: Event):
            self.calls += 1

        @register_event_handler(EventA)
        async def bar(event: EventA):
            self.calls += 1

        await handle_event(EventA(id=1, version=1, created_at=None))
        self.assertEqual(self.calls, 2)

    
    async def test_no_handler_registered(self, *_):
        with self.assertRaises(EventHandlerError):
            await handle_event(EventA(id=1, version=1, created_at=None))


    async def test_event_handler_wrong_parameter_count(self, *_):
        with self.assertRaises(EventHandlerRegistrationError):
            @register_event_handler(EventA)
            async def foo(_, __): pass

    async def test_event_handler_is_not_coroutine(self, *_):
        with self.assertRaises(EventHandlerRegistrationError):
            @register_event_handler(EventA)
            def foo(_, __): pass

    async def test_event_handler_is_not_callable(self, *_):
        with self.assertRaises(EventHandlerRegistrationError):
            @register_event_handler(EventA)
            class Foo: pass

    async def test_failed_event_handler_catches_exception(self, *_):
        @register_event_handler(EventA)
        async def foo(event): 
            raise Exception
        
        await handle_event(EventA(id=1, version=1, created_at=None))