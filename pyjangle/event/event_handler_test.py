import unittest
from unittest.mock import patch
from pyjangle.event.event import Event

from pyjangle.event.event_handler import EventHandlerError, handle_event, register_event_handler
from pyjangle.test.test_types import EventA

class TestEventHandler(unittest.TestCase):
    count = 0

    @patch("pyjangle.event.event_handler.__event_type_to_event_handler_handler_map", dict())
    def test_can_register_multiple_handlers_for_same_event(self):
        
        @register_event_handler(EventA)
        def foo(event: Event):
            TestEventHandler.count += 1

        @register_event_handler(EventA)
        def bar(event: EventA):
            TestEventHandler.count += 1

        handled=False

        def callback(event):
            handled = True

        handle_event(EventA(id=1, version=1, created_at=None),event_handled_callback=callback)
        self.assertEqual(TestEventHandler.count, 2)

    @patch("pyjangle.event.event_handler.__event_type_to_event_handler_handler_map", dict())
    def test_no_handler_registered(self):
        with self.assertRaises(EventHandlerError):
            handle_event(EventA(id=1, version=1, created_at=None), event_handled_callback=None)