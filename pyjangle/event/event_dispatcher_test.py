from typing import Callable
import unittest
from unittest.mock import patch
from pyjangle.event.event import Event

from pyjangle.event.event_dispatcher import EventDispatcherError, RegisterEventDispatcher, event_dispatcher_instance

@patch("pyjangle.event.event_dispatcher.__event_dispatcher", None)
class TestEventDispatcher(unittest.TestCase):

    def test_register_multiple_event_dispatcher(self):
        with self.assertRaises(EventDispatcherError): 
            @RegisterEventDispatcher
            def Event_dispatcher1(Event):
                pass
            
            @RegisterEventDispatcher
            def Event_dispatcher2(Event):
                pass

    def test_register_event_dispatcher_wrong_function_signature(self):
        with self.assertRaises(EventDispatcherError):
            @RegisterEventDispatcher
            def Event_dispatcher(Event):
                pass

            self.assertEqual(event_dispatcher_instance().__name__, Event_dispatcher.__name__)

    def test_register_event_dispatcher(self):
        @RegisterEventDispatcher
        def Event_dispatcher(event: Event, event_handled_callback: Callable[[Event], None]):
            pass

        self.assertEqual(event_dispatcher_instance().__name__, Event_dispatcher.__name__)
        