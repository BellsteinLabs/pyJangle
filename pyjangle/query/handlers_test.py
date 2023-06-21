import unittest
from unittest.mock import patch

from query.handlers import QueryError, handle_query, register_query_handler

class TestHandlers(unittest.TestCase):
    @patch("query.handlers._query_type_to_query_handler_map", dict())
    def test_cant_register_multiple_handlers_for_same_event(self):
        with self.assertRaises(QueryError):
            @register_query_handler(int)
            def foo(event: int):
                TestHandlers.count += 1

            @register_query_handler(int)
            def bar(event: int):
                TestHandlers.count += 1

    @patch("query.handlers._query_type_to_query_handler_map", dict())
    def test_no_handler_registered(self):
        with self.assertRaises(QueryError):
            handle_query(1)