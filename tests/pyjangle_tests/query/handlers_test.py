import unittest

from pyjangle import (
    QueryHandlerRegistrationBadSignatureError,
    DuplicateQueryRegistrationError,
    QueryHandlerMissingError,
    handle_query,
    register_query_handler,
)
from test_helpers.reset import ResetPyJangleState


@ResetPyJangleState
class TestHandlers(unittest.IsolatedAsyncioTestCase):
    async def test_cant_register_multiple_handlers_for_same_query(self, *_):
        with self.assertRaises(DuplicateQueryRegistrationError):

            @register_query_handler(int)
            async def foo(query: int):
                pass

            @register_query_handler(int)
            async def bar(event: int):
                pass

    async def test_query_handler_happy_path(self, *_):
        @register_query_handler(str)
        async def foo(query: str):
            self.called = True

        await handle_query("A query")
        self.assertTrue(self.called)

    async def test_no_handler_registered(self, *_):
        with self.assertRaises(QueryHandlerMissingError):
            await handle_query(1)

    async def test_handler_is_not_function(self, *_):
        with self.assertRaises(QueryHandlerRegistrationBadSignatureError):

            @register_query_handler(int)
            class Foo:
                pass

    async def test_handler_is_not_coroutine(self, *_):
        with self.assertRaises(QueryHandlerRegistrationBadSignatureError):

            @register_query_handler(int)
            def foo(query):
                pass

    async def test_handler_has_wrong_params_count(self, *_):
        with self.assertRaises(QueryHandlerRegistrationBadSignatureError):

            @register_query_handler(int)
            async def foo(query, something_else):
                pass
