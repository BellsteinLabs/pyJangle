from unittest import IsolatedAsyncioTestCase


class TestSagaDaemon(IsolatedAsyncioTestCase):
    async def test_when_saga_needs_retry_then_retried_by_saga_daemon(seld, *_):
        pass

    async def test_when_no_saga_repository_then_exception(self, *_):
        pass