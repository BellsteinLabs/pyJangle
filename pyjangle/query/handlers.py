import inspect

from pyjangle import JangleError
from pyjangle.logging.logging import LogToggles, log

# Maps query types to corresponding query handlers.
_query_type_to_query_handler_map = dict()
_SIGNATURE = "async def func_name(query) -> None"


class QueryHandlerRegistrationBadSignatureError(JangleError):
    "Invalid query handler signature."
    pass


class DuplicateQueryRegistrationError(JangleError):
    "Duplicate query type registration."
    pass


class QueryHandlerMissingError(JangleError):
    "Query has no registered query handler."
    pass


def register_query_handler(query_type: any):
    """Decorates and registers a function as a handler of queries of a certain type.

    A query handler responds to an external request for data.  In the case of a web
    application, these approximately correspond to GET endpoints.

    Args:
        query_type:
            The type of query that should be handled by the decorated function.

    Raises:
        QueryRegistrationBadSignatureError:
            Invalid query handler signature.

        DuplicateQueryRegistrationError:
            Duplicate query type registration.
    """

    def decorator(wrapped):
        if not inspect.isfunction(wrapped):
            raise QueryHandlerRegistrationBadSignatureError(
                f"Decorated member is not callable: {_SIGNATURE}"
            )
        if not inspect.iscoroutinefunction(wrapped):
            raise QueryHandlerRegistrationBadSignatureError(
                f"Decorated function is not a coroutine (async): {_SIGNATURE}"
            )
        if len(inspect.signature(wrapped).parameters) != 1:
            raise QueryHandlerRegistrationBadSignatureError(
                f"Decorated function must have one query parameter: {_SIGNATURE}"
            )

        if query_type in _query_type_to_query_handler_map:
            raise DuplicateQueryRegistrationError(
                "Query type '"
                + str(query_type)
                + "' is already registered to '"
                + str(_query_type_to_query_handler_map[query_type])
                + "'"
            )
        _query_type_to_query_handler_map[query_type] = wrapped
        log(
            LogToggles.query_handler_registration,
            "Query handler registered",
            {"query_type": str(query_type), "query_handler_type": str(wrapped)},
        )
        return wrapped

    return decorator


async def handle_query(query: any):
    """forwards a query to a corresponding handler.

    This method is the glue between the query and a query handler registered with
    `register_query_handler`.

    Raises:
    ------
    QueryError when there is no registered handler
    matching the query."""
    query_type = type(query)
    if not query_type in _query_type_to_query_handler_map:
        raise QueryHandlerMissingError(
            "No query handler registered for " + str(query_type)
        )
    return await _query_type_to_query_handler_map[query_type](query)
