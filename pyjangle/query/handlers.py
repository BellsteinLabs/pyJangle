import inspect
import logging

from pyjangle import JangleError
from pyjangle.logging.logging import LogToggles, log

# Singleton dictionary mapping query types
# to their corresponding handlers.
_query_type_to_query_handler_map = dict()
_SIGNATURE = "async def func_name(query) -> None"


class QueryRegistrationError(JangleError):
    pass


class QueryError(JangleError):
    pass


def register_query_handler(query_type: any):
    """Registers a method as a handler of queries.

    THROWS
    ------
    QueryError if a query is already registered to a handler.

    Query handlers respond to an external request for data.
    Commonly, your GET endpoints on a webserver will 
    generate a query which will be mapped to a query handler
    which will run the db query and return the data back
    to the caller."""
    def decorator(wrapped):
        if not inspect.isfunction(wrapped):
            raise QueryRegistrationError(
                f"Decorated member is not callable: {_SIGNATURE}")
        if not inspect.iscoroutinefunction(wrapped):
            raise QueryRegistrationError(
                f"Decorated function is not a coroutine (async): {_SIGNATURE}")
        if len(inspect.signature(wrapped).parameters) != 1:
            raise QueryRegistrationError(
                f"Decorated function must have one query parameter: {_SIGNATURE}")

        if query_type in _query_type_to_query_handler_map:
            raise QueryError("Query type '" + str(query_type) + "' is already registered to '" +
                             str(_query_type_to_query_handler_map[query_type]) + "'")
        _query_type_to_query_handler_map[query_type] = wrapped
        log(LogToggles.query_handler_registration, "Query handler registered", {
            "query_type": str(query_type), "query_handler_type": str(type(wrapped))})
        return wrapped
    return decorator


async def handle_query(query: any):
    """Maps queries to a corresponding handler.

    This method is the glue between the query and
    the query handler.  Decorate query handlers 
    with @register_query_handler, and then just
    call this method to handle the query.

    THROWS
    ------
    QueryError when there is no registered handler 
    matching the query."""
    query_type = type(query)
    if not query_type in _query_type_to_query_handler_map:
        raise QueryError("No query handler registered for " + str(query_type))
    return await _query_type_to_query_handler_map[query_type](query)
