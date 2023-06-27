import functools
import logging

from pyjangle.error.error import JangleError
from pyjangle.log_tools.log_tools import LogToggles, log

logger = logging.getLogger(__name__)

#Singleton dictionary mapping query types 
#to their corresponding handlers.
_query_type_to_query_handler_map = dict()


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
        if query_type in _query_type_to_query_handler_map:
            raise QueryError("Query type '" + str(query_type) + "' is already registered to '" + str(_query_type_to_query_handler_map[query_type]) + "'")
        _query_type_to_query_handler_map[query_type] = wrapped
        log(LogToggles.query_handler_registration, "Query handler registered", {"query_type": str(query_type), "query_handler_type": str(type(wrapped))})
        @functools.wraps(wrapped)
        def wrapper(*args, **kwargs):
            return wrapped(*args, **kwargs)
        return wrapper
    return decorator

def handle_query(query: any):
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
    return _query_type_to_query_handler_map[query_type]