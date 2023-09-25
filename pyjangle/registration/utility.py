import inspect
from typing import Callable, Iterator, List


def _find_user_defined_callable_methods(obj: any) -> List:
    """Finds user-defined methods on an object.

    Finds functions that do not start with a dunder and are user-defined instance
    methods.

    Args:
        obj:
            Methods associated with this instance will be returned.

    Returns:
        List of user-defined methods on obj.
    """
    return [
        getattr(obj, method_name)
        for method_name in dir(obj)
        if not method_name.startswith("__")
        and inspect.ismethod(getattr(obj, method_name))
    ]


def _find_methods(obj: any, predicate: Callable[[any], bool]) -> Iterator[tuple]:
    """Finds user-defined methods on an object matching a specific criteria.

    Args:
        obj:
            Methods associated with this instance will be returned.
        predicate:
            A function filters methods.
                def predicate(method) -> bool

    Returns:
       Generator returning tuple[method_name: str, method_instance]
    """
    for method in _find_user_defined_callable_methods(obj):
        if predicate(method):
            yield (method.__name__, method)


def find_decorated_method_names(
    obj: any, method_predicate: Callable[[Callable], bool]
) -> List[str]:
    """Finds user-defined methods on an object matching a specific criteria.

    Args:
        obj:
            Methods associated with this instance will be returned.
        predicate:
            A function filters methods.
                def predicate(method) -> bool

    Returns:
        List of method names.
    """
    return [method[0] for method in _find_methods(obj, method_predicate)]


def register_instance_methods(
    obj: any,
    backing_dictionary_attribute_name: str,
    attribute_name_on_decorated_function: str,
    names_of_methods_to_potentially_register: List[str],
):
    """

    Args:
        obj:
            Methods of this instance will be registered.
        backing_dictionary_attribute_name:
            Attribute name on `obj` containing a dictionary of method registations.
        attribute_name_on_decorated_function:
            Methods containing an attribute with this name will be registered.
        names_of_methods_to_potentially_register:
            The set of method names that are considered for registation on `obj`.
    """
    # Create map for mapping value of attribute_name_on_decorated_function to methods
    setattr(obj, backing_dictionary_attribute_name, dict())
    # find command validator methods
    for method_name in names_of_methods_to_potentially_register:
        method = getattr(obj, method_name)
        type_to_method_map = getattr(obj, backing_dictionary_attribute_name)
        value_on_decorated_function = getattr(
            method, attribute_name_on_decorated_function
        )
        type_to_method_map[value_on_decorated_function] = method
