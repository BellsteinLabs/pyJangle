import inspect
from typing import Callable, Iterator, List

def _find_user_defined_callable_methods(obj: any) -> List:
    """Finds user-defined methods on an object."""
    return [getattr(obj, method_name) for method_name in dir(obj) if not method_name.startswith("__") and inspect.ismethod(getattr(obj, method_name))]

def _find_methods(obj: any, predicate: Callable[[any], bool]) -> Iterator[tuple]:
    """Finds user-defined methods on an object matching a specific criteria."""
    for method in _find_user_defined_callable_methods(obj):
        if predicate(method): yield (method.__name__, method)

def find_decorated_method_names(obj: any, method_predicate: Callable[[Callable], bool]) -> List[str]:
        """Returns the names of all methods on an object matching a criteria."""
        return [method[0] for method in _find_methods(obj, method_predicate)]

def register_methods(obj: any, backing_dictionary_attribute_name: str, decorated_function_attribute_name: str, names_of_methods_to_register: List[str]):
        #Create map for mapping value of decorated_function_attribute_name to methods
        setattr(obj, backing_dictionary_attribute_name, dict())
        #find command validator methods
        for method_name in names_of_methods_to_register:
            method = getattr(obj, method_name)
            type_to_method_map = getattr(obj, backing_dictionary_attribute_name)
            value_on_decorated_function = getattr(method, decorated_function_attribute_name)
            type_to_method_map[value_on_decorated_function] = method