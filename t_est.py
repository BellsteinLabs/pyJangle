import abc
import functools
import inspect
from typing import Any
import uuid
from example_commands import CreateAccount
from pyjangle.command.register import RegisterCommand, command_to_aggregate_map
from pyjangle.event.event import Event

class Aggregate:
    pass

foo = CreateAccount(name="foo", account_id="foo", initial_deposit=0)
print(type(foo))

@RegisterCommand(Aggregate)
class TestCommand:
    def bark():
        print("bark")

foo = TestCommand()
foo.bark()
print(type(foo))
#bar = command_to_aggregate_map[0]()
#bar.bark()
print(command_to_aggregate_map[TestCommand.__qualname__])



exit()


__registered_things = set()


def classyReg(cls):
    __registered_things.add(cls())
    return cls

def OutterReg(cls):
    return cls

@classyReg
class RegisterMe:
    pass

print(__registered_things)

exit()



def register(func):
    #def decorator(func):
        #setattr(func,"__isDecorated", None)
        func.__isDecorated = None
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            value = func(*args, **kwargs)
            return value
        return wrapper
    #decorator.__isDecorated = True
    #return decorator

def another_decorator():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            value = func(*args, **kwargs)
            return value
        return wrapper
    return decorator

class classy_dec:
    def __init__(self, func):
        self.__isDecorated = None
        #setattr(self, "__isDecorated", None)
        print(self)
        print("hai")
        self.func = func

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        def decorator(func):
            #self.__isDecorated = None
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                value = func(*args, **kwargs)
                return value
            return wrapper
        return decorator
    
    def __set_name__(self, owner, name):
        # do something with owner, i.e.
        print(f"decorating {self.fn} and using {owner}")
        self.fn.class_name = owner.__name__

        # then replace ourself with the original method
        setattr(owner, name, self.func)

# def register(func):
#     func.__isDecorated = True
#     return func

class RegisterableMethods(type):
    def __init__(cls, name, bases, attributes):
        registered_methods = set()
        for method_name, method in attributes.items():
            if hasattr(method, "__isDecorated"):
                registered_methods.add(method)
                print(method_name + " is decorated")

class Foo(metaclass=RegisterableMethods):
    


    @another_decorator()
    @register
    #@classy_dec
    def test(self, value):
        print(value)
    
    # @register
    # def test2(self, value):
    #     print(value)
    
    # @register
    # def test3(self, value):
    #     print(value)
    
    # @register
    # def test4(self, value):
    #     print(value)

    # @register(str)
    # def test(self, value):
    #     print(value)
    
    # @register(int)
    # def test2(self, value):
    #     print(value)
    
    # @register(float)
    # def test3(self, value):
    #     print(value)
    
    # @register(bool)
    # def test4(self, value):
    #     print(value)

    def _criteria(member) -> bool:
        return inspect.ismethod(member)

    def do_test(self, value_string, value_int, value_float, value_bool):
        try:
            self.__funcs__[str](value_string)
        except KeyError:
            pass
        try:
            self.__funcs__[int](value_int)
        except KeyError:
            pass
        try:
            self.__funcs__[float](value_float)
        except KeyError:
            pass
        try:
            self.__funcs__[bool](value_bool)
        except KeyError:
            pass
        # atts = [getattr(self,att) for att in dir(Foo) if inspect.isfunction(getattr(self, att))]
        # atts = [getattr(self, x) for x in dir(Foo)]
        # for x in atts:

        #     print(x)

value = Foo()
# value.do_test("test", 42, 42.42, True)
# #value.test("test")
# value.test2(42)
# value.test3(42.42)
# value.test4(True)

