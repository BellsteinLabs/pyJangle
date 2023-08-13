import abc
from typing import Callable

from colorama import Fore, Style


class InputSpec:
    def __init__(self, prompt_maker: str | Callable[[any], str], validator: Callable[[str], str | None], converter: Callable[[str], any] = None):
        self.prompt_maker = prompt_maker if not isinstance(
            prompt_maker, str) else lambda x: prompt_maker
        self.validator = validator
        self.converter = converter


class TerminalContext(metaclass=abc.ABCMeta):

    def __init__(self, data: any = None, options: list[tuple[str, type]] = []) -> None:
        self.data = data
        self._converted_input = []
        self.options = options

    async def show_context(self):
        pass

    @property
    def options(self) -> dict[str, type]:
        return {}

    @property
    @abc.abstractmethod
    def input_spec(self) -> list[InputSpec]:
        pass

    @abc.abstractmethod
    def get_next_context(self):
        pass

    async def action(self):
        pass

    async def run(self):
        await self.show_context()
        self.show_options()
        self.get_input()
        await self.action()
        return self.get_next_context()

    def converted_input(self):
        return self._converted_input

    def get_input(self):
        for i, spec in enumerate(self.input_spec):
            field_value = None
            validation_result = None
            while not field_value:
                if validation_result:
                    print(f"{Fore.RED}{validation_result}{Style.RESET_ALL}")
                field_value = input(spec.prompt_maker(self)).strip()
                validation_result = spec.validator(field_value)
                if validation_result:
                    continue
                converted = spec.converter(
                    field_value) if spec.converter else field_value
                self._converted_input.append(converted)

    def show_options(self):
        if self.options:
            print("Select an option...")
            for i, option in enumerate(self.options, 1):
                print(f"{i}) {option[0]}")

    def make_option_validator(self) -> str | None:
        def validator(user_input: str):
            error_text = f"Valid options are 1 to {len(self.options)}."
            try:
                as_int = int(user_input)
            except TypeError:
                return error_text
            if as_int < 1 or as_int > len(self.options):
                return error_text
        return validator
    
    def enter_to_continue(self):
        input("Enter to continue...")
