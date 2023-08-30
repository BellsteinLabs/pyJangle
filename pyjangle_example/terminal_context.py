import abc
from decimal import Decimal
import os
import sys
from typing import Callable
from asyncio import get_event_loop

from colorama import Fore, Style
from pyjangle.command.command_handler import handle_command
from pyjangle.query.handlers import handle_query
from pyjangle_example.commands import AcceptRequest, CreateAccount, DeleteAccount, DepositFunds, Request, RejectRequest, ForgiveDebt, Transfer, WithdrawFunds

from pyjangle_example.queries import AccountLedger, AccountSummary, BankStats, AccountsList

ACCOUNT_ID = "account_id"
ACCOUNT_SUMMARY = "account_summary"


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
        self._options = options

    async def show_context(self):
        pass

    @property
    def options(self) -> dict[str, type]:
        return self._options

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
        await self.get_input()
        await self.action()
        return self.get_next_context()

    @property
    def converted_input(self):
        return self._converted_input

    async def get_input(self):
        for i, spec in enumerate(self.input_spec):
            field_value = None
            validation_result = None
            while not field_value:
                if validation_result:
                    print(f"{Fore.RED}{validation_result}{Style.RESET_ALL}")
                field_value = await get_event_loop().run_in_executor(None, lambda: input(spec.prompt_maker(self)).strip())
                validation_result = spec.validator(field_value)
                if validation_result:
                    field_value = None
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
            except:
                return error_text
            if as_int < 1 or as_int > len(self.options):
                return error_text
        return validator

    async def enter_to_continue(self):
        await get_event_loop().run_in_executor(None, lambda: input("Enter to continue..."))


class CreateAccountContext(TerminalContext):
    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("Account name (5-15 chars):",
                      self.validate_account_name),
            InputSpec("Starting balance (ENTER for 0):",
                      self.validate_initial_balance, self.convert_initial_balance),
        ]

    async def action(self):
        command = CreateAccount(
            name=self.converted_input[0], initial_deposit=self.converted_input[1])
        result = await handle_command(command)
        if result.is_success:
            print(f"Account successfully created with id '{result.data}'")
        else:
            print(f"There was an error creating the account: {result.data}")

    def get_next_context(self):
        return RootContext()

    @staticmethod
    def convert_initial_balance(balance: str):
        if not balance:
            return 0
        return Decimal(balance)

    @staticmethod
    def validate_initial_balance(balance: str):
        error_string = f"Must be a decimal that is zero or greater."
        if not balance:
            return
        try:
            as_int = int(balance)
        except:
            return error_string
        if as_int < 0:
            return error_string

    @staticmethod
    def validate_account_name(name: str):
        if not name:
            return "Account name required."
        if len(name) < 4:
            return "Account name must be at least 4 characters."
        if len(name) > 15:
            return "Account name must be no londer than 15 characters."


class DeleteAccountContext(TerminalContext):

    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("ID of account to delete:", validate_account_id)
        ]

    async def action(self):
        command = DeleteAccount(account_id=self.converted_input[0])
        result = await handle_command(command)
        if result.is_success:
            print(
                f"Account with id {self.converted_input[0]} successfully deleted")
        else:
            print(f"There was an error deleting the account: {result.data}")
        await self.enter_to_continue()

    def get_next_context(self):
        return RootContext()


class SelectAccountContext(TerminalContext):

    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("ID of account to select:", validate_account_id)
        ]

    def get_next_context(self):
        return AccountContext(data={ACCOUNT_ID: self.converted_input[0]})


class AccountContext(TerminalContext):

    async def show_context(self):
        self.account_summary = await handle_query(AccountSummary(account_id=self.data[ACCOUNT_ID]))
        display_width = os.get_terminal_size().columns
        xfer_index_width = 4
        xfer_account_id_width = 7
        expires_at_width = 17
        amount_width = display_width - xfer_index_width - \
            xfer_account_id_width - expires_at_width - 6
        print(f"Account ID:       {self.account_summary.account_id}")
        print(f"Account Name:     {self.account_summary.name}")
        print(f"Balance:          {self.account_summary.balance}")
        print(f"Pending Outbound Transfers:")
        headers = [{"index": "#", "funded_account": "AcctID",
                    "amount": "Amount", "timeout_at": "Expires At"}]
        pending_outbound_transfers: list[dict] = headers + \
            [vars(x) for x in self.account_summary.transfer_requests]
        for i, row in enumerate(pending_outbound_transfers):
            print("  {1!s: >{xfer_index_width}} {0[funded_account]!s: <{xfer_account_id_width}} {0[amount]!s: >0{amount_width}.2} {0[timeout_at]!s: >{expires_at_width}}".format(
                row, i if i > 0 else '', xfer_index_width=xfer_index_width, xfer_account_id_width=xfer_account_id_width, amount_width=amount_width, expires_at_width=expires_at_width))

    @property
    def options(self):
        return [
            ("Back", RootContext),
            ("Deposit funds", DepositFundsContext),
            ("Withdraw funds", WithdrawFundsContext),
            ("Send funds to another account", TransferContext),
            ("Request funds from another account", RequestContext),
            ("Forgive debt", ForgiveDebtContext),
            ("Accept funds request", AcceptRequestContext),
            ("Reject funds request", RejectRequestContext),
            ("View account ledger", ViewAccountLedgerContext),
        ]

    @property
    def input_spec(self):
        if not self.account_summary:
            return [InputSpec("Account not found.", lambda x: None)]

        return [
            InputSpec(lambda x: "Select an option:",
                      self.make_option_validator(), lambda x: int(x))
        ]

    def get_next_context(self):
        if not self.account_summary:
            return RootContext()

        next_context_type = self.options[self.converted_input[0] - 1][1]
        self.data[ACCOUNT_SUMMARY] = self.account_summary
        return next_context_type(data=self.data if next_context_type != RootContext else None)


class DepositFundsContext(TerminalContext):
    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("How much to deposit?",
                      validate_funds_amount, lambda x: Decimal(x))
        ]

    async def action(self):
        result = await handle_command(DepositFunds(account_id=self.data[ACCOUNT_ID], amount=self.converted_input[0]))
        if result.is_success:
            print(
                f"{str(self.converted_input[0])} successfully deposited into {self.data[ACCOUNT_ID]}.")
        else:
            print(f"There was an error depositing funds: {result.data}")
        await self.enter_to_continue()

    def get_next_context(self):
        return AccountContext({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class WithdrawFundsContext(TerminalContext):
    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("How much to withdraw?",
                      validate_funds_amount, lambda x: Decimal(x))
        ]

    async def action(self):
        result = await handle_command(WithdrawFunds(account_id=self.data[ACCOUNT_ID], amount=self.converted_input[0]))
        if result.is_success:
            print(
                f"{str(self.converted_input[0])} successfully withdrew from {self.data[ACCOUNT_ID]}.")
        else:
            print(f"There was an error withdrawing funds: {result.data}")
        await self.enter_to_continue()

    def get_next_context(self):
        return AccountContext({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class TransferContext(TerminalContext):

    async def show_context(self):
        await show_accounts_summary()

    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("Account Id to send funds to:", validate_account_id),
            InputSpec("Amount to send:", validate_funds_amount,
                      lambda x: Decimal(x))
        ]

    async def action(self):
        result = await handle_command(Transfer(funded_account_id=self.converted_input[0], funding_account_id=self.data[ACCOUNT_ID], amount=self.converted_input[1]))
        if result.is_success:
            print(
                f"Sent {str(self.converted_input[1])} to account ID {self.converted_input[0]}.")
        else:
            print(
                f"There was an error sending funds to {self.converted_input[0]}: {result.data}")
        await self.enter_to_continue()

    def get_next_context(self):
        return AccountContext({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class RequestContext(TerminalContext):

    async def show_context(self):
        await show_accounts_summary()

    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("Account ID to request funds from:",
                      validate_account_id),
            InputSpec("Amount to request:",
                      validate_funds_amount, lambda x: Decimal(x))
        ]

    async def action(self):
        result = await handle_command(Request(funded_account_id=self.data[ACCOUNT_ID], funding_account_id=self.converted_input[0], amount=self.converted_input[1]))
        if result.is_success:
            print(
                f"Requested {str(self.converted_input[1])} from account ID {self.converted_input[0]}.")
        else:
            print(
                f"There was an error requesting funds from {self.converted_input[0]}: {result.data}")
        await self.enter_to_continue()

    def get_next_context(self):
        return AccountContext({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class ForgiveDebtContext(TerminalContext):

    @property
    def input_spec(self) -> list[InputSpec]:
        return []

    async def action(self):
        result = await handle_command(ForgiveDebt(account_id=self.data[ACCOUNT_ID]))
        if result.is_success:
            print(f"Your debt has been forgiven!  Balance reset to 0.")
        else:
            print(f"Debt forgiveness failed: {result.data}")
        await self.enter_to_continue()

    def get_next_context(self):
        return AccountContext({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class AcceptRequestContext(TerminalContext):
    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("Which entry to confirm?", make_index_validator(
                len(self.data[ACCOUNT_SUMMARY].transfer_requests)))
        ]

    async def action(self):
        pending_transfer = self.data[ACCOUNT_SUMMARY].transfer_requests[int(
            self.converted_input[0]) - 1]
        result = await handle_command(AcceptRequest(funding_account_id=self.data[ACCOUNT_ID], transaction_id=pending_transfer.transaction_id))
        if result.is_success:
            print(
                f"Accepted request to transfer {pending_transfer.amount} to account ID {pending_transfer.funded_account}")
        else:
            print(
                f"There was an error accepting request to transfer funds: {result.data}")
        await self.enter_to_continue()

    def get_next_context(self):
        return AccountContext({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class RejectRequestContext(TerminalContext):
    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("Which entry to reject?", make_index_validator(
                len(self.data[ACCOUNT_SUMMARY].transfer_requests)))
        ]

    async def action(self):
        pending_transfer = self.data[ACCOUNT_SUMMARY].transfer_requests[int(
            self.converted_input[0]) - 1]
        result = await handle_command(RejectRequest(funding_account_id=self.data[ACCOUNT_ID], transaction_id=pending_transfer.transaction_id))
        if result.is_success:
            print(
                f"Rejected request to transfer {pending_transfer.amount} to account ID {pending_transfer.funded_account}")
        else:
            print(
                f"There was an error rejecting request to transfer funds: {result.data}")
        await self.enter_to_continue()

    def get_next_context(self):
        return AccountContext({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class ViewAccountLedgerContext(TerminalContext):
    async def show_context(self):
        self.ledger = await handle_query(AccountLedger(account_id=self.data[ACCOUNT_ID]))
        display_width = os.get_terminal_size().columns
        index_width = 4
        initiated_at_width = 17
        amount_width = display_width - 5 - index_width - initiated_at_width
        description_width = amount_width
        print(f"Account ID:       {self.data[ACCOUNT_SUMMARY].account_id}")
        print(f"Account Name:     {self.data[ACCOUNT_SUMMARY].name}")
        print(f"Balance:          {self.data[ACCOUNT_SUMMARY].balance}")
        print(f"Ledger:")
        headers = [{"index": "#", "initiated_at": "Initiated At",
                    "amount": "Amount", "transaction_type": "Description"}]
        transactions: list[dict] = headers + [vars(x) for x in self.ledger]
        for i, row in enumerate(transactions):
            print("  {1!s: >{index_width}} {0[initiated_at]!s: >{initiated_at_width}} {0[amount]!s: >0{amount_width}.2} {0[transaction_type]!s: >{description_width}}".format(
                row, i, index_width=index_width, initiated_at_width=initiated_at_width, amount_width=amount_width, description_width=description_width))

    @property
    def input_spec(self) -> list[InputSpec]:
        return []

    async def action(self):
        await self.enter_to_continue()

    def get_next_context(self):
        return AccountContext({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class ViewStatisticsContext(TerminalContext):
    async def show_context(self):
        stats = await handle_query(BankStats())
        display_width = os.get_terminal_size().columns
        half_display_width = display_width // 2
        for stat in stats:
            print("{0[description]: >{half_display_width}}:  {0[value]: <{half_display_width}}".format(
                stat, display_width=display_width, half_display_width=half_display_width))

    @property
    def input_spec(self) -> list[InputSpec]:
        return []

    async def action(self):
        await self.enter_to_continue()

    def get_next_context(self):
        return RootContext()


class EraseAllEvents(TerminalContext):
    pass


class ReplayAllEvents(TerminalContext):
    pass


class RootContext(TerminalContext):

    async def show_context(self):
        await show_accounts_summary()

    @property
    def options(self):
        return [
            ("Create account", CreateAccountContext),
            ("Delete account", DeleteAccountContext),
            ("Select account", SelectAccountContext),
            ("Show bank statistics", ViewStatisticsContext)
        ]

    @property
    def input_spec(self):
        return [
            InputSpec(lambda x: "Select an option:",
                      self.make_option_validator(), lambda x: int(x))
        ]

    def get_next_context(self):
        next_context_type = self.options[self.converted_input[0] - 1][1]
        return next_context_type()


def validate_account_id(user_input: str):
    if not user_input:
        return "Account name required."
    if len(user_input) != 6:
        return "Account name must be 6 characters."


def validate_funds_amount(user_input: str):
    error_string = "Amount must be a valid decimal."
    if not user_input:
        return error_string
    try:
        amount = Decimal(user_input)
    except:
        return error_string
    if amount <= 0:
        return error_string


def make_index_validator(max_index: int):
    error_string = f"Specify a valid entry # between 1 and {max_index}."

    def validator(user_input: str):
        if not user_input:
            return error_string
        try:
            converted = int(user_input)
        except:
            return error_string
        if converted < 1 or converted > max_index:
            return error_string

    return validator


async def show_accounts_summary():
    display_width = os.get_terminal_size().columns
    id_width = 6
    name_width = 15
    out_width = 11
    in_width = 10
    balance_width = display_width - id_width - name_width - out_width - in_width - 4

    headers = [{"account_id": "ID", "name": "Name", "balance": "Balance",
                "pending_request_count": "Pending Out"}]
    bank_summary: list[dict] = headers + await handle_query(AccountsList())
    for row in bank_summary:
        print("{0[account_id]!s: >{id_width}} {0[name]!s: <{name_width}} {0[balance]!s: >0{balance_width}.2} {0[pending_request_count]!s: ^}".format(
            vars(row) if not isinstance(row, dict) else row, id_width=id_width, name_width=name_width, balance_width=balance_width, out_width=out_width, in_width=in_width))
