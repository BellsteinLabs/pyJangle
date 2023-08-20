import decimal
import os
from pyjangle.command.command_handler import handle_command
from pyjangle_example.commands import AcceptReceiveFundsRequest, CreateAccount, DeleteAccount, DepositFunds, ReceiveFunds, RejectReceiveFundsRequest, RequestForgiveness, SendFunds, WithdrawFunds
from pyjangle_example.terminal_context import InputSpec
from terminal_context import TerminalContext

from pyjangle.query.handlers import handle_query
from pyjangle_example.queries import AccountLedger, AccountSummary, BankStats, BankSummary

ACCOUNT_ID = "account_id"
ACCOUNT_SUMMARY = "account_summary"


async def main():
    print("Jangle Banking terminal initializing...")

    context = RootContext()

    while True:
        context = await context.run()


class CreateAccountContext(TerminalContext):
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

    def convert_initial_balance(balance: str):
        if not balance:
            return 0
        return decimal(balance)

    def validate_initial_balance(balance: str):
        error_string = f"Must be a decimal that is zero or greater."
        if not balance:
            return
        try:
            as_int = int(balance)
        except TypeError:
            return error_string
        if as_int < 0:
            return error_string

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
        command = DeleteAccount(self.converted_input[0])
        result = await handle_command(command)
        if result.is_success:
            print(
                f"Account with id {self.converted_input[0]} successfully deleted")
        else:
            print(f"There was an error deleting the account: {result.data}")
        self.enter_to_continue()

    def get_next_context(self):
        return RootContext()


class SelectAccountContext(TerminalContext):

    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("ID of account to select:", validate_account_id)
        ]

    def get_next_context(self):
        return AccountContext(data=self.converted_input[0])


class AccountContext(TerminalContext):

    async def show_context(self):
        self.account_summary = await handle_query(AccountSummary(account_id=self.data[ACCOUNT_ID]))
        display_width = os.get_terminal_size().columns
        xfer_index_width = 4
        xfer_account_id_width = 7
        initiated_at_width = 17
        expires_at_width = 17
        amount_width = display_width - xfer_index_width - \
            xfer_account_id_width - initiated_at_width - expires_at_width - 6
        print(f"Account ID:       {self.account_summary.id}")
        print(f"Account Name:     {self.account_summary.name}")
        print(f"Balance:          {self.account_summary.balance}")
        print(f"Pending Outbound Transfers:")
        headers = [{"index": "#", "account_id": "AcctID", "amount": "Amount",
                    "initiated_at": "Initiated At", "expires_at": "Expires At"}]
        pending_outbound_transfers: list[dict] = headers + \
            self.account_summary.pending_outbound_transfers
        for row in pending_outbound_transfers:
            print("  {0[index]!s: >{xfer_index_width}} {0[account_id]!s: <{xfer_account_id_width}} {0[amount]!s: >0{amount_width},.2} {0[initiated_at]!s: >{initiated_at_width}} {0[expires_at]!s: >{expires_at_width}}".format(
                row, xfer_index_width=xfer_index_width, xfer_account_id_width=xfer_account_id_width, amount_width=amount_width, initiated_at_width=initiated_at_width, expires_at_width=expires_at_width))
        print(f"Pending Inbound Transfers:")
        headers = [{"index": "#", "account_id": "AcctID", "amount": "Amount",
                    "initiated_at": "Initiated At", "expires_at": "Expires At"}]
        pending_inbound_transfers: list[dict] = headers + \
            self.account_summary.pending_inbound_transfers
        for row in pending_inbound_transfers:
            print("  {0[index]!s: >{xfer_index_width}} {0[account_id]!s: <{xfer_account_id_width}} {0[amount]!s: >0{amount_width},.2} {0[initiated_at]!s: >{initiated_at_width}} {0[expires_at]!s: >{expires_at_width}}".format(
                row, xfer_index_width=xfer_index_width, xfer_account_id_width=xfer_account_id_width, amount_width=amount_width, initiated_at_width=initiated_at_width, expires_at_width=expires_at_width))

    @property
    def options(self):
        return {
            "Back": RootContext,
            "Deposit funds": DepositFundsContext,
            "Withdraw funds": WithdrawFundsContext,
            "Send funds to another account": SendFundsContext,
            "Request funds from another account": ReceiveFundsContext,
            "Forgive debt": ForgiveDebtContext,
            "Accept funds request": AcceptReceiveFundsContext,
            "Reject funds request": RejectReceiveFundsContext,
            "View account ledger": ViewAccountLedgerContext,
        }

    @property
    def input_spec(self):
        if not self.account_summary:
            return [InputSpec("Account not found.", lambda x: None)]

        return [
            InputSpec(lambda x: "Select an option:",
                      self.make_option_validator, lambda x: int(x))
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
                      validate_funds_amount, lambda x: decimal(x))
        ]

    async def action(self):
        result = await handle_command(DepositFunds(account_id=self.data[ACCOUNT_ID], amount=self.converted_input[0]))
        if result.is_success:
            print(
                f"{str(self.converted_input[0])} successfully deposited into {self.data[ACCOUNT_ID]}.")
        else:
            print(f"There was an error depositing funds: {result.data}")
        self.enter_to_continue()

    def get_next_context(self):
        return AccountSummary({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class WithdrawFundsContext(TerminalContext):
    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("How much to withdraw?",
                      validate_funds_amount, lambda x: decimal(x))
        ]

    async def action(self):
        result = await handle_command(WithdrawFunds(account_id=self.data[ACCOUNT_ID], amount=self.converted_input[0]))
        if result.is_success:
            print(
                f"{str(self.converted_input[0])} successfully withdrew from {self.data[ACCOUNT_ID]}.")
        else:
            print(f"There was an error withdrawing funds: {result.data}")
        self.enter_to_continue()

    def get_next_context(self):
        return AccountSummary({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class SendFundsContext(TerminalContext):

    async def show_context(self):
        await show_accounts_summary()

    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("Account Id to send funds to:", validate_account_id),
            InputSpec("Amount to send:", validate_funds_amount,
                      lambda x: decimal(x))
        ]

    async def action(self):
        result = await handle_command(SendFunds(funded_account_id=self.converted_input[0], funding_account_id=self.data[ACCOUNT_ID], amount=self.converted_input[1]))
        if result.is_success:
            print(
                f"Sent {str(self.converted_input[1])} to account ID {self.converted_input[0]}.")
        else:
            print(
                f"There was an error sending funds to {self.converted_input[0]}: {result.data}")
        self.enter_to_continue()

    def get_next_context(self):
        return AccountSummary({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class ReceiveFundsContext(TerminalContext):

    async def show_context(self):
        await show_accounts_summary()

    @property
    def input_spec(self) -> list[InputSpec]:
        return [
            InputSpec("Account ID to request funds from:",
                      validate_account_id),
            InputSpec("Amount to request:",
                      validate_funds_amount, lambda x: decimal(x))
        ]

    async def action(self):
        result = await handle_command(ReceiveFunds(funded_account_id=self.data[ACCOUNT_ID], funding_account_id=self.converted_input[0], amount=self.converted_input[1]))
        if result.is_success:
            print(
                f"Requested {str(self.converted_input[1])} from account ID {self.converted_input[0]}.")
        else:
            print(
                f"There was an error requesting funds from {self.converted_input[0]}: {result.data}")
        self.enter_to_continue()

    def get_next_context(self):
        return AccountSummary({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class ForgiveDebtContext(TerminalContext):

    @property
    def input_spec(self) -> list[InputSpec]:
        return []

    async def action(self):
        result = await handle_command(RequestForgiveness(self.data[ACCOUNT_ID]))
        if result.is_success:
            print(f"Your debt has been forgiven!  Balance reset to 0.")
        else:
            print(f"Debt forgiveness failed: {result.data}")
        self.enter_to_continue()

    def get_next_context(self):
        return AccountSummary({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class AcceptReceiveFundsContext(TerminalContext):
    @property
    def input_spec(self) -> list[InputSpec]:
        [
            InputSpec("Which entry to confirm?", make_index_validator(
                len(self.data[ACCOUNT_SUMMARY].pending_outbound_transfers)))
        ]

    async def action(self):
        pending_transfer = self.data[ACCOUNT_SUMMARY].pending_outbound_transfers[self.converted_input[0]]
        result = await handle_command(AcceptReceiveFundsRequest(funding_account_id=pending_transfer.account_id, transaction_id=pending_transfer.transaction_id))
        if result.is_success:
            print(
                f"Accepted request to transfer {pending_transfer.amount} to account ID {pending_transfer.account_id}")
        else:
            print(
                f"There was an error accepting request to transfer funds: {result.data}")
        self.enter_to_continue()

    def get_next_context(self):
        return AccountSummary({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class RejectReceiveFundsContext(TerminalContext):
    @property
    def input_spec(self) -> list[InputSpec]:
        [
            InputSpec("Which entry to reject?", make_index_validator(
                len(self.data[ACCOUNT_SUMMARY].pending_outbound_transfers)))
        ]

    async def action(self):
        pending_transfer = self.data[ACCOUNT_SUMMARY].pending_outbound_transfers[self.converted_input[0]]
        result = await handle_command(RejectReceiveFundsRequest(funding_account_id=pending_transfer.account_id, transaction_id=pending_transfer.transaction_id))
        if result.is_success:
            print(
                f"Rejected request to transfer {pending_transfer.amount} to account ID {pending_transfer.account_id}")
        else:
            print(
                f"There was an error rejecting request to transfer funds: {result.data}")
        self.enter_to_continue()

    def get_next_context(self):
        return AccountSummary({ACCOUNT_ID: self.data[ACCOUNT_ID]})


class ViewAccountLedgerContext(TerminalContext):
    async def show_context(self):
        self.ledger = await handle_query(AccountLedger(account_id=self.data[ACCOUNT_ID]))
        display_width = os.get_terminal_size().columns
        index_width = 4
        initiated_at_width = 17
        amount_width = display_width - 5 - index_width - initiated_at_width
        description_width = amount_width
        print(f"Account ID:       {self.account_summary.id}")
        print(f"Account Name:     {self.account_summary.name}")
        print(f"Balance:          {self.account_summary.balance}")
        print(f"Ledger:")
        headers = [{"index": "#", "initiated_at": "Initiated At",
                    "amount": "Amount", "description": "Description"}]
        transactions: list[dict] = headers + self.ledger
        for row in transactions:
            print("  {0[index]!s: >{index_width}} {0[initiated_at]!s: >{initiated_at_width}} {0[amount]!s: >0{amount_width},.2} {0[description]!s: >{description_width}}".format(
                row, index_width=index_width, initiated_at_width=initiated_at_width, amount_width=amount_width, description_width=description_width))

    @property
    def input_spec(self) -> list[InputSpec]:
        []

    async def action(self):
        self.enter_to_continue()

    def get_next_context(self):
        return AccountSummary({ACCOUNT_ID: self.data[ACCOUNT_ID]})


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
        []

    async def action(self):
        self.enter_to_continue()

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
                      self.make_option_validator, lambda x: int(x))
        ]

    def get_next_context(self):
        next_context_type = self.options[self.converted_input[0] - 1][1]
        return next_context_type()


def validate_account_id(user_input: str):
    if not user_input:
        return "Account name required."
    if len(user_input) != 7:
        return "Account name must be 7 characters."


def validate_funds_amount(user_input: str):
    error_string = "Amount must be a valid decimal."
    if not user_input:
        return error_string
    try:
        amount = decimal(user_input)
    except TypeError:
        return error_string
    if amount < 0:
        return error_string


def make_index_validator(max_index: int):
    error_string = f"Specify a valid entry # between 1 and {max_index}."

    def validator(user_input: str):
        if not user_input:
            return error_string
        try:
            converted = int(user_input)
        except TypeError:
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

    headers = [{"id": "ID", "name": "Name", "balance": "Balance",
                "pending_out": "Pending Out", "pending_in": "Pending In"}]
    bank_summary: list[dict] = headers + await handle_query(BankSummary())
    for row in bank_summary:
        print("{0[id]!s: >{id_width}} {0[name]!s: <{name_width}} {0[balance]!s: >0{balance_width},.2} {0[pending_out]!s: ^} {0[pending_in]!s: ^}".format(
            row, id_width=id_width, name_width=name_width, balance_width=balance_width, out_width=out_width, in_width=in_width))