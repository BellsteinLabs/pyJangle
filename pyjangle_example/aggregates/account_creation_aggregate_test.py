from unittest import TestCase
from unittest.mock import patch
from pyjangle_example.aggregates.account_creation_aggregate import AccountCreationAggregate
from pyjangle_example.commands import CreateAccount
from pyjangle_example.events import AccountCreated, AccountIdProvisioned, FundsDeposited
from pyjangle_example.test_helpers import ACCOUNT_ID_FLD, ACCOUNT_NAME, AGGREGATE_ID, AMOUNT_FLD, BALANCE_FLD, INITIAL_DEPOSIT, KNOWN_UUID, NAME_FLD, TRANSACTION_ID_FLD, ExpectedEvent, get_account_id, verify_events


@patch("uuid.uuid4", return_value=KNOWN_UUID)
class TestExampleAccountCreationAggregate(TestCase):
    def test_when_create_account_command_without_deposit_then_2_events(self, *_):
        a = AccountCreationAggregate(AGGREGATE_ID)
        c = CreateAccount(name=ACCOUNT_NAME)

        r = a.validate(c)
        self.assertTrue(r.is_success)
        verify_events(test_case=self, events=a.new_events, expected_length=2, expected_events=[
            ExpectedEvent(index=0, event_type=AccountIdProvisioned,
                          agg_id=AGGREGATE_ID, version=1),
            ExpectedEvent(index=1, event_type=AccountCreated, agg_id=get_account_id(1), version=1, attributes={
                ACCOUNT_ID_FLD: get_account_id(1),
                NAME_FLD: ACCOUNT_NAME,
            })
        ])

    def test_when_create_account_command_with_deposit_then_3_events(self, *_):
        a = AccountCreationAggregate(AGGREGATE_ID)
        c = CreateAccount(name=ACCOUNT_NAME, initial_deposit=INITIAL_DEPOSIT)

        self.assertTrue(a.validate(c))
        verify_events(test_case=self, events=a.new_events, expected_length=3, expected_events=[
            ExpectedEvent(index=0, event_type=AccountIdProvisioned,
                          agg_id=AGGREGATE_ID, version=1),
            ExpectedEvent(index=1, event_type=AccountCreated, agg_id=get_account_id(1), version=1, attributes={
                ACCOUNT_ID_FLD: get_account_id(1),
                NAME_FLD: ACCOUNT_NAME,
            }),
            ExpectedEvent(index=2, event_type=FundsDeposited, agg_id=get_account_id(1), version=2, attributes={
                ACCOUNT_ID_FLD: get_account_id(1),
                AMOUNT_FLD: INITIAL_DEPOSIT,
                BALANCE_FLD: INITIAL_DEPOSIT,
                TRANSACTION_ID_FLD: KNOWN_UUID
            }),
        ])
