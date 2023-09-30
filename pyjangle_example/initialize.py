from pyjangle_example.data_access.db_settings import set_db_jangle_banking_path
from pyjangle_example.data_access.sqlite3_bank_data_access_object import (
    Sqlite3BankDataAccessObject,
    create_database,
)


def initialize_pyjangle_example(db_path: str):
    if not db_path:
        raise Exception("DB path is required.")
    set_db_jangle_banking_path(db_path)
    create_database()
