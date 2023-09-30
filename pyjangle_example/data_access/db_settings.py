import os

DEFAULT_DB_PATH = "DELETE_ME_JANGLE_BANKING_DB_PATH_NOT_SET.db"
_db_jangle_banking_path = os.getenv("DB_JANGLE_BANKING_PATH", DEFAULT_DB_PATH)


def get_db_jangle_banking_path():
    return _db_jangle_banking_path


def set_db_jangle_banking_path(path: str):
    global _db_jangle_banking_path
    _db_jangle_banking_path = path
