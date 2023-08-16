import os


DB_JANGLE_BANKING_PATH = os.getenv("DB_JANGLE_BANKING_PATH", "jangle_banking.db")
BATCH_SIZE = int(os.getenv("DB_JANGLE_BANKING_BATCH_SIZE", "100"))