CREATE TABLE IF NOT EXISTS "bank_summary" (
    "account_id"                TEXT NOT NULL UNIQUE,
    "name"                      TEXT NOT NULL,
    "balance"                   INTEGER NOT NULL,
    "balance_version"           INTEGER NOT NULL,
    "is_deleted"                INTEGER NOT NULL,
    PRIMARY KEY("account_id")
);

CREATE TABLE IF NOT EXISTS "transactions" (
    "transaction_id"            TEXT NOT NULL UNIQUE,        
    "account_id"                TEXT NOT NULL,
    "initiated_at"              TEXT NOT NULL,
    "amount"                    INTEGER NOT NULL,
    "description"               TEXT NOT NULL,
    PRIMARY KEY("transaction_id")
);

CREATE TABLE IF NOT EXISTS "transfers" (
    "transaction_id"            TEXT NOT NULL UNIQUE,
    "funding_account"           TEXT NOT NULL,
    "funded_account"            TEXT NOT NULL,
    "amount"                    INTEGER NOT NULL,
    "state"                     INTEGER NOT NULL,
    PRIMARY KEY("transaction_id")
);

CREATE TABLE IF NOT EXISTS "transfer_requests" (
    "transaction_id"            TEXT NOT NULL UNIQUE,
    "funded_account"            TEXT NOT NULL,
    "funding_account"           TEXT NOT NULL,
    "amount"                    INTEGER NOT NULL,
    "state"                     INTEGER NOT NULL,
    "timeout_at"                NULL,
    PRIMARY KEY("transaction_id")
);

CREATE TABLE IF NOT EXISTS "deposits" (
    "transaction_id"            TEXT NOT NULL UNIQUE,
    "amount"                    INTEGER NOT NULL,
    PRIMARY KEY("transaction_id")
);

CREATE TABLE IF NOT EXISTS "withdrawals" (
    "transaction_id"            TEXT NOT NULL UNIQUE,
    "amount"                    INTEGER NOT NULL,
    PRIMARY KEY("transaction_id")
);

CREATE TABLE IF NOT EXISTS "debts_forgiven" (
    "transaction_id"            TEXT NOT NULL UNIQUE,
    "amount"                    INTEGER NOT NULL,
    PRIMARY KEY("transaction_id")
);

CREATE TABLE IF NOT EXISTS "transaction_types" (
    "value"                     INTEGER NOT NULL UNIQUE,
    "description"               TEXT NOT NULL UNIQUE,
    PRIMARY KEY("value")
);

CREATE TABLE IF NOT EXISTS "transaction_states" (
    "value"                     INTEGER NOT NULL UNIQUE,
    "description"               TEXT NOT NULL UNIQUE,
    PRIMARY KEY("value")
);

INSERT INTO "transaction_types" ("value", "description") VALUES (1, "deposit")
INSERT INTO "transaction_types" ("value", "description") VALUES (2, "withdrawal")
INSERT INTO "transaction_types" ("value", "description") VALUES (3, "transfer_debit")
INSERT INTO "transaction_types" ("value", "description") VALUES (4, "transfer_debit_rollback")
INSERT INTO "transaction_types" ("value", "description") VALUES (5, "transfer_credit")
INSERT INTO "transaction_types" ("value", "description") VALUES (6, "request_debit")
INSERT INTO "transaction_types" ("value", "description") VALUES (7, "request_debit_rollback")
INSERT INTO "transaction_types" ("value", "description") VALUES (8, "request_credit")
INSERT INTO "transaction_types" ("value", "description") VALUES (9, "debt_forgiveness")

INSERT INTO "transaction_states" ("value", "description") VALUES (1, "request_sent")
INSERT INTO "transaction_states" ("value", "description") VALUES (2, "request_received")
INSERT INTO "transaction_states" ("value", "description") VALUES (3, "request_rejected")
INSERT INTO "transaction_states" ("value", "description") VALUES (4, "request_accepted")
INSERT INTO "transaction_states" ("value", "description") VALUES (5, "rejection_received")
INSERT INTO "transaction_states" ("value", "description") VALUES (6, "request_debited")
INSERT INTO "transaction_states" ("value", "description") VALUES (7, "request_credited")
INSERT INTO "transaction_states" ("value", "description") VALUES (8, "request_debit_rollback")
INSERT INTO "transaction_states" ("value", "description") VALUES (9, "transfer_debit")
INSERT INTO "transaction_states" ("value", "description") VALUES (10, "transfer_credit")
INSERT INTO "transaction_states" ("value", "description") VALUES (11, "transfer_debit_rollback")