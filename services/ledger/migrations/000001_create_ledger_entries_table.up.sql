CREATE TABLE ledger_entries (
    id              UUID        PRIMARY KEY,
    transaction_id  UUID        NOT NULL,
    account_id      UUID        NOT NULL,
    direction       TEXT        NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount_minor    BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency        CHAR(3)     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_ledger_transaction_id ON ledger_entries (transaction_id);
CREATE INDEX idx_ledger_account_id ON ledger_entries (account_id, created_at DESC);
