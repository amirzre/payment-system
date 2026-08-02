CREATE TABLE payment_intents (
    id                  UUID        PRIMARY KEY,
    idempotency_key     TEXT        NOT NULL UNIQUE,
    source_account_id   UUID        NOT NULL,
    destination_account_id UUID    NOT NULL,
    amount_minor        BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency            CHAR(3)     NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'created'
                                    CHECK (status IN ('created', 'funds_reserved', 'ledger_posted', 'completed', 'failed', 'payment_failed', 'compensated')),
    transaction_id      UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_payment_intents_idempotency_key ON payment_intents (idempotency_key);
