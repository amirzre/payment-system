CREATE UNIQUE INDEX idx_payment_transaction_id
    ON payment_intents (transaction_id)
    WHERE transaction_id IS NOT NULL;

ALTER TABLE payment_intents DROP CONSTRAINT payment_intents_status_check;
ALTER TABLE payment_intents ADD CONSTRAINT payment_intents_status_check
    CHECK (status IN ('created', 'posting', 'funds_reserved', 'ledger_posted', 'completed', 'failed', 'payment_failed', 'compensated'));
