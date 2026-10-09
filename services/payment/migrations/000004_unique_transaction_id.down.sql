DROP INDEX IF EXISTS idx_payment_transaction_id;
ALTER TABLE payment_intents DROP CONSTRAINT payment_intents_status_check;
ALTER TABLE payment_intents ADD CONSTRAINT payment_intents_status_check
    CHECK (status IN ('created', 'funds_reserved', 'ledger_posted', 'completed', 'failed', 'payment_failed', 'compensated'));
