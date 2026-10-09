DROP INDEX IF EXISTS wager_transactions_pending_reference_retry_idx;

ALTER TABLE wager_transactions
    DROP COLUMN IF EXISTS reference_expires_at,
    DROP COLUMN IF EXISTS reference_next_retry_at,
    DROP COLUMN IF EXISTS reference_retry_count;

CREATE INDEX wager_transactions_pending_reference_lookup_idx
    ON wager_transactions (
        provider_id,
        reference_external_transaction_id,
        created_at,
        id
    )
    WHERE status = 'PENDING_REFERENCE';
