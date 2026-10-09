ALTER TABLE wager_transactions
    ADD COLUMN reference_retry_count INTEGER NOT NULL DEFAULT 0
        CHECK (reference_retry_count >= 0),
    ADD COLUMN reference_next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN reference_expires_at TIMESTAMPTZ NOT NULL
        DEFAULT (NOW() + INTERVAL '24 hours');

UPDATE wager_transactions
SET reference_next_retry_at = created_at,
    reference_expires_at = created_at + INTERVAL '24 hours'
WHERE status = 'PENDING_REFERENCE';

DROP INDEX IF EXISTS wager_transactions_pending_reference_lookup_idx;

CREATE INDEX wager_transactions_pending_reference_retry_idx
    ON wager_transactions (
        reference_next_retry_at,
        created_at,
        id
    )
    WHERE status = 'PENDING_REFERENCE';
