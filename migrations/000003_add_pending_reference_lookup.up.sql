CREATE INDEX wager_transactions_pending_reference_lookup_idx
    ON wager_transactions (
        provider_id,
        reference_external_transaction_id,
        created_at,
        id
    )
    WHERE status = 'PENDING_REFERENCE';
