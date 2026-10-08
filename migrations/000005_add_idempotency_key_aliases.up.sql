CREATE TABLE wager_idempotency_keys (
    provider_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    transaction_id UUID NOT NULL REFERENCES wager_transactions (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider_id, idempotency_key)
);

INSERT INTO wager_idempotency_keys (
    provider_id,
    idempotency_key,
    transaction_id
)
SELECT provider_id, idempotency_key, id
FROM wager_transactions;
