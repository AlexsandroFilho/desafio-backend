CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id TEXT NOT NULL,
    currency VARCHAR(3) NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency)
);

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,
    provider_id TEXT NOT NULL,
    external_transaction_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    wallet_id UUID NOT NULL REFERENCES wallets (id),
    player_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    game_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (
        kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')
    ),
    amount BIGINT NOT NULL CHECK (amount >= 0),
    currency VARCHAR(3) NOT NULL,
    reference_external_transaction_id TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')
    ),
    failure_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT wager_transactions_provider_external_unique
        UNIQUE (provider_id, external_transaction_id)
);

CREATE INDEX wager_transactions_wallet_id_idx
    ON wager_transactions (wallet_id);

CREATE INDEX wager_transactions_reference_idx
    ON wager_transactions (provider_id, reference_external_transaction_id)
    WHERE reference_external_transaction_id IS NOT NULL;

CREATE TABLE wallet_ledger_entries (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets (id),
    transaction_id UUID NOT NULL REFERENCES wager_transactions (id),
    direction TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount BIGINT NOT NULL CHECK (amount >= 0),
    balance_before BIGINT NOT NULL CHECK (balance_before >= 0),
    balance_after BIGINT NOT NULL CHECK (balance_after >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT wallet_ledger_entries_wallet_transaction_unique
        UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_entries_balance_consistency CHECK (
        (direction = 'DEBIT' AND balance_after = balance_before - amount)
        OR
        (direction = 'CREDIT' AND balance_after = balance_before + amount)
    )
);

CREATE INDEX wallet_ledger_entries_wallet_created_at_idx
    ON wallet_ledger_entries (wallet_id, created_at);

CREATE TABLE inbox (
    consumer_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    hash TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_retry_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX outbox_pending_events_idx
    ON outbox (status, next_retry_at, created_at);
