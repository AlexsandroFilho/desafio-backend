ALTER TABLE wager_transactions
    ADD COLUMN result_balance BIGINT,
    ADD COLUMN result_wallet_version BIGINT;

UPDATE wager_transactions AS transaction
SET result_balance = COALESCE(
        (
            SELECT ledger.balance_after
            FROM wallet_ledger_entries AS ledger
            WHERE ledger.transaction_id = transaction.id
            LIMIT 1
        ),
        wallet.balance
    ),
    result_wallet_version = wallet.version
FROM wallets AS wallet
WHERE wallet.id = transaction.wallet_id;

ALTER TABLE wager_transactions
    ALTER COLUMN result_balance SET NOT NULL,
    ALTER COLUMN result_wallet_version SET NOT NULL,
    ADD CONSTRAINT wager_transactions_result_balance_non_negative
        CHECK (result_balance >= 0),
    ADD CONSTRAINT wager_transactions_result_version_positive
        CHECK (result_wallet_version >= 1);

CREATE UNIQUE INDEX wager_transactions_provider_idempotency_key_unique
    ON wager_transactions (provider_id, idempotency_key);
