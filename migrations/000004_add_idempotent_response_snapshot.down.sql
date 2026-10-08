DROP INDEX IF EXISTS wager_transactions_provider_idempotency_key_unique;

ALTER TABLE wager_transactions
    DROP CONSTRAINT IF EXISTS wager_transactions_result_version_positive,
    DROP CONSTRAINT IF EXISTS wager_transactions_result_balance_non_negative,
    DROP COLUMN IF EXISTS result_wallet_version,
    DROP COLUMN IF EXISTS result_balance;
