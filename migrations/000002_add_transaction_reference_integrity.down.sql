DROP INDEX IF EXISTS wager_transactions_single_processed_reversal_idx;
DROP INDEX IF EXISTS wager_transactions_reference_transaction_id_idx;

ALTER TABLE wager_transactions
    DROP COLUMN IF EXISTS reference_transaction_id;
