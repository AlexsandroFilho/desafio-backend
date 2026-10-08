ALTER TABLE wager_transactions
    ADD COLUMN reference_transaction_id UUID
        REFERENCES wager_transactions (id);

UPDATE wager_transactions AS reversal
SET reference_transaction_id = original.id
FROM wager_transactions AS original
WHERE reversal.reference_transaction_id IS NULL
  AND reversal.kind IN ('REFUND', 'ROLLBACK')
  AND reversal.reference_external_transaction_id IS NOT NULL
  AND original.provider_id = reversal.provider_id
  AND original.external_transaction_id =
      reversal.reference_external_transaction_id;

CREATE INDEX wager_transactions_reference_transaction_id_idx
    ON wager_transactions (reference_transaction_id)
    WHERE reference_transaction_id IS NOT NULL;

CREATE UNIQUE INDEX wager_transactions_single_processed_reversal_idx
    ON wager_transactions (reference_transaction_id)
    WHERE reference_transaction_id IS NOT NULL
      AND kind IN ('REFUND', 'ROLLBACK')
      AND status = 'PROCESSED';
