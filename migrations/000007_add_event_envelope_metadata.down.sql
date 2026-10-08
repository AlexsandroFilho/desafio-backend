UPDATE outbox
SET payload = payload -> 'data'
WHERE payload ? 'eventId'
  AND payload ? 'data';

ALTER TABLE wager_transactions
    DROP COLUMN IF EXISTS causation_id,
    DROP COLUMN IF EXISTS correlation_id;
