ALTER TABLE wager_transactions
    ADD COLUMN correlation_id TEXT,
    ADD COLUMN causation_id TEXT;

UPDATE wager_transactions
SET correlation_id = id::text
WHERE correlation_id IS NULL;

ALTER TABLE wager_transactions
    ALTER COLUMN correlation_id SET NOT NULL;

UPDATE outbox
SET payload = jsonb_strip_nulls(
    jsonb_build_object(
        'eventId', id::text,
        'eventType', event_type,
        'aggregateId', aggregate_id::text,
        'correlationId', aggregate_id::text,
        'causationId', NULL,
        'occurredAt', created_at,
        'version', 1,
        'data', payload
    )
)
WHERE NOT (payload ? 'eventId');
