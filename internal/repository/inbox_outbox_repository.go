package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"alexsandro-filho/desafio-backend/internal/event"
	"github.com/jackc/pgx/v5"
)

var ErrInboxHashMismatch = errors.New("inbox message hash mismatch")

type InboxOutboxRepository interface {
	SaveInbox(
		ctx context.Context,
		tx pgx.Tx,
		consumerName string,
		messageID string,
		hash string,
	) error
	SaveOutbox(
		ctx context.Context,
		tx pgx.Tx,
		envelope event.Envelope,
	) error
}

type PostgresInboxOutboxRepository struct{}

func NewPostgresInboxOutboxRepository() *PostgresInboxOutboxRepository {
	return &PostgresInboxOutboxRepository{}
}

func (r *PostgresInboxOutboxRepository) SaveInbox(
	ctx context.Context,
	tx pgx.Tx,
	consumerName string,
	messageID string,
	hash string,
) error {
	const query = `
		INSERT INTO inbox (consumer_name, message_id, hash, processed_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (consumer_name, message_id)
		DO UPDATE SET hash = inbox.hash
		RETURNING inbox.hash
	`

	var storedHash string
	if err := tx.QueryRow(ctx, query, consumerName, messageID, hash).Scan(&storedHash); err != nil {
		return fmt.Errorf("save inbox message: %w", err)
	}
	if storedHash != hash {
		return ErrInboxHashMismatch
	}

	return nil
}

func (r *PostgresInboxOutboxRepository) SaveOutbox(
	ctx context.Context,
	tx pgx.Tx,
	envelope event.Envelope,
) error {
	encodedPayload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}

	const query = `
		INSERT INTO outbox (
			id,
			event_type,
			aggregate_id,
			payload,
			status,
			retry_count,
			next_retry_at,
			created_at
		)
		VALUES (
			$1,
			$2,
			$3,
			$4,
			'PENDING',
			0,
			NULL,
			NOW()
		)
	`

	if _, err := tx.Exec(
		ctx,
		query,
		envelope.EventID,
		envelope.EventType,
		envelope.AggregateID,
		string(encodedPayload),
	); err != nil {
		return fmt.Errorf("save outbox event: %w", err)
	}

	return nil
}

var _ InboxOutboxRepository = (*PostgresInboxOutboxRepository)(nil)
