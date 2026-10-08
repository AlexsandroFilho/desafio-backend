package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultOutboxInterval  = 500 * time.Millisecond
	defaultOutboxBatchSize = 50
	maxOutboxBackoff       = time.Minute
)

type SQSClient interface {
	SendMessage(
		ctx context.Context,
		params *sqs.SendMessageInput,
		optFns ...func(*sqs.Options),
	) (*sqs.SendMessageOutput, error)
}

type OutboxMetrics interface {
	ObserveOutboxEvent(outcome string)
	SetOutboxPending(count int64)
}

type OutboxWorker struct {
	db        *pgxpool.Pool
	sqsClient SQSClient
	queueURL  string
	interval  time.Duration
	batchSize int
	logger    *slog.Logger
	metrics   OutboxMetrics

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
	wg      sync.WaitGroup
}

func NewOutboxWorker(
	db *pgxpool.Pool,
	sqsClient SQSClient,
	queueURL string,
	metrics ...OutboxMetrics,
) *OutboxWorker {
	worker := &OutboxWorker{
		db:        db,
		sqsClient: sqsClient,
		queueURL:  queueURL,
		interval:  defaultOutboxInterval,
		batchSize: defaultOutboxBatchSize,
		logger:    slog.Default(),
	}
	if len(metrics) > 0 {
		worker.metrics = metrics[0]
	}
	return worker
}

// Start launches one background publishing loop. Repeated calls while the
// worker is running are ignored.
func (w *OutboxWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}

	workerCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.running = true
	w.wg.Add(1)
	w.mu.Unlock()

	go w.run(workerCtx)
}

// Stop cancels the publishing loop and waits for its graceful termination.
func (w *OutboxWorker) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	cancel := w.cancel
	w.mu.Unlock()

	cancel()
	w.wg.Wait()
}

func (w *OutboxWorker) run(ctx context.Context) {
	defer func() {
		w.mu.Lock()
		w.running = false
		w.cancel = nil
		w.mu.Unlock()
		w.wg.Done()
	}()

	w.processCycle(ctx)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.processCycle(ctx)
		}
	}
}

func (w *OutboxWorker) processCycle(ctx context.Context) {
	if err := w.publishPendingEvents(ctx); err != nil && ctx.Err() == nil {
		w.logger.Error("outbox publishing cycle failed", "error", err)
	}
	w.updatePendingGauge(ctx)
}

func (w *OutboxWorker) publishPendingEvents(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin outbox transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	events, err := loadPendingOutboxEvents(ctx, tx, w.batchSize)
	if err != nil {
		return err
	}

	published := 0
	retries := 0
	for _, event := range events {
		err := w.sendEvent(ctx, event)
		if err != nil {
			if updateErr := markOutboxRetry(ctx, tx, event, err); updateErr != nil {
				return updateErr
			}
			retries++
			w.logger.Warn(
				"outbox event publication failed",
				"outbox_id", event.id,
				"event_type", event.eventType,
				"retry_count", event.retryCount+1,
				"error", err,
			)
			continue
		}

		if err := markOutboxPublished(ctx, tx, event.id); err != nil {
			return err
		}
		published++
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox transaction: %w", err)
	}
	if w.metrics != nil {
		for range published {
			w.metrics.ObserveOutboxEvent("published")
		}
		for range retries {
			w.metrics.ObserveOutboxEvent("retry")
		}
	}

	return nil
}

func (w *OutboxWorker) updatePendingGauge(ctx context.Context) {
	if w.metrics == nil || ctx.Err() != nil {
		return
	}

	var pending int64
	if err := w.db.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM outbox WHERE status = 'PENDING'`,
	).Scan(&pending); err != nil {
		w.logger.Warn("failed to collect pending outbox metric", "error", err)
		return
	}
	w.metrics.SetOutboxPending(pending)
}

type outboxEvent struct {
	id          string
	eventType   string
	aggregateID string
	payload     []byte
	retryCount  int
}

func loadPendingOutboxEvents(
	ctx context.Context,
	tx pgx.Tx,
	limit int,
) ([]outboxEvent, error) {
	const query = `
		SELECT id, event_type, aggregate_id, payload, retry_count
		FROM outbox
		WHERE status = 'PENDING'
		  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		ORDER BY created_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	`

	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]outboxEvent, 0, limit)
	for rows.Next() {
		var event outboxEvent
		if err := rows.Scan(
			&event.id,
			&event.eventType,
			&event.aggregateID,
			&event.payload,
			&event.retryCount,
		); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox events: %w", err)
	}

	return events, nil
}

func (w *OutboxWorker) sendEvent(ctx context.Context, event outboxEvent) error {
	_, err := w.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(w.queueURL),
		MessageBody:            aws.String(string(event.payload)),
		MessageGroupId:         aws.String(event.aggregateID),
		MessageDeduplicationId: aws.String(event.id),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"event_type": {
				DataType:    aws.String("String"),
				StringValue: aws.String(event.eventType),
			},
			"outbox_id": {
				DataType:    aws.String("String"),
				StringValue: aws.String(event.id),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("send SQS message: %w", err)
	}

	return nil
}

func markOutboxPublished(ctx context.Context, tx pgx.Tx, eventID string) error {
	const query = `
		UPDATE outbox
		SET status = 'PUBLISHED',
			next_retry_at = NULL
		WHERE id = $1
	`

	commandTag, err := tx.Exec(ctx, query, eventID)
	if err != nil {
		return fmt.Errorf("mark outbox event as published: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("mark outbox event as published: event %s not found", eventID)
	}

	return nil
}

func markOutboxRetry(
	ctx context.Context,
	tx pgx.Tx,
	event outboxEvent,
	publishErr error,
) error {
	const query = `
		UPDATE outbox
		SET retry_count = retry_count + 1,
			next_retry_at = $1
		WHERE id = $2
	`

	nextRetryAt := time.Now().UTC().Add(outboxBackoff(event.retryCount + 1))
	commandTag, err := tx.Exec(ctx, query, nextRetryAt, event.id)
	if err != nil {
		return fmt.Errorf(
			"schedule retry for outbox event %s after publication error %v: %w",
			event.id,
			publishErr,
			err,
		)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("schedule outbox retry: event %s not found", event.id)
	}

	return nil
}

func outboxBackoff(retryCount int) time.Duration {
	if retryCount <= 0 {
		return time.Second
	}

	backoff := time.Second
	for attempt := 1; attempt < retryCount && backoff < maxOutboxBackoff; attempt++ {
		backoff *= 2
	}
	if backoff > maxOutboxBackoff {
		return maxOutboxBackoff
	}
	return backoff
}
