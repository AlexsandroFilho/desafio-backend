package worker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/repository"
	"alexsandro-filho/desafio-backend/internal/usecase"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSQSRedeliveryAfterCommitWithoutDeleteIsIdempotentIntegration(t *testing.T) {
	fixture := newRecoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	wallet := fixture.createWallet(t, ctx, "100.00")
	body := fixture.wagerMessage(
		"msg-"+fixture.providerID+"-redeliver",
		"bet-redeliver-001",
		wallet.ID(),
		"10.00",
		domain.TransactionKindBet,
	)
	consumer := NewSQSConsumer(nil, fixture.wager, "command-queue")

	if !consumer.processMessage(ctx, body) {
		t.Fatal("first SQS delivery must be acknowledged after commit")
	}
	if consumer.processMessage(ctx, body) != true {
		t.Fatal("redelivered SQS message must be acknowledged after idempotent replay")
	}

	var transactions, ledgerEntries int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND kind = 'BET'),
			(SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $2 AND amount = 1000)
	`, fixture.providerID, wallet.ID()).Scan(&transactions, &ledgerEntries); err != nil {
		t.Fatalf("query redelivery accounting: %v", err)
	}
	if transactions != 1 || ledgerEntries != 1 {
		t.Errorf(
			"redelivery accounting = transactions %d ledger %d, want 1 and 1",
			transactions,
			ledgerEntries,
		)
	}
	fixture.assertWalletBalance(t, ctx, wallet.ID(), 9000)
}

func TestHTTPThenSQSSameOperationReplaysWithoutSecondDebitIntegration(t *testing.T) {
	fixture := newRecoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	wallet := fixture.createWallet(t, ctx, "100.00")
	amount := recoveryMoney(t, "10.00")
	httpResponse, err := fixture.wager.ProcessTransaction(ctx, usecase.WagerRequest{
		ProviderID:            fixture.providerID,
		ExternalTransactionID: "http-then-sqs-001",
		IdempotencyKey:        "http-then-sqs-001",
		WalletID:              wallet.ID(),
		PlayerID:              fixture.playerID,
		RoundID:               fixture.roundID,
		GameID:                "recovery-game",
		Kind:                  domain.TransactionKindBet,
		Amount:                amount,
	})
	if err != nil {
		t.Fatalf("HTTP ProcessTransaction() error = %v", err)
	}
	if httpResponse.Status != domain.TransactionStatusProcessed {
		t.Fatalf("HTTP status = %s, want PROCESSED", httpResponse.Status)
	}

	consumer := NewSQSConsumer(nil, fixture.wager, "command-queue")
	body := fixture.wagerMessage(
		"msg-"+fixture.providerID+"-http-then-sqs",
		"http-then-sqs-001",
		wallet.ID(),
		"10.00",
		domain.TransactionKindBet,
	)
	if !consumer.processMessage(ctx, body) {
		t.Fatal("SQS delivery of an HTTP-processed operation must be acknowledged")
	}

	sqsResponse, err := fixture.wager.GetTransactionByExternalID(
		ctx,
		fixture.providerID,
		"http-then-sqs-001",
	)
	if err != nil {
		t.Fatalf("GetTransactionByExternalID() error = %v", err)
	}
	if sqsResponse.ID() != httpResponse.TransactionID {
		t.Errorf(
			"SQS replay ID = %s, want HTTP transaction %s",
			sqsResponse.ID(),
			httpResponse.TransactionID,
		)
	}
	fixture.assertWalletBalance(t, ctx, wallet.ID(), 9000)
}

func TestPendingReferenceSurvivesRestartAndResolvesIntegration(t *testing.T) {
	fixture := newRecoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	wallet := fixture.createWallet(t, ctx, "100.00")
	amount := recoveryMoney(t, "10.00")
	pending, err := fixture.wager.ProcessTransaction(ctx, usecase.WagerRequest{
		ProviderID:                     fixture.providerID,
		ExternalTransactionID:          "refund-restart-001",
		IdempotencyKey:                 "refund-restart-001",
		WalletID:                       wallet.ID(),
		PlayerID:                       fixture.playerID,
		RoundID:                        fixture.roundID,
		GameID:                         "recovery-game",
		Kind:                           domain.TransactionKindRefund,
		Amount:                         amount,
		ReferenceExternalTransactionID: "bet-restart-001",
	})
	if err != nil {
		t.Fatalf("create pending refund: %v", err)
	}
	if pending.Status != domain.TransactionStatusPendingReference {
		t.Fatalf("pending status = %s, want PENDING_REFERENCE", pending.Status)
	}

	restarted := usecase.NewWagerUseCase(
		fixture.secondPool,
		repository.NewPostgresWalletRepository(),
		repository.NewPostgresTransactionRepository(),
		repository.NewPostgresLedgerRepository(),
		repository.NewPostgresInboxOutboxRepository(),
	)
	if _, err := restarted.ProcessTransaction(ctx, usecase.WagerRequest{
		ProviderID:            fixture.providerID,
		ExternalTransactionID: "bet-restart-001",
		IdempotencyKey:        "bet-restart-001",
		WalletID:              wallet.ID(),
		PlayerID:              fixture.playerID,
		RoundID:               fixture.roundID,
		GameID:                "recovery-game",
		Kind:                  domain.TransactionKindBet,
		Amount:                amount,
	}); err != nil {
		t.Fatalf("process original bet after restart: %v", err)
	}

	if _, err := restarted.ProcessPendingReferences(ctx, 10); err != nil {
		t.Fatalf("ProcessPendingReferences after restart: %v", err)
	}

	resolved, err := restarted.GetTransactionByID(ctx, fixture.providerID, pending.TransactionID)
	if err != nil {
		t.Fatalf("GetTransactionByID after restart: %v", err)
	}
	if resolved.Status() != domain.TransactionStatusProcessed {
		t.Errorf("restarted refund status = %s, want PROCESSED", resolved.Status())
	}
	fixture.assertWalletBalance(t, ctx, wallet.ID(), 10000)
}

func TestCompetingOutboxWorkersPublishEachEventOnceIntegration(t *testing.T) {
	fixture := newRecoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const events = 12
	ids := make([]string, 0, events)
	for index := 0; index < events; index++ {
		ids = append(ids, fixture.insertOutboxEvent(t, ctx, fmt.Sprintf("event-%02d", index)))
	}

	client := &countingSQSClient{}
	workerA := NewOutboxWorker(fixture.pool, client, "event-queue")
	workerB := NewOutboxWorker(fixture.secondPool, client, "event-queue")

	start := make(chan struct{})
	errors := make(chan error, 2)
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		errors <- publishOutboxIDs(ctx, fixture.pool, workerA, ids)
	}()
	go func() {
		defer group.Done()
		<-start
		errors <- publishOutboxIDs(ctx, fixture.secondPool, workerB, ids)
	}()
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Errorf("competing outbox worker error = %v", err)
		}
	}

	publishedIDs := client.deduplicationIDs()
	if len(publishedIDs) != events {
		t.Errorf("published events = %d, want %d", len(publishedIDs), events)
	}
	seen := map[string]int{}
	for _, id := range publishedIDs {
		seen[id]++
	}
	for _, id := range ids {
		if seen[id] != 1 {
			t.Errorf("event %s published %d times, want 1", id, seen[id])
		}
	}

	var pending int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM outbox
		WHERE id = ANY($1::uuid[])
		  AND status = 'PENDING'
	`, ids).Scan(&pending); err != nil {
		t.Fatalf("count pending outbox events: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending outbox events after competing publishers = %d, want 0", pending)
	}
}

func TestOutboxRepublishesSameEventIDAfterCrashBeforeAckIntegration(t *testing.T) {
	fixture := newRecoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	eventID := fixture.insertOutboxEvent(t, ctx, "crash-before-ack")
	client := &countingSQSClient{}
	publisher := NewOutboxWorker(fixture.pool, client, "event-queue")

	tx, err := fixture.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin crash simulation: %v", err)
	}
	event, err := claimOutboxEvent(ctx, tx, eventID)
	if err != nil {
		t.Fatalf("claim outbox event: %v", err)
	}
	if err := publisher.sendEvent(ctx, event); err != nil {
		t.Fatalf("sendEvent before crash: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback crash simulation: %v", err)
	}

	var status string
	if err := fixture.pool.QueryRow(ctx, `
		SELECT status FROM outbox WHERE id = $1
	`, eventID).Scan(&status); err != nil {
		t.Fatalf("query outbox status after crash: %v", err)
	}
	if status != "PENDING" {
		t.Errorf("outbox status after crash = %s, want PENDING", status)
	}

	recovered := NewOutboxWorker(fixture.secondPool, client, "event-queue")
	if err := publishOutboxIDs(ctx, fixture.secondPool, recovered, []string{eventID}); err != nil {
		t.Fatalf("recovering publisher error = %v", err)
	}

	ids := client.deduplicationIDs()
	if len(ids) != 2 || ids[0] != eventID || ids[1] != eventID {
		t.Errorf("republished IDs = %v, want [%s %s]", ids, eventID, eventID)
	}
	if err := fixture.pool.QueryRow(ctx, `
		SELECT status FROM outbox WHERE id = $1
	`, eventID).Scan(&status); err != nil {
		t.Fatalf("query recovered outbox status: %v", err)
	}
	if status != "PUBLISHED" {
		t.Errorf("outbox status after recovery = %s, want PUBLISHED", status)
	}
}

func TestOutboxRetryThenSuccessfulPublishIntegration(t *testing.T) {
	fixture := newRecoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	eventID := fixture.insertOutboxEvent(t, ctx, "retry-then-publish")
	client := &countingSQSClient{failures: 1}
	publisher := NewOutboxWorker(fixture.pool, client, "event-queue")
	if err := publishOutboxIDs(ctx, fixture.pool, publisher, []string{eventID}); err != nil {
		t.Fatalf("first publish cycle error = %v", err)
	}

	if _, err := fixture.pool.Exec(ctx, `
		UPDATE outbox
		SET next_retry_at = NOW()
		WHERE id = $1
	`, eventID); err != nil {
		t.Fatalf("force outbox retry window: %v", err)
	}
	if err := publishOutboxIDs(ctx, fixture.pool, publisher, []string{eventID}); err != nil {
		t.Fatalf("retry publish cycle error = %v", err)
	}

	ids := client.deduplicationIDs()
	if len(ids) != 1 || ids[0] != eventID {
		t.Errorf("successful publish IDs = %v, want [%s]", ids, eventID)
	}
	var status string
	var retryCount int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT status, retry_count FROM outbox WHERE id = $1
	`, eventID).Scan(&status, &retryCount); err != nil {
		t.Fatalf("query retried outbox event: %v", err)
	}
	if status != "PUBLISHED" || retryCount != 1 {
		t.Errorf("retried outbox = status %s retry %d, want PUBLISHED and 1", status, retryCount)
	}
}

type recoveryFixture struct {
	pool       *pgxpool.Pool
	secondPool *pgxpool.Pool
	wager      *usecase.WagerUseCase
	wallet     *usecase.WalletUseCase
	providerID string
	playerID   string
	roundID    string
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	secondPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		pool.Close()
		t.Fatalf("pgxpool.New() second instance error = %v", err)
	}
	suffix := recoveryUUID(t)
	fixture := &recoveryFixture{
		pool:       pool,
		secondPool: secondPool,
		providerID: "recovery-provider-" + suffix,
		playerID:   "recovery-player-" + suffix,
		roundID:    "recovery-round-" + suffix,
	}
	fixture.wager = usecase.NewWagerUseCase(
		pool,
		repository.NewPostgresWalletRepository(),
		repository.NewPostgresTransactionRepository(),
		repository.NewPostgresLedgerRepository(),
		repository.NewPostgresInboxOutboxRepository(),
	)
	fixture.wallet = usecase.NewWalletUseCase(
		pool,
		repository.NewPostgresWalletRepository(),
		repository.NewPostgresTransactionRepository(),
		repository.NewPostgresLedgerRepository(),
		repository.NewPostgresInboxOutboxRepository(),
	)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		tx, err := pool.Begin(cleanupCtx)
		if err != nil {
			secondPool.Close()
			pool.Close()
			return
		}
		defer func() { _ = tx.Rollback(cleanupCtx) }()
		_, _ = tx.Exec(cleanupCtx, `SET LOCAL app.allow_ledger_maintenance = 'on'`)
		_, _ = tx.Exec(cleanupCtx, `
			DELETE FROM outbox
			WHERE aggregate_id IN (
					SELECT id FROM wallets WHERE provider_id = $1
			   )
			   OR aggregate_id IN (
					SELECT id FROM wager_transactions WHERE provider_id = $1
			   )
			   OR payload->>'eventType' = 'RecoveryTestEvent'
		`, fixture.providerID)
		_, _ = tx.Exec(cleanupCtx, `
			DELETE FROM wallet_ledger_entries
			WHERE wallet_id IN (SELECT id FROM wallets WHERE provider_id = $1)
		`, fixture.providerID)
		_, _ = tx.Exec(
			cleanupCtx,
			`DELETE FROM inbox WHERE message_id LIKE $1`,
			"%"+fixture.providerID+"%",
		)
		_, _ = tx.Exec(cleanupCtx, `DELETE FROM wager_idempotency_keys WHERE provider_id = $1`, fixture.providerID)
		_, _ = tx.Exec(cleanupCtx, `DELETE FROM wager_transactions WHERE provider_id = $1`, fixture.providerID)
		_, _ = tx.Exec(cleanupCtx, `DELETE FROM wallets WHERE provider_id = $1`, fixture.providerID)
		_ = tx.Commit(cleanupCtx)
		secondPool.Close()
		pool.Close()
	})
	return fixture
}

func (fixture *recoveryFixture) createWallet(
	t *testing.T,
	ctx context.Context,
	balance string,
) *domain.Wallet {
	t.Helper()
	wallet, err := fixture.wallet.CreateWallet(ctx, fixture.providerID, usecase.CreateWalletCommand{
		PlayerID:       fixture.playerID,
		InitialBalance: recoveryMoney(t, balance),
	})
	if err != nil {
		t.Fatalf("CreateWallet() error = %v", err)
	}
	return wallet
}

func (fixture *recoveryFixture) wagerMessage(
	messageID string,
	externalID string,
	walletID string,
	amount string,
	kind domain.TransactionKind,
) string {
	payload := map[string]any{
		"messageId":  messageID,
		"type":       wagerRequestedType,
		"occurredAt": "2026-09-08T12:00:00.000Z",
		"data": map[string]any{
			"providerId":            fixture.providerID,
			"externalTransactionId": externalID,
			"idempotencyKey":        externalID,
			"walletId":              walletID,
			"playerId":              fixture.playerID,
			"roundId":               fixture.roundID,
			"gameId":                "recovery-game",
			"kind":                  kind,
			"money": map[string]string{
				"amount":   amount,
				"currency": "BRL",
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return string(body)
}

func (fixture *recoveryFixture) insertOutboxEvent(
	t *testing.T,
	ctx context.Context,
	label string,
) string {
	t.Helper()
	eventID := recoveryUUID(t)
	aggregateID := recoveryUUID(t)
	payload := fmt.Sprintf(
		`{"eventId":%q,"eventType":"RecoveryTestEvent","aggregateId":%q,"label":%q}`,
		eventID,
		aggregateID,
		label,
	)
	if _, err := fixture.pool.Exec(ctx, `
		INSERT INTO outbox (
			id, event_type, aggregate_id, payload, status, retry_count, created_at
		)
		VALUES ($1, 'RecoveryTestEvent', $2, $3::jsonb, 'PENDING', 0, NOW())
	`, eventID, aggregateID, payload); err != nil {
		t.Fatalf("insert outbox fixture %s: %v", label, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = fixture.pool.Exec(cleanupCtx, `DELETE FROM outbox WHERE id = $1`, eventID)
	})
	return eventID
}

func (fixture *recoveryFixture) assertWalletBalance(
	t *testing.T,
	ctx context.Context,
	walletID string,
	wantCents int64,
) {
	t.Helper()
	var balance int64
	if err := fixture.pool.QueryRow(ctx, `
		SELECT balance FROM wallets WHERE id = $1
	`, walletID).Scan(&balance); err != nil {
		t.Fatalf("query wallet balance: %v", err)
	}
	if balance != wantCents {
		t.Errorf("wallet balance = %d, want %d", balance, wantCents)
	}
}

type countingSQSClient struct {
	mu       sync.Mutex
	failures int
	ids      []string
}

func (c *countingSQSClient) SendMessage(
	_ context.Context,
	input *sqs.SendMessageInput,
	_ ...func(*sqs.Options),
) (*sqs.SendMessageOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failures > 0 {
		c.failures--
		return nil, fmt.Errorf("simulated SQS publish failure")
	}
	c.ids = append(c.ids, aws.ToString(input.MessageDeduplicationId))
	return &sqs.SendMessageOutput{}, nil
}

func (c *countingSQSClient) deduplicationIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	copied := make([]string, len(c.ids))
	copy(copied, c.ids)
	return copied
}

func publishOutboxIDs(
	ctx context.Context,
	pool *pgxpool.Pool,
	publisher *OutboxWorker,
	ids []string,
) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin scoped outbox transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	events, err := claimOutboxEvents(ctx, tx, ids)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := publisher.sendEvent(ctx, event); err != nil {
			if updateErr := markOutboxRetry(ctx, tx, event, err); updateErr != nil {
				return updateErr
			}
			continue
		}
		if err := markOutboxPublished(ctx, tx, event.id); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit scoped outbox transaction: %w", err)
	}
	return nil
}

func claimOutboxEvent(
	ctx context.Context,
	tx pgx.Tx,
	eventID string,
) (outboxEvent, error) {
	events, err := claimOutboxEvents(ctx, tx, []string{eventID})
	if err != nil {
		return outboxEvent{}, err
	}
	if len(events) != 1 {
		return outboxEvent{}, fmt.Errorf("claimed %d outbox events, want 1", len(events))
	}
	return events[0], nil
}

func claimOutboxEvents(
	ctx context.Context,
	tx pgx.Tx,
	ids []string,
) ([]outboxEvent, error) {
	const query = `
		SELECT id, event_type, aggregate_id, payload, retry_count
		FROM outbox
		WHERE id = ANY($1::uuid[])
		  AND status = 'PENDING'
		  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
		ORDER BY created_at, id
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.Query(ctx, query, ids)
	if err != nil {
		return nil, fmt.Errorf("claim scoped outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]outboxEvent, 0, len(ids))
	for rows.Next() {
		var event outboxEvent
		if err := rows.Scan(
			&event.id,
			&event.eventType,
			&event.aggregateID,
			&event.payload,
			&event.retryCount,
		); err != nil {
			return nil, fmt.Errorf("scan scoped outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scoped outbox events: %w", err)
	}
	return events, nil
}

func recoveryMoney(t *testing.T, amount string) domain.Money {
	t.Helper()
	money, err := domain.NewMoneyFromString(amount, "BRL")
	if err != nil {
		t.Fatalf("NewMoneyFromString(%q) error = %v", amount, err)
	}
	return money
}

func recoveryUUID(t *testing.T) string {
	t.Helper()
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("rand.Read() error = %v", err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		value[0:4],
		value[4:6],
		value[6:8],
		value[8:10],
		value[10:16],
	)
}
