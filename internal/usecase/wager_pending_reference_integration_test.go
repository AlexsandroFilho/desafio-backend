package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/repository"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPendingReferenceIsProcessedAfterOriginalArrivesIntegration(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	defer pool.Close()

	walletID, err := newUUID()
	if err != nil {
		t.Fatalf("newUUID() error = %v", err)
	}
	suffix, err := newUUID()
	if err != nil {
		t.Fatalf("newUUID() error = %v", err)
	}
	providerID := "pending-provider-" + suffix
	playerID := "pending-player-" + suffix
	roundID := "pending-round-" + suffix

	if _, err := pool.Exec(ctx, `
		INSERT INTO wallets (id, provider_id, player_id, currency, balance)
		VALUES ($1, $2, $3, 'BRL', 10000)
	`, walletID, providerID, playerID); err != nil {
		t.Fatalf("insert wallet fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupTx, beginErr := pool.Begin(cleanupCtx)
		if beginErr != nil {
			return
		}
		defer func() { _ = cleanupTx.Rollback(cleanupCtx) }()
		_, _ = cleanupTx.Exec(
			cleanupCtx,
			`SET LOCAL app.allow_ledger_maintenance = 'on'`,
		)
		_, _ = cleanupTx.Exec(cleanupCtx, `
			DELETE FROM outbox
			WHERE aggregate_id = $1
			   OR aggregate_id IN (
				SELECT id FROM wager_transactions WHERE provider_id = $2
			)
		`, walletID, providerID)
		_, _ = cleanupTx.Exec(cleanupCtx, `
			DELETE FROM wallet_ledger_entries
			WHERE transaction_id IN (
				SELECT id FROM wager_transactions WHERE provider_id = $1
			)
		`, providerID)
		_, _ = cleanupTx.Exec(
			cleanupCtx,
			`DELETE FROM wager_idempotency_keys WHERE provider_id = $1`,
			providerID,
		)
		_, _ = cleanupTx.Exec(
			cleanupCtx,
			`DELETE FROM wager_transactions WHERE provider_id = $1`,
			providerID,
		)
		_, _ = cleanupTx.Exec(
			cleanupCtx,
			`DELETE FROM wallets WHERE id = $1`,
			walletID,
		)
		_ = cleanupTx.Commit(cleanupCtx)
	})

	walletRepository := repository.NewPostgresWalletRepository()
	transactionRepository := repository.NewPostgresTransactionRepository()
	ledgerRepository := repository.NewPostgresLedgerRepository()
	inboxOutboxRepository := repository.NewPostgresInboxOutboxRepository()
	uc := NewWagerUseCase(
		pool,
		walletRepository,
		transactionRepository,
		ledgerRepository,
		inboxOutboxRepository,
	)
	walletUseCase := NewWalletUseCase(
		pool,
		walletRepository,
		transactionRepository,
		ledgerRepository,
		inboxOutboxRepository,
	)
	amount := mustMoney(t, "10.00")

	otherProviderID := "other-" + providerID
	if _, err := walletUseCase.GetWallet(
		ctx,
		otherProviderID,
		walletID,
	); !errors.Is(err, repository.ErrWalletNotFound) {
		t.Fatalf("cross-provider GetWallet() error = %v, want wallet not found", err)
	}
	if _, _, err := walletUseCase.GetLedger(
		ctx,
		otherProviderID,
		walletID,
		"",
		10,
	); !errors.Is(err, repository.ErrWalletNotFound) {
		t.Fatalf("cross-provider GetLedger() error = %v, want wallet not found", err)
	}
	if _, err := uc.ProcessTransaction(ctx, WagerRequest{
		ProviderID:            otherProviderID,
		ExternalTransactionID: "cross-provider-" + suffix,
		IdempotencyKey:        "cross-provider-key-" + suffix,
		WalletID:              walletID,
		PlayerID:              playerID,
		RoundID:               roundID,
		GameID:                "game-1",
		Kind:                  domain.TransactionKindBet,
		Amount:                amount,
	}); !errors.Is(err, repository.ErrWalletNotFound) {
		t.Fatalf(
			"cross-provider ProcessTransaction() error = %v, want wallet not found",
			err,
		)
	}

	refundResponse, err := uc.ProcessTransaction(ctx, WagerRequest{
		ProviderID:                     providerID,
		ExternalTransactionID:          "refund-" + suffix,
		IdempotencyKey:                 "refund-key-" + suffix,
		WalletID:                       walletID,
		PlayerID:                       playerID,
		RoundID:                        roundID,
		GameID:                         "game-1",
		Kind:                           domain.TransactionKindRefund,
		Amount:                         amount,
		ReferenceExternalTransactionID: "bet-" + suffix,
	})
	if err != nil {
		t.Fatalf("ProcessTransaction(refund) error = %v", err)
	}
	if refundResponse.Status != domain.TransactionStatusPendingReference {
		t.Fatalf(
			"refund status = %s, want %s",
			refundResponse.Status,
			domain.TransactionStatusPendingReference,
		)
	}

	betRequest := WagerRequest{
		ProviderID:            providerID,
		ExternalTransactionID: "bet-" + suffix,
		IdempotencyKey:        "bet-key-" + suffix,
		WalletID:              walletID,
		PlayerID:              playerID,
		RoundID:               roundID,
		GameID:                "game-1",
		Kind:                  domain.TransactionKindBet,
		Amount:                amount,
	}
	betResponse, err := uc.ProcessTransaction(ctx, betRequest)
	if err != nil {
		t.Fatalf("ProcessTransaction(bet) error = %v", err)
	}
	if betResponse.Status != domain.TransactionStatusProcessed {
		t.Fatalf("bet status = %s, want PROCESSED", betResponse.Status)
	}
	byExternalID, err := uc.GetTransactionByExternalID(
		ctx,
		providerID,
		betRequest.ExternalTransactionID,
	)
	if err != nil {
		t.Fatalf("GetTransactionByExternalID() error = %v", err)
	}
	if byExternalID.ID() != betResponse.TransactionID {
		t.Errorf(
			"external lookup transaction ID = %s, want %s",
			byExternalID.ID(),
			betResponse.TransactionID,
		)
	}
	if _, err := uc.GetTransactionByExternalID(
		ctx,
		otherProviderID,
		betRequest.ExternalTransactionID,
	); !errors.Is(err, repository.ErrTransactionNotFound) {
		t.Errorf(
			"cross-provider external lookup error = %v, want transaction not found",
			err,
		)
	}
	if _, err := uc.GetTransactionByID(
		ctx,
		otherProviderID,
		betResponse.TransactionID,
	); !errors.Is(err, repository.ErrTransactionNotFound) {
		t.Fatalf(
			"cross-provider GetTransactionByID() error = %v, want transaction not found",
			err,
		)
	}

	processed, err := uc.ProcessPendingReferences(ctx, 10)
	if err != nil {
		t.Fatalf("ProcessPendingReferences() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("ProcessPendingReferences() = %d, want 1", processed)
	}

	refund, err := uc.GetTransactionByID(ctx, providerID, refundResponse.TransactionID)
	if err != nil {
		t.Fatalf("GetTransactionByID(refund) error = %v", err)
	}
	if refund.Status() != domain.TransactionStatusProcessed {
		t.Errorf("resolved refund status = %s, want PROCESSED", refund.Status())
	}
	if refund.ReferenceTransactionID() != betResponse.TransactionID {
		t.Errorf(
			"resolved reference ID = %q, want %q",
			refund.ReferenceTransactionID(),
			betResponse.TransactionID,
		)
	}

	var balance int64
	if err := pool.QueryRow(
		ctx,
		`SELECT balance FROM wallets WHERE id = $1`,
		walletID,
	).Scan(&balance); err != nil {
		t.Fatalf("query final wallet balance: %v", err)
	}
	if balance != 10000 {
		t.Errorf("final wallet balance = %d, want 10000", balance)
	}

	var ledgerEntries int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM wallet_ledger_entries
		WHERE wallet_id = $1
	`, walletID).Scan(&ledgerEntries); err != nil {
		t.Fatalf("count ledger entries: %v", err)
	}
	if ledgerEntries != 2 {
		t.Errorf("ledger entries = %d, want 2", ledgerEntries)
	}

	winAmount := mustMoney(t, "5.00")
	if _, err := uc.ProcessTransaction(ctx, WagerRequest{
		ProviderID:            providerID,
		ExternalTransactionID: "win-" + suffix,
		IdempotencyKey:        "win-key-" + suffix,
		WalletID:              walletID,
		PlayerID:              playerID,
		RoundID:               roundID,
		GameID:                "game-1",
		Kind:                  domain.TransactionKindWin,
		Amount:                winAmount,
	}); err != nil {
		t.Fatalf("ProcessTransaction(win) error = %v", err)
	}

	replay, err := uc.ProcessTransaction(ctx, betRequest)
	if err != nil {
		t.Fatalf("ProcessTransaction(bet replay) error = %v", err)
	}
	if !replay.IdempotentReplay {
		t.Error("bet replay must be identified as idempotent")
	}
	if replay.TransactionID != betResponse.TransactionID {
		t.Errorf(
			"replay transaction ID = %q, want %q",
			replay.TransactionID,
			betResponse.TransactionID,
		)
	}
	if replay.Balance.AmountCents() != 9000 || replay.WalletVersion != 2 {
		t.Errorf(
			"replay result = balance %d version %d, want balance 9000 version 2",
			replay.Balance.AmountCents(),
			replay.WalletVersion,
		)
	}

	differentKey := betRequest
	differentKey.IdempotencyKey = "different-key-" + suffix
	differentKeyReplay, err := uc.ProcessTransaction(ctx, differentKey)
	if err != nil {
		t.Fatalf("same external ID with different key error = %v", err)
	}
	if !differentKeyReplay.IdempotentReplay ||
		differentKeyReplay.TransactionID != betResponse.TransactionID {
		t.Error("same external ID and payload must replay regardless of a new key")
	}

	reusedAlias := differentKey
	reusedAlias.ExternalTransactionID = "alias-reused-external-" + suffix
	if _, err := uc.ProcessTransaction(ctx, reusedAlias); !errors.Is(
		err,
		ErrIdempotencyConflict,
	) {
		t.Errorf("reused replay alias error = %v, want conflict", err)
	}

	reusedKey := betRequest
	reusedKey.ExternalTransactionID = "different-external-" + suffix
	if _, err := uc.ProcessTransaction(ctx, reusedKey); !errors.Is(
		err,
		ErrIdempotencyConflict,
	) {
		t.Errorf("reused idempotency key error = %v, want conflict", err)
	}

	if err := pool.QueryRow(
		ctx,
		`SELECT balance FROM wallets WHERE id = $1`,
		walletID,
	).Scan(&balance); err != nil {
		t.Fatalf("query wallet balance after replay: %v", err)
	}
	if balance != 10500 {
		t.Errorf("wallet balance after replay = %d, want 10500", balance)
	}

	expiringResponse, err := uc.ProcessTransaction(ctx, WagerRequest{
		ProviderID:                     providerID,
		ExternalTransactionID:          "expiring-refund-" + suffix,
		IdempotencyKey:                 "expiring-refund-key-" + suffix,
		WalletID:                       walletID,
		PlayerID:                       playerID,
		RoundID:                        "expiring-round-" + suffix,
		GameID:                         "game-1",
		Kind:                           domain.TransactionKindRefund,
		Amount:                         mustMoney(t, "3.00"),
		ReferenceExternalTransactionID: "never-arrives-" + suffix,
	})
	if err != nil {
		t.Fatalf("create expiring pending reference: %v", err)
	}
	if expiringResponse.Status != domain.TransactionStatusPendingReference {
		t.Fatalf(
			"expiring reference status = %s, want PENDING_REFERENCE",
			expiringResponse.Status,
		)
	}

	processed, err = uc.ProcessPendingReferences(ctx, 10)
	if err != nil {
		t.Fatalf("schedule pending reference retry: %v", err)
	}
	if processed != 0 {
		t.Fatalf("finalized pending references = %d, want 0", processed)
	}

	var (
		retryCount  int
		nextRetryAt time.Time
	)
	if err := pool.QueryRow(ctx, `
		SELECT reference_retry_count, reference_next_retry_at
		FROM wager_transactions
		WHERE id = $1
	`, expiringResponse.TransactionID).Scan(
		&retryCount,
		&nextRetryAt,
	); err != nil {
		t.Fatalf("query pending reference retry: %v", err)
	}
	if retryCount != 1 || !nextRetryAt.After(time.Now().UTC()) {
		t.Errorf(
			"pending retry = count %d next %s, want count 1 in the future",
			retryCount,
			nextRetryAt,
		)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE wager_transactions
		SET reference_retry_count = $1,
			reference_next_retry_at = NOW()
		WHERE id = $2
	`, pendingReferenceMaxAttempts-1, expiringResponse.TransactionID); err != nil {
		t.Fatalf("prepare pending reference expiration: %v", err)
	}
	processed, err = uc.ProcessPendingReferences(ctx, 10)
	if err != nil {
		t.Fatalf("expire pending reference: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expired pending references = %d, want 1", processed)
	}

	expired, err := uc.GetTransactionByID(
		ctx,
		providerID,
		expiringResponse.TransactionID,
	)
	if err != nil {
		t.Fatalf("GetTransactionByID(expired) error = %v", err)
	}
	if expired.Status() != domain.TransactionStatusRejected ||
		expired.FailureCode() != "REFERENCE_NOT_FOUND" {
		t.Errorf(
			"expired reference = status %s failure %q, want REJECTED REFERENCE_NOT_FOUND",
			expired.Status(),
			expired.FailureCode(),
		)
	}
}
