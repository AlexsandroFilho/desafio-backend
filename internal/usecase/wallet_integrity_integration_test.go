package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/repository"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestZeroBalanceWalletDoesNotCreateFinancialOpeningIntegration(t *testing.T) {
	pool := newWalletIntegrityPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	suffix, err := newUUID()
	if err != nil {
		t.Fatalf("newUUID() error = %v", err)
	}
	providerID := "zero-opening-provider-" + suffix
	playerID := "zero-opening-player-" + suffix
	walletUseCase, _ := newWalletIntegrityUseCases(pool)

	wallet, err := walletUseCase.CreateWallet(ctx, providerID, CreateWalletCommand{
		PlayerID:       playerID,
		InitialBalance: mustMoney(t, "0.00"),
	})
	if err != nil {
		t.Fatalf("CreateWallet() error = %v", err)
	}
	registerWalletIntegrityCleanup(t, pool, providerID, wallet.ID())

	var transactions, ledgerEntries, outboxEvents int
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1),
			(SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1),
			(SELECT COUNT(*) FROM outbox WHERE aggregate_id = $1)
	`, wallet.ID()).Scan(
		&transactions,
		&ledgerEntries,
		&outboxEvents,
	); err != nil {
		t.Fatalf("query zero opening records: %v", err)
	}
	if transactions != 0 || ledgerEntries != 0 || outboxEvents != 0 {
		t.Errorf(
			"zero opening created transactions=%d ledger=%d outbox=%d, want all zero",
			transactions,
			ledgerEntries,
			outboxEvents,
		)
	}
}

func TestWalletResidualIntegrityRulesIntegration(t *testing.T) {
	pool := newWalletIntegrityPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	suffix, err := newUUID()
	if err != nil {
		t.Fatalf("newUUID() error = %v", err)
	}
	providerID := "wallet-integrity-provider-" + suffix
	playerID := "wallet-integrity-player-" + suffix
	walletUseCase, wagerUseCase := newWalletIntegrityUseCases(pool)

	wallet, err := walletUseCase.CreateWallet(ctx, providerID, CreateWalletCommand{
		PlayerID:       playerID,
		InitialBalance: mustMoney(t, "10.00"),
	})
	if err != nil {
		t.Fatalf("CreateWallet() error = %v", err)
	}
	registerWalletIntegrityCleanup(t, pool, providerID, wallet.ID())

	if _, err := walletUseCase.CreateWallet(ctx, providerID, CreateWalletCommand{
		PlayerID:       playerID,
		InitialBalance: mustMoney(t, "1.00"),
	}); !errors.Is(err, repository.ErrWalletAlreadyExists) {
		t.Errorf("duplicate CreateWallet() error = %v, want wallet already exists", err)
	}

	reconciliation, err := walletUseCase.ReconcileWallet(
		ctx,
		providerID,
		wallet.ID(),
	)
	if err != nil {
		t.Fatalf("ReconcileWallet() error = %v", err)
	}
	if reconciliation.WalletID != wallet.ID() ||
		reconciliation.StoredBalance.AmountCents() != 1000 ||
		reconciliation.CalculatedBalance.AmountCents() != 1000 ||
		!reconciliation.Difference.IsZero() ||
		!reconciliation.Consistent ||
		reconciliation.CheckedEntries != 1 {
		t.Errorf("unexpected reconciliation result: %+v", reconciliation)
	}

	usd, err := domain.NewMoneyFromString("1.00", "USD")
	if err != nil {
		t.Fatalf("create USD money: %v", err)
	}
	if _, err := wagerUseCase.ProcessTransaction(ctx, WagerRequest{
		ProviderID:            providerID,
		ExternalTransactionID: "currency-mismatch-" + suffix,
		IdempotencyKey:        "currency-mismatch-key-" + suffix,
		WalletID:              wallet.ID(),
		PlayerID:              playerID,
		RoundID:               "round-" + suffix,
		GameID:                "game-1",
		Kind:                  domain.TransactionKindBet,
		Amount:                usd,
	}); !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Errorf("currency mismatch error = %v, want currency mismatch", err)
	}

	if _, err := wagerUseCase.ProcessTransaction(ctx, WagerRequest{
		ProviderID:            providerID,
		ExternalTransactionID: "external-opening-" + suffix,
		IdempotencyKey:        "external-opening-key-" + suffix,
		WalletID:              wallet.ID(),
		PlayerID:              playerID,
		Kind:                  domain.TransactionKindOpening,
		Amount:                mustMoney(t, "1.00"),
	}); !errors.Is(err, ErrUnsupportedWagerKind) {
		t.Errorf("external OPENING error = %v, want unsupported kind", err)
	}

	var ledgerID string
	if err := pool.QueryRow(ctx, `
		SELECT id
		FROM wallet_ledger_entries
		WHERE wallet_id = $1
	`, wallet.ID()).Scan(&ledgerID); err != nil {
		t.Fatalf("query opening ledger entry: %v", err)
	}
	assertLedgerMutationRejected(t, ctx, pool, `
		UPDATE wallet_ledger_entries SET amount = amount + 1 WHERE id = $1
	`, ledgerID)
	assertLedgerMutationRejected(t, ctx, pool, `
		DELETE FROM wallet_ledger_entries WHERE id = $1
	`, ledgerID)
}

func assertLedgerMutationRejected(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	query string,
	ledgerID string,
) {
	t.Helper()

	_, err := pool.Exec(ctx, query, ledgerID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" {
		t.Errorf("ledger mutation error = %v, want PostgreSQL 55000", err)
	}
}

func newWalletIntegrityPool(t *testing.T) *pgxpool.Pool {
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
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping integration database: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newWalletIntegrityUseCases(
	pool *pgxpool.Pool,
) (*WalletUseCase, *WagerUseCase) {
	walletRepository := repository.NewPostgresWalletRepository()
	transactionRepository := repository.NewPostgresTransactionRepository()
	ledgerRepository := repository.NewPostgresLedgerRepository()
	inboxOutboxRepository := repository.NewPostgresInboxOutboxRepository()
	return NewWalletUseCase(
			pool,
			walletRepository,
			transactionRepository,
			ledgerRepository,
			inboxOutboxRepository,
		), NewWagerUseCase(
			pool,
			walletRepository,
			transactionRepository,
			ledgerRepository,
			inboxOutboxRepository,
		)
}

func registerWalletIntegrityCleanup(
	t *testing.T,
	pool *pgxpool.Pool,
	providerID string,
	walletID string,
) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := pool.Begin(ctx)
		if err != nil {
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()
		_, _ = tx.Exec(ctx, `SET LOCAL app.allow_ledger_maintenance = 'on'`)
		_, _ = tx.Exec(ctx, `
			DELETE FROM outbox
			WHERE aggregate_id = $1
			   OR aggregate_id IN (
					SELECT id FROM wager_transactions WHERE provider_id = $2
			   )
		`, walletID, providerID)
		_, _ = tx.Exec(
			ctx,
			`DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`,
			walletID,
		)
		_, _ = tx.Exec(
			ctx,
			`DELETE FROM wager_idempotency_keys WHERE provider_id = $1`,
			providerID,
		)
		_, _ = tx.Exec(
			ctx,
			`DELETE FROM wager_transactions WHERE provider_id = $1`,
			providerID,
		)
		_, _ = tx.Exec(
			ctx,
			`DELETE FROM wallets WHERE provider_id = $1 AND id = $2`,
			providerID,
			walletID,
		)
		_ = tx.Commit(ctx)
	})
}
