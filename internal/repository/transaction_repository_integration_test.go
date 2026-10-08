package repository

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresTransactionRepositoryReferenceIntegrityIntegration(t *testing.T) {
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
	defer pool.Close()

	dbTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin() error = %v", err)
	}
	defer func() { _ = dbTx.Rollback(ctx) }()

	walletID := integrationUUID(t)
	if _, err := dbTx.Exec(ctx, `
		INSERT INTO wallets (id, provider_id, player_id, currency, balance)
		VALUES ($1, 'integration-provider', 'integration-player', 'BRL', 10000)
	`, walletID); err != nil {
		t.Fatalf("insert wallet fixture: %v", err)
	}

	repository := NewPostgresTransactionRepository()
	amount := integrationMoney(t, "10.00")
	original := integrationTransaction(
		t,
		integrationUUID(t),
		"integration-original-"+integrationUUID(t),
		walletID,
		domain.TransactionKindBet,
		amount,
		"",
		"",
	)
	if err := repository.Create(ctx, dbTx, original); err != nil {
		t.Fatalf("create original transaction: %v", err)
	}

	reversal := integrationTransaction(
		t,
		integrationUUID(t),
		"integration-refund-"+integrationUUID(t),
		walletID,
		domain.TransactionKindRefund,
		amount,
		original.ExternalTransactionID(),
		original.ID(),
	)
	if err := repository.Create(ctx, dbTx, reversal); err != nil {
		t.Fatalf("create first reversal: %v", err)
	}

	loaded, err := repository.GetByProviderAndExternalIDForUpdate(
		ctx,
		dbTx,
		reversal.ProviderID(),
		reversal.ExternalTransactionID(),
	)
	if err != nil {
		t.Fatalf("load reversal for update: %v", err)
	}
	if loaded.ReferenceTransactionID() != original.ID() {
		t.Fatalf(
			"ReferenceTransactionID() = %q, want %q",
			loaded.ReferenceTransactionID(),
			original.ID(),
		)
	}

	duplicate := integrationTransaction(
		t,
		integrationUUID(t),
		"integration-rollback-"+integrationUUID(t),
		walletID,
		domain.TransactionKindRollback,
		amount,
		original.ExternalTransactionID(),
		original.ID(),
	)
	err = repository.Create(ctx, dbTx, duplicate)

	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) ||
		postgresError.Code != "23505" ||
		postgresError.ConstraintName != "wager_transactions_single_processed_reversal_idx" {
		t.Fatalf("duplicate reversal error = %v, want unique reversal constraint", err)
	}
}

func integrationTransaction(
	t *testing.T,
	id string,
	externalID string,
	walletID string,
	kind domain.TransactionKind,
	amount domain.Money,
	referenceExternalID string,
	referenceTransactionID string,
) *domain.WagerTransaction {
	t.Helper()

	transaction, err := domain.NewWagerTransaction(domain.NewWagerTransactionParams{
		ID:                             id,
		ProviderID:                     "integration-provider",
		ExternalTransactionID:          externalID,
		IdempotencyKey:                 externalID,
		PayloadHash:                    "integration-hash",
		CorrelationID:                  id,
		WalletID:                       walletID,
		PlayerID:                       "integration-player",
		RoundID:                        "integration-round",
		GameID:                         "integration-game",
		Kind:                           kind,
		Money:                          amount,
		ReferenceExternalTransactionID: referenceExternalID,
		ReferenceTransactionID:         referenceTransactionID,
	})
	if err != nil {
		t.Fatalf("NewWagerTransaction() error = %v", err)
	}
	if err := transaction.RecordWalletResult(amount, 1); err != nil {
		t.Fatalf("RecordWalletResult() error = %v", err)
	}
	if err := transaction.MarkProcessed(); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}
	return transaction
}

func integrationMoney(t *testing.T, amount string) domain.Money {
	t.Helper()

	money, err := domain.NewMoneyFromString(amount, "BRL")
	if err != nil {
		t.Fatalf("NewMoneyFromString() error = %v", err)
	}
	return money
}

func integrationUUID(t *testing.T) string {
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
