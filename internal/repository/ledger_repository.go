package repository

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidLedgerCursor = errors.New("invalid ledger cursor")
	ErrInvalidLedgerLimit  = errors.New("ledger limit must be greater than zero")
)

type DBExecutor interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type LedgerRepository interface {
	Create(ctx context.Context, tx pgx.Tx, entry *domain.WalletLedgerEntry) error
	GetEntriesByWalletID(
		ctx context.Context,
		db DBExecutor,
		providerID string,
		walletID string,
		cursor string,
		limit int,
	) ([]domain.WalletLedgerEntry, string, error)
}

type PostgresLedgerRepository struct{}

func NewPostgresLedgerRepository() *PostgresLedgerRepository {
	return &PostgresLedgerRepository{}
}

func (r *PostgresLedgerRepository) Create(
	ctx context.Context,
	tx pgx.Tx,
	entry *domain.WalletLedgerEntry,
) error {
	const query = `
		INSERT INTO wallet_ledger_entries (
			id,
			wallet_id,
			transaction_id,
			direction,
			amount,
			balance_before,
			balance_after,
			created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err := tx.Exec(
		ctx,
		query,
		entry.ID(),
		entry.WalletID(),
		entry.TransactionID(),
		entry.Direction(),
		entry.Amount().AmountCents(),
		entry.BalanceBefore().AmountCents(),
		entry.BalanceAfter().AmountCents(),
		entry.CreatedAt(),
	)
	if err != nil {
		return fmt.Errorf("create wallet ledger entry: %w", err)
	}

	return nil
}

func (r *PostgresLedgerRepository) GetEntriesByWalletID(
	ctx context.Context,
	db DBExecutor,
	providerID string,
	walletID string,
	cursor string,
	limit int,
) ([]domain.WalletLedgerEntry, string, error) {
	if limit <= 0 {
		return nil, "", ErrInvalidLedgerLimit
	}

	const firstPageQuery = `
		SELECT
			e.id,
			e.wallet_id,
			e.transaction_id,
			e.direction,
			e.amount,
			e.balance_before,
			e.balance_after,
			e.created_at,
			w.currency
		FROM wallet_ledger_entries e
		INNER JOIN wallets w ON w.id = e.wallet_id
		WHERE w.provider_id = $1
		  AND e.wallet_id = $2
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $3
	`

	const nextPageQuery = `
		SELECT
			e.id,
			e.wallet_id,
			e.transaction_id,
			e.direction,
			e.amount,
			e.balance_before,
			e.balance_after,
			e.created_at,
			w.currency
		FROM wallet_ledger_entries e
		INNER JOIN wallets w ON w.id = e.wallet_id
		WHERE w.provider_id = $1
		  AND e.wallet_id = $2
		  AND (e.created_at, e.id) < ($3, $4::uuid)
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $5
	`

	fetchLimit := limit + 1
	var (
		rows pgx.Rows
		err  error
	)

	if cursor == "" {
		rows, err = db.Query(ctx, firstPageQuery, providerID, walletID, fetchLimit)
	} else {
		decodedCursor, decodeErr := decodeLedgerCursor(cursor)
		if decodeErr != nil {
			return nil, "", decodeErr
		}
		rows, err = db.Query(
			ctx,
			nextPageQuery,
			providerID,
			walletID,
			decodedCursor.CreatedAt,
			decodedCursor.ID,
			fetchLimit,
		)
	}
	if err != nil {
		return nil, "", fmt.Errorf("query wallet ledger entries: %w", err)
	}
	defer rows.Close()

	entries := make([]domain.WalletLedgerEntry, 0, fetchLimit)
	for rows.Next() {
		entry, scanErr := scanLedgerEntry(rows)
		if scanErr != nil {
			return nil, "", scanErr
		}
		entries = append(entries, *entry)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate wallet ledger entries: %w", err)
	}

	if len(entries) <= limit {
		return entries, "", nil
	}

	entries = entries[:limit]
	nextCursor, err := encodeLedgerCursor(entries[len(entries)-1])
	if err != nil {
		return nil, "", err
	}

	return entries, nextCursor, nil
}

func scanLedgerEntry(row pgx.Row) (*domain.WalletLedgerEntry, error) {
	var (
		id            string
		walletID      string
		transactionID string
		direction     domain.LedgerDirection
		amount        int64
		balanceBefore int64
		balanceAfter  int64
		createdAt     time.Time
		currency      string
	)

	if err := row.Scan(
		&id,
		&walletID,
		&transactionID,
		&direction,
		&amount,
		&balanceBefore,
		&balanceAfter,
		&createdAt,
		&currency,
	); err != nil {
		return nil, fmt.Errorf("scan wallet ledger entry: %w", err)
	}

	amountMoney, err := moneyFromCents(amount, currency)
	if err != nil {
		return nil, fmt.Errorf("map ledger amount: %w", err)
	}
	beforeMoney, err := moneyFromCents(balanceBefore, currency)
	if err != nil {
		return nil, fmt.Errorf("map ledger balance before: %w", err)
	}
	afterMoney, err := moneyFromCents(balanceAfter, currency)
	if err != nil {
		return nil, fmt.Errorf("map ledger balance after: %w", err)
	}

	entry, err := domain.RehydrateWalletLedgerEntry(
		id,
		walletID,
		transactionID,
		direction,
		amountMoney,
		beforeMoney,
		afterMoney,
		createdAt,
	)
	if err != nil {
		return nil, fmt.Errorf("rehydrate wallet ledger entry: %w", err)
	}

	return entry, nil
}

type ledgerCursor struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

func encodeLedgerCursor(entry domain.WalletLedgerEntry) (string, error) {
	data, err := json.Marshal(ledgerCursor{
		ID:        entry.ID(),
		CreatedAt: entry.CreatedAt(),
	})
	if err != nil {
		return "", fmt.Errorf("encode ledger cursor: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeLedgerCursor(cursor string) (ledgerCursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return ledgerCursor{}, ErrInvalidLedgerCursor
	}

	var decoded ledgerCursor
	if err := json.Unmarshal(data, &decoded); err != nil {
		return ledgerCursor{}, ErrInvalidLedgerCursor
	}
	if decoded.ID == "" || decoded.CreatedAt.IsZero() {
		return ledgerCursor{}, ErrInvalidLedgerCursor
	}

	return decoded, nil
}

var _ LedgerRepository = (*PostgresLedgerRepository)(nil)
