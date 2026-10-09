package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrWalletNotFound       = errors.New("wallet not found")
	ErrWalletAlreadyExists  = errors.New("wallet already exists for provider, player, and currency")
	ErrWalletUpdateConflict = errors.New("wallet update conflict")
)

type WalletRepository interface {
	Create(ctx context.Context, tx pgx.Tx, providerID string, wallet *domain.Wallet) error
	Get(
		ctx context.Context,
		tx pgx.Tx,
		providerID string,
		walletID string,
	) (*domain.Wallet, error)
	GetForUpdate(
		ctx context.Context,
		tx pgx.Tx,
		providerID string,
		walletID string,
	) (*domain.Wallet, error)
	UpdateBalance(
		ctx context.Context,
		tx pgx.Tx,
		providerID string,
		wallet *domain.Wallet,
	) error
}

type PostgresWalletRepository struct{}

func NewPostgresWalletRepository() *PostgresWalletRepository {
	return &PostgresWalletRepository{}
}

func (r *PostgresWalletRepository) Create(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	wallet *domain.Wallet,
) error {
	const query = `
		INSERT INTO wallets (
			id,
			provider_id,
			player_id,
			currency,
			balance,
			version,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err := tx.Exec(
		ctx,
		query,
		wallet.ID(),
		providerID,
		wallet.PlayerID(),
		wallet.Currency(),
		wallet.Balance().AmountCents(),
		wallet.Version(),
		wallet.CreatedAt(),
		wallet.UpdatedAt(),
	)
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return ErrWalletAlreadyExists
		}
		return fmt.Errorf("create wallet: %w", err)
	}

	return nil
}

func (r *PostgresWalletRepository) Get(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	walletID string,
) (*domain.Wallet, error) {
	const query = `
		SELECT id, player_id, currency, balance, version, created_at, updated_at
		FROM wallets
		WHERE provider_id = $1
		  AND id = $2
	`

	return scanWallet(
		tx.QueryRow(ctx, query, providerID, walletID),
		"get wallet",
	)
}

func (r *PostgresWalletRepository) GetForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	walletID string,
) (*domain.Wallet, error) {
	const query = `
		SELECT id, player_id, currency, balance, version, created_at, updated_at
		FROM wallets
		WHERE provider_id = $1
		  AND id = $2
		FOR UPDATE
	`

	return scanWallet(
		tx.QueryRow(ctx, query, providerID, walletID),
		"get wallet for update",
	)
}

func scanWallet(row pgx.Row, operation string) (*domain.Wallet, error) {
	var (
		id        string
		playerID  string
		currency  string
		balance   int64
		version   int64
		createdAt time.Time
		updatedAt time.Time
	)

	err := row.Scan(
		&id,
		&playerID,
		&currency,
		&balance,
		&version,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}
		return nil, fmt.Errorf("%s: %w", operation, err)
	}

	money, err := moneyFromCents(balance, currency)
	if err != nil {
		return nil, fmt.Errorf("map wallet balance: %w", err)
	}

	return domain.RehydrateWallet(
		id,
		playerID,
		currency,
		money,
		version,
		createdAt,
		updatedAt,
	), nil
}

func (r *PostgresWalletRepository) UpdateBalance(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	wallet *domain.Wallet,
) error {
	const query = `
		UPDATE wallets
		SET balance = $1,
			version = $2,
			updated_at = $3
		WHERE provider_id = $4
		  AND id = $5
		  AND version = $6
	`

	previousVersion := wallet.Version() - 1
	commandTag, err := tx.Exec(
		ctx,
		query,
		wallet.Balance().AmountCents(),
		wallet.Version(),
		wallet.UpdatedAt(),
		providerID,
		wallet.ID(),
		previousVersion,
	)
	if err != nil {
		return fmt.Errorf("update wallet balance: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrWalletUpdateConflict
	}

	return nil
}

func moneyFromCents(amount int64, currency string) (domain.Money, error) {
	if amount >= 0 {
		return domain.NewMoneyFromString(
			fmt.Sprintf("%d.%02d", amount/100, amount%100),
			currency,
		)
	}

	magnitude := uint64(-(amount + 1)) + 1
	return domain.NewMoneyFromString(
		fmt.Sprintf("-%d.%02d", magnitude/100, magnitude%100),
		currency,
	)
}

var _ WalletRepository = (*PostgresWalletRepository)(nil)
