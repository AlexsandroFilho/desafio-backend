package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"github.com/jackc/pgx/v5"
)

var (
	ErrTransactionNotFound    = errors.New("transaction not found")
	ErrIdempotencyKeyConflict = errors.New("idempotency key belongs to another transaction")
)

type TransactionRepository interface {
	Create(ctx context.Context, tx pgx.Tx, txn *domain.WagerTransaction) error
	GetByProviderAndExternalID(
		ctx context.Context,
		tx pgx.Tx,
		providerID string,
		externalID string,
	) (*domain.WagerTransaction, error)
	GetByProviderAndIdempotencyKey(
		ctx context.Context,
		tx pgx.Tx,
		providerID string,
		idempotencyKey string,
	) (*domain.WagerTransaction, error)
	BindIdempotencyKey(
		ctx context.Context,
		tx pgx.Tx,
		providerID string,
		idempotencyKey string,
		transactionID string,
	) error
	GetByProviderAndExternalIDForUpdate(
		ctx context.Context,
		tx pgx.Tx,
		providerID string,
		externalID string,
	) (*domain.WagerTransaction, error)
	HasProcessedReversal(
		ctx context.Context,
		tx pgx.Tx,
		referenceTransactionID string,
	) (bool, error)
	GetPendingReferenceForUpdate(
		ctx context.Context,
		tx pgx.Tx,
	) (*domain.WagerTransaction, int, time.Time, error)
	SchedulePendingReferenceRetry(
		ctx context.Context,
		tx pgx.Tx,
		transactionID string,
		retryCount int,
		nextRetryAt time.Time,
	) error
	UpdatePendingReference(
		ctx context.Context,
		tx pgx.Tx,
		txn *domain.WagerTransaction,
	) error
	GetByID(
		ctx context.Context,
		tx pgx.Tx,
		providerID string,
		transactionID string,
	) (*domain.WagerTransaction, error)
	UpdateStatus(
		ctx context.Context,
		tx pgx.Tx,
		id string,
		status domain.TransactionStatus,
		failureCode string,
	) error
}

type PostgresTransactionRepository struct{}

func NewPostgresTransactionRepository() *PostgresTransactionRepository {
	return &PostgresTransactionRepository{}
}

func (r *PostgresTransactionRepository) Create(
	ctx context.Context,
	tx pgx.Tx,
	txn *domain.WagerTransaction,
) error {
	const query = `
		INSERT INTO wager_transactions (
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			correlation_id,
			causation_id,
			wallet_id,
			player_id,
			round_id,
			game_id,
			kind,
			amount,
			currency,
			reference_external_transaction_id,
			reference_transaction_id,
			status,
			failure_code,
			result_balance,
			result_wallet_version,
			created_at,
			updated_at
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9,
			$10, $11, $12, $13, $14, $15, $16, $17, $18,
			$19, $20, $21, $22
		)
	`

	_, err := tx.Exec(
		ctx,
		query,
		txn.ID(),
		txn.ProviderID(),
		txn.ExternalTransactionID(),
		txn.IdempotencyKey(),
		txn.PayloadHash(),
		txn.CorrelationID(),
		nullIfEmpty(txn.CausationID()),
		txn.WalletID(),
		txn.PlayerID(),
		txn.RoundID(),
		txn.GameID(),
		txn.Kind(),
		txn.Money().AmountCents(),
		txn.Money().Currency(),
		nullIfEmpty(txn.ReferenceExternalTransactionID()),
		nullIfEmpty(txn.ReferenceTransactionID()),
		txn.Status(),
		nullIfEmpty(txn.FailureCode()),
		txn.ResultBalance().AmountCents(),
		txn.ResultWalletVersion(),
		txn.CreatedAt(),
		txn.UpdatedAt(),
	)
	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}
	if err := r.BindIdempotencyKey(
		ctx,
		tx,
		txn.ProviderID(),
		txn.IdempotencyKey(),
		txn.ID(),
	); err != nil {
		return err
	}

	return nil
}

func (r *PostgresTransactionRepository) GetByProviderAndExternalID(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	externalID string,
) (*domain.WagerTransaction, error) {
	const query = `
		SELECT
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			COALESCE(correlation_id, ''),
			COALESCE(causation_id, ''),
			wallet_id,
			player_id,
			round_id,
			game_id,
			kind,
			amount,
			currency,
			COALESCE(reference_external_transaction_id, ''),
			COALESCE(reference_transaction_id::text, ''),
			status,
			COALESCE(failure_code, ''),
			result_balance,
			result_wallet_version,
			created_at,
			updated_at
		FROM wager_transactions
		WHERE provider_id = $1
		  AND external_transaction_id = $2
	`

	return scanWagerTransaction(
		tx.QueryRow(ctx, query, providerID, externalID),
		"get transaction by provider and external ID",
	)
}

func (r *PostgresTransactionRepository) GetByProviderAndIdempotencyKey(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	idempotencyKey string,
) (*domain.WagerTransaction, error) {
	const query = `
		SELECT
			transaction.id,
			transaction.provider_id,
			transaction.external_transaction_id,
			transaction.idempotency_key,
			transaction.payload_hash,
			COALESCE(transaction.correlation_id, ''),
			COALESCE(transaction.causation_id, ''),
			transaction.wallet_id,
			transaction.player_id,
			transaction.round_id,
			transaction.game_id,
			transaction.kind,
			transaction.amount,
			transaction.currency,
			COALESCE(transaction.reference_external_transaction_id, ''),
			COALESCE(transaction.reference_transaction_id::text, ''),
			transaction.status,
			COALESCE(transaction.failure_code, ''),
			transaction.result_balance,
			transaction.result_wallet_version,
			transaction.created_at,
			transaction.updated_at
		FROM wager_idempotency_keys AS key
		JOIN wager_transactions AS transaction
		  ON transaction.id = key.transaction_id
		WHERE key.provider_id = $1
		  AND key.idempotency_key = $2
	`

	return scanWagerTransaction(
		tx.QueryRow(ctx, query, providerID, idempotencyKey),
		"get transaction by provider and idempotency key",
	)
}

func (r *PostgresTransactionRepository) BindIdempotencyKey(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	idempotencyKey string,
	transactionID string,
) error {
	const query = `
		INSERT INTO wager_idempotency_keys (
			provider_id,
			idempotency_key,
			transaction_id
		)
		VALUES ($1, $2, $3)
		ON CONFLICT (provider_id, idempotency_key)
		DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
		RETURNING transaction_id::text
	`

	var storedTransactionID string
	if err := tx.QueryRow(
		ctx,
		query,
		providerID,
		idempotencyKey,
		transactionID,
	).Scan(&storedTransactionID); err != nil {
		return fmt.Errorf("bind idempotency key: %w", err)
	}
	if storedTransactionID != transactionID {
		return ErrIdempotencyKeyConflict
	}
	return nil
}

func (r *PostgresTransactionRepository) GetByProviderAndExternalIDForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	externalID string,
) (*domain.WagerTransaction, error) {
	const query = `
		SELECT
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			COALESCE(correlation_id, ''),
			COALESCE(causation_id, ''),
			wallet_id,
			player_id,
			round_id,
			game_id,
			kind,
			amount,
			currency,
			COALESCE(reference_external_transaction_id, ''),
			COALESCE(reference_transaction_id::text, ''),
			status,
			COALESCE(failure_code, ''),
			result_balance,
			result_wallet_version,
			created_at,
			updated_at
		FROM wager_transactions
		WHERE provider_id = $1
		  AND external_transaction_id = $2
		FOR UPDATE
	`

	return scanWagerTransaction(
		tx.QueryRow(ctx, query, providerID, externalID),
		"lock transaction by provider and external ID",
	)
}

func (r *PostgresTransactionRepository) HasProcessedReversal(
	ctx context.Context,
	tx pgx.Tx,
	referenceTransactionID string,
) (bool, error) {
	const query = `
		SELECT EXISTS (
			SELECT 1
			FROM wager_transactions
			WHERE reference_transaction_id = $1
			  AND kind IN ('REFUND', 'ROLLBACK')
			  AND status = 'PROCESSED'
		)
	`

	var exists bool
	if err := tx.QueryRow(ctx, query, referenceTransactionID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check processed reversal: %w", err)
	}
	return exists, nil
}

func (r *PostgresTransactionRepository) GetPendingReferenceForUpdate(
	ctx context.Context,
	tx pgx.Tx,
) (*domain.WagerTransaction, int, time.Time, error) {
	const query = `
		SELECT
			pending.id,
			pending.provider_id,
			pending.external_transaction_id,
			pending.idempotency_key,
			pending.payload_hash,
			COALESCE(pending.correlation_id, ''),
			COALESCE(pending.causation_id, ''),
			pending.wallet_id,
			pending.player_id,
			pending.round_id,
			pending.game_id,
			pending.kind,
			pending.amount,
			pending.currency,
			COALESCE(pending.reference_external_transaction_id, ''),
			COALESCE(pending.reference_transaction_id::text, ''),
			pending.status,
			COALESCE(pending.failure_code, ''),
			pending.result_balance,
			pending.result_wallet_version,
			pending.created_at,
			pending.updated_at
		FROM wager_transactions AS pending
		WHERE pending.status = 'PENDING_REFERENCE'
		  AND pending.reference_next_retry_at <= NOW()
		ORDER BY pending.reference_next_retry_at, pending.created_at, pending.id
		FOR UPDATE OF pending SKIP LOCKED
		LIMIT 1
	`

	transaction, err := scanWagerTransaction(
		tx.QueryRow(ctx, query),
		"get pending reference for update",
	)
	if err != nil {
		return nil, 0, time.Time{}, err
	}

	var retryCount int
	var expiresAt time.Time
	if err := tx.QueryRow(ctx, `
		SELECT reference_retry_count, reference_expires_at
		FROM wager_transactions
		WHERE id = $1
	`, transaction.ID()).Scan(&retryCount, &expiresAt); err != nil {
		return nil, 0, time.Time{}, fmt.Errorf(
			"get pending reference retry metadata: %w",
			err,
		)
	}
	return transaction, retryCount, expiresAt, nil
}

func (r *PostgresTransactionRepository) SchedulePendingReferenceRetry(
	ctx context.Context,
	tx pgx.Tx,
	transactionID string,
	retryCount int,
	nextRetryAt time.Time,
) error {
	const query = `
		UPDATE wager_transactions
		SET reference_retry_count = $1,
			reference_next_retry_at = $2,
			updated_at = NOW()
		WHERE id = $3
		  AND status = 'PENDING_REFERENCE'
	`

	commandTag, err := tx.Exec(
		ctx,
		query,
		retryCount,
		nextRetryAt,
		transactionID,
	)
	if err != nil {
		return fmt.Errorf("schedule pending reference retry: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrTransactionNotFound
	}
	return nil
}

func (r *PostgresTransactionRepository) UpdatePendingReference(
	ctx context.Context,
	tx pgx.Tx,
	txn *domain.WagerTransaction,
) error {
	if txn.Status() != domain.TransactionStatusProcessed &&
		txn.Status() != domain.TransactionStatusRejected &&
		txn.Status() != domain.TransactionStatusFailed {
		return domain.ErrInvalidTransactionStatus
	}
	if txn.Status() == domain.TransactionStatusProcessed &&
		txn.ReferenceTransactionID() == "" {
		return domain.ErrInvalidReferenceResolution
	}

	const query = `
		UPDATE wager_transactions
		SET reference_transaction_id = $1,
			status = $2,
			failure_code = $3,
			result_balance = $4,
			result_wallet_version = $5,
			updated_at = $6
		WHERE id = $7
		  AND status = 'PENDING_REFERENCE'
	`

	commandTag, err := tx.Exec(
		ctx,
		query,
		nullIfEmpty(txn.ReferenceTransactionID()),
		txn.Status(),
		nullIfEmpty(txn.FailureCode()),
		txn.ResultBalance().AmountCents(),
		txn.ResultWalletVersion(),
		txn.UpdatedAt(),
		txn.ID(),
	)
	if err != nil {
		return fmt.Errorf("update pending reference: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrTransactionNotFound
	}
	return nil
}

func (r *PostgresTransactionRepository) GetByID(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	transactionID string,
) (*domain.WagerTransaction, error) {
	const query = `
		SELECT
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			COALESCE(correlation_id, ''),
			COALESCE(causation_id, ''),
			wallet_id,
			player_id,
			round_id,
			game_id,
			kind,
			amount,
			currency,
			COALESCE(reference_external_transaction_id, ''),
			COALESCE(reference_transaction_id::text, ''),
			status,
			COALESCE(failure_code, ''),
			result_balance,
			result_wallet_version,
			created_at,
			updated_at
		FROM wager_transactions
		WHERE provider_id = $1
		  AND id = $2
	`

	return scanWagerTransaction(
		tx.QueryRow(ctx, query, providerID, transactionID),
		"get transaction by ID",
	)
}

func scanWagerTransaction(
	row pgx.Row,
	operation string,
) (*domain.WagerTransaction, error) {
	var (
		id                             string
		providerID                     string
		externalID                     string
		idempotencyKey                 string
		payloadHash                    string
		correlationID                  string
		causationID                    string
		walletID                       string
		playerID                       string
		roundID                        string
		gameID                         string
		kind                           domain.TransactionKind
		amount                         int64
		currency                       string
		referenceExternalTransactionID string
		referenceTransactionID         string
		status                         domain.TransactionStatus
		failureCode                    string
		resultBalance                  int64
		resultWalletVersion            int64
		createdAt                      time.Time
		updatedAt                      time.Time
	)

	err := row.Scan(
		&id,
		&providerID,
		&externalID,
		&idempotencyKey,
		&payloadHash,
		&correlationID,
		&causationID,
		&walletID,
		&playerID,
		&roundID,
		&gameID,
		&kind,
		&amount,
		&currency,
		&referenceExternalTransactionID,
		&referenceTransactionID,
		&status,
		&failureCode,
		&resultBalance,
		&resultWalletVersion,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTransactionNotFound
		}
		return nil, fmt.Errorf("%s: %w", operation, err)
	}

	money, err := moneyFromCents(amount, currency)
	if err != nil {
		return nil, fmt.Errorf("map transaction amount: %w", err)
	}
	resultMoney, err := moneyFromCents(resultBalance, currency)
	if err != nil {
		return nil, fmt.Errorf("map transaction result balance: %w", err)
	}

	return domain.RehydrateWagerTransaction(domain.RehydrateWagerTransactionParams{
		ID:                             id,
		ProviderID:                     providerID,
		ExternalTransactionID:          externalID,
		IdempotencyKey:                 idempotencyKey,
		PayloadHash:                    payloadHash,
		CorrelationID:                  correlationID,
		CausationID:                    causationID,
		WalletID:                       walletID,
		PlayerID:                       playerID,
		RoundID:                        roundID,
		GameID:                         gameID,
		Kind:                           kind,
		Money:                          money,
		ReferenceExternalTransactionID: referenceExternalTransactionID,
		ReferenceTransactionID:         referenceTransactionID,
		Status:                         status,
		FailureCode:                    failureCode,
		ResultBalance:                  resultMoney,
		ResultWalletVersion:            resultWalletVersion,
		CreatedAt:                      createdAt,
		UpdatedAt:                      updatedAt,
	}), nil
}

func (r *PostgresTransactionRepository) UpdateStatus(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	status domain.TransactionStatus,
	failureCode string,
) error {
	if !status.IsValid() {
		return domain.ErrInvalidTransactionStatus
	}

	const query = `
		UPDATE wager_transactions
		SET status = $1,
			failure_code = $2,
			updated_at = NOW()
		WHERE id = $3
	`

	commandTag, err := tx.Exec(ctx, query, status, nullIfEmpty(failureCode), id)
	if err != nil {
		return fmt.Errorf("update transaction status: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrTransactionNotFound
	}

	return nil
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

var _ TransactionRepository = (*PostgresTransactionRepository)(nil)
