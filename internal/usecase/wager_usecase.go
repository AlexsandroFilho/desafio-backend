package usecase

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/event"
	"alexsandro-filho/desafio-backend/internal/repository"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrIdempotencyConflict         = errors.New("idempotency key or payload conflict")
	ErrUnsupportedWagerKind        = errors.New("unsupported wager transaction kind")
	ErrWalletPlayerMismatch        = errors.New("wallet does not belong to player")
	ErrInvalidTransactionAmount    = errors.New("transaction amount must be greater than zero")
	ErrLossAmountMustBeZero        = errors.New("loss transaction amount must be zero")
	ErrReferenceRequired           = errors.New("reference transaction is required")
	ErrReferenceNotFound           = errors.New("reference transaction not found")
	ErrInvalidReference            = errors.New("invalid reference transaction")
	ErrReferenceAmountMismatch     = errors.New("reference transaction amount or currency mismatch")
	ErrReferenceAlreadyReversed    = errors.New("reference transaction was already reversed")
	ErrRollbackInsufficientBalance = errors.New("insufficient balance to rollback transaction")
)

type WagerRequest struct {
	ConsumerName                   string
	MessageID                      string
	MessageHash                    string
	CorrelationID                  string
	CausationID                    string
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	WalletID                       string
	PlayerID                       string
	RoundID                        string
	GameID                         string
	Kind                           domain.TransactionKind
	Amount                         domain.Money
	ReferenceExternalTransactionID string
}

type WagerResponse struct {
	TransactionID                  string
	ProviderID                     string
	ExternalID                     string
	WalletID                       string
	PlayerID                       string
	RoundID                        string
	GameID                         string
	Kind                           domain.TransactionKind
	Amount                         domain.Money
	ReferenceExternalTransactionID string
	Status                         domain.TransactionStatus
	FailureCode                    string
	Balance                        domain.Money
	WalletVersion                  int64
	IdempotentReplay               bool
}

type WagerUseCase struct {
	db                    *pgxpool.Pool
	walletRepository      repository.WalletRepository
	transactionRepository repository.TransactionRepository
	ledgerRepository      repository.LedgerRepository
	inboxOutboxRepository repository.InboxOutboxRepository
	observer              WagerObserver
}

type WagerObserver interface {
	ObserveWagerTransaction(
		kind domain.TransactionKind,
		status string,
		duration time.Duration,
	)
}

func NewWagerUseCase(
	db *pgxpool.Pool,
	walletRepository repository.WalletRepository,
	transactionRepository repository.TransactionRepository,
	ledgerRepository repository.LedgerRepository,
	inboxOutboxRepository repository.InboxOutboxRepository,
	observers ...WagerObserver,
) *WagerUseCase {
	useCase := &WagerUseCase{
		db:                    db,
		walletRepository:      walletRepository,
		transactionRepository: transactionRepository,
		ledgerRepository:      ledgerRepository,
		inboxOutboxRepository: inboxOutboxRepository,
	}
	if len(observers) > 0 {
		useCase.observer = observers[0]
	}
	return useCase
}

func (uc *WagerUseCase) GetTransactionByID(
	ctx context.Context,
	providerID string,
	transactionID string,
) (*domain.WagerTransaction, error) {
	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin transaction lookup: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	transaction, err := uc.transactionRepository.GetByID(
		ctx,
		tx,
		providerID,
		transactionID,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction lookup: %w", err)
	}

	return transaction, nil
}

func (uc *WagerUseCase) ProcessTransaction(
	ctx context.Context,
	req WagerRequest,
) (response *WagerResponse, responseErr error) {
	startedAt := time.Now()
	defer func() {
		if uc.observer == nil {
			return
		}
		status := "ERROR"
		if responseErr == nil && response != nil {
			status = string(response.Status)
		}
		uc.observer.ObserveWagerTransaction(
			req.Kind,
			status,
			time.Since(startedAt),
		)
	}()

	if req.Amount.IsNegative() {
		return nil, domain.ErrInvalidWalletAmount
	}

	payloadHash, err := hashWagerRequest(req)
	if err != nil {
		return nil, fmt.Errorf("hash wager request: %w", err)
	}

	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin wager transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := lockIdempotencyKeys(
		ctx,
		tx,
		req.ProviderID,
		req.ExternalTransactionID,
		req.IdempotencyKey,
	); err != nil {
		return nil, err
	}

	if req.MessageID != "" {
		if err := uc.inboxOutboxRepository.SaveInbox(
			ctx,
			tx,
			req.ConsumerName,
			req.MessageID,
			req.MessageHash,
		); err != nil {
			return nil, fmt.Errorf("save inbox message: %w", err)
		}
	}

	existing, err := uc.findIdempotentTransaction(
		ctx,
		tx,
		req,
		payloadHash,
	)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if err := uc.transactionRepository.BindIdempotencyKey(
			ctx,
			tx,
			req.ProviderID,
			req.IdempotencyKey,
			existing.ID(),
		); err != nil {
			if errors.Is(err, repository.ErrIdempotencyKeyConflict) {
				return nil, ErrIdempotencyConflict
			}
			return nil, fmt.Errorf("bind replay idempotency key: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit idempotent replay: %w", err)
		}
		return responseFromTransaction(existing, true), nil
	}

	wallet, err := uc.walletRepository.GetForUpdate(
		ctx,
		tx,
		req.ProviderID,
		req.WalletID,
	)
	if err != nil {
		return nil, fmt.Errorf("lock wallet: %w", err)
	}

	transactionID, err := newUUID()
	if err != nil {
		return nil, fmt.Errorf("generate transaction ID: %w", err)
	}

	reference, businessErr, err := uc.resolveReference(ctx, tx, req)
	if err != nil {
		return nil, err
	}

	referenceTransactionID := ""
	if reference != nil {
		referenceTransactionID = reference.ID()
	}
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = transactionID
	}

	txn, err := domain.NewWagerTransaction(domain.NewWagerTransactionParams{
		ID:                             transactionID,
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		IdempotencyKey:                 req.IdempotencyKey,
		PayloadHash:                    payloadHash,
		CorrelationID:                  correlationID,
		CausationID:                    req.CausationID,
		WalletID:                       req.WalletID,
		PlayerID:                       req.PlayerID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           req.Kind,
		Money:                          req.Amount,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		ReferenceTransactionID:         referenceTransactionID,
	})
	if err != nil {
		return nil, fmt.Errorf("create wager transaction: %w", err)
	}
	if err := txn.RecordWalletResult(wallet.Balance(), wallet.Version()); err != nil {
		return nil, fmt.Errorf("record initial wallet result: %w", err)
	}

	balanceBefore := wallet.Balance()
	if errors.Is(businessErr, ErrReferenceNotFound) {
		return uc.commitPendingReference(ctx, tx, txn)
	}
	if businessErr != nil {
		return uc.commitRejection(ctx, tx, txn, businessErr)
	}

	operation, businessErr := applyWalletOperation(wallet, req, reference)
	if businessErr != nil {
		return uc.commitRejection(ctx, tx, txn, businessErr)
	}
	if err := txn.RecordWalletResult(wallet.Balance(), wallet.Version()); err != nil {
		return nil, fmt.Errorf("record processed wallet result: %w", err)
	}

	if operation.balanceChanged {
		if err := uc.walletRepository.UpdateBalance(
			ctx,
			tx,
			req.ProviderID,
			wallet,
		); err != nil {
			return nil, fmt.Errorf("update wallet balance: %w", err)
		}
	}
	if err := txn.MarkProcessed(); err != nil {
		return nil, fmt.Errorf("mark wager transaction as processed: %w", err)
	}
	if err := uc.transactionRepository.Create(ctx, tx, txn); err != nil {
		return nil, fmt.Errorf("save processed wager transaction: %w", err)
	}

	if operation.balanceChanged {
		if err := uc.saveLedgerEntry(
			ctx,
			tx,
			txn,
			wallet,
			operation.direction,
			balanceBefore,
		); err != nil {
			return nil, err
		}
	}

	if err := uc.saveProcessedEvents(
		ctx,
		tx,
		txn,
		wallet,
		balanceBefore,
		operation.direction,
		operation.balanceChanged,
	); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit processed wager transaction: %w", err)
	}

	return responseFromTransaction(txn, false), nil
}

func (uc *WagerUseCase) findIdempotentTransaction(
	ctx context.Context,
	tx pgx.Tx,
	req WagerRequest,
	payloadHash string,
) (*domain.WagerTransaction, error) {
	byExternalID, externalErr := uc.transactionRepository.GetByProviderAndExternalID(
		ctx,
		tx,
		req.ProviderID,
		req.ExternalTransactionID,
	)
	if externalErr != nil &&
		!errors.Is(externalErr, repository.ErrTransactionNotFound) {
		return nil, fmt.Errorf("check external transaction idempotency: %w", externalErr)
	}

	byIdempotencyKey, keyErr := uc.transactionRepository.GetByProviderAndIdempotencyKey(
		ctx,
		tx,
		req.ProviderID,
		req.IdempotencyKey,
	)
	if keyErr != nil && !errors.Is(keyErr, repository.ErrTransactionNotFound) {
		return nil, fmt.Errorf("check idempotency key: %w", keyErr)
	}

	externalFound := externalErr == nil
	keyFound := keyErr == nil
	if !externalFound && !keyFound {
		return nil, nil
	}
	if externalFound && keyFound && byExternalID.ID() != byIdempotencyKey.ID() {
		return nil, ErrIdempotencyConflict
	}

	existing := byExternalID
	if !externalFound {
		existing = byIdempotencyKey
	}

	hashMatches := existing.PayloadHash() == payloadHash
	if !hashMatches {
		legacyRequest := req
		legacyRequest.IdempotencyKey = existing.IdempotencyKey()
		legacyHash, err := hashLegacyWagerRequest(legacyRequest)
		if err != nil {
			return nil, fmt.Errorf("hash legacy wager request: %w", err)
		}
		hashMatches = existing.PayloadHash() == legacyHash
	}
	if !hashMatches {
		return nil, ErrIdempotencyConflict
	}
	return existing, nil
}

func (uc *WagerUseCase) commitPendingReference(
	ctx context.Context,
	tx pgx.Tx,
	txn *domain.WagerTransaction,
) (*WagerResponse, error) {
	if err := txn.MarkPendingReference(); err != nil {
		return nil, fmt.Errorf("mark wager transaction as pending reference: %w", err)
	}
	if err := uc.transactionRepository.Create(ctx, tx, txn); err != nil {
		return nil, fmt.Errorf("save pending reference transaction: %w", err)
	}

	pendingEvent, err := event.NewWagerTransactionPendingReference(
		txn.ID(),
		txn.CorrelationID(),
		txn.CausationID(),
		wagerEventData(txn),
	)
	if err != nil {
		return nil, fmt.Errorf("create pending reference event: %w", err)
	}
	if err := uc.inboxOutboxRepository.SaveOutbox(ctx, tx, pendingEvent); err != nil {
		return nil, fmt.Errorf("save pending reference event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit pending reference transaction: %w", err)
	}
	return responseFromTransaction(txn, false), nil
}

func (uc *WagerUseCase) commitRejection(
	ctx context.Context,
	tx pgx.Tx,
	txn *domain.WagerTransaction,
	businessErr error,
) (*WagerResponse, error) {
	if err := txn.MarkRejected(failureCodeFor(businessErr)); err != nil {
		return nil, fmt.Errorf("mark wager transaction as rejected: %w", err)
	}
	if err := uc.transactionRepository.Create(ctx, tx, txn); err != nil {
		return nil, fmt.Errorf("save rejected wager transaction: %w", err)
	}
	rejectedEvent, err := event.NewWagerTransactionRejected(
		txn.ID(),
		txn.CorrelationID(),
		txn.CausationID(),
		wagerEventData(txn),
	)
	if err != nil {
		return nil, fmt.Errorf("create rejected wager event: %w", err)
	}
	if err := uc.inboxOutboxRepository.SaveOutbox(ctx, tx, rejectedEvent); err != nil {
		return nil, fmt.Errorf("save rejected wager event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit rejected wager transaction: %w", err)
	}

	return responseFromTransaction(txn, false), nil
}

func (uc *WagerUseCase) ProcessPendingReferences(
	ctx context.Context,
	limit int,
) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	processed := 0
	for processed < limit {
		found, err := uc.processNextPendingReference(ctx)
		if err != nil {
			return processed, err
		}
		if !found {
			break
		}
		processed++
	}
	return processed, nil
}

func (uc *WagerUseCase) processNextPendingReference(
	ctx context.Context,
) (bool, error) {
	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin pending reference transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	txn, err := uc.transactionRepository.GetResolvablePendingReferenceForUpdate(
		ctx,
		tx,
	)
	if errors.Is(err, repository.ErrTransactionNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim pending reference transaction: %w", err)
	}

	wallet, err := uc.walletRepository.GetForUpdate(
		ctx,
		tx,
		txn.ProviderID(),
		txn.WalletID(),
	)
	if err != nil {
		return false, fmt.Errorf("lock pending reference wallet: %w", err)
	}
	reference, err := uc.transactionRepository.GetByProviderAndExternalIDForUpdate(
		ctx,
		tx,
		txn.ProviderID(),
		txn.ReferenceExternalTransactionID(),
	)
	if err != nil {
		return false, fmt.Errorf("lock resolved reference transaction: %w", err)
	}
	if err := txn.ResolveReference(reference.ID()); err != nil {
		return false, fmt.Errorf("resolve pending transaction reference: %w", err)
	}

	req := requestFromTransaction(txn)
	businessErr := validateReference(req, reference)
	if businessErr == nil {
		reversed, err := uc.transactionRepository.HasProcessedReversal(
			ctx,
			tx,
			reference.ID(),
		)
		if err != nil {
			return false, fmt.Errorf("check pending reference reversal: %w", err)
		}
		if reversed {
			businessErr = ErrReferenceAlreadyReversed
		}
	}

	balanceBefore := wallet.Balance()
	var operation walletOperation
	if businessErr == nil {
		operation, businessErr = applyWalletOperation(wallet, req, reference)
	}
	if err := txn.RecordWalletResult(wallet.Balance(), wallet.Version()); err != nil {
		return false, fmt.Errorf("record pending reference wallet result: %w", err)
	}
	if businessErr != nil {
		if err := txn.MarkRejected(failureCodeFor(businessErr)); err != nil {
			return false, fmt.Errorf("reject pending reference transaction: %w", err)
		}
		if err := uc.transactionRepository.UpdatePendingReference(
			ctx,
			tx,
			txn,
		); err != nil {
			return false, fmt.Errorf("save rejected pending reference: %w", err)
		}
		rejectedEvent, err := event.NewWagerTransactionRejected(
			txn.ID(),
			txn.CorrelationID(),
			txn.CausationID(),
			wagerEventData(txn),
		)
		if err != nil {
			return false, fmt.Errorf("create rejected pending reference event: %w", err)
		}
		if err := uc.inboxOutboxRepository.SaveOutbox(
			ctx,
			tx,
			rejectedEvent,
		); err != nil {
			return false, fmt.Errorf("save rejected pending reference event: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit rejected pending reference: %w", err)
		}
		return true, nil
	}

	if operation.balanceChanged {
		if err := uc.walletRepository.UpdateBalance(
			ctx,
			tx,
			txn.ProviderID(),
			wallet,
		); err != nil {
			return false, fmt.Errorf("update pending reference wallet balance: %w", err)
		}
	}
	if err := txn.MarkProcessed(); err != nil {
		return false, fmt.Errorf("process pending reference transaction: %w", err)
	}
	if err := uc.transactionRepository.UpdatePendingReference(
		ctx,
		tx,
		txn,
	); err != nil {
		return false, fmt.Errorf("save processed pending reference: %w", err)
	}
	if operation.balanceChanged {
		if err := uc.saveLedgerEntry(
			ctx,
			tx,
			txn,
			wallet,
			operation.direction,
			balanceBefore,
		); err != nil {
			return false, err
		}
	}
	if err := uc.saveProcessedEvents(
		ctx,
		tx,
		txn,
		wallet,
		balanceBefore,
		operation.direction,
		operation.balanceChanged,
	); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit processed pending reference: %w", err)
	}
	return true, nil
}

func requestFromTransaction(txn *domain.WagerTransaction) WagerRequest {
	return WagerRequest{
		CorrelationID:                  txn.CorrelationID(),
		CausationID:                    txn.CausationID(),
		ProviderID:                     txn.ProviderID(),
		ExternalTransactionID:          txn.ExternalTransactionID(),
		IdempotencyKey:                 txn.IdempotencyKey(),
		WalletID:                       txn.WalletID(),
		PlayerID:                       txn.PlayerID(),
		RoundID:                        txn.RoundID(),
		GameID:                         txn.GameID(),
		Kind:                           txn.Kind(),
		Amount:                         txn.Money(),
		ReferenceExternalTransactionID: txn.ReferenceExternalTransactionID(),
	}
}

func (uc *WagerUseCase) saveLedgerEntry(
	ctx context.Context,
	tx pgx.Tx,
	txn *domain.WagerTransaction,
	wallet *domain.Wallet,
	direction domain.LedgerDirection,
	balanceBefore domain.Money,
) error {
	ledgerID, err := newUUID()
	if err != nil {
		return fmt.Errorf("generate ledger entry ID: %w", err)
	}

	entry, err := domain.NewWalletLedgerEntry(
		ledgerID,
		wallet.ID(),
		txn.ID(),
		direction,
		txn.Money(),
		balanceBefore,
		wallet.Balance(),
	)
	if err != nil {
		return fmt.Errorf("create wallet ledger entry: %w", err)
	}
	if err := uc.ledgerRepository.Create(ctx, tx, entry); err != nil {
		return fmt.Errorf("save wallet ledger entry: %w", err)
	}

	return nil
}

func (uc *WagerUseCase) saveProcessedEvents(
	ctx context.Context,
	tx pgx.Tx,
	txn *domain.WagerTransaction,
	wallet *domain.Wallet,
	balanceBefore domain.Money,
	direction domain.LedgerDirection,
	balanceChanged bool,
) error {
	transactionEvent, err := event.NewWagerTransactionProcessed(
		txn.ID(),
		txn.CorrelationID(),
		txn.CausationID(),
		wagerEventData(txn),
	)
	if err != nil {
		return fmt.Errorf("create wager processed event: %w", err)
	}
	if err := uc.inboxOutboxRepository.SaveOutbox(ctx, tx, transactionEvent); err != nil {
		return fmt.Errorf("save wager processed event: %w", err)
	}
	if !balanceChanged {
		return nil
	}

	balanceEvent, err := event.NewWalletBalanceChanged(
		wallet.ID(),
		txn.CorrelationID(),
		txn.CausationID(),
		event.WalletBalanceChangedData{
			WalletID:      wallet.ID(),
			TransactionID: txn.ID(),
			Direction:     direction,
			Money:         event.MoneyFromDomain(txn.Money()),
			BalanceBefore: event.MoneyFromDomain(balanceBefore),
			BalanceAfter:  event.MoneyFromDomain(wallet.Balance()),
			WalletVersion: wallet.Version(),
		},
	)
	if err != nil {
		return fmt.Errorf("create wallet balance changed event: %w", err)
	}
	if err := uc.inboxOutboxRepository.SaveOutbox(ctx, tx, balanceEvent); err != nil {
		return fmt.Errorf("save wallet balance changed event: %w", err)
	}

	return nil
}

func wagerEventData(txn *domain.WagerTransaction) event.WagerTransactionData {
	return event.WagerTransactionData{
		TransactionID:                  txn.ID(),
		ProviderID:                     txn.ProviderID(),
		ExternalTransactionID:          txn.ExternalTransactionID(),
		WalletID:                       txn.WalletID(),
		PlayerID:                       txn.PlayerID(),
		RoundID:                        txn.RoundID(),
		GameID:                         txn.GameID(),
		Kind:                           txn.Kind(),
		Money:                          event.MoneyFromDomain(txn.Money()),
		ReferenceExternalTransactionID: txn.ReferenceExternalTransactionID(),
		Status:                         txn.Status(),
		FailureCode:                    txn.FailureCode(),
		Balance:                        event.MoneyFromDomain(txn.ResultBalance()),
		WalletVersion:                  txn.ResultWalletVersion(),
	}
}

func (uc *WagerUseCase) resolveReference(
	ctx context.Context,
	tx pgx.Tx,
	req WagerRequest,
) (*domain.WagerTransaction, error, error) {
	if req.Kind != domain.TransactionKindRefund &&
		req.Kind != domain.TransactionKindRollback {
		return nil, nil, nil
	}
	if req.ReferenceExternalTransactionID == "" {
		return nil, ErrReferenceRequired, nil
	}

	reference, err := uc.transactionRepository.GetByProviderAndExternalIDForUpdate(
		ctx,
		tx,
		req.ProviderID,
		req.ReferenceExternalTransactionID,
	)
	if err != nil {
		if errors.Is(err, repository.ErrTransactionNotFound) {
			return nil, ErrReferenceNotFound, nil
		}
		return nil, nil, fmt.Errorf("get reference transaction: %w", err)
	}
	if businessErr := validateReference(req, reference); businessErr != nil {
		return reference, businessErr, nil
	}

	reversed, err := uc.transactionRepository.HasProcessedReversal(
		ctx,
		tx,
		reference.ID(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("check reference reversal: %w", err)
	}
	if reversed {
		return reference, ErrReferenceAlreadyReversed, nil
	}

	return reference, nil, nil
}

func validateReference(
	req WagerRequest,
	reference *domain.WagerTransaction,
) error {
	if reference == nil ||
		reference.Status() != domain.TransactionStatusProcessed ||
		reference.ProviderID() != req.ProviderID ||
		reference.WalletID() != req.WalletID ||
		reference.PlayerID() != req.PlayerID ||
		reference.RoundID() != req.RoundID {
		return ErrInvalidReference
	}
	if reference.Money() != req.Amount {
		return ErrReferenceAmountMismatch
	}

	switch req.Kind {
	case domain.TransactionKindRefund:
		if reference.Kind() != domain.TransactionKindBet {
			return ErrInvalidReference
		}
	case domain.TransactionKindRollback:
		if reference.Kind() != domain.TransactionKindBet &&
			reference.Kind() != domain.TransactionKindWin &&
			reference.Kind() != domain.TransactionKindRefund {
			return ErrInvalidReference
		}
	default:
		return ErrInvalidReference
	}

	return nil
}

type walletOperation struct {
	balanceChanged bool
	direction      domain.LedgerDirection
}

func applyWalletOperation(
	wallet *domain.Wallet,
	req WagerRequest,
	reference *domain.WagerTransaction,
) (walletOperation, error) {
	if wallet.PlayerID() != req.PlayerID {
		return walletOperation{}, ErrWalletPlayerMismatch
	}
	if wallet.Currency() != req.Amount.Currency() {
		return walletOperation{}, domain.ErrCurrencyMismatch
	}

	if req.Kind == domain.TransactionKindLoss {
		if !req.Amount.IsZero() {
			return walletOperation{}, ErrLossAmountMustBeZero
		}
		return walletOperation{}, nil
	}
	if req.Amount.IsZero() {
		return walletOperation{}, ErrInvalidTransactionAmount
	}

	switch req.Kind {
	case domain.TransactionKindBet:
		if err := wallet.Debit(req.Amount); err != nil {
			return walletOperation{}, err
		}
		return walletOperation{
			balanceChanged: true,
			direction:      domain.LedgerDirectionDebit,
		}, nil
	case domain.TransactionKindWin, domain.TransactionKindRefund:
		if err := wallet.Credit(req.Amount); err != nil {
			return walletOperation{}, err
		}
		return walletOperation{
			balanceChanged: true,
			direction:      domain.LedgerDirectionCredit,
		}, nil
	case domain.TransactionKindRollback:
		if reference == nil {
			return walletOperation{}, ErrReferenceRequired
		}
		if reference.Kind() == domain.TransactionKindBet {
			if err := wallet.Credit(req.Amount); err != nil {
				return walletOperation{}, err
			}
			return walletOperation{
				balanceChanged: true,
				direction:      domain.LedgerDirectionCredit,
			}, nil
		}
		if reference.Kind() == domain.TransactionKindWin ||
			reference.Kind() == domain.TransactionKindRefund {
			if err := wallet.Debit(req.Amount); err != nil {
				if errors.Is(err, domain.ErrInsufficientBalance) {
					return walletOperation{}, ErrRollbackInsufficientBalance
				}
				return walletOperation{}, err
			}
			return walletOperation{
				balanceChanged: true,
				direction:      domain.LedgerDirectionDebit,
			}, nil
		}
		return walletOperation{}, ErrInvalidReference
	default:
		return walletOperation{}, ErrUnsupportedWagerKind
	}
}

func hashWagerRequest(req WagerRequest) (string, error) {
	canonicalPayload := struct {
		ProviderID                     string                 `json:"providerId"`
		ExternalTransactionID          string                 `json:"externalTransactionId"`
		WalletID                       string                 `json:"walletId"`
		PlayerID                       string                 `json:"playerId"`
		RoundID                        string                 `json:"roundId"`
		GameID                         string                 `json:"gameId"`
		Kind                           domain.TransactionKind `json:"kind"`
		AmountCents                    int64                  `json:"amountCents"`
		Currency                       string                 `json:"currency"`
		ReferenceExternalTransactionID string                 `json:"referenceExternalTransactionId"`
	}{
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		WalletID:                       req.WalletID,
		PlayerID:                       req.PlayerID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           req.Kind,
		AmountCents:                    req.Amount.AmountCents(),
		Currency:                       req.Amount.Currency(),
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
	}

	payload, err := json.Marshal(canonicalPayload)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

func hashLegacyWagerRequest(req WagerRequest) (string, error) {
	legacyPayload := struct {
		ProviderID                     string                 `json:"providerId"`
		ExternalTransactionID          string                 `json:"externalTransactionId"`
		IdempotencyKey                 string                 `json:"idempotencyKey"`
		WalletID                       string                 `json:"walletId"`
		PlayerID                       string                 `json:"playerId"`
		RoundID                        string                 `json:"roundId"`
		GameID                         string                 `json:"gameId"`
		Kind                           domain.TransactionKind `json:"kind"`
		AmountCents                    int64                  `json:"amountCents"`
		Currency                       string                 `json:"currency"`
		ReferenceExternalTransactionID string                 `json:"referenceExternalTransactionId"`
	}{
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		IdempotencyKey:                 req.IdempotencyKey,
		WalletID:                       req.WalletID,
		PlayerID:                       req.PlayerID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           req.Kind,
		AmountCents:                    req.Amount.AmountCents(),
		Currency:                       req.Amount.Currency(),
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
	}

	payload, err := json.Marshal(legacyPayload)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

func lockIdempotencyKeys(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	externalTransactionID string,
	idempotencyKey string,
) error {
	const query = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`

	keys := []string{
		fmt.Sprintf(
			"%d:%s:external:%s",
			len(providerID),
			providerID,
			externalTransactionID,
		),
		fmt.Sprintf(
			"%d:%s:idempotency:%s",
			len(providerID),
			providerID,
			idempotencyKey,
		),
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := tx.Exec(ctx, query, key); err != nil {
			return fmt.Errorf("lock transaction idempotency key: %w", err)
		}
	}
	return nil
}

func responseFromTransaction(
	txn *domain.WagerTransaction,
	idempotentReplay bool,
) *WagerResponse {
	return &WagerResponse{
		TransactionID:                  txn.ID(),
		ProviderID:                     txn.ProviderID(),
		ExternalID:                     txn.ExternalTransactionID(),
		WalletID:                       txn.WalletID(),
		PlayerID:                       txn.PlayerID(),
		RoundID:                        txn.RoundID(),
		GameID:                         txn.GameID(),
		Kind:                           txn.Kind(),
		Amount:                         txn.Money(),
		ReferenceExternalTransactionID: txn.ReferenceExternalTransactionID(),
		Status:                         txn.Status(),
		FailureCode:                    txn.FailureCode(),
		Balance:                        txn.ResultBalance(),
		WalletVersion:                  txn.ResultWalletVersion(),
		IdempotentReplay:               idempotentReplay,
	}
}

func failureCodeFor(err error) string {
	switch {
	case errors.Is(err, domain.ErrInsufficientBalance):
		return "INSUFFICIENT_BALANCE"
	case errors.Is(err, ErrRollbackInsufficientBalance):
		return "ROLLBACK_INSUFFICIENT_BALANCE"
	case errors.Is(err, domain.ErrCurrencyMismatch):
		return "CURRENCY_MISMATCH"
	case errors.Is(err, domain.ErrInvalidWalletAmount),
		errors.Is(err, ErrInvalidTransactionAmount):
		return "INVALID_AMOUNT"
	case errors.Is(err, ErrLossAmountMustBeZero):
		return "LOSS_AMOUNT_MUST_BE_ZERO"
	case errors.Is(err, domain.ErrMoneyOverflow):
		return "AMOUNT_OVERFLOW"
	case errors.Is(err, domain.ErrWalletVersionOverflow):
		return "WALLET_VERSION_OVERFLOW"
	case errors.Is(err, ErrWalletPlayerMismatch):
		return "WALLET_PLAYER_MISMATCH"
	case errors.Is(err, ErrUnsupportedWagerKind):
		return "UNSUPPORTED_TRANSACTION_KIND"
	case errors.Is(err, ErrReferenceRequired):
		return "REFERENCE_REQUIRED"
	case errors.Is(err, ErrReferenceNotFound):
		return "REFERENCE_NOT_FOUND"
	case errors.Is(err, ErrInvalidReference):
		return "INVALID_REFERENCE"
	case errors.Is(err, ErrReferenceAmountMismatch):
		return "REFERENCE_AMOUNT_MISMATCH"
	case errors.Is(err, ErrReferenceAlreadyReversed):
		return "REFERENCE_ALREADY_REVERSED"
	default:
		return "BUSINESS_RULE_VIOLATION"
	}
}

func newUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
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
	), nil
}
