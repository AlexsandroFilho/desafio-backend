package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/event"
	"alexsandro-filho/desafio-backend/internal/repository"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CreateWalletCommand struct {
	ID             string
	PlayerID       string
	InitialBalance domain.Money
}

type WalletReconciliation struct {
	WalletID          string
	StoredBalance     domain.Money
	CalculatedBalance domain.Money
	Difference        domain.Money
	Consistent        bool
	CheckedEntries    int
}

type ReconciliationObserver interface {
	ObserveReconciliation(consistent bool)
}

type WalletUseCase struct {
	db                    *pgxpool.Pool
	walletRepository      repository.WalletRepository
	transactionRepository repository.TransactionRepository
	ledgerRepository      repository.LedgerRepository
	inboxOutboxRepository repository.InboxOutboxRepository
	observer              ReconciliationObserver
}

func NewWalletUseCase(
	db *pgxpool.Pool,
	walletRepository repository.WalletRepository,
	transactionRepository repository.TransactionRepository,
	ledgerRepository repository.LedgerRepository,
	inboxOutboxRepository repository.InboxOutboxRepository,
	observers ...ReconciliationObserver,
) *WalletUseCase {
	useCase := &WalletUseCase{
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

func (uc *WalletUseCase) CreateWallet(
	ctx context.Context,
	providerID string,
	command CreateWalletCommand,
) (*domain.Wallet, error) {
	walletID := command.ID
	if walletID == "" {
		generatedID, err := newUUID()
		if err != nil {
			return nil, fmt.Errorf("generate wallet ID: %w", err)
		}
		walletID = generatedID
	}

	wallet, err := domain.NewWallet(walletID, command.PlayerID, command.InitialBalance)
	if err != nil {
		return nil, err
	}

	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin wallet creation: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := uc.walletRepository.Create(ctx, tx, providerID, wallet); err != nil {
		return nil, err
	}
	if !wallet.Balance().IsZero() {
		if err := uc.createOpeningRecords(ctx, tx, providerID, wallet); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit wallet creation: %w", err)
	}

	return wallet, nil
}

func (uc *WalletUseCase) GetWallet(
	ctx context.Context,
	providerID string,
	walletID string,
) (*domain.Wallet, error) {
	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin wallet lookup: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	wallet, err := uc.walletRepository.Get(
		ctx,
		tx,
		providerID,
		walletID,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit wallet lookup: %w", err)
	}

	return wallet, nil
}

func (uc *WalletUseCase) GetLedger(
	ctx context.Context,
	providerID string,
	walletID string,
	cursor string,
	limit int,
) ([]domain.WalletLedgerEntry, string, error) {
	if _, err := uc.GetWallet(ctx, providerID, walletID); err != nil {
		return nil, "", err
	}
	return uc.ledgerRepository.GetEntriesByWalletID(
		ctx,
		uc.db,
		providerID,
		walletID,
		cursor,
		limit,
	)
}

func (uc *WalletUseCase) ReconcileWallet(
	ctx context.Context,
	providerID string,
	walletID string,
) (*WalletReconciliation, error) {
	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("begin wallet reconciliation: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	wallet, err := uc.walletRepository.Get(ctx, tx, providerID, walletID)
	if err != nil {
		return nil, err
	}

	ledgerBalance, checkedEntries, err := uc.calculateLedgerBalance(
		ctx,
		tx,
		providerID,
		wallet,
	)
	if err != nil {
		return nil, err
	}
	difference, err := wallet.Balance().Subtract(ledgerBalance)
	if err != nil {
		return nil, fmt.Errorf("calculate reconciliation difference: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit wallet reconciliation: %w", err)
	}

	result := &WalletReconciliation{
		WalletID:          wallet.ID(),
		StoredBalance:     wallet.Balance(),
		CalculatedBalance: ledgerBalance,
		Difference:        difference,
		Consistent:        difference.IsZero(),
		CheckedEntries:    checkedEntries,
	}
	if uc.observer != nil {
		uc.observer.ObserveReconciliation(result.Consistent)
	}
	if !result.Consistent {
		slog.Error(
			"wallet reconciliation divergence detected",
			"provider_id", providerID,
			"wallet_id", wallet.ID(),
			"stored_balance", result.StoredBalance.StringFormat(),
			"calculated_balance", result.CalculatedBalance.StringFormat(),
			"difference", result.Difference.StringFormat(),
			"currency", result.StoredBalance.Currency(),
			"checked_entries", result.CheckedEntries,
		)
	}
	return result, nil
}

func (uc *WalletUseCase) createOpeningRecords(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	wallet *domain.Wallet,
) error {
	transactionID, err := newUUID()
	if err != nil {
		return fmt.Errorf("generate opening transaction ID: %w", err)
	}

	externalID := "wallet-opening:" + wallet.ID()
	hash := sha256.Sum256([]byte(
		providerID + "\x00" + externalID + "\x00" + wallet.Balance().StringFormat(),
	))
	transaction, err := domain.NewWagerTransaction(domain.NewWagerTransactionParams{
		ID:                    transactionID,
		ProviderID:            providerID,
		ExternalTransactionID: externalID,
		IdempotencyKey:        externalID,
		PayloadHash:           hex.EncodeToString(hash[:]),
		CorrelationID:         transactionID,
		WalletID:              wallet.ID(),
		PlayerID:              wallet.PlayerID(),
		Kind:                  domain.TransactionKindOpening,
		Money:                 wallet.Balance(),
	})
	if err != nil {
		return err
	}
	if err := transaction.RecordWalletResult(
		wallet.Balance(),
		wallet.Version(),
	); err != nil {
		return err
	}
	if err := transaction.MarkProcessed(); err != nil {
		return err
	}
	if err := uc.transactionRepository.Create(ctx, tx, transaction); err != nil {
		return err
	}

	zero, err := domain.NewMoneyFromString("0.00", wallet.Currency())
	if err != nil {
		return err
	}
	ledgerID, err := newUUID()
	if err != nil {
		return fmt.Errorf("generate opening ledger ID: %w", err)
	}
	entry, err := domain.NewWalletLedgerEntry(
		ledgerID,
		wallet.ID(),
		transaction.ID(),
		domain.LedgerDirectionCredit,
		wallet.Balance(),
		zero,
		wallet.Balance(),
	)
	if err != nil {
		return err
	}
	if err := uc.ledgerRepository.Create(ctx, tx, entry); err != nil {
		return err
	}

	transactionEvent, err := event.NewWagerTransactionProcessed(
		transaction.ID(),
		transaction.CorrelationID(),
		transaction.CausationID(),
		wagerEventData(transaction),
	)
	if err != nil {
		return err
	}
	if err := uc.inboxOutboxRepository.SaveOutbox(ctx, tx, transactionEvent); err != nil {
		return err
	}

	balanceEvent, err := event.NewWalletBalanceChanged(
		wallet.ID(),
		transaction.CorrelationID(),
		transaction.CausationID(),
		event.WalletBalanceChangedData{
			WalletID:      wallet.ID(),
			TransactionID: transaction.ID(),
			Direction:     domain.LedgerDirectionCredit,
			Money:         event.MoneyFromDomain(wallet.Balance()),
			BalanceBefore: event.MoneyFromDomain(zero),
			BalanceAfter:  event.MoneyFromDomain(wallet.Balance()),
			WalletVersion: wallet.Version(),
		},
	)
	if err != nil {
		return err
	}
	if err := uc.inboxOutboxRepository.SaveOutbox(ctx, tx, balanceEvent); err != nil {
		return err
	}

	return nil
}

func (uc *WalletUseCase) calculateLedgerBalance(
	ctx context.Context,
	db repository.DBExecutor,
	providerID string,
	wallet *domain.Wallet,
) (domain.Money, int, error) {
	total, err := domain.NewMoneyFromString("0.00", wallet.Currency())
	if err != nil {
		return domain.Money{}, 0, err
	}

	cursor := ""
	checkedEntries := 0
	for {
		entries, nextCursor, err := uc.ledgerRepository.GetEntriesByWalletID(
			ctx,
			db,
			providerID,
			wallet.ID(),
			cursor,
			100,
		)
		if err != nil {
			return domain.Money{}, 0, err
		}

		for _, entry := range entries {
			checkedEntries++
			switch entry.Direction() {
			case domain.LedgerDirectionCredit:
				total, err = total.Add(entry.Amount())
			case domain.LedgerDirectionDebit:
				total, err = total.Subtract(entry.Amount())
			default:
				return domain.Money{}, 0, domain.ErrInvalidLedgerDirection
			}
			if err != nil {
				return domain.Money{}, 0, err
			}
		}

		if nextCursor == "" {
			return total, checkedEntries, nil
		}
		cursor = nextCursor
	}
}
