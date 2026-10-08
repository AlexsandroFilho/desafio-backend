package domain

import (
	"fmt"
	"time"
)

type TransactionKind string

const (
	TransactionKindOpening  TransactionKind = "OPENING"
	TransactionKindBet      TransactionKind = "BET"
	TransactionKindWin      TransactionKind = "WIN"
	TransactionKindLoss     TransactionKind = "LOSS"
	TransactionKindRefund   TransactionKind = "REFUND"
	TransactionKindRollback TransactionKind = "ROLLBACK"
)

func (k TransactionKind) IsValid() bool {
	switch k {
	case TransactionKindOpening,
		TransactionKindBet,
		TransactionKindWin,
		TransactionKindLoss,
		TransactionKindRefund,
		TransactionKindRollback:
		return true
	default:
		return false
	}
}

type TransactionStatus string

const (
	TransactionStatusPending          TransactionStatus = "PENDING"
	TransactionStatusPendingReference TransactionStatus = "PENDING_REFERENCE"
	TransactionStatusProcessed        TransactionStatus = "PROCESSED"
	TransactionStatusRejected         TransactionStatus = "REJECTED"
	TransactionStatusFailed           TransactionStatus = "FAILED"
)

func (s TransactionStatus) IsValid() bool {
	switch s {
	case TransactionStatusPending,
		TransactionStatusPendingReference,
		TransactionStatusProcessed,
		TransactionStatusRejected,
		TransactionStatusFailed:
		return true
	default:
		return false
	}
}

func (s TransactionStatus) IsTerminal() bool {
	switch s {
	case TransactionStatusProcessed, TransactionStatusRejected, TransactionStatusFailed:
		return true
	default:
		return false
	}
}

type WagerTransaction struct {
	id                             string
	providerID                     string
	externalTransactionID          string
	idempotencyKey                 string
	payloadHash                    string
	correlationID                  string
	causationID                    string
	walletID                       string
	playerID                       string
	roundID                        string
	gameID                         string
	kind                           TransactionKind
	money                          Money
	referenceExternalTransactionID string
	referenceTransactionID         string
	status                         TransactionStatus
	failureCode                    string
	resultBalance                  Money
	resultWalletVersion            int64
	createdAt                      time.Time
	updatedAt                      time.Time
}

type NewWagerTransactionParams struct {
	ID                             string
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	CorrelationID                  string
	CausationID                    string
	WalletID                       string
	PlayerID                       string
	RoundID                        string
	GameID                         string
	Kind                           TransactionKind
	Money                          Money
	ReferenceExternalTransactionID string
	ReferenceTransactionID         string
}

func NewWagerTransaction(params NewWagerTransactionParams) (*WagerTransaction, error) {
	if !params.Kind.IsValid() {
		return nil, ErrInvalidTransactionKind
	}

	now := time.Now().UTC()
	return &WagerTransaction{
		id:                             params.ID,
		providerID:                     params.ProviderID,
		externalTransactionID:          params.ExternalTransactionID,
		idempotencyKey:                 params.IdempotencyKey,
		payloadHash:                    params.PayloadHash,
		correlationID:                  params.CorrelationID,
		causationID:                    params.CausationID,
		walletID:                       params.WalletID,
		playerID:                       params.PlayerID,
		roundID:                        params.RoundID,
		gameID:                         params.GameID,
		kind:                           params.Kind,
		money:                          params.Money,
		referenceExternalTransactionID: params.ReferenceExternalTransactionID,
		referenceTransactionID:         params.ReferenceTransactionID,
		status:                         TransactionStatusPending,
		createdAt:                      now,
		updatedAt:                      now,
	}, nil
}

type RehydrateWagerTransactionParams struct {
	ID                             string
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	CorrelationID                  string
	CausationID                    string
	WalletID                       string
	PlayerID                       string
	RoundID                        string
	GameID                         string
	Kind                           TransactionKind
	Money                          Money
	ReferenceExternalTransactionID string
	ReferenceTransactionID         string
	Status                         TransactionStatus
	FailureCode                    string
	ResultBalance                  Money
	ResultWalletVersion            int64
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

func RehydrateWagerTransaction(params RehydrateWagerTransactionParams) *WagerTransaction {
	return &WagerTransaction{
		id:                             params.ID,
		providerID:                     params.ProviderID,
		externalTransactionID:          params.ExternalTransactionID,
		idempotencyKey:                 params.IdempotencyKey,
		payloadHash:                    params.PayloadHash,
		correlationID:                  params.CorrelationID,
		causationID:                    params.CausationID,
		walletID:                       params.WalletID,
		playerID:                       params.PlayerID,
		roundID:                        params.RoundID,
		gameID:                         params.GameID,
		kind:                           params.Kind,
		money:                          params.Money,
		referenceExternalTransactionID: params.ReferenceExternalTransactionID,
		referenceTransactionID:         params.ReferenceTransactionID,
		status:                         params.Status,
		failureCode:                    params.FailureCode,
		resultBalance:                  params.ResultBalance,
		resultWalletVersion:            params.ResultWalletVersion,
		createdAt:                      params.CreatedAt,
		updatedAt:                      params.UpdatedAt,
	}
}

func (t *WagerTransaction) MarkPendingReference() error {
	return t.transitionTo(TransactionStatusPendingReference, "")
}

func (t *WagerTransaction) MarkProcessed() error {
	return t.transitionTo(TransactionStatusProcessed, "")
}

func (t *WagerTransaction) MarkRejected(failureCode string) error {
	return t.transitionTo(TransactionStatusRejected, failureCode)
}

func (t *WagerTransaction) MarkFailed(failureCode string) error {
	return t.transitionTo(TransactionStatusFailed, failureCode)
}

func (t *WagerTransaction) ResolveReference(referenceTransactionID string) error {
	if t.status != TransactionStatusPendingReference ||
		t.referenceTransactionID != "" ||
		referenceTransactionID == "" {
		return ErrInvalidReferenceResolution
	}

	t.referenceTransactionID = referenceTransactionID
	t.updatedAt = time.Now().UTC()
	return nil
}

func (t *WagerTransaction) RecordWalletResult(
	balance Money,
	version int64,
) error {
	if balance.Currency() != t.money.Currency() {
		return ErrCurrencyMismatch
	}
	if balance.IsNegative() {
		return ErrInvalidWalletBalance
	}
	if version < 1 {
		return ErrInvalidWalletVersion
	}

	t.resultBalance = balance
	t.resultWalletVersion = version
	t.updatedAt = time.Now().UTC()
	return nil
}

func (t *WagerTransaction) transitionTo(next TransactionStatus, failureCode string) error {
	if t.status.IsTerminal() {
		return ErrTransactionTerminal
	}
	if !isAllowedTransition(t.status, next) {
		return fmt.Errorf(
			"%w: %s to %s",
			ErrInvalidStateTransition,
			t.status,
			next,
		)
	}

	t.status = next
	t.failureCode = failureCode
	t.updatedAt = time.Now().UTC()
	return nil
}

func isAllowedTransition(current, next TransactionStatus) bool {
	switch current {
	case TransactionStatusPending:
		return next == TransactionStatusPendingReference || next.IsTerminal()
	case TransactionStatusPendingReference:
		return next.IsTerminal()
	default:
		return false
	}
}

func (t WagerTransaction) ID() string {
	return t.id
}

func (t WagerTransaction) ProviderID() string {
	return t.providerID
}

func (t WagerTransaction) ExternalTransactionID() string {
	return t.externalTransactionID
}

func (t WagerTransaction) IdempotencyKey() string {
	return t.idempotencyKey
}

func (t WagerTransaction) PayloadHash() string {
	return t.payloadHash
}

func (t WagerTransaction) CorrelationID() string {
	return t.correlationID
}

func (t WagerTransaction) CausationID() string {
	return t.causationID
}

func (t WagerTransaction) WalletID() string {
	return t.walletID
}

func (t WagerTransaction) PlayerID() string {
	return t.playerID
}

func (t WagerTransaction) RoundID() string {
	return t.roundID
}

func (t WagerTransaction) GameID() string {
	return t.gameID
}

func (t WagerTransaction) Kind() TransactionKind {
	return t.kind
}

func (t WagerTransaction) Money() Money {
	return t.money
}

func (t WagerTransaction) ReferenceExternalTransactionID() string {
	return t.referenceExternalTransactionID
}

func (t WagerTransaction) ReferenceTransactionID() string {
	return t.referenceTransactionID
}

func (t WagerTransaction) Status() TransactionStatus {
	return t.status
}

func (t WagerTransaction) FailureCode() string {
	return t.failureCode
}

func (t WagerTransaction) ResultBalance() Money {
	return t.resultBalance
}

func (t WagerTransaction) ResultWalletVersion() int64 {
	return t.resultWalletVersion
}

func (t WagerTransaction) CreatedAt() time.Time {
	return t.createdAt
}

func (t WagerTransaction) UpdatedAt() time.Time {
	return t.updatedAt
}

type LedgerDirection string

const (
	LedgerDirectionDebit  LedgerDirection = "DEBIT"
	LedgerDirectionCredit LedgerDirection = "CREDIT"
)

func (d LedgerDirection) IsValid() bool {
	return d == LedgerDirectionDebit || d == LedgerDirectionCredit
}

type WalletLedgerEntry struct {
	id            string
	walletID      string
	transactionID string
	direction     LedgerDirection
	amount        Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

func NewWalletLedgerEntry(
	id string,
	walletID string,
	transactionID string,
	direction LedgerDirection,
	amount Money,
	balanceBefore Money,
	balanceAfter Money,
) (*WalletLedgerEntry, error) {
	return newWalletLedgerEntry(
		id,
		walletID,
		transactionID,
		direction,
		amount,
		balanceBefore,
		balanceAfter,
		time.Now().UTC(),
	)
}

func RehydrateWalletLedgerEntry(
	id string,
	walletID string,
	transactionID string,
	direction LedgerDirection,
	amount Money,
	balanceBefore Money,
	balanceAfter Money,
	createdAt time.Time,
) (*WalletLedgerEntry, error) {
	return newWalletLedgerEntry(
		id,
		walletID,
		transactionID,
		direction,
		amount,
		balanceBefore,
		balanceAfter,
		createdAt,
	)
}

func newWalletLedgerEntry(
	id string,
	walletID string,
	transactionID string,
	direction LedgerDirection,
	amount Money,
	balanceBefore Money,
	balanceAfter Money,
	createdAt time.Time,
) (*WalletLedgerEntry, error) {
	if !direction.IsValid() {
		return nil, ErrInvalidLedgerDirection
	}
	if amount.IsNegative() {
		return nil, ErrInvalidLedgerAmount
	}

	var expectedBalance Money
	var err error
	switch direction {
	case LedgerDirectionDebit:
		expectedBalance, err = balanceBefore.Subtract(amount)
	case LedgerDirectionCredit:
		expectedBalance, err = balanceBefore.Add(amount)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLedgerBalance, err)
	}
	if expectedBalance != balanceAfter {
		return nil, ErrInvalidLedgerBalance
	}

	return &WalletLedgerEntry{
		id:            id,
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		amount:        amount,
		balanceBefore: balanceBefore,
		balanceAfter:  balanceAfter,
		createdAt:     createdAt,
	}, nil
}

func (e WalletLedgerEntry) ID() string {
	return e.id
}

func (e WalletLedgerEntry) WalletID() string {
	return e.walletID
}

func (e WalletLedgerEntry) TransactionID() string {
	return e.transactionID
}

func (e WalletLedgerEntry) Direction() LedgerDirection {
	return e.direction
}

func (e WalletLedgerEntry) Amount() Money {
	return e.amount
}

func (e WalletLedgerEntry) BalanceBefore() Money {
	return e.balanceBefore
}

func (e WalletLedgerEntry) BalanceAfter() Money {
	return e.balanceAfter
}

func (e WalletLedgerEntry) CreatedAt() time.Time {
	return e.createdAt
}
