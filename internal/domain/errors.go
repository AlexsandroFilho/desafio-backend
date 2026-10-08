package domain

import "errors"

var (
	ErrInvalidAmount    = errors.New("invalid money amount")
	ErrInvalidCurrency  = errors.New("invalid ISO 4217 currency")
	ErrCurrencyMismatch = errors.New("money currency mismatch")
	ErrMoneyOverflow    = errors.New("money amount overflows int64")

	ErrInvalidWalletID       = errors.New("wallet ID must not be empty")
	ErrInvalidPlayerID       = errors.New("player ID must not be empty")
	ErrInvalidWalletBalance  = errors.New("wallet initial balance must not be negative")
	ErrInvalidWalletAmount   = errors.New("wallet transaction amount must not be negative")
	ErrInsufficientBalance   = errors.New("insufficient wallet balance")
	ErrInvalidWalletVersion  = errors.New("wallet version must be positive")
	ErrWalletVersionOverflow = errors.New("wallet version overflows int64")

	ErrInvalidTransactionKind     = errors.New("invalid transaction kind")
	ErrInvalidTransactionStatus   = errors.New("invalid transaction status")
	ErrInvalidStateTransition     = errors.New("invalid transaction status transition")
	ErrTransactionTerminal        = errors.New("transaction is in a terminal state")
	ErrInvalidReferenceResolution = errors.New("transaction reference cannot be resolved")

	ErrInvalidTransactionTransition = ErrInvalidStateTransition

	ErrInvalidLedgerDirection = errors.New("invalid ledger direction")
	ErrInvalidLedgerAmount    = errors.New("ledger amount must not be negative")
	ErrInvalidLedgerBalance   = errors.New("ledger balance is inconsistent with its direction and amount")
)
