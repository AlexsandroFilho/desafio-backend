package domain

import (
	"math"
	"strings"
	"time"
)

type Wallet struct {
	id        string
	playerID  string
	currency  string
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func NewWallet(id, playerID string, initialBalance Money) (*Wallet, error) {
	if strings.TrimSpace(id) == "" {
		return nil, ErrInvalidWalletID
	}
	if strings.TrimSpace(playerID) == "" {
		return nil, ErrInvalidPlayerID
	}
	if initialBalance.IsNegative() {
		return nil, ErrInvalidWalletBalance
	}
	if !isCurrencyCode(initialBalance.Currency()) {
		return nil, ErrInvalidCurrency
	}

	now := time.Now().UTC()
	return &Wallet{
		id:        id,
		playerID:  playerID,
		currency:  initialBalance.Currency(),
		balance:   initialBalance,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}, nil
}

func RehydrateWallet(
	id string,
	playerID string,
	currency string,
	balance Money,
	version int64,
	createdAt time.Time,
	updatedAt time.Time,
) *Wallet {
	return &Wallet{
		id:        id,
		playerID:  playerID,
		currency:  currency,
		balance:   balance,
		version:   version,
		createdAt: createdAt,
		updatedAt: updatedAt,
	}
}

func (w *Wallet) ID() string {
	return w.id
}

func (w *Wallet) PlayerID() string {
	return w.playerID
}

func (w *Wallet) Currency() string {
	return w.currency
}

func (w *Wallet) Balance() Money {
	return w.balance
}

func (w *Wallet) Version() int64 {
	return w.version
}

func (w *Wallet) CreatedAt() time.Time {
	return w.createdAt
}

func (w *Wallet) UpdatedAt() time.Time {
	return w.updatedAt
}

func (w *Wallet) Debit(amount Money) error {
	if amount.IsNegative() {
		return ErrInvalidWalletAmount
	}

	nextBalance, err := w.balance.Subtract(amount)
	if err != nil {
		return err
	}
	if nextBalance.IsNegative() {
		return ErrInsufficientBalance
	}
	if amount.IsZero() {
		return nil
	}
	if w.version == math.MaxInt64 {
		return ErrWalletVersionOverflow
	}

	w.balance = nextBalance
	w.version++
	w.updatedAt = time.Now().UTC()
	return nil
}

func (w *Wallet) Credit(amount Money) error {
	if amount.IsNegative() {
		return ErrInvalidWalletAmount
	}

	nextBalance, err := w.balance.Add(amount)
	if err != nil {
		return err
	}
	if amount.IsZero() {
		return nil
	}
	if w.version == math.MaxInt64 {
		return ErrWalletVersionOverflow
	}

	w.balance = nextBalance
	w.version++
	w.updatedAt = time.Now().UTC()
	return nil
}
