package domain

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Money struct {
	amount   int64
	currency string
}

func ZeroMoney(currency string) (Money, error) {
	return NewMoneyFromString("0.00", currency)
}

func NewMoneyFromString(amountStr, currency string) (Money, error) {
	if !isCurrencyCode(currency) {
		return Money{}, ErrInvalidCurrency
	}

	cents, err := parseAmountCents(amountStr)
	if err != nil {
		return Money{}, err
	}

	return Money{amount: cents, currency: currency}, nil
}

func (m Money) AmountCents() int64 {
	return m.amount
}

func (m Money) Currency() string {
	return m.currency
}

func (m Money) StringFormat() string {
	if m.amount >= 0 {
		return fmt.Sprintf("%d.%02d", m.amount/100, m.amount%100)
	}

	magnitude := uint64(-(m.amount + 1)) + 1
	return fmt.Sprintf("-%d.%02d", magnitude/100, magnitude%100)
}

func (m Money) Add(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}
	if (other.amount > 0 && m.amount > int64(^uint64(0)>>1)-other.amount) ||
		(other.amount < 0 && m.amount < -int64(^uint64(0)>>1)-1-other.amount) {
		return Money{}, ErrMoneyOverflow
	}

	return Money{amount: m.amount + other.amount, currency: m.currency}, nil
}

func (m Money) Subtract(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}
	if (other.amount < 0 && m.amount > int64(^uint64(0)>>1)+other.amount) ||
		(other.amount > 0 && m.amount < -int64(^uint64(0)>>1)-1+other.amount) {
		return Money{}, ErrMoneyOverflow
	}

	return Money{amount: m.amount - other.amount, currency: m.currency}, nil
}

func (m Money) Negate() (Money, error) {
	const minInt64 = -int64(^uint64(0)>>1) - 1
	if m.amount == minInt64 {
		return Money{}, ErrMoneyOverflow
	}

	return Money{amount: -m.amount, currency: m.currency}, nil
}

func (m Money) IsZero() bool {
	return m.amount == 0
}

func (m Money) IsNegative() bool {
	return m.amount < 0
}

func (m Money) Equal(other Money) (bool, error) {
	if m.currency != other.currency {
		return false, ErrCurrencyMismatch
	}
	return m.amount == other.amount, nil
}

func (m Money) Compare(other Money) (int, error) {
	if m.currency != other.currency {
		return 0, ErrCurrencyMismatch
	}
	switch {
	case m.amount < other.amount:
		return -1, nil
	case m.amount > other.amount:
		return 1, nil
	default:
		return 0, nil
	}
}

func parseAmountCents(amount string) (int64, error) {
	if amount == "" || strings.ContainsAny(amount, "eE") {
		return 0, ErrInvalidAmount
	}

	unsigned := amount
	if amount[0] == '-' {
		unsigned = amount[1:]
	}
	if unsigned == "" {
		return 0, ErrInvalidAmount
	}

	parts := strings.Split(unsigned, ".")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) != 2 {
		return 0, ErrInvalidAmount
	}
	if !allDigits(parts[0]) || !allDigits(parts[1]) {
		return 0, ErrInvalidAmount
	}

	normalized := parts[0] + parts[1]
	if amount[0] == '-' {
		normalized = "-" + normalized
	}

	cents, err := strconv.ParseInt(normalized, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, ErrMoneyOverflow
		}
		return 0, ErrInvalidAmount
	}

	return cents, nil
}

func isCurrencyCode(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for i := range currency {
		if currency[i] < 'A' || currency[i] > 'Z' {
			return false
		}
	}
	return true
}

func allDigits(value string) bool {
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
