package domain

import (
	"errors"
	"math"
	"testing"
)

func TestNewMoneyFromStringValidAmounts(t *testing.T) {
	tests := []struct {
		name        string
		amount      string
		wantCents   int64
		wantDisplay string
	}{
		{
			name:        "whole amount",
			amount:      "25.00",
			wantCents:   2500,
			wantDisplay: "25.00",
		},
		{
			name:        "amount with cents",
			amount:      "100.50",
			wantCents:   10050,
			wantDisplay: "100.50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			money, err := NewMoneyFromString(tt.amount, "BRL")
			if err != nil {
				t.Fatalf("NewMoneyFromString() unexpected error: %v", err)
			}
			if money.AmountCents() != tt.wantCents {
				t.Errorf("AmountCents() = %d, want %d", money.AmountCents(), tt.wantCents)
			}
			if money.Currency() != "BRL" {
				t.Errorf("Currency() = %q, want %q", money.Currency(), "BRL")
			}
			if money.StringFormat() != tt.wantDisplay {
				t.Errorf("StringFormat() = %q, want %q", money.StringFormat(), tt.wantDisplay)
			}
		})
	}
}

func TestNewMoneyFromStringRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name     string
		amount   string
		currency string
		wantErr  error
	}{
		{
			name:     "three decimal places",
			amount:   "25.001",
			currency: "BRL",
			wantErr:  ErrInvalidAmount,
		},
		{
			name:     "scientific notation",
			amount:   "1e2",
			currency: "BRL",
			wantErr:  ErrInvalidAmount,
		},
		{
			name:     "lowercase currency",
			amount:   "25.00",
			currency: "brl",
			wantErr:  ErrInvalidCurrency,
		},
		{
			name:     "currency with invalid length",
			amount:   "25.00",
			currency: "BR",
			wantErr:  ErrInvalidCurrency,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewMoneyFromString(tt.amount, tt.currency)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("NewMoneyFromString() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestMoneyArithmetic(t *testing.T) {
	left := mustMoney(t, "100.50", "BRL")
	right := mustMoney(t, "25.00", "BRL")

	sum, err := left.Add(right)
	if err != nil {
		t.Fatalf("Add() unexpected error: %v", err)
	}
	if sum.AmountCents() != 12550 {
		t.Errorf("Add() cents = %d, want %d", sum.AmountCents(), 12550)
	}

	difference, err := left.Subtract(right)
	if err != nil {
		t.Fatalf("Subtract() unexpected error: %v", err)
	}
	if difference.AmountCents() != 7550 {
		t.Errorf("Subtract() cents = %d, want %d", difference.AmountCents(), 7550)
	}
}

func TestZeroMoneyAndCompare(t *testing.T) {
	zero, err := ZeroMoney("BRL")
	if err != nil {
		t.Fatalf("ZeroMoney() unexpected error: %v", err)
	}
	if !zero.IsZero() || zero.Currency() != "BRL" {
		t.Fatalf("ZeroMoney() = %+v, want zero BRL", zero)
	}

	smaller := mustMoney(t, "10.00", "BRL")
	larger := mustMoney(t, "25.00", "BRL")
	usd := mustMoney(t, "10.00", "USD")

	equal, err := smaller.Equal(mustMoney(t, "10.00", "BRL"))
	if err != nil || !equal {
		t.Fatalf("Equal() = %v, %v, want true", equal, err)
	}
	comparison, err := smaller.Compare(larger)
	if err != nil || comparison >= 0 {
		t.Fatalf("Compare() = %d, %v, want negative", comparison, err)
	}
	if _, err := smaller.Compare(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Compare() error = %v, want %v", err, ErrCurrencyMismatch)
	}
}

func TestMoneyArithmeticRejectsCurrencyMismatch(t *testing.T) {
	brl := mustMoney(t, "25.00", "BRL")
	usd := mustMoney(t, "10.00", "USD")

	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add() error = %v, want %v", err, ErrCurrencyMismatch)
	}
	if _, err := brl.Subtract(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Subtract() error = %v, want %v", err, ErrCurrencyMismatch)
	}
}

func TestMoneyArithmeticOverflow(t *testing.T) {
	max := mustMoney(t, "92233720368547758.07", "BRL")
	min := mustMoney(t, "-92233720368547758.08", "BRL")
	oneCent := mustMoney(t, "0.01", "BRL")

	if max.AmountCents() != math.MaxInt64 {
		t.Fatalf("maximum amount cents = %d, want %d", max.AmountCents(), int64(math.MaxInt64))
	}
	if min.AmountCents() != math.MinInt64 {
		t.Fatalf("minimum amount cents = %d, want %d", min.AmountCents(), int64(math.MinInt64))
	}

	if _, err := max.Add(oneCent); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("Add() error = %v, want %v", err, ErrMoneyOverflow)
	}
	if _, err := min.Subtract(oneCent); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("Subtract() error = %v, want %v", err, ErrMoneyOverflow)
	}
	if _, err := min.Negate(); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("Negate() error = %v, want %v", err, ErrMoneyOverflow)
	}
}

func mustMoney(t *testing.T, amount, currency string) Money {
	t.Helper()

	money, err := NewMoneyFromString(amount, currency)
	if err != nil {
		t.Fatalf("NewMoneyFromString(%q, %q) unexpected error: %v", amount, currency, err)
	}
	return money
}
