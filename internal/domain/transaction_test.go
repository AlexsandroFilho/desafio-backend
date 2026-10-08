package domain

import (
	"errors"
	"testing"
)

func TestWagerTransactionResolveReference(t *testing.T) {
	transaction := newReferenceTransactionForTest(t)
	if err := transaction.MarkPendingReference(); err != nil {
		t.Fatalf("MarkPendingReference() error = %v", err)
	}

	if err := transaction.ResolveReference("original-transaction"); err != nil {
		t.Fatalf("ResolveReference() error = %v", err)
	}
	if transaction.ReferenceTransactionID() != "original-transaction" {
		t.Errorf(
			"ReferenceTransactionID() = %q, want original-transaction",
			transaction.ReferenceTransactionID(),
		)
	}
	if err := transaction.MarkProcessed(); err != nil {
		t.Fatalf("MarkProcessed() error = %v", err)
	}
}

func TestWagerTransactionRejectsInvalidReferenceResolution(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*WagerTransaction) error
		id      string
	}{
		{
			name:    "transaction is not pending reference",
			prepare: func(*WagerTransaction) error { return nil },
			id:      "original-transaction",
		},
		{
			name: "reference ID is empty",
			prepare: func(transaction *WagerTransaction) error {
				return transaction.MarkPendingReference()
			},
			id: "",
		},
		{
			name: "reference was already resolved",
			prepare: func(transaction *WagerTransaction) error {
				if err := transaction.MarkPendingReference(); err != nil {
					return err
				}
				return transaction.ResolveReference("first-original")
			},
			id: "second-original",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transaction := newReferenceTransactionForTest(t)
			if err := tt.prepare(transaction); err != nil {
				t.Fatalf("prepare transaction: %v", err)
			}
			if err := transaction.ResolveReference(tt.id); !errors.Is(
				err,
				ErrInvalidReferenceResolution,
			) {
				t.Fatalf(
					"ResolveReference() error = %v, want %v",
					err,
					ErrInvalidReferenceResolution,
				)
			}
		})
	}
}

func newReferenceTransactionForTest(t *testing.T) *WagerTransaction {
	t.Helper()

	money, err := NewMoneyFromString("10.00", "BRL")
	if err != nil {
		t.Fatalf("NewMoneyFromString() error = %v", err)
	}
	transaction, err := NewWagerTransaction(NewWagerTransactionParams{
		ID:                             "refund-transaction",
		ProviderID:                     "provider-1",
		ExternalTransactionID:          "refund-external",
		IdempotencyKey:                 "refund-key",
		PayloadHash:                    "refund-hash",
		WalletID:                       "wallet-1",
		PlayerID:                       "player-1",
		RoundID:                        "round-1",
		GameID:                         "game-1",
		Kind:                           TransactionKindRefund,
		Money:                          money,
		ReferenceExternalTransactionID: "bet-external",
	})
	if err != nil {
		t.Fatalf("NewWagerTransaction() error = %v", err)
	}
	return transaction
}
