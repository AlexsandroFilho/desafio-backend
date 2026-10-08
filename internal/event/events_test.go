package event

import (
	"encoding/json"
	"testing"

	"alexsandro-filho/desafio-backend/internal/domain"
)

func TestWalletBalanceChangedEnvelope(t *testing.T) {
	event, err := NewWalletBalanceChanged(
		"wallet-1",
		"correlation-1",
		"message-1",
		WalletBalanceChangedData{
			WalletID:      "wallet-1",
			TransactionID: "transaction-1",
			Direction:     domain.LedgerDirectionDebit,
			Money:         Money{Amount: "25.00", Currency: "BRL"},
			BalanceBefore: Money{Amount: "100.00", Currency: "BRL"},
			BalanceAfter:  Money{Amount: "75.00", Currency: "BRL"},
			WalletVersion: 2,
		},
	)
	if err != nil {
		t.Fatalf("NewWalletBalanceChanged() error = %v", err)
	}
	if event.EventID == "" ||
		event.EventType != WalletBalanceChangedType ||
		event.AggregateID != "wallet-1" ||
		event.CorrelationID != "correlation-1" ||
		event.CausationID != "message-1" ||
		event.Version != 1 ||
		event.OccurredAt.IsZero() {
		t.Fatalf("invalid event envelope: %+v", event)
	}

	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	data, ok := document["data"].(map[string]any)
	if !ok {
		t.Fatalf("event data is not an object: %s", encoded)
	}
	for _, field := range []string{
		"walletId",
		"transactionId",
		"direction",
		"money",
		"balanceBefore",
		"balanceAfter",
		"walletVersion",
	} {
		if _, exists := data[field]; !exists {
			t.Errorf("WalletBalanceChanged data is missing %s", field)
		}
	}
}
