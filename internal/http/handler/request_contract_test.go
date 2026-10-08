package handler

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateWalletRequestUsesNestedMoneyAndGeneratedID(t *testing.T) {
	body := `{
		"playerId": "player-1",
		"initialBalance": {
			"amount": "100.00",
			"currency": "BRL"
		}
	}`

	var request createWalletRequest
	if err := decodeContractBody(body, &request); err != nil {
		t.Fatalf("decode official wallet request: %v", err)
	}
	if request.PlayerID != "player-1" ||
		request.InitialBalance.Amount != "100.00" ||
		request.InitialBalance.Currency != "BRL" {
		t.Fatalf("decoded wallet request is invalid: %+v", request)
	}

	legacy := `{
		"id": "client-generated-id",
		"playerId": "player-1",
		"initialBalance": "100.00",
		"currency": "BRL"
	}`
	if err := decodeContractBody(legacy, &createWalletRequest{}); err == nil {
		t.Fatal("legacy wallet request contract was accepted")
	}
}

func TestWagerRequestUsesProviderAndNestedMoney(t *testing.T) {
	body := `{
		"providerId": "provider-a",
		"externalTransactionId": "transaction-123",
		"walletId": "wallet-1",
		"playerId": "player-1",
		"roundId": "round-1",
		"gameId": "game-1",
		"kind": "BET",
		"money": {
			"amount": "25.00",
			"currency": "BRL"
		}
	}`

	var request processWagerRequest
	if err := decodeContractBody(body, &request); err != nil {
		t.Fatalf("decode official wager request: %v", err)
	}
	if request.ProviderID != "provider-a" ||
		request.Money.Amount != "25.00" ||
		request.Money.Currency != "BRL" {
		t.Fatalf("decoded wager request is invalid: %+v", request)
	}

	legacy := `{
		"externalTransactionId": "transaction-123",
		"walletId": "wallet-1",
		"playerId": "player-1",
		"roundId": "round-1",
		"gameId": "game-1",
		"kind": "BET",
		"amount": "25.00",
		"currency": "BRL"
	}`
	if err := decodeContractBody(legacy, &processWagerRequest{}); err == nil {
		t.Fatal("legacy wager request contract was accepted")
	}
}

func decodeContractBody(body string, destination any) error {
	request := httptest.NewRequest("POST", "/", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	return decodeJSON(recorder, request, destination)
}
