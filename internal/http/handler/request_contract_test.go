package handler

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/usecase"
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

func TestWagerResponseIncludesOfficialTransactionID(t *testing.T) {
	amount, err := domain.NewMoneyFromString("975.00", "BRL")
	if err != nil {
		t.Fatalf("money: %v", err)
	}
	encoded, err := json.Marshal(wagerResponseFromUseCase(&usecase.WagerResponse{
		TransactionID:    "0192f298-345e-7e38-af88-e43f851a819d",
		Status:           domain.TransactionStatusProcessed,
		Amount:           amount,
		Balance:          amount,
		IdempotentReplay: false,
	}))
	if err != nil {
		t.Fatalf("marshal wager response: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatalf("unmarshal wager response: %v", err)
	}
	if body["transactionId"] != "0192f298-345e-7e38-af88-e43f851a819d" {
		t.Fatalf("transactionId = %v, want official contract field", body["transactionId"])
	}
	if body["idempotentReplay"] != false {
		t.Fatalf("idempotentReplay = %v, want false", body["idempotentReplay"])
	}
}

func decodeContractBody(body string, destination any) error {
	request := httptest.NewRequest("POST", "/", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	return decodeJSON(recorder, request, destination)
}
