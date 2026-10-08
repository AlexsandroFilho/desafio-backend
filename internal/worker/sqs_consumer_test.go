package worker

import (
	"context"
	"strings"
	"testing"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/usecase"
)

const validWagerMessage = `{
	"messageId": "msg-123",
	"type": "WagerTransactionRequested",
	"occurredAt": "2026-09-08T12:00:00.000Z",
	"data": {
		"providerId": "provider-a",
		"externalTransactionId": "transaction-123",
		"idempotencyKey": "provider-a:transaction-123",
		"playerId": "player-1",
		"walletId": "wallet-1",
		"roundId": "round-987",
		"gameId": "fortune-chimp",
		"kind": "BET",
		"money": {
			"amount": "25.00",
			"currency": "BRL"
		}
	}
}`

func TestDecodeWagerEnvelopeOfficialContract(t *testing.T) {
	envelope, err := decodeWagerEnvelope(validWagerMessage)
	if err != nil {
		t.Fatalf("decodeWagerEnvelope() error = %v", err)
	}
	if envelope.MessageID != "msg-123" ||
		envelope.Type != wagerRequestedType ||
		envelope.Data.Money.Amount != "25.00" ||
		envelope.Data.Money.Currency != "BRL" {
		t.Fatalf("decoded envelope does not match official contract: %+v", envelope)
	}
}

func TestDecodeWagerEnvelopeRejectsLegacyContract(t *testing.T) {
	legacy := `{
		"messageId": "msg-123",
		"payload": {
			"providerId": "provider-a",
			"externalTransactionId": "transaction-123"
		}
	}`
	if _, err := decodeWagerEnvelope(legacy); err == nil {
		t.Fatal("decodeWagerEnvelope() accepted legacy payload contract")
	}
}

func TestSQSConsumerMapsOfficialContractToUseCase(t *testing.T) {
	processor := &recordingWagerProcessor{}
	consumer := NewSQSConsumer(nil, processor, "queue-url")

	if deleteMessage := consumer.processMessage(
		context.Background(),
		validWagerMessage,
	); !deleteMessage {
		t.Fatal("processMessage() did not accept valid official message")
	}

	request := processor.request
	if request.ProviderID != "provider-a" ||
		request.ExternalTransactionID != "transaction-123" ||
		request.IdempotencyKey != "provider-a:transaction-123" ||
		request.Amount.AmountCents() != 2500 ||
		request.Amount.Currency() != "BRL" {
		t.Fatalf("mapped WagerRequest is invalid: %+v", request)
	}
	if request.MessageID != "msg-123" ||
		request.ConsumerName != wagerConsumerName ||
		strings.TrimSpace(request.MessageHash) == "" {
		t.Fatalf("inbox metadata was not mapped: %+v", request)
	}
}

type recordingWagerProcessor struct {
	request usecase.WagerRequest
}

func (p *recordingWagerProcessor) ProcessTransaction(
	_ context.Context,
	request usecase.WagerRequest,
) (*usecase.WagerResponse, error) {
	p.request = request
	return &usecase.WagerResponse{
		TransactionID: "transaction-id",
		Status:        domain.TransactionStatusProcessed,
	}, nil
}
