package event

import (
	"crypto/rand"
	"fmt"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
)

const (
	WagerTransactionProcessedType        = "WagerTransactionProcessed"
	WagerTransactionRejectedType         = "WagerTransactionRejected"
	WagerTransactionPendingReferenceType = "WagerTransactionPendingReference"
	WalletBalanceChangedType             = "WalletBalanceChanged"
	currentVersion                       = 1
)

type Envelope struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          any       `json:"data"`
}

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type WagerTransactionData struct {
	TransactionID                  string                   `json:"transactionId"`
	ProviderID                     string                   `json:"providerId"`
	ExternalTransactionID          string                   `json:"externalTransactionId"`
	WalletID                       string                   `json:"walletId"`
	PlayerID                       string                   `json:"playerId"`
	RoundID                        string                   `json:"roundId"`
	GameID                         string                   `json:"gameId"`
	Kind                           domain.TransactionKind   `json:"kind"`
	Money                          Money                    `json:"money"`
	ReferenceExternalTransactionID string                   `json:"referenceExternalTransactionId,omitempty"`
	Status                         domain.TransactionStatus `json:"status"`
	FailureCode                    string                   `json:"failureCode,omitempty"`
	Balance                        Money                    `json:"balance"`
	WalletVersion                  int64                    `json:"walletVersion"`
}

type WalletBalanceChangedData struct {
	WalletID      string                 `json:"walletId"`
	TransactionID string                 `json:"transactionId"`
	Direction     domain.LedgerDirection `json:"direction"`
	Money         Money                  `json:"money"`
	BalanceBefore Money                  `json:"balanceBefore"`
	BalanceAfter  Money                  `json:"balanceAfter"`
	WalletVersion int64                  `json:"walletVersion"`
}

func NewWagerTransactionProcessed(
	aggregateID string,
	correlationID string,
	causationID string,
	data WagerTransactionData,
) (Envelope, error) {
	return newEnvelope(
		WagerTransactionProcessedType,
		aggregateID,
		correlationID,
		causationID,
		data,
	)
}

func NewWagerTransactionRejected(
	aggregateID string,
	correlationID string,
	causationID string,
	data WagerTransactionData,
) (Envelope, error) {
	return newEnvelope(
		WagerTransactionRejectedType,
		aggregateID,
		correlationID,
		causationID,
		data,
	)
}

func NewWagerTransactionPendingReference(
	aggregateID string,
	correlationID string,
	causationID string,
	data WagerTransactionData,
) (Envelope, error) {
	return newEnvelope(
		WagerTransactionPendingReferenceType,
		aggregateID,
		correlationID,
		causationID,
		data,
	)
}

func NewWalletBalanceChanged(
	aggregateID string,
	correlationID string,
	causationID string,
	data WalletBalanceChangedData,
) (Envelope, error) {
	return newEnvelope(
		WalletBalanceChangedType,
		aggregateID,
		correlationID,
		causationID,
		data,
	)
}

func MoneyFromDomain(value domain.Money) Money {
	return Money{
		Amount:   value.StringFormat(),
		Currency: value.Currency(),
	}
}

func newEnvelope(
	eventType string,
	aggregateID string,
	correlationID string,
	causationID string,
	data any,
) (Envelope, error) {
	eventID, err := newUUID()
	if err != nil {
		return Envelope{}, err
	}
	if correlationID == "" {
		correlationID = eventID
	}
	return Envelope{
		EventID:       eventID,
		EventType:     eventType,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    time.Now().UTC(),
		Version:       currentVersion,
		Data:          data,
	}, nil
}

func newUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate event ID: %w", err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		value[0:4],
		value[4:6],
		value[6:8],
		value[8:10],
		value[10:16],
	), nil
}
