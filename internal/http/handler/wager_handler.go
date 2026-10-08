package handler

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/repository"
	"alexsandro-filho/desafio-backend/internal/usecase"
)

type WagerProcessor interface {
	ProcessTransaction(
		ctx context.Context,
		request usecase.WagerRequest,
	) (*usecase.WagerResponse, error)
}

type TransactionReader interface {
	GetTransactionByID(
		ctx context.Context,
		providerID string,
		transactionID string,
	) (*domain.WagerTransaction, error)
}

type WagerHandler struct {
	processor WagerProcessor
	reader    TransactionReader
}

type processWagerRequest struct {
	ProviderID                     string                 `json:"providerId"`
	ExternalTransactionID          string                 `json:"externalTransactionId"`
	WalletID                       string                 `json:"walletId"`
	PlayerID                       string                 `json:"playerId"`
	RoundID                        string                 `json:"roundId"`
	GameID                         string                 `json:"gameId"`
	Kind                           domain.TransactionKind `json:"kind"`
	Money                          moneyRequest           `json:"money"`
	ReferenceExternalTransactionID string                 `json:"referenceExternalTransactionId,omitempty"`
}

func NewWagerHandler(
	processor WagerProcessor,
	reader TransactionReader,
) *WagerHandler {
	return &WagerHandler{
		processor: processor,
		reader:    reader,
	}
}

func (h *WagerHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /wagering/transactions", h.Process)
	mux.HandleFunc("GET /wagering/transactions/{transactionId}", h.Get)
}

func (h *WagerHandler) Process(w http.ResponseWriter, r *http.Request) {
	providerID, ok := requireProviderID(w, r)
	if !ok {
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		writeError(
			w,
			http.StatusBadRequest,
			"IDEMPOTENCY_KEY_REQUIRED",
			"Idempotency-Key header is required",
		)
		return
	}

	var request processWagerRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	if strings.TrimSpace(request.ProviderID) == "" {
		writeError(w, http.StatusBadRequest, "PROVIDER_ID_REQUIRED", "providerId is required")
		return
	}
	if request.ProviderID != providerID {
		writeError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "providerId does not match authenticated provider")
		return
	}
	requiredFields := map[string]string{
		"externalTransactionId": request.ExternalTransactionID,
		"walletId":              request.WalletID,
		"playerId":              request.PlayerID,
		"roundId":               request.RoundID,
		"gameId":                request.GameID,
	}
	for name, value := range requiredFields {
		if strings.TrimSpace(value) == "" {
			writeError(w, http.StatusBadRequest, "REQUIRED_FIELD", name+" is required")
			return
		}
	}
	if !request.Kind.IsValid() {
		writeError(w, http.StatusBadRequest, "INVALID_TRANSACTION_KIND", "invalid transaction kind")
		return
	}

	amount, err := domain.NewMoneyFromString(
		request.Money.Amount,
		request.Money.Currency,
	)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_MONEY", err.Error())
		return
	}

	response, err := h.processor.ProcessTransaction(r.Context(), usecase.WagerRequest{
		CorrelationID:                  strings.TrimSpace(r.Header.Get("X-Correlation-ID")),
		ProviderID:                     providerID,
		ExternalTransactionID:          request.ExternalTransactionID,
		IdempotencyKey:                 idempotencyKey,
		WalletID:                       request.WalletID,
		PlayerID:                       request.PlayerID,
		RoundID:                        request.RoundID,
		GameID:                         request.GameID,
		Kind:                           request.Kind,
		Amount:                         amount,
		ReferenceExternalTransactionID: request.ReferenceExternalTransactionID,
	})
	if err != nil {
		writeWagerError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, wagerResponseFromUseCase(response))
}

func (h *WagerHandler) Get(w http.ResponseWriter, r *http.Request) {
	providerID, ok := requireProviderID(w, r)
	if !ok {
		return
	}

	transaction, err := h.reader.GetTransactionByID(
		r.Context(),
		providerID,
		r.PathValue("transactionId"),
	)
	if err != nil {
		writeWagerError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, wagerResponseFromDomain(transaction))
}

type wagerHTTPResponse struct {
	ID                             string                   `json:"id"`
	ProviderID                     string                   `json:"providerId"`
	ExternalTransactionID          string                   `json:"externalTransactionId"`
	WalletID                       string                   `json:"walletId"`
	PlayerID                       string                   `json:"playerId,omitempty"`
	RoundID                        string                   `json:"roundId,omitempty"`
	GameID                         string                   `json:"gameId,omitempty"`
	Kind                           domain.TransactionKind   `json:"kind"`
	Amount                         moneyResponse            `json:"amount"`
	ReferenceExternalTransactionID string                   `json:"referenceExternalTransactionId,omitempty"`
	Status                         domain.TransactionStatus `json:"status"`
	FailureCode                    string                   `json:"failureCode,omitempty"`
	Balance                        moneyResponse            `json:"balance"`
	WalletVersion                  int64                    `json:"walletVersion"`
	IdempotentReplay               bool                     `json:"idempotentReplay"`
	CreatedAt                      *time.Time               `json:"createdAt,omitempty"`
	UpdatedAt                      *time.Time               `json:"updatedAt,omitempty"`
}

func wagerResponseFromUseCase(response *usecase.WagerResponse) wagerHTTPResponse {
	return wagerHTTPResponse{
		ID:                             response.TransactionID,
		ProviderID:                     response.ProviderID,
		ExternalTransactionID:          response.ExternalID,
		WalletID:                       response.WalletID,
		PlayerID:                       response.PlayerID,
		RoundID:                        response.RoundID,
		GameID:                         response.GameID,
		Kind:                           response.Kind,
		Amount:                         moneyResponseFromDomain(response.Amount),
		ReferenceExternalTransactionID: response.ReferenceExternalTransactionID,
		Status:                         response.Status,
		FailureCode:                    response.FailureCode,
		Balance:                        moneyResponseFromDomain(response.Balance),
		WalletVersion:                  response.WalletVersion,
		IdempotentReplay:               response.IdempotentReplay,
	}
}

func wagerResponseFromDomain(transaction *domain.WagerTransaction) wagerHTTPResponse {
	createdAt := transaction.CreatedAt()
	updatedAt := transaction.UpdatedAt()

	return wagerHTTPResponse{
		ID:                             transaction.ID(),
		ProviderID:                     transaction.ProviderID(),
		ExternalTransactionID:          transaction.ExternalTransactionID(),
		WalletID:                       transaction.WalletID(),
		PlayerID:                       transaction.PlayerID(),
		RoundID:                        transaction.RoundID(),
		GameID:                         transaction.GameID(),
		Kind:                           transaction.Kind(),
		Amount:                         moneyResponseFromDomain(transaction.Money()),
		ReferenceExternalTransactionID: transaction.ReferenceExternalTransactionID(),
		Status:                         transaction.Status(),
		FailureCode:                    transaction.FailureCode(),
		Balance:                        moneyResponseFromDomain(transaction.ResultBalance()),
		WalletVersion:                  transaction.ResultWalletVersion(),
		CreatedAt:                      &createdAt,
		UpdatedAt:                      &updatedAt,
	}
}

func writeWagerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecase.ErrIdempotencyConflict),
		errors.Is(err, repository.ErrInboxHashMismatch):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", err.Error())
	case errors.Is(err, repository.ErrTransactionNotFound):
		writeError(w, http.StatusNotFound, "TRANSACTION_NOT_FOUND", err.Error())
	case errors.Is(err, repository.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", err.Error())
	case errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, domain.ErrInvalidCurrency),
		errors.Is(err, domain.ErrInvalidTransactionKind),
		errors.Is(err, domain.ErrInvalidWalletAmount):
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case isServiceUnavailable(err):
		writeError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "service temporarily unavailable")
	default:
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
	}
}
