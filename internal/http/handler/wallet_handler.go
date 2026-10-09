package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/repository"
	"alexsandro-filho/desafio-backend/internal/usecase"
)

type WalletApplication interface {
	CreateWallet(
		ctx context.Context,
		providerID string,
		command usecase.CreateWalletCommand,
	) (*domain.Wallet, error)
	GetWallet(
		ctx context.Context,
		providerID string,
		walletID string,
	) (*domain.Wallet, error)
	GetLedger(
		ctx context.Context,
		providerID string,
		walletID string,
		cursor string,
		limit int,
	) ([]domain.WalletLedgerEntry, string, error)
	ReconcileWallet(
		ctx context.Context,
		providerID string,
		walletID string,
	) (*usecase.WalletReconciliation, error)
}

type WalletHandler struct {
	application WalletApplication
}

type createWalletRequest struct {
	PlayerID       string       `json:"playerId"`
	InitialBalance moneyRequest `json:"initialBalance"`
}

func NewWalletHandler(application WalletApplication) *WalletHandler {
	return &WalletHandler{application: application}
}

func (h *WalletHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /wallets", h.Create)
	mux.HandleFunc("GET /wallets/{walletId}", h.Get)
	mux.HandleFunc("GET /wallets/{walletId}/ledger", h.GetLedger)
	mux.HandleFunc("POST /wallets/{walletId}/reconciliation", h.Reconcile)
}

func (h *WalletHandler) Create(w http.ResponseWriter, r *http.Request) {
	providerID, ok := requireProviderID(w, r)
	if !ok {
		return
	}

	var request createWalletRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}

	initialBalance, err := domain.NewMoneyFromString(
		request.InitialBalance.Amount,
		request.InitialBalance.Currency,
	)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_MONEY", err.Error())
		return
	}

	wallet, err := h.application.CreateWallet(
		r.Context(),
		providerID,
		usecase.CreateWalletCommand{
			PlayerID:       request.PlayerID,
			InitialBalance: initialBalance,
		},
	)
	if err != nil {
		writeWalletError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, walletResponseFromDomain(wallet))
}

func (h *WalletHandler) Get(w http.ResponseWriter, r *http.Request) {
	providerID, ok := requireProviderID(w, r)
	if !ok {
		return
	}

	wallet, err := h.application.GetWallet(
		r.Context(),
		providerID,
		r.PathValue("walletId"),
	)
	if err != nil {
		writeWalletError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, walletResponseFromDomain(wallet))
}

func (h *WalletHandler) GetLedger(w http.ResponseWriter, r *http.Request) {
	providerID, ok := requireProviderID(w, r)
	if !ok {
		return
	}

	limit := 50
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 100 {
			writeError(
				w,
				http.StatusBadRequest,
				"INVALID_LIMIT",
				"limit must be between 1 and 100",
			)
			return
		}
		limit = parsed
	}

	entries, nextCursor, err := h.application.GetLedger(
		r.Context(),
		providerID,
		r.PathValue("walletId"),
		r.URL.Query().Get("cursor"),
		limit,
	)
	if err != nil {
		writeWalletError(w, err)
		return
	}

	responseEntries := make([]ledgerEntryResponse, 0, len(entries))
	for _, entry := range entries {
		responseEntries = append(responseEntries, ledgerEntryResponseFromDomain(entry))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"entries":    responseEntries,
		"nextCursor": nextCursor,
	})
}

func (h *WalletHandler) Reconcile(w http.ResponseWriter, r *http.Request) {
	providerID, ok := requireProviderID(w, r)
	if !ok {
		return
	}

	result, err := h.application.ReconcileWallet(
		r.Context(),
		providerID,
		r.PathValue("walletId"),
	)
	if err != nil {
		writeWalletError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"walletId":          result.WalletID,
		"storedBalance":     moneyResponseFromDomain(result.StoredBalance),
		"calculatedBalance": moneyResponseFromDomain(result.CalculatedBalance),
		"difference":        moneyResponseFromDomain(result.Difference),
		"consistent":        result.Consistent,
		"checkedEntries":    result.CheckedEntries,
	})
}

type moneyResponse struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type moneyRequest struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type walletResponse struct {
	ID        string        `json:"id"`
	PlayerID  string        `json:"playerId"`
	Balance   moneyResponse `json:"balance"`
	Version   int64         `json:"version"`
	CreatedAt time.Time     `json:"createdAt"`
	UpdatedAt time.Time     `json:"updatedAt"`
}

type ledgerEntryResponse struct {
	ID            string        `json:"id"`
	TransactionID string        `json:"transactionId"`
	Direction     string        `json:"direction"`
	Amount        moneyResponse `json:"amount"`
	BalanceBefore moneyResponse `json:"balanceBefore"`
	BalanceAfter  moneyResponse `json:"balanceAfter"`
	CreatedAt     time.Time     `json:"createdAt"`
}

func moneyResponseFromDomain(money domain.Money) moneyResponse {
	return moneyResponse{
		Amount:   money.StringFormat(),
		Currency: money.Currency(),
	}
}

func walletResponseFromDomain(wallet *domain.Wallet) walletResponse {
	return walletResponse{
		ID:        wallet.ID(),
		PlayerID:  wallet.PlayerID(),
		Balance:   moneyResponseFromDomain(wallet.Balance()),
		Version:   wallet.Version(),
		CreatedAt: wallet.CreatedAt(),
		UpdatedAt: wallet.UpdatedAt(),
	}
}

func ledgerEntryResponseFromDomain(entry domain.WalletLedgerEntry) ledgerEntryResponse {
	return ledgerEntryResponse{
		ID:            entry.ID(),
		TransactionID: entry.TransactionID(),
		Direction:     string(entry.Direction()),
		Amount:        moneyResponseFromDomain(entry.Amount()),
		BalanceBefore: moneyResponseFromDomain(entry.BalanceBefore()),
		BalanceAfter:  moneyResponseFromDomain(entry.BalanceAfter()),
		CreatedAt:     entry.CreatedAt(),
	}
}

func writeWalletError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repository.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", err.Error())
	case errors.Is(err, repository.ErrWalletAlreadyExists):
		writeError(w, http.StatusConflict, "WALLET_ALREADY_EXISTS", err.Error())
	case errors.Is(err, repository.ErrWalletUpdateConflict):
		writeError(w, http.StatusConflict, "WALLET_CONFLICT", err.Error())
	case errors.Is(err, repository.ErrInvalidLedgerCursor),
		errors.Is(err, repository.ErrInvalidLedgerLimit),
		errors.Is(err, domain.ErrInvalidWalletID),
		errors.Is(err, domain.ErrInvalidPlayerID),
		errors.Is(err, domain.ErrInvalidWalletBalance):
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case isServiceUnavailable(err):
		writeError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "service temporarily unavailable")
	default:
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
	}
}
