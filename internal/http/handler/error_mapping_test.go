package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/repository"
	"alexsandro-filho/desafio-backend/internal/usecase"
)

func TestWagerInputErrorsReturnBadRequest(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "currency mismatch", err: domain.ErrCurrencyMismatch},
		{name: "external opening", err: usecase.ErrUnsupportedWagerKind},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writeWagerError(recorder, test.err)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", recorder.Code)
			}
			var response map[string]string
			if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if response["code"] != "INVALID_REQUEST" {
				t.Errorf("code = %q, want INVALID_REQUEST", response["code"])
			}
		})
	}
}

func TestDuplicateWalletReturnsConflict(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeWalletError(recorder, repository.ErrWalletAlreadyExists)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", recorder.Code)
	}
	var response map[string]string
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response["code"] != "WALLET_ALREADY_EXISTS" {
		t.Errorf("code = %q, want WALLET_ALREADY_EXISTS", response["code"])
	}
}
