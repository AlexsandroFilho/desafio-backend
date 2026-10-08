package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"alexsandro-filho/desafio-backend/internal/http/middleware"
	"github.com/jackc/pgx/v5/pgconn"
)

const maxRequestBodySize = 1 << 20

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return err
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain a single JSON value")
		}
		return err
	}

	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{
		"code":    code,
		"message": message,
	})
}

func isServiceUnavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		return strings.HasPrefix(postgresError.Code, "08") ||
			strings.HasPrefix(postgresError.Code, "53") ||
			postgresError.Code == "57P01"
	}

	var connectError *pgconn.ConnectError
	return errors.As(err, &connectError)
}

func requireProviderID(w http.ResponseWriter, r *http.Request) (string, bool) {
	providerID, ok := middleware.ProviderIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "authenticated provider is required")
		return "", false
	}
	return providerID, true
}
