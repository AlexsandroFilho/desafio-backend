package observability

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
)

func TestMetricsExposeApplicationAndHTTPMeasurements(t *testing.T) {
	metrics := NewMetrics()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()
	metrics.HTTPMiddleware(logger, mux).ServeHTTP(response, request)

	metrics.ObserveWagerTransaction(
		domain.TransactionKindBet,
		string(domain.TransactionStatusProcessed),
		10*time.Millisecond,
	)
	metrics.ObserveSQSMessage("processed")
	metrics.ObserveSQSReceiveError()
	metrics.ObserveOutboxEvent("published")
	metrics.SetOutboxPending(3)
	metrics.ObservePendingReferences("resolved", 2)
	metrics.ObserveReconciliation(false)

	metricsResponse := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(
		metricsResponse,
		httptest.NewRequest(http.MethodGet, "/metrics", nil),
	)
	body := metricsResponse.Body.String()
	expectedMetrics := []string{
		`wager_http_requests_total{method="GET",route="GET /health",status="204"} 1`,
		`wager_transactions_processed_total{kind="BET",status="PROCESSED"} 1`,
		`wager_sqs_consumer_messages_total{outcome="processed"} 1`,
		`wager_sqs_consumer_receive_errors_total 1`,
		`wager_outbox_events_total{outcome="published"} 1`,
		`wager_outbox_pending_events 3`,
		`wager_pending_references_cycles_total{outcome="processed"} 1`,
		`wager_pending_references_resolved_total 2`,
		`wager_reconciliation_checks_total{result="divergent"} 1`,
	}
	for _, expected := range expectedMetrics {
		if !strings.Contains(body, expected) {
			t.Errorf("metrics response does not contain %q", expected)
		}
	}
	if response.Header().Get("X-Request-ID") == "" {
		t.Error("HTTP middleware must return X-Request-ID")
	}
}
