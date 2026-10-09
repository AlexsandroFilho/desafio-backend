package observability

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry

	httpRequests      *prometheus.CounterVec
	httpDuration      *prometheus.HistogramVec
	wagerTransactions *prometheus.CounterVec
	wagerDuration     *prometheus.HistogramVec
	sqsMessages       *prometheus.CounterVec
	sqsReceiveErrors  prometheus.Counter
	outboxEvents      *prometheus.CounterVec
	outboxPending     prometheus.Gauge
	pendingCycles     *prometheus.CounterVec
	pendingResolved   prometheus.Counter
	reconciliations   *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wager",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total number of HTTP requests.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "wager",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),
		wagerTransactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wager",
			Subsystem: "transactions",
			Name:      "processed_total",
			Help:      "Total number of wager transaction processing attempts.",
		}, []string{"kind", "status"}),
		wagerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "wager",
			Subsystem: "transactions",
			Name:      "processing_duration_seconds",
			Help:      "Wager transaction processing duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"kind"}),
		sqsMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wager",
			Subsystem: "sqs_consumer",
			Name:      "messages_total",
			Help:      "Total number of command queue messages by outcome.",
		}, []string{"outcome"}),
		sqsReceiveErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wager",
			Subsystem: "sqs_consumer",
			Name:      "receive_errors_total",
			Help:      "Total number of command queue receive failures.",
		}),
		outboxEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wager",
			Subsystem: "outbox",
			Name:      "events_total",
			Help:      "Total number of outbox publication attempts by outcome.",
		}, []string{"outcome"}),
		outboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "wager",
			Subsystem: "outbox",
			Name:      "pending_events",
			Help:      "Current number of pending outbox events.",
		}),
		pendingCycles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wager",
			Subsystem: "pending_references",
			Name:      "cycles_total",
			Help:      "Total pending-reference worker cycles by outcome.",
		}, []string{"outcome"}),
		pendingResolved: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wager",
			Subsystem: "pending_references",
			Name:      "resolved_total",
			Help:      "Total number of pending references resolved.",
		}),
		reconciliations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wager",
			Subsystem: "reconciliation",
			Name:      "checks_total",
			Help:      "Total wallet reconciliation checks by result.",
		}, []string{"result"}),
	}

	registry.MustRegister(
		metrics.httpRequests,
		metrics.httpDuration,
		metrics.wagerTransactions,
		metrics.wagerDuration,
		metrics.sqsMessages,
		metrics.sqsReceiveErrors,
		metrics.outboxEvents,
		metrics.outboxPending,
		metrics.pendingCycles,
		metrics.pendingResolved,
		metrics.reconciliations,
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return metrics
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveWagerTransaction(
	kind domain.TransactionKind,
	status string,
	duration time.Duration,
) {
	m.wagerTransactions.WithLabelValues(string(kind), status).Inc()
	m.wagerDuration.WithLabelValues(string(kind)).Observe(duration.Seconds())
}

func (m *Metrics) ObserveSQSMessage(outcome string) {
	m.sqsMessages.WithLabelValues(outcome).Inc()
}

func (m *Metrics) ObserveSQSReceiveError() {
	m.sqsReceiveErrors.Inc()
}

func (m *Metrics) ObserveOutboxEvent(outcome string) {
	m.outboxEvents.WithLabelValues(outcome).Inc()
}

func (m *Metrics) SetOutboxPending(count int64) {
	m.outboxPending.Set(float64(count))
}

func (m *Metrics) ObservePendingReferences(outcome string, count int) {
	if outcome == "resolved" {
		m.pendingCycles.WithLabelValues("processed").Inc()
		if count > 0 {
			m.pendingResolved.Add(float64(count))
		}
		return
	}
	m.pendingCycles.WithLabelValues(outcome).Inc()
}

func (m *Metrics) ObserveReconciliation(consistent bool) {
	result := "consistent"
	if !consistent {
		result = "divergent"
	}
	m.reconciliations.WithLabelValues(result).Inc()
}

func (m *Metrics) HTTPMiddleware(
	logger *slog.Logger,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		requestID := strings.TrimSpace(r.Header.Get("X-Correlation-ID"))
		if requestID == "" {
			requestID = newRequestID()
		}
		w.Header().Set("X-Request-ID", requestID)

		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		duration := time.Since(startedAt)
		status := strconv.Itoa(recorder.status)
		m.httpRequests.WithLabelValues(r.Method, route, status).Inc()
		m.httpDuration.WithLabelValues(r.Method, route).Observe(duration.Seconds())

		logger.Info(
			"http request completed",
			"request_id", requestID,
			"method", r.Method,
			"route", route,
			"status", recorder.status,
			"duration_ms", duration.Milliseconds(),
			"remote_address", r.RemoteAddr,
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(body)
}

func newRequestID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(value[:])
}
