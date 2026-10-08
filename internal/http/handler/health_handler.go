package handler

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const readinessTimeout = 3 * time.Second

type DatabasePinger interface {
	Ping(ctx context.Context) error
}

type SQSHealthClient interface {
	GetQueueAttributes(
		ctx context.Context,
		params *sqs.GetQueueAttributesInput,
		optFns ...func(*sqs.Options),
	) (*sqs.GetQueueAttributesOutput, error)
}

type HealthHandler struct {
	database        DatabasePinger
	sqsClient       SQSHealthClient
	commandQueueURL string
	eventQueueURL   string
}

func NewHealthHandler(
	database DatabasePinger,
	sqsClient SQSHealthClient,
	commandQueueURL string,
	eventQueueURL string,
) *HealthHandler {
	return &HealthHandler{
		database:        database,
		sqsClient:       sqsClient,
		commandQueueURL: commandQueueURL,
		eventQueueURL:   eventQueueURL,
	}
}

func (h *HealthHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health/live", h.Live)
	mux.HandleFunc("GET /health/ready", h.Ready)
}

func (h *HealthHandler) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	var (
		databaseErr     error
		commandQueueErr error
		eventQueueErr   error
		waitGroup       sync.WaitGroup
	)
	waitGroup.Add(3)

	go func() {
		defer waitGroup.Done()
		databaseErr = h.database.Ping(ctx)
	}()
	go func() {
		defer waitGroup.Done()
		_, commandQueueErr = h.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(h.commandQueueURL),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		})
	}()
	go func() {
		defer waitGroup.Done()
		_, eventQueueErr = h.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(h.eventQueueURL),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		})
	}()

	waitGroup.Wait()

	checks := map[string]string{
		"postgres":    "up",
		"sqs_command": "up",
		"sqs_event":   "up",
	}
	status := http.StatusOK
	overallStatus := "ready"

	if databaseErr != nil {
		checks["postgres"] = "down"
		status = http.StatusServiceUnavailable
		overallStatus = "not_ready"
	}
	if commandQueueErr != nil {
		checks["sqs_command"] = "down"
		status = http.StatusServiceUnavailable
		overallStatus = "not_ready"
	}
	if eventQueueErr != nil {
		checks["sqs_event"] = "down"
		status = http.StatusServiceUnavailable
		overallStatus = "not_ready"
	}

	writeJSON(w, status, map[string]any{
		"status": overallStatus,
		"checks": checks,
	})
}
