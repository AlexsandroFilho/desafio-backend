package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"alexsandro-filho/desafio-backend/internal/domain"
	"alexsandro-filho/desafio-backend/internal/repository"
	"alexsandro-filho/desafio-backend/internal/usecase"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	wagerConsumerName      = "wager-transactions-consumer"
	sqsLongPollingSeconds  = 20
	sqsVisibilityTimeout   = 30
	sqsReceiveErrorBackoff = time.Second
	wagerRequestedType     = "WagerTransactionRequested"
)

type SQSReceiver interface {
	ReceiveMessage(
		ctx context.Context,
		params *sqs.ReceiveMessageInput,
		optFns ...func(*sqs.Options),
	) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(
		ctx context.Context,
		params *sqs.DeleteMessageInput,
		optFns ...func(*sqs.Options),
	) (*sqs.DeleteMessageOutput, error)
}

type WagerProcessor interface {
	ProcessTransaction(
		ctx context.Context,
		req usecase.WagerRequest,
	) (*usecase.WagerResponse, error)
}

type SQSConsumerMetrics interface {
	ObserveSQSMessage(outcome string)
	ObserveSQSReceiveError()
}

type SQSConsumer struct {
	sqsClient SQSReceiver
	processor WagerProcessor
	queueURL  string
	logger    *slog.Logger
	metrics   SQSConsumerMetrics

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
	wg      sync.WaitGroup
}

func NewSQSConsumer(
	sqsClient SQSReceiver,
	processor WagerProcessor,
	queueURL string,
	metrics ...SQSConsumerMetrics,
) *SQSConsumer {
	consumer := &SQSConsumer{
		sqsClient: sqsClient,
		processor: processor,
		queueURL:  queueURL,
		logger:    slog.Default(),
	}
	if len(metrics) > 0 {
		consumer.metrics = metrics[0]
	}
	return consumer
}

func (c *SQSConsumer) Start(ctx context.Context) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}

	consumerCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.running = true
	c.wg.Add(1)
	c.mu.Unlock()

	go c.run(consumerCtx)
}

func (c *SQSConsumer) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	cancel := c.cancel
	c.mu.Unlock()

	cancel()
	c.wg.Wait()
}

func (c *SQSConsumer) run(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		c.running = false
		c.cancel = nil
		c.mu.Unlock()
		c.wg.Done()
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		output, err := c.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     sqsLongPollingSeconds,
			VisibilityTimeout:   sqsVisibilityTimeout,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if c.metrics != nil {
				c.metrics.ObserveSQSReceiveError()
			}
			c.logger.Error("failed to receive SQS messages", "error", err)
			if !waitForContext(ctx, sqsReceiveErrorBackoff) {
				return
			}
			continue
		}

		for _, message := range output.Messages {
			if ctx.Err() != nil {
				return
			}

			if !c.processMessage(ctx, aws.ToString(message.Body)) {
				continue
			}
			if ctx.Err() != nil {
				return
			}

			_, err := c.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(c.queueURL),
				ReceiptHandle: message.ReceiptHandle,
			})
			if err != nil && ctx.Err() == nil {
				if c.metrics != nil {
					c.metrics.ObserveSQSMessage("delete_error")
				}
				c.logger.Error(
					"failed to delete processed SQS message",
					"sqs_message_id", aws.ToString(message.MessageId),
					"error", err,
				)
			}
		}
	}
}

func (c *SQSConsumer) processMessage(ctx context.Context, body string) bool {
	envelope, err := decodeWagerEnvelope(body)
	if err != nil {
		// Malformed messages are left in the queue so the configured SQS
		// redrive policy can eventually move them to the DLQ.
		c.logger.Warn("invalid wager SQS message", "error", err)
		if c.metrics != nil {
			c.metrics.ObserveSQSMessage("invalid")
		}
		return false
	}

	amount, err := domain.NewMoneyFromString(
		envelope.Data.Money.Amount,
		envelope.Data.Money.Currency,
	)
	if err != nil {
		c.logger.Warn(
			"invalid wager amount in SQS message",
			"message_id", envelope.MessageID,
			"error", err,
		)
		if c.metrics != nil {
			c.metrics.ObserveSQSMessage("invalid")
		}
		return false
	}

	hash := sha256.Sum256([]byte(body))
	response, err := c.processor.ProcessTransaction(ctx, usecase.WagerRequest{
		ConsumerName:                   wagerConsumerName,
		MessageID:                      envelope.MessageID,
		MessageHash:                    hex.EncodeToString(hash[:]),
		CorrelationID:                  envelope.MessageID,
		CausationID:                    envelope.MessageID,
		ProviderID:                     envelope.Data.ProviderID,
		ExternalTransactionID:          envelope.Data.ExternalTransactionID,
		IdempotencyKey:                 envelope.Data.IdempotencyKey,
		WalletID:                       envelope.Data.WalletID,
		PlayerID:                       envelope.Data.PlayerID,
		RoundID:                        envelope.Data.RoundID,
		GameID:                         envelope.Data.GameID,
		Kind:                           envelope.Data.Kind,
		Amount:                         amount,
		ReferenceExternalTransactionID: envelope.Data.ReferenceExternalTransactionID,
	})
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		if isPermanentWagerError(err) {
			if c.metrics != nil {
				c.metrics.ObserveSQSMessage("permanent_failure")
			}
			c.logger.Warn(
				"discarding definitively rejected wager message",
				"message_id", envelope.MessageID,
				"error", err,
			)
			return true
		}

		if c.metrics != nil {
			c.metrics.ObserveSQSMessage("transient_failure")
		}
		c.logger.Error(
			"temporary wager processing failure",
			"message_id", envelope.MessageID,
			"error", err,
		)
		return false
	}

	if c.metrics != nil {
		c.metrics.ObserveSQSMessage("processed")
	}
	c.logger.Info(
		"wager message processed",
		"message_id", envelope.MessageID,
		"transaction_id", response.TransactionID,
		"status", response.Status,
		"idempotent_replay", response.IdempotentReplay,
	)
	return true
}

type WagerMessageEnvelope struct {
	MessageID  string              `json:"messageId"`
	Type       string              `json:"type"`
	OccurredAt time.Time           `json:"occurredAt"`
	Data       WagerMessagePayload `json:"data"`
}

type WagerMessageMoney struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type WagerMessagePayload struct {
	ProviderID                     string                 `json:"providerId"`
	ExternalTransactionID          string                 `json:"externalTransactionId"`
	IdempotencyKey                 string                 `json:"idempotencyKey"`
	WalletID                       string                 `json:"walletId"`
	PlayerID                       string                 `json:"playerId"`
	RoundID                        string                 `json:"roundId"`
	GameID                         string                 `json:"gameId"`
	Kind                           domain.TransactionKind `json:"kind"`
	Money                          WagerMessageMoney      `json:"money"`
	ReferenceExternalTransactionID string                 `json:"referenceExternalTransactionId,omitempty"`
}

func decodeWagerEnvelope(body string) (WagerMessageEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(body))
	decoder.DisallowUnknownFields()

	var envelope WagerMessageEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return WagerMessageEnvelope{}, fmt.Errorf("decode wager message: %w", err)
	}
	if err := ensureJSONDocumentEnded(decoder); err != nil {
		return WagerMessageEnvelope{}, err
	}
	if err := validateWagerEnvelope(envelope); err != nil {
		return WagerMessageEnvelope{}, err
	}

	return envelope, nil
}

func ensureJSONDocumentEnded(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("wager message contains multiple JSON values")
		}
		return fmt.Errorf("decode trailing wager message content: %w", err)
	}
	return nil
}

func validateWagerEnvelope(envelope WagerMessageEnvelope) error {
	requiredFields := map[string]string{
		"messageId":             envelope.MessageID,
		"providerId":            envelope.Data.ProviderID,
		"externalTransactionId": envelope.Data.ExternalTransactionID,
		"idempotencyKey":        envelope.Data.IdempotencyKey,
		"walletId":              envelope.Data.WalletID,
		"playerId":              envelope.Data.PlayerID,
		"roundId":               envelope.Data.RoundID,
		"gameId":                envelope.Data.GameID,
		"money.amount":          envelope.Data.Money.Amount,
		"money.currency":        envelope.Data.Money.Currency,
	}
	for name, value := range requiredFields {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("wager message field %s is required", name)
		}
	}
	if envelope.Type != wagerRequestedType {
		return fmt.Errorf("unsupported wager message type %q", envelope.Type)
	}
	if envelope.OccurredAt.IsZero() {
		return errors.New("wager message field occurredAt is required")
	}
	if _, offset := envelope.OccurredAt.Zone(); offset != 0 {
		return errors.New("wager message occurredAt must use UTC")
	}
	if !envelope.Data.Kind.IsValid() {
		return domain.ErrInvalidTransactionKind
	}

	return nil
}

func isPermanentWagerError(err error) bool {
	return errors.Is(err, usecase.ErrIdempotencyConflict) ||
		errors.Is(err, domain.ErrInvalidWalletAmount) ||
		errors.Is(err, domain.ErrInvalidTransactionKind) ||
		errors.Is(err, repository.ErrInboxHashMismatch)
}

func waitForContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
