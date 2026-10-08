package worker

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func TestOutboxWorkerPublishesOnlyToEventQueue(t *testing.T) {
	client := &recordingSQSClient{}
	worker := NewOutboxWorker(nil, client, "event-queue-url")
	event := outboxEvent{
		id:          "event-1",
		eventType:   "WalletBalanceChanged",
		aggregateID: "wallet-1",
		payload:     []byte(`{"eventId":"event-1","eventType":"WalletBalanceChanged"}`),
	}

	if err := worker.sendEvent(context.Background(), event); err != nil {
		t.Fatalf("sendEvent() error = %v", err)
	}
	if got := aws.ToString(client.input.QueueUrl); got != "event-queue-url" {
		t.Errorf("QueueUrl = %q, want event-queue-url", got)
	}
	if got := aws.ToString(client.input.MessageBody); got != string(event.payload) {
		t.Errorf("MessageBody = %q, want event envelope", got)
	}
	if got := aws.ToString(client.input.MessageDeduplicationId); got != event.id {
		t.Errorf("MessageDeduplicationId = %q, want %q", got, event.id)
	}
}

type recordingSQSClient struct {
	input *sqs.SendMessageInput
}

func (c *recordingSQSClient) SendMessage(
	_ context.Context,
	input *sqs.SendMessageInput,
	_ ...func(*sqs.Options),
) (*sqs.SendMessageOutput, error) {
	c.input = input
	return &sqs.SendMessageOutput{}, nil
}
