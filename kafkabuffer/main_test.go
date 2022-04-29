package kafkabuffer

import (
	"context"
	"testing"
	"time"
)

func TestBuffer_Run(t *testing.T) {
	type fields struct {
		broker               string
		messages             chan Message
		buffer               []kafka.Message
		lastSend             time.Time
		failureState         bool
		failureRetryInterval time.Duration
		lastHealthCheck      time.Time
		batchSize            int
		interval             time.Duration
		writer               kwriter
		maxBatchSize         int
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Buffer{
				broker:               tt.fields.broker,
				messages:             tt.fields.messages,
				buffer:               tt.fields.buffer,
				lastSend:             tt.fields.lastSend,
				failureState:         tt.fields.failureState,
				failureRetryInterval: tt.fields.failureRetryInterval,
				lastHealthCheck:      tt.fields.lastHealthCheck,
				batchSize:            tt.fields.batchSize,
				interval:             tt.fields.interval,
				writer:               tt.fields.writer,
				maxBatchSize:         tt.fields.maxBatchSize,
			}
			k.Run(tt.args.ctx)
		})
	}
}
