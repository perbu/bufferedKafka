package kafkabuffer

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"time"
)

type kwriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

const (
	maxBatchSize = 8000
)

type Buffer struct {
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
	topic                string
}

type Message struct {
	Topic   string
	Content []byte
}

func Initialize(broker string, flush int, interval time.Duration) *Buffer {
	return &Buffer{
		broker:               broker,
		batchSize:            flush,
		interval:             interval,
		failureState:         false,
		failureRetryInterval: interval * 10,
		messages:             make(chan Message, 10),
		buffer:               make([]kafka.Message, 0, 10000),
		writer:               &kafka.Writer{},
		maxBatchSize:         maxBatchSize,
	}
}

// Run starts monitoring the channel and sends messages to the broker.
func (k *Buffer) Run(ctx context.Context) {
	ticker := time.NewTicker(k.interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			k.Send()
		case m := <-k.messages:
			k.Process(m)
		}
	}
}

// Process adds a message to the buffer
// It'll transform it from the Message type (used by MQTT) to what Kafka expects.
func (k *Buffer) Process(msg Message) {
	msgJson, err := json.Marshal(msg)
	if err != nil {
		// todo: Log the error. There is nothing else we can do here.
		return
	}
	m := kafka.Message{
		Value: msgJson,
	}
	k.buffer = append(k.buffer, m)
	if len(k.buffer) > k.batchSize {
		k.Send()
		return
	}
}

// Send will send all messages in the buffer to the kafka broker
func (k *Buffer) Send() {
	// send might have been called prematurely. Detect it and return if that is the case.
	if time.Since(k.lastSend) < k.interval || len(k.buffer) < k.batchSize {
		return
	}
	// bail out if we're in a failure state and it isn't time to retry yet.
	if k.failureState && time.Since(k.lastHealthCheck) < k.failureRetryInterval {
		return
	}
	var err error
	if len(k.buffer) < k.maxBatchSize {
		err = k.sendAll()
	} else {
		err = k.sendBatched()
	}
	if err != nil {
		k.failureState = true
		return
	}
	if err == nil {
		k.failureState = false
		k.buffer = k.buffer[:0] // clear the buffer
		k.lastSend = time.Now()
	}
}

// sendAll sends all messages in the buffer.
func (k *Buffer) sendAll() error {
	return k.writer.WriteMessages(context.Background(), k.buffer...)
}

// sendBatched sends messages in batches of maxBatchSize.
func (k *Buffer) sendBatched() error {
	l := len(k.buffer)
	for i := 0; i < l; i += k.maxBatchSize {
		end := i + k.maxBatchSize
		if end > l {
			end = l
		}
		err := k.writer.WriteMessages(context.Background(), k.buffer[i:end]...)
		if err != nil {
			return err
		}
	}
	return nil
}

// sendTestMessage sends a test message with the mqtt topic "test".
// You wanna ignore these messages in the Kafka consumers.
func (k *Buffer) sendTestMessage() error {
	testMsg := kafka.Message{
		Topic: "test",
		Value: []byte("Just a test"),
	}
	return k.writer.WriteMessages(context.Background(), testMsg)
}
