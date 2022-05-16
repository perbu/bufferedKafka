package kafkabuffer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
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
	C                    chan Message
	buffer               []kafka.Message
	lastSendAttempt      time.Time
	failureState         bool
	failureRetryInterval time.Duration
	batchSize            int
	interval             time.Duration
	writer               kwriter
	maxBatchSize         int
	topic                string
	kafkaTimeout         time.Duration
	failures             int
}

type Message struct {
	Topic   string `json:"topic"`
	Content []byte `json:"content"`
}

func Initialize(broker string, flush int, interval time.Duration) *Buffer {
	return &Buffer{
		broker:               broker,
		batchSize:            flush,
		interval:             interval,
		failureState:         false,
		failureRetryInterval: interval * 10,
		C:                    make(chan Message, 10),
		buffer:               make([]kafka.Message, 0, 10000),
		writer:               &kafka.Writer{},
		maxBatchSize:         maxBatchSize,
	}
}

// Run starts monitoring the channel and sends messages to the broker.
func (k *Buffer) Run(ctx context.Context) error {
	err := k.sendTestMessage()
	if err != nil {
		return fmt.Errorf("failed to send initial test message: %w", err)
	}
	ticker := time.NewTicker(k.interval)
	log.Infof("Kafka buffer started with interval %v", k.interval)
loop:
	for {
		select {
		case <-ctx.Done():
			log.Info("Kafka buffer: context cancelled")
			break loop
		case <-ticker.C:
			if time.Since(k.lastSendAttempt) > k.interval {
				log.Trace("Tick: Send it!")
				k.Send()
			}
		case m := <-k.C:
			log.Trace("Kafka buffer: Message received")
			k.Enqueue("mqtt", m)
		}
	}
	ticker.Stop()
	log.Info("Final flush of the buffer")
	k.Send()
	return nil
}

// Enqueue adds a message to the buffer
// It'll transform it from the Message type (used by MQTT) to what Kafka expects.
// if the number of enqueued messages is greater than the batch size, it'll send them.
func (k *Buffer) Enqueue(topic string, msg Message) {
	msgJson, err := json.Marshal(msg)
	if err != nil {
		// todo: Log the error. There is nothing else we can do here.
		return
	}
	m := kafka.Message{
		Topic: topic,
		Value: msgJson,
	}
	k.buffer = append(k.buffer, m)
	if len(k.buffer) > k.batchSize {
		if k.failureState {
			// Not triggering flush if we're failing.
			return
		}
		log.Debugf("Triggering flush (buffer is %d, batchSize is %d)", len(k.buffer), k.batchSize)
		k.Send()
		return
	}
}

// Send will send all messages in the buffer to the kafka broker
func (k *Buffer) Send() {

	if len(k.buffer) == 0 {
		log.Trace("buffer empty")
		return
	}
	if k.failureState && time.Since(k.lastSendAttempt) < k.failureRetryInterval {
		log.Tracef("In a failed state. Not time to retry yet. Time since last check: %v (%v)",
			time.Since(k.lastSendAttempt), k.failureRetryInterval)
		return
	}
	log.Debug("Attempting to send messages")
	defer k.updateLastSendAttempt() // update the attempt time, even if we fail.
	var err error
	start := time.Now()
	if len(k.buffer) < k.maxBatchSize {
		err = k.sendAll()
	} else {
		err = k.sendBatched()
	}
	if err != nil {
		log.Warnf("kafkabuffer/Send: %s (time taken: %v, failures: %d)", err, time.Since(start), k.failures)
		k.failureState = true
		k.failures++
		return
	}
	log.Debugf("kafkabuffer/Send: Wrote %d messages in %v", len(k.buffer), time.Since(start))
	k.failureState = false
	k.buffer = k.buffer[:0] // clear the buffer
	k.updateLastSendAttempt()
}

// sendAll sends all messages in the buffer.
func (k *Buffer) sendAll() error {
	log.Debugf("Sending all messages (%d) in the buffer", len(k.buffer))
	ctx, cancel := context.WithTimeout(context.Background(), k.kafkaTimeout)
	defer cancel()
	return k.writer.WriteMessages(ctx, k.buffer...)
}

// sendBatched sends messages in batches of maxBatchSize.
func (k *Buffer) sendBatched() error {
	l := len(k.buffer)
	log.Debugf("Sending all (%d) messages in batches", l)
	batches := l / k.maxBatchSize
	timeout := k.kafkaTimeout * time.Duration(batches)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	batch := 0
	for {
		batch++
		log.Debug("attempting to send batch:", batch)
		l := len(k.buffer)
		if l == 0 {
			break
		}
		if l < k.maxBatchSize {
			err := k.writer.WriteMessages(ctx, k.buffer...)
			if err != nil {
				return fmt.Errorf("error batch %d", batch)
			}
			k.buffer = k.buffer[:0] // done. clear the buffer.
			break
		} else {
			err := k.writer.WriteMessages(ctx, k.buffer[:k.maxBatchSize]...)
			if err != nil {
				return err
			}
			k.buffer = k.buffer[k.maxBatchSize:] // remove the first k.maxBatchSize messages from the buffer.
		}
	}
	return nil
}

// sendTestMessage sends a test message with the mqtt topic "test".
// You wanna ignore these messages in the Kafka consumers.
func (k *Buffer) sendTestMessage() error {
	ctx, cancel := context.WithTimeout(context.Background(), k.kafkaTimeout)
	defer cancel()
	return k.writer.WriteMessages(ctx, generateTestMessage())
}

func (k *Buffer) updateLastSendAttempt() {
	k.lastSendAttempt = time.Now()
}

func generateTestMessage() kafka.Message {
	msg := Message{
		Topic:   "test",
		Content: []byte("Internal test to see if kafka is alive at startup"),
	}
	msgJson, err := json.Marshal(msg)
	if err != nil {
		// todo: Log the error. There is nothing else we can do here.
		log.Fatalf("mashalling test message: %s", err)
	}
	testMsg := kafka.Message{
		Topic: "mqtt",
		Value: msgJson,
	}
	return testMsg
}
