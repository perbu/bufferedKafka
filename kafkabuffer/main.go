package kafkabuffer

import (
	"context"
	"encoding/json"
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
		C:                    make(chan Message, 10),
		buffer:               make([]kafka.Message, 0, 10000),
		writer:               &kafka.Writer{},
		maxBatchSize:         maxBatchSize,
	}
}

// Run starts monitoring the channel and sends messages to the broker.
func (k *Buffer) Run(ctx context.Context) {
	ticker := time.NewTicker(k.interval)
	log.Debugf("Kafka buffer started with interval %v", k.interval)
loop:
	for {
		select {
		case <-ctx.Done():
			log.Info("Kafka buffer: context cancelled")
			break loop
		case <-ticker.C:
			if time.Since(k.lastSend) < k.interval {
				log.Trace("Tick: Send it!")
				k.Send()
			} else {
				log.Trace("Tick: Not sending anything")
			}
		case m := <-k.C:
			log.Trace("Kafka buffer: Message received")
			k.Process(m)
			log.Trace("Message processed")
		}
	}
	ticker.Stop()
	k.Send()
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
	if k.failureState && time.Since(k.lastHealthCheck) < k.failureRetryInterval {
		log.Tracef("In a failed state. Time since last check: %v (%v)", time.Since(k.lastHealthCheck), k.failureRetryInterval)
		return
	}
	var err error
	start := time.Now()
	if len(k.buffer) < k.maxBatchSize {
		err = k.sendAll()
	} else {
		err = k.sendBatched()
	}
	if err != nil {
		log.Errorf("kafkabuffer/Send: %s (time taken: %v)", err, time.Since(start))
		k.failureState = true
		return
	}
	log.Debugf("kafkabuffer/Send: Wrote %d messages in %v", len(k.buffer), time.Since(start))
	k.failureState = false
	k.buffer = k.buffer[:0] // clear the buffer
	k.lastSend = time.Now()
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
