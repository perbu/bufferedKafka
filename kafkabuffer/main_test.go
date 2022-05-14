package kafkabuffer

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockWriter struct {
	mu           sync.Mutex
	messageDelay time.Duration
	storage      []kafka.Message
	failed       bool
	msgs         uint64
	writes       uint64
}

func (m *mockWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	time.Sleep(m.messageDelay)
	if m.failed {
		return errors.New("storage is in a failed state")
	}
	if m.storage == nil {
		m.storage = make([]kafka.Message, 0)
	}
	l := uint64(len(msgs))
	log.Debugf("Writing %d messages to pretend kafka", l)
	m.storage = append(m.storage, msgs...)
	atomic.AddUint64(&m.msgs, l)
	atomic.AddUint64(&m.writes, 1)
	return nil
}

func (m *mockWriter) setDelay(d time.Duration) {
	log.Info("Setting storage delay to ", d)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messageDelay = d
}
func (m *mockWriter) setState(failed bool) {
	log.Info("Setting storage failed state to ", failed)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed = failed
}

func (m *mockWriter) getMessage(id int) kafka.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.storage[id]
}

func waitForAtomic(a *uint64, v uint64, timeout, sleeptime time.Duration) error {
	start := time.Now()
	for time.Since(start) < timeout {
		if atomic.LoadUint64(a) == v {
			return nil
		}
		time.Sleep(sleeptime)
	}
	return errors.New("kafkaTimeout")
}

func TestMain(m *testing.M) {
	log.SetLevel(log.TraceLevel)
	log.Debug("Running test suite")
	ret := m.Run()
	log.Debug("Test suite complete")
	os.Exit(ret)
}

func TestBuffer_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	storage := &mockWriter{}
	buffer := makeTestBuffer(storage)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := buffer.Run(ctx)
		if err != nil {
			log.Errorf("Error %s", err)
		}
		log.Info("Buffer run complete")
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	log.Debug("Cancel issued. Waiting.")
	wg.Wait()
	log.Debug("Done")
}

func makeTestBuffer(writer *mockWriter) Buffer {
	return Buffer{
		interval:             10 * time.Millisecond,
		failureRetryInterval: 50 * time.Millisecond,
		buffer:               make([]kafka.Message, 0, 10),
		topic:                "unittest",
		writer:               writer,
		C:                    make(chan Message, 0),
		batchSize:            5,
		maxBatchSize:         20,
		kafkaTimeout:         25 * time.Millisecond,
	}
}

func TestBuffer_Process_ok(t *testing.T) {
	storage := &mockWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	buffer := makeTestBuffer(storage)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := buffer.Run(ctx)
		if err != nil {
			log.Errorf("Error %s", err)
		}
		log.Info("Buffer run complete")
	}()

	for i := 0; i < 10; i++ {
		buffer.C <- makeMessage("test", i)
	}
	cancel()
	wg.Wait()
	for i := 0; i < 10; i++ {
		m := storage.getMessage(i)
		fmt.Printf("Message: %s\n", string(m.Value))
	}
	log.Debug("Done")
}

func TestBuffer_Process_fail(t *testing.T) {
	storage := &mockWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	buffer := makeTestBuffer(storage)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := buffer.Run(ctx)
		if err != nil {
			log.Errorf("Error %s", err)
		}
		log.Info("Buffer run complete")
	}()
	log.Info("Sending msgs 0 -> 5 ")
	for i := 0; i < 5; i++ {
		buffer.C <- makeMessage("test", i)
	}
	storage.setState(true)
	log.Info("Sending msgs 5 -> 10")
	for i := 5; i < 10; i++ {
		buffer.C <- makeMessage("test", i)
	}
	log.Info("Done with msgs")
	time.Sleep(100 * time.Millisecond)
	storage.setState(false)
	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()
	for i := 0; i < 10; i++ {
		m := storage.getMessage(i)
		fmt.Printf("Message: %s\n", string(m.Value))
	}
	log.Debug("Done")
}

func TestBuffer_Process_initial_fail(t *testing.T) {
	storage := &mockWriter{}
	storage.setState(true)
	ctx, cancel := context.WithCancel(context.Background())
	buffer := makeTestBuffer(storage)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := buffer.Run(ctx)
		if err != nil {
			log.Errorf("Error %s", err)
		}
		log.Info("Buffer run complete")
	}()
	for i := 0; i < 5; i++ {
		buffer.C <- makeMessage("test", i)
	}
	storage.setState(false)
	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()
	for i := 0; i < 5; i++ {
		m := storage.getMessage(i)
		fmt.Printf("Message: %s\n", string(m.Value))
	}
	log.Debug("Done")
}

func TestBuffer_Process_slow(t *testing.T) {
	storage := &mockWriter{}
	storage.setState(true)
	storage.setDelay(10 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	buffer := makeTestBuffer(storage)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := buffer.Run(ctx)
		if err != nil {
			log.Errorf("Error %s", err)
		}
		log.Info("Buffer run complete")
	}()
	for i := 0; i < 5; i++ {
		buffer.C <- makeMessage("test", i)
	}
	storage.setState(false)
	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()
	for i := 0; i < 5; i++ {
		m := storage.getMessage(i)
		fmt.Printf("Message: %s\n", string(m.Value))
	}
	log.Debug("Done")
}

func TestBuffer_Batching(t *testing.T) {
	storage := &mockWriter{}
	buffer := makeTestBuffer(storage)
	buffer.batchSize = 100
	buffer.maxBatchSize = 1000
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := buffer.Run(ctx)
		if err != nil {
			log.Errorf("Error %s", err)
		}
		log.Info("Buffer run complete")
	}()
	storage.setState(true)
	for i := 0; i < 10000; i++ {
		buffer.C <- makeMessage("test", i)
	}
	storage.setState(false)
	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()
	for i := 0; i < 5; i++ {
		m := storage.getMessage(i)
		fmt.Printf("Message: %s\n", string(m.Value))
	}
	log.Debug("Done")

}

func makeMessage(topic string, id int) Message {
	return Message{
		Topic:   "/test/topic",
		Content: []byte(fmt.Sprintf("Test message %d", id)),
	}
}
