package kafkabuffer

import (
	"context"
	"github.com/segmentio/kafka-go"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// Init code goes here.
	ret := m.Run()
	// Cleanup code goes here.
	os.Exit(ret)
}

func TestBuffer_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	buffer := makeTestBuffer()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		buffer.Run(ctx)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()
}

func makeTestBuffer(interval time.Duration) Buffer {

	return Buffer{
		interval: interval,
		buffer:   make([]kafka.Message, 10),
		topic:    "unittest",
	}
}
