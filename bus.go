package bus

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
)

type Publisher struct {
	kcl   *kgo.Client
	topic string
}

func NewPublisher(brokers []string, topic string) (*Publisher, error) {
	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	pub := &Publisher{
		kcl:   kcl,
		topic: topic,
	}
	return pub, nil
}

func (pub *Publisher) Publish(ctx context.Context, msg []byte) error {
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	record := &kgo.Record{Topic: pub.topic, Value: msg}
	pub.kcl.Produce(ctx, record, func(_ *kgo.Record, e error) {
		err = e
		wg.Done()
	})
	wg.Wait()

	if err != nil {
		return fmt.Errorf("kgo had a produce error: %w", err)
	}
	return nil
}

func (pub *Publisher) Close() {
	pub.kcl.Close()
}

type Consumer struct {
	kcl   *kgo.Client
	Done  chan struct{}
	Error error
}

func NewConsumer(
	ctx context.Context,
	brokers []string,
	topic string,
	f func(msg []byte),
) (*Consumer, error) {
	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}

	consumer := &Consumer{
		kcl:   kcl,
		Done:  make(chan struct{}),
		Error: nil,
	}
	go consumer.each(ctx, f)
	return consumer, nil
}

func (consumer *Consumer) each(ctx context.Context, f func(msg []byte)) {
	defer consumer.kcl.Close()
	defer close(consumer.Done)

	for {
		fetches := consumer.kcl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			wrapped := []error{}
			for _, err := range errs {
				wrapped = append(wrapped,
					fmt.Errorf("fetch error: %w", err.Err),
				)
			}
			// All errors are retried internally when fetching, but non-retriable errors are
			// returned from polls so that users can notice and take action.
			consumer.Error = errors.Join(wrapped...)
			return
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			f(record.Value)
		}
	}
}