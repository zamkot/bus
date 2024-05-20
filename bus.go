package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
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

func (pub *Publisher) PublishJSON(ctx context.Context, msg any) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}
	return pub.Publish(ctx, bytes)
}

func (pub *Publisher) Close() {
	pub.kcl.Close()
}

type Consumer struct {
	kcl   *kgo.Client
	Done  chan struct{}
	Error error
}

func Consume(
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

func ConsumeJSON[T any](
	ctx context.Context,
	brokers []string,
	topic string,
	f func(msg T),
) (*Consumer, error) {
	return Consume(ctx, brokers, topic, func(msg []byte) {
		var parsed T
		err := json.Unmarshal(msg, &parsed)
		if err != nil {
			fmt.Printf("[bus] cannot parse message: \"%s\" as %T: %v\n", msg, parsed, err)
			return
		}
		f(parsed)
	})
}

func ConsumeChanJSON[T any](
	ctx context.Context,
	brokers []string,
	topic string,
	c chan T,
) (*Consumer, error) {
	consumer, err := ConsumeJSON(ctx, brokers, topic, func(msg T) {
		c <- msg
	})
	if err != nil {
		return nil, fmt.Errorf("from ConsumeJSON: %w", err)
	}
	go func() {
		<-consumer.Done
		close(c)
	}()
	return consumer, nil
}
