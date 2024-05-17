package main

import (
	"context"
	"fmt"
	"github.com/zamkot/bus"
	"time"
)

var (
	brokers = []string{"127.0.0.1:9094"}
	topic   = "interesting-stuff"
)

type CoolMessage struct {
	Text   string `json:"text"`
	Number int    `json:"number"`
}

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(context.Background())

	pub, err := bus.NewPublisher(brokers, topic)
	if err != nil {
		panic(fmt.Errorf("new client pub: %w", err))
	}
	defer pub.Close()

	// There is no one listening - this message goes into nothingness
	err = pub.Publish(ctx, []byte("one"))
	assertNoErr("publish one", err)

	// Any message produced from now one will trigger this callback
	c1, err := bus.NewConsumer(ctx, brokers, topic, func(msg []byte) {
		fmt.Printf("c1: %s\n", msg)
	})
	assertNoErr("create c1", err)

	// We can also defer parsing JSON to the consumer
	cJSON, err := bus.NewConsumerJSON(ctx, brokers, topic, func(msg CoolMessage) {
		fmt.Printf("cJSON: got text=\"%s\" and number=%d\n", msg.Text, msg.Number)
	})
	assertNoErr("create cJSON", err)

	time.Sleep(100 * time.Millisecond)

	// This message will be seen by the active consumers
	// The JSON guy won't be happy though
	err = pub.Publish(ctx, []byte("two"))
	assertNoErr("publish two", err)

	// And finally a well-formed JSON message
	err = pub.PublishJSON(ctx, CoolMessage{
		Text:   "hello",
		Number: 600,
	})
	assertNoErr("publish json", err)

	time.Sleep(100 * time.Millisecond)
	cancel()

	<-c1.Done
	fmt.Printf("c1 exited with reason: %v\n", c1.Error)
	<-cJSON.Done
	fmt.Printf("cJSON exited with reason: %v\n", cJSON.Error)
}

func assertNoErr(where string, err error) {
	if err != nil {
		panic(fmt.Errorf("%s: %w", where, err))
	}
}
