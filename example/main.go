package main

import (
	"context"
	"fmt"
	"time"

	"github.com/zamkot/bus"
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

	pub, err := bus.NewProducer(brokers, topic)
	if err != nil {
		panic(fmt.Errorf("new client pub: %w", err))
	}
	defer pub.Close()

	// There is no one listening - this message goes into nothingness
	err = pub.Produce(ctx, []byte("one"))
	assertNoErr("produce one", err)

	// Any message produced from now on will trigger this callback
	c1, err := bus.Consume(ctx, brokers, topic, func(msg []byte) {
		fmt.Printf("c1: %s\n", msg)
	})
	assertNoErr("create c1", err)

	// Bus can also deserialize messages into our own custom type
	cJSON, err := bus.ConsumeJSON(ctx, brokers, topic, func(msg CoolMessage) {
		fmt.Printf("cJSON: got text=\"%s\" and number=%d\n", msg.Text, msg.Number)
	})
	assertNoErr("create cJSON", err)

	// Here's another convenience method for a channel based API
	coolMsgChan := make(chan CoolMessage)
	cChanJSON, err := bus.ConsumeChanJSON(ctx, brokers, topic, coolMsgChan)
	assertNoErr("create cJSON", err)
	go func() {
		for msg := range coolMsgChan {
			fmt.Printf("cChanJSON: got text=\"%s\" and number=%d\n", msg.Text, msg.Number)
		}
		fmt.Println("cChanJSON closed the channel")
	}()

	time.Sleep(100 * time.Millisecond)

	// This message will be seen by the active consumers
	// The JSON guys won't be happy about the format though
	err = pub.Produce(ctx, []byte("two"))
	assertNoErr("produce two", err)

	// And finally a well-formed JSON message
	err = pub.ProduceJSON(ctx, CoolMessage{
		Text:   "hello",
		Number: 600,
	})
	assertNoErr("produce json", err)

	time.Sleep(100 * time.Millisecond)
	cancel()

	<-c1.Done
	fmt.Printf("c1 exited with reason: %v\n", c1.Error)
	<-cJSON.Done
	fmt.Printf("cJSON exited with reason: %v\n", cJSON.Error)
	<-cChanJSON.Done
	fmt.Printf("cChanJSON exited with reason: %v\n", cChanJSON.Error)
}

func assertNoErr(where string, err error) {
	if err != nil {
		panic(fmt.Errorf("%s: %w", where, err))
	}
}
