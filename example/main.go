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

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(context.Background())

	pub, err := bus.NewPublisher(brokers, topic)
	if err != nil {
		panic(fmt.Errorf("new client pub: %w", err))
	}
	defer pub.Close()

	c1, err := bus.NewConsumer(ctx, brokers, topic, func(msg []byte) {
		fmt.Printf("c1: %s\n", msg)
	})
	assertNoErr("create c1", err)

	err = pub.Publish(ctx, []byte("one"))
	assertNoErr("publish one", err)

	time.Sleep(100 * time.Millisecond)

	c2, err := bus.NewConsumer(ctx, brokers, topic, func(msg []byte) {
		fmt.Printf("c2: %s\n", msg)
	})
	assertNoErr("create c2", err)

	time.Sleep(100 * time.Millisecond)

	err = pub.Publish(ctx, []byte("two"))
	assertNoErr("publish two", err)

	time.Sleep(100 * time.Millisecond)

	cancel()
	<-c1.Done
	fmt.Printf("c1 exited with reason: %v\n", c1.Error)
	<-c2.Done
	fmt.Printf("c2 exited with reason: %v\n", c2.Error)
}

func assertNoErr(where string, err error) {
	if err != nil {
		panic(fmt.Errorf("%s: %w", where, err))
	}
}
