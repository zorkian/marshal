/*
 * simple Marshal example consumer
 */

package main

import (
	"github.com/op/go-logging"
	"github.com/zorkian/marshal/marshal"
)

func main() {
	log := logging.MustGetLogger("MarshalExample")

	// Construct the marshaler. There will be one of these globally, and it's thread safe so can
	// be used from any goroutine.
	marshaler, err := marshal.NewMarshaler(
		"marshal_example_client_id",
		"marshal_example_consumer_group_id",
		[]string{"127.0.0.1:9092"})
	if err != nil {
		log.Fatalf("Failed to construct marshaler: %s", err)
	}

	// Make sure to terminate the Marshaler. This ensures that we release all of the partition
	// locks we're holding so other consumers can pick them up.
	defer marshaler.Terminate()

	// Now let's start up an aggressive consumer, this is most appropriate when you have a low
	// QPS topic that can be fully consumed by a single process. You can run multiple processes,
	// but you will likely see unbalanced loads. That's OK.
	littleConsumer, err := marshal.NewConsumer(marshaler, "quiet-topic", marshal.CbAggressive)
	if err != nil {
		log.Fatalf("Failed to construct consumer: %s", err)
	}
	defer littleConsumer.Terminate()

	// Let's send off a goroutine to handle this topic. We think one is going to be enough,
	// so it's not too busy.
	go func() {
		for {
			msg := littleConsumer.Consume()
			log.Info("Got quiet topic message: %s", msg)
		}
	}()

	// Now let's build a balanced consumer. This is best in situations where a topic has many
	// partitions and high QPS. This helps the cluster start out more evenly loaded at the cost
	// of a slightly slower start.
	bigConsumer, err := marshal.NewConsumer(marshaler, "busy-topic", marshal.CbBalanced)
	if err != nil {
		log.Fatalf("Failed to construct consumer: %s", err)
	}
	defer bigConsumer.Terminate()

	// Since this is a busy topic, let's spin up a couple of goroutines to handle the messages.
	// Note that Consume is safe to call from lots of places at the same time.
	for i := 0; i < 10; i++ {
		go func() {
			for {
				msg := bigConsumer.Consume()
				log.Info("Got busy topic message: %s", msg)
			}
		}()
	}
}
