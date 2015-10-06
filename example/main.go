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

	// Now we set up a basic consumer; and we enable GreedyClaims which is useful in low QPS
	// environments as it will cause the consumer to claim as many partitions as it can
	// up front. Of course, if you have a very busy topic with many partitions, you will
	// not want to use this.
	options := marshal.NewConsumerOptions()
	options.GreedyClaims = true

	consumer, err := marshal.NewConsumer(marshaler, "some-topic", options)
	if err != nil {
		log.Fatalf("Failed to construct consumer: %s", err)
	}
	defer consumer.Terminate()

	// You can spin up many goroutines to process messages; how many depends entirely on the type
	// of workload you have.
	for i := 0; i < 10; i++ {
		i := i
		go func() {
			for {
				msg := consumer.Consume()
				log.Info("[%d] got message: %s", i, msg)
			}
		}()
	}
}
