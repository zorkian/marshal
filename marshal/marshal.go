/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/optiopay/kafka"
)

const (
	// MarshalTopic is the main topic used for coordination. This must be constant across all
	// consumers that you want to coordinate.
	MarshalTopic = "__marshal"

	// HeartbeatInterval is the main timing used to determine how "chatty" the system is and how
	// fast it responds to failures of consumers. THIS VALUE MUST BE THE SAME BETWEEN ALL CONSUMERS
	// as it is critical to coordination.
	HeartbeatInterval = 60 // Measured in seconds.
)

// NewMarshaler connects to a cluster (given broker addresses) and prepares to handle marshalling
// requests. Given the way this system works, the marshaler has to process all messages in the
// topic before it's safely able to begin operating. This might take a while.
//
// TODO: This should possibly not return until it has actually finished processing and is set
// up? I.e., all rationalizers report ready.
func NewMarshaler(clientID, groupID string, brokers []string) (*Marshaler, error) {
	// TODO: It might be nice to make the marshaler agnostic of clients and able to support
	// requests from N clients/groups. For now, though, we require instantiating a new
	// marshaler for every client/group.
	brokerConf := kafka.NewBrokerConf("PortalMarshal")

	kfka, err := kafka.Dial(brokers, brokerConf)
	if err != nil {
		return nil, err
	}
	ws := &Marshaler{
		quit:     new(int32),
		clientID: clientID,
		groupID:  groupID,
		kafka:    kfka,
		producer: kfka.Producer(kafka.NewProducerConf()),
		topics:   make(map[string]int),
		groups:   make(map[string]map[string]*topicState),
	}

	// Do an initial metadata fetch, this will block a bit
	err = ws.refreshMetadata()
	if err != nil {
		return nil, fmt.Errorf("Failed to get metadata: %s", err)
	}

	// If there is no marshal topic, then we can't run. The admins must go create the topic
	// before they can use this library. Please see the README.
	marshalPartitions := ws.Partitions(MarshalTopic)
	if marshalPartitions == 0 {
		return nil, errors.New("Marshalling topic not found. Please see the documentation.")
	}

	// Now we start a goroutine to start consuming each of the partitions in the marshal
	// topic. Note that this doesn't handle increasing the partition count on that topic
	// without stopping all consumers.
	for id := 0; id < marshalPartitions; id++ {
		go ws.rationalize(id, ws.kafkaConsumerChannel(id))
	}

	// Now start the metadata refreshing goroutine
	go func() {
		for {
			time.Sleep(HeartbeatInterval * time.Second)
			if atomic.LoadInt32(ws.quit) == 1 {
				return
			}
			ws.refreshMetadata()
		}
	}()

	return ws, nil
}
