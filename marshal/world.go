/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

// Marshaler is the coordinator type. It is designed to be used once globally and
// is thread safe. Creating one of these will create connections to your Kafka
// cluster and begin actively monitoring the coordination topic.
type Marshaler struct {
	quit     *int32
	clientID string
	groupID  string

	lock   sync.RWMutex
	topics map[string]int
	groups map[string]map[string]*topicState

	kafka    *kafka.Broker
	producer kafka.Producer

	// This is for testing only. When this is non-zero, the rationalizer will answer
	// queries based on THIS time instead of the current, actual time.
	ts int64
}

// refreshMetadata is periodically used to update our internal state with topic information
// about the world.
func (w *Marshaler) refreshMetadata() error {
	md, err := w.kafka.Metadata()
	if err != nil {
		return err
	}

	newTopics := make(map[string]int)
	for _, topic := range md.Topics {
		newTopics[topic.Name] = len(topic.Partitions)
	}

	w.lock.Lock()
	defer w.lock.Unlock()
	w.topics = newTopics
	return nil
}

// getTopicState returns a topicState and possibly creates it and the partition state within
// the State.
func (w *Marshaler) getTopicState(topicName string, partID int) *topicState {
	w.lock.Lock()
	defer w.lock.Unlock()

	group, ok := w.groups[w.groupID]
	if !ok {
		group = make(map[string]*topicState)
		w.groups[w.groupID] = group
	}

	topic, ok := group[topicName]
	if !ok {
		topic = &topicState{
			partitions: make([]PartitionClaim, partID+1),
		}
		group[topicName] = topic
	}

	// They might be referring to a partition we don't know about, maybe extend it
	// TODO: This should have the topic lock
	if len(topic.partitions) < partID+1 {
		for i := len(topic.partitions); i <= partID; i++ {
			topic.partitions = append(topic.partitions, PartitionClaim{})
		}
	}

	return topic
}

// Topics returns the list of known topics.
func (w *Marshaler) Topics() []string {
	w.lock.RLock()
	defer w.lock.RUnlock()

	topics := make([]string, 0, len(w.topics))
	for topic := range w.topics {
		topics = append(topics, topic)
	}
	return topics
}

// Partitions returns the count of how many partitions are in a given topic. Returns 0 if a
// topic is unknown.
func (w *Marshaler) Partitions(topicName string) int {
	w.lock.RLock()
	defer w.lock.RUnlock()

	count, _ := w.topics[topicName]
	return count
}

// Terminate is called when we're done with the marshaler and want to shut down.
func (w *Marshaler) Terminate() {
	atomic.StoreInt32(w.quit, 1)
}

// IsClaimed returns the current status on whether or not a partition is claimed by any other
// consumer in our group (including ourselves). A topic/partition that does not exist is
// considered to be unclaimed.
func (w *Marshaler) IsClaimed(topicName string, partID int) bool {
	// The contract of this method is that if it returns something and the heartbeat is
	// non-zero, the partition is claimed.
	claim := w.GetPartitionClaim(topicName, partID)
	return claim.LastHeartbeat > 0
}

// GetPartitionClaim returns a PartitionClaim structure for a given partition. The structure
// describes the consumer that is currently claiming this partition. This is a copy of the
// claim structure, so changing it cannot change the world state.
func (w *Marshaler) GetPartitionClaim(topicName string, partID int) PartitionClaim {
	topic := w.getTopicState(topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	if topic.partitions[partID].isClaimed(w.ts) {
		return topic.partitions[partID] // copy.
	}
	return PartitionClaim{}
}

// GetLastPartitionClaim returns a PartitionClaim structure for a given partition. The structure
// describes the consumer that is currently or most recently claiming this partition. This is a
// copy of the claim structure, so changing it cannot change the world state.
func (w *Marshaler) GetLastPartitionClaim(topicName string, partID int) PartitionClaim {
	topic := w.getTopicState(topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	return topic.partitions[partID] // copy.
}

// GetPartitionOffsets returns the current state of a topic/partition. This has to hit Kafka
// twice to ask about a partition, but it returns the full state of information that can be
// used to calculate consumer lag.
func (w *Marshaler) GetPartitionOffsets(topicName string, partID int) (
	offsetEarliest, offsetLatest, offsetCurrent int64, err error) {

	offsetEarliest, err = w.kafka.OffsetEarliest(topicName, int32(partID))
	if err != nil {
		return 0, 0, 0, err
	}

	offsetLatest, err = w.kafka.OffsetLatest(topicName, int32(partID))
	if err != nil {
		return 0, 0, 0, err
	}

	// Use the last claim we know about, whatever it is
	claim := w.GetLastPartitionClaim(topicName, partID)
	return offsetEarliest, offsetLatest, claim.LastOffset, nil
}

// ClaimPartition is how you can actually claim a partition. If you call this, Marshal will
// attempt to claim the partition on your behalf. This is the low level function, you probably
// want to use a MarshaledConsumer. Returns a bool on whether or not the claim succeeded and
// whether you can continue.
func (w *Marshaler) ClaimPartition(topicName string, partID int) bool {
	topic := w.getTopicState(topicName, partID)

	// Unlock is later, since this function might take a while
	topic.lock.Lock()

	// If the topic is already claimed, we can short circuit the decision process
	if topic.partitions[partID].isClaimed(w.ts) {
		defer topic.lock.Unlock()
		if topic.partitions[partID].GroupID == w.groupID &&
			topic.partitions[partID].ClientID == w.clientID {
			return true
		}
		log.Warningf("Attempt to claim already claimed partition.")
		return false
	}

	// Make a channel for results, append it to the list so we hear about claims
	out := make(chan bool, 1)
	topic.partitions[partID].pendingClaims = append(
		topic.partitions[partID].pendingClaims, out)
	topic.lock.Unlock()

	// Produce message to kafka
	// TODO: Make this work on more than just partition 0. Hash by the topic/partition we're
	// trying to claim, or something...
	cl := &msgClaimingPartition{
		msgBase: msgBase{
			Time:     int(time.Now().Unix()),
			ClientID: w.clientID,
			GroupID:  w.groupID,
			Topic:    topicName,
			PartID:   partID,
		},
	}
	_, err := w.producer.Produce(MarshalTopic, 0,
		&proto.Message{Value: []byte(cl.Encode())})
	if err != nil {
		// If we failed to produce, this is probably serious so we should undo the work
		// we did and then return failure
		log.Errorf("Failed to produce to Kafka: %s", err)
		return false
	}

	// Finally wait and return the result. The rationalizer should see the above message
	// and know it was from us, and will be able to know if we won or not.
	return <-out
}

// Heartbeat will send an update for other people to know that we're still alive and
// still owning this partition. Returns an error if anything has gone wrong (at which
// point we can no longer assert we have the lock).
func (w *Marshaler) Heartbeat(topicName string, partID int, lastOffset int64) error {
	topic := w.getTopicState(topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	// If the topic is not claimed, we can short circuit the decision process
	if !topic.partitions[partID].isClaimed(w.ts) {
		return fmt.Errorf("Partition %s:%d is not claimed!", topicName, partID)
	}

	// And if it's not claimed by us...
	if topic.partitions[partID].GroupID != w.groupID ||
		topic.partitions[partID].ClientID != w.clientID {
		return fmt.Errorf("Partition %s:%d is not claimed by us!", topicName, partID)
	}

	// All good, let's heartbeat
	cl := &msgHeartbeat{
		msgBase: msgBase{
			Time:     int(time.Now().Unix()),
			ClientID: w.clientID,
			GroupID:  w.groupID,
			Topic:    topicName,
			PartID:   partID,
		},
		LastOffset: lastOffset,
	}
	// TODO: Use non-0 partition
	_, err := w.producer.Produce(MarshalTopic, 0,
		&proto.Message{Value: []byte(cl.Encode())})
	if err != nil {
		return fmt.Errorf("Failed to produce heartbeat to Kafka: %s", err)
	}

	return nil
}

// ReleasePartition will send an update for other people to know that we're done with
// a partition. Returns an error if anything has gone wrong (at which
// point we can no longer assert we have the lock).
func (w *Marshaler) ReleasePartition(topicName string, partID int, lastOffset int64) error {
	topic := w.getTopicState(topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	// If the topic is not claimed, we can short circuit the decision process
	if !topic.partitions[partID].isClaimed(w.ts) {
		return fmt.Errorf("Partition %s:%d is not claimed!", topicName, partID)
	}

	// And if it's not claimed by us...
	if topic.partitions[partID].GroupID != w.groupID ||
		topic.partitions[partID].ClientID != w.clientID {
		return fmt.Errorf("Partition %s:%d is not claimed by us!", topicName, partID)
	}

	// All good, let's release
	cl := &msgReleasingPartition{
		msgBase: msgBase{
			Time:     int(time.Now().Unix()),
			ClientID: w.clientID,
			GroupID:  w.groupID,
			Topic:    topicName,
			PartID:   partID,
		},
		LastOffset: lastOffset,
	}
	// TODO: Use non-0 partition
	_, err := w.producer.Produce(MarshalTopic, 0,
		&proto.Message{Value: []byte(cl.Encode())})
	if err != nil {
		return fmt.Errorf("Failed to produce release to Kafka: %s", err)
	}

	return nil
}
