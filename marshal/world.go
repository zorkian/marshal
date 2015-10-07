/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"crypto/md5"
	"encoding/binary"
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
	// These members are not protected by the lock and can be read at any
	// time as they're write-once or only ever atomically updated. They must
	// never be overwritten once a Marshaler is created.
	quit       *int32
	clientID   string
	groupID    string
	jitters    chan time.Duration
	kafka      *kafka.Broker
	offsets    kafka.OffsetCoordinator
	partitions int

	// Lock protects the following members; you must have this lock in order to
	// read from or write to these.
	lock     sync.RWMutex
	topics   map[string]int
	groups   map[string]map[string]*topicState
	producer kafka.Producer

	// This WaitGroup is used for signalling when all of the rationalizers have
	// finished processing.
	rationalizers sync.WaitGroup

	// rsteps is updated whenever a rationalizer processes a log entry, this is
	// used mainly by the test suite.
	rsteps *int32

	// This is for testing only. When this is non-zero, the rationalizer will answer
	// queries based on THIS time instead of the current, actual time.
	ts int64
}

// refreshMetadata is periodically used to update our internal state with topic information
// about the world.
func (m *Marshaler) refreshMetadata() error {
	md, err := m.kafka.Metadata()
	if err != nil {
		return err
	}

	newTopics := make(map[string]int)
	for _, topic := range md.Topics {
		newTopics[topic.Name] = len(topic.Partitions)
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	m.topics = newTopics
	return nil
}

// waitForRsteps is used by the test suite to ask the rationalizer to wait until some number
// of events have been processed. This also returns the current rsteps when it returns.
func (m *Marshaler) waitForRsteps(steps int) int {
	for {
		cval := atomic.LoadInt32(m.rsteps)
		if cval >= int32(steps) {
			return int(cval)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// getClaimPartition calculates which partition a topic should use for coordination. This uses
// a hashing function (non-cryptographic) to predictably partition the topic space.
func (m *Marshaler) getClaimPartition(topicName string) int {
	// We use MD5 because it's a fast and good hashing algorithm and we don't need cryptographic
	// properties. We then take the first 8 bytes and treat them as a uint64 and modulo that
	// across how many partitions we have.
	hash := md5.Sum([]byte(topicName))
	uval := binary.LittleEndian.Uint64(hash[0:8])
	return int(uval % uint64(m.partitions))
}

// getTopicState returns a topicState and possibly creates it and the partition state within
// the State.
func (m *Marshaler) getTopicState(topicName string, partID int) *topicState {
	m.lock.Lock()
	defer m.lock.Unlock()

	group, ok := m.groups[m.groupID]
	if !ok {
		group = make(map[string]*topicState)
		m.groups[m.groupID] = group
	}

	topic, ok := group[topicName]
	if !ok {
		topic = &topicState{
			claimPartition: m.getClaimPartition(topicName),
			partitions:     make([]PartitionClaim, partID+1),
		}
		group[topicName] = topic
	}

	// Take the topic lock if we can
	topic.lock.Lock()
	defer topic.lock.Unlock()

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
func (m *Marshaler) Topics() []string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	topics := make([]string, 0, len(m.topics))
	for topic := range m.topics {
		topics = append(topics, topic)
	}
	return topics
}

// Partitions returns the count of how many partitions are in a given topic. Returns 0 if a
// topic is unknown.
func (m *Marshaler) Partitions(topicName string) int {
	m.lock.RLock()
	defer m.lock.RUnlock()

	count, _ := m.topics[topicName]
	return count
}

// Terminate is called when we're done with the marshaler and want to shut down.
func (m *Marshaler) Terminate() {
	atomic.StoreInt32(m.quit, 1)
}

// IsClaimed returns the current status on whether or not a partition is claimed by any other
// consumer in our group (including ourselves). A topic/partition that does not exist is
// considered to be unclaimed.
func (m *Marshaler) IsClaimed(topicName string, partID int) bool {
	// The contract of this method is that if it returns something and the heartbeat is
	// non-zero, the partition is claimed.
	claim := m.GetPartitionClaim(topicName, partID)
	return claim.LastHeartbeat > 0
}

// GetPartitionClaim returns a PartitionClaim structure for a given partition. The structure
// describes the consumer that is currently claiming this partition. This is a copy of the
// claim structure, so changing it cannot change the world state.
func (m *Marshaler) GetPartitionClaim(topicName string, partID int) PartitionClaim {
	topic := m.getTopicState(topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	if topic.partitions[partID].isClaimed(m.ts) {
		return topic.partitions[partID] // copy.
	}
	return PartitionClaim{}
}

// GetLastPartitionClaim returns a PartitionClaim structure for a given partition. The structure
// describes the consumer that is currently or most recently claiming this partition. This is a
// copy of the claim structure, so changing it cannot change the world state.
func (m *Marshaler) GetLastPartitionClaim(topicName string, partID int) PartitionClaim {
	topic := m.getTopicState(topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	return topic.partitions[partID] // copy.
}

// GetPartitionOffsets returns the current state of a topic/partition. This has to hit Kafka
// thrice to ask about a partition, but it returns the full state of information that can be
// used to calculate consumer lag.
func (m *Marshaler) GetPartitionOffsets(topicName string, partID int) (PartitionOffsets, error) {
	var err error

	o := PartitionOffsets{}
	o.Earliest, err = m.kafka.OffsetEarliest(topicName, int32(partID))
	if err != nil {
		return PartitionOffsets{}, err
	}

	o.Latest, err = m.kafka.OffsetLatest(topicName, int32(partID))
	if err != nil {
		return PartitionOffsets{}, err
	}

	o.Committed, _, err = m.offsets.Offset(topicName, int32(partID))
	if err != nil {
		return PartitionOffsets{}, err
	}

	// Use the last claim we know about, whatever it is
	claim := m.GetLastPartitionClaim(topicName, partID)
	o.Current = claim.LastOffset
	return o, nil
}

// ClaimPartition is how you can actually claim a partition. If you call this, Marshal will
// attempt to claim the partition on your behalf. This is the low level function, you probably
// want to use a MarshaledConsumer. Returns a bool on whether or not the claim succeeded and
// whether you can continue.
func (m *Marshaler) ClaimPartition(topicName string, partID int) bool {
	topic := m.getTopicState(topicName, partID)

	// Unlock is later, since this function might take a while
	// TODO: Move this logic to a func and defer the lock (for sanity sake)
	topic.lock.Lock()

	// If the topic is already claimed, we can short circuit the decision process
	if topic.partitions[partID].isClaimed(m.ts) {
		defer topic.lock.Unlock()
		if topic.partitions[partID].GroupID == m.groupID &&
			topic.partitions[partID].ClientID == m.clientID {
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
			ClientID: m.clientID,
			GroupID:  m.groupID,
			Topic:    topicName,
			PartID:   partID,
		},
	}
	_, err := m.producer.Produce(MarshalTopic, int32(topic.claimPartition),
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
func (m *Marshaler) Heartbeat(topicName string, partID int, lastOffset int64) error {
	topic := m.getTopicState(topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	// If the topic is not claimed, we can short circuit the decision process
	if !topic.partitions[partID].isClaimed(m.ts) {
		return fmt.Errorf("Partition %s:%d is not claimed!", topicName, partID)
	}

	// And if it's not claimed by us...
	if topic.partitions[partID].GroupID != m.groupID ||
		topic.partitions[partID].ClientID != m.clientID {
		return fmt.Errorf("Partition %s:%d is not claimed by us!", topicName, partID)
	}

	// All good, let's heartbeat
	cl := &msgHeartbeat{
		msgBase: msgBase{
			Time:     int(time.Now().Unix()),
			ClientID: m.clientID,
			GroupID:  m.groupID,
			Topic:    topicName,
			PartID:   partID,
		},
		LastOffset: lastOffset,
	}
	_, err := m.producer.Produce(MarshalTopic, int32(topic.claimPartition),
		&proto.Message{Value: []byte(cl.Encode())})
	if err != nil {
		return fmt.Errorf("Failed to produce heartbeat to Kafka: %s", err)
	}

	// Finally, also commit this offset to Kafka so it's available in the long-term storage
	// of the offset coordination system
	err = m.offsets.Commit(topicName, int32(partID), lastOffset)
	if err != nil {
		// Do not count this as a returned error as that will cause us to drop consumption, but
		// do log it so people can see it
		log.Errorf("%s:%d failed to commit offsets: %s", topicName, partID, err)
	}

	return nil
}

// ReleasePartition will send an update for other people to know that we're done with
// a partition. Returns an error if anything has gone wrong (at which
// point we can no longer assert we have the lock).
func (m *Marshaler) ReleasePartition(topicName string, partID int, lastOffset int64) error {
	topic := m.getTopicState(topicName, partID)

	topic.lock.RLock()
	defer topic.lock.RUnlock()

	// If the topic is not claimed, we can short circuit the decision process
	if !topic.partitions[partID].isClaimed(m.ts) {
		return fmt.Errorf("Partition %s:%d is not claimed!", topicName, partID)
	}

	// And if it's not claimed by us...
	if topic.partitions[partID].GroupID != m.groupID ||
		topic.partitions[partID].ClientID != m.clientID {
		return fmt.Errorf("Partition %s:%d is not claimed by us!", topicName, partID)
	}

	// All good, let's release
	cl := &msgReleasingPartition{
		msgBase: msgBase{
			Time:     int(time.Now().Unix()),
			ClientID: m.clientID,
			GroupID:  m.groupID,
			Topic:    topicName,
			PartID:   partID,
		},
		LastOffset: lastOffset,
	}
	_, err := m.producer.Produce(MarshalTopic, int32(topic.claimPartition),
		&proto.Message{Value: []byte(cl.Encode())})
	if err != nil {
		return fmt.Errorf("Failed to produce release to Kafka: %s", err)
	}

	// Finally, also commit this offset to Kafka so it's available in the long-term storage
	// of the offset coordination system
	err = m.offsets.Commit(topicName, int32(partID), lastOffset)
	if err != nil {
		// Do not count this as a returned error as that will cause us to drop consumption, but
		// do log it so people can see it
		log.Errorf("%s:%d failed to commit offsets: %s", topicName, partID, err)
	}

	return nil
}

// ClientID returns the client ID we're using
func (m *Marshaler) ClientID() string {
	return m.clientID
}

// GroupID returns the group ID we're using
func (m *Marshaler) GroupID() string {
	return m.groupID
}
