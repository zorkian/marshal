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
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka"
)

// KafkaCluster is a user-agnostic view of the world. It connects to a Kafka cluster
// and runs rationalizers to observe the complete world state.
type KafkaCluster struct {
	// These members are not protected by the lock and can be read at any
	// time as they're write-once or only ever atomically updated. They must
	// never be overwritten once a KafkaCluster is created.
	quit       *int32
	name       string
	broker     *kafka.Broker
	producer   kafka.Producer
	partitions int
	jitters    chan time.Duration

	// Lock protects the following members; you must have this lock in order to
	// read from or write to these.
	lock       sync.RWMutex
	marshalers []*Marshaler
	topics     map[string]int
	groups     map[string]map[string]*topicState

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

// Dial returns a new cluster object which can be used to instantiate a number of Marshalers
// that all use the same cluster.
func Dial(name string, brokers []string) (*KafkaCluster, error) {
	brokerConf := kafka.NewBrokerConf("PortalMarshal")
	broker, err := kafka.Dial(brokers, brokerConf)
	if err != nil {
		return nil, err
	}

	c := &KafkaCluster{
		quit:     new(int32),
		rsteps:   new(int32),
		name:     name,
		broker:   broker,
		producer: broker.Producer(kafka.NewProducerConf()),
		topics:   make(map[string]int),
		groups:   make(map[string]map[string]*topicState),
		jitters:  make(chan time.Duration, 100),
	}

	// Do an initial metadata fetch, this will block a bit
	err = c.refreshMetadata()
	if err != nil {
		return nil, fmt.Errorf("Failed to get metadata: %s", err)
	}

	// If there is no marshal topic, then we can't run. The admins must go create the topic
	// before they can use this library. Please see the README.
	c.partitions = c.getTopicPartitions(MarshalTopic)
	if c.partitions == 0 {
		return nil, errors.New("Marshalling topic not found. Please see the documentation.")
	}

	// Now we start a goroutine to start consuming each of the partitions in the marshal
	// topic. Note that this doesn't handle increasing the partition count on that topic
	// without stopping all consumers.
	c.rationalizers.Add(c.partitions)
	for id := 0; id < c.partitions; id++ {
		go c.rationalize(id, c.kafkaConsumerChannel(id))
	}

	// A jitter calculator, just fills a channel with random numbers so that other
	// people don't have to build their own random generator...
	go func() {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			jitter := rnd.Intn(HeartbeatInterval/2) + (HeartbeatInterval / 2)
			c.jitters <- time.Duration(jitter) * time.Second
		}
	}()

	// Now start the metadata refreshing goroutine
	go func() {
		for !c.Terminated() {
			time.Sleep(<-c.jitters)
			log.Infof("[%s] Refreshing topic metadata.", c.name)
			c.refreshMetadata()

			// See if the number of partitions in the marshal topic changed. This is bad if
			// it happens, since it means we can no longer coordinate correctly.
			if c.getTopicPartitions(MarshalTopic) != c.partitions {
				log.Errorf("[%s] Marshal topic partition count changed. Terminating!", c.name)
				c.Terminate()
			}
		}
	}()

	// Wait for all rationalizers to come alive
	log.Infof("[%s] Waiting for all rationalizers to come alive.", c.name)
	c.rationalizers.Wait()
	log.Infof("[%s] All rationalizers alive, KafkaCluster now alive.", c.name)

	return c, nil
}

// NewMarshaler creates a Marshaler off of an existing cluster. This is more efficient
// if you're creating multiple instances, since they can share the same underlying cluster.
func (c *KafkaCluster) NewMarshaler(clientID, groupID string) (*Marshaler, error) {
	if c.Terminated() {
		return nil, errors.New("Cluster is terminated.")
	}

	// Get offset coordinator so we can look up (and save) committed offsets later.
	coordinator, err := c.getOffsetCoordinator(groupID)
	if err != nil {
		return nil, err
	}

	m := &Marshaler{
		quit:       new(int32),
		cluster:    c,
		instanceID: newInstanceID(),
		clientID:   clientID,
		groupID:    groupID,
		offsets:    coordinator,
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove any dead marshalers from our slice and add the new one
	filtered := make([]*Marshaler, 0)
	for _, marshaler := range c.marshalers {
		if !marshaler.Terminated() {
			filtered = append(filtered, marshaler)
		}
	}
	filtered = append(filtered, m)
	c.marshalers = filtered

	return m, nil
}

// refreshMetadata is periodically used to update our internal state with topic information
// about the world.
func (c *KafkaCluster) refreshMetadata() error {
	md, err := c.broker.Metadata()
	if err != nil {
		return err
	}

	newTopics := make(map[string]int)
	for _, topic := range md.Topics {
		newTopics[topic.Name] = len(topic.Partitions)
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.topics = newTopics
	return nil
}

// getOffsetCoordinator returns a kafka.OffsetCoordinator for a specific group.
func (c *KafkaCluster) getOffsetCoordinator(groupID string) (kafka.OffsetCoordinator, error) {
	return c.broker.OffsetCoordinator(
		kafka.NewOffsetCoordinatorConf(groupID))
}

// getClaimPartition calculates which partition a topic should use for coordination. This uses
// a hashing function (non-cryptographic) to predictably partition the topic space.
func (c *KafkaCluster) getClaimPartition(topicName string) int {
	// We use MD5 because it's a fast and good hashing algorithm and we don't need cryptographic
	// properties. We then take the first 8 bytes and treat them as a uint64 and modulo that
	// across how many partitions we have.
	hash := md5.Sum([]byte(topicName))
	uval := binary.LittleEndian.Uint64(hash[0:8])
	return int(uval % uint64(c.partitions))
}

// getPartitionState returns a topicState and possibly creates it and the partition state within
// the State.
func (c *KafkaCluster) getPartitionState(groupID, topicName string, partID int) *topicState {
	c.lock.Lock()
	defer c.lock.Unlock()

	group, ok := c.groups[groupID]
	if !ok {
		group = make(map[string]*topicState)
		c.groups[groupID] = group
	}

	topic, ok := group[topicName]
	if !ok {
		topic = &topicState{
			claimPartition: c.getClaimPartition(topicName),
			partitions:     make([]PartitionClaim, partID+1),
		}
		group[topicName] = topic
	}

	// Take the topic lock if we can
	topic.lock.Lock()
	defer topic.lock.Unlock()

	// They might be referring to a partition we don't know about, maybe extend it
	if len(topic.partitions) < partID+1 {
		for i := len(topic.partitions); i <= partID; i++ {
			topic.partitions = append(topic.partitions, PartitionClaim{})
		}
	}

	return topic
}

// getTopics returns the list of known topics.
func (c *KafkaCluster) getTopics() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	topics := make([]string, 0, len(c.topics))
	for topic := range c.topics {
		topics = append(topics, topic)
	}
	return topics
}

// getTopicPartitions returns the count of how many partitions are in a given topic. Returns 0 if a
// topic is unknown.
func (c *KafkaCluster) getTopicPartitions(topicName string) int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	count, _ := c.topics[topicName]
	return count
}

// waitForRsteps is used by the test suite to ask the rationalizer to wait until some number
// of events have been processed. This also returns the current rsteps when it returns.
func (c *KafkaCluster) waitForRsteps(steps int) int {
	for {
		cval := atomic.LoadInt32(c.rsteps)
		if cval >= int32(steps) {
			return int(cval)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Terminate is called when we're done with the marshaler and want to shut down.
func (c *KafkaCluster) Terminate() {
	if !atomic.CompareAndSwapInt32(c.quit, 0, 1) {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Terminate all Marshalers which will in turn terminate all Consumers and
	// let everybody know we're all done.
	for _, marshaler := range c.marshalers {
		marshaler.Terminate()
	}
	c.marshalers = nil

	// Close the broker!
	c.broker.Close()
}

// Terminated returns whether or not we have been terminated.
func (c *KafkaCluster) Terminated() bool {
	return atomic.LoadInt32(c.quit) == 1
}
