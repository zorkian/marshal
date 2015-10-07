/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

// claim is instantiated for each partition "claim" we have. This type is responsible for
// pulling data from Kafka and managing its cursors, heartbeating as necessary, and health
// checking itself.
type claim struct {
	// These items are read-only. They are never changed after the object is created,
	// so access to these may be done without the lock.
	topic  string
	partID int

	// lock protects all access to the member variables of this struct except for the
	// messages channel, which can be read from or written to without holding the lock.
	lock          sync.RWMutex
	offsets       PartitionOffsets
	marshal       *Marshaler
	rand          *rand.Rand
	claimed       *int32
	lastHeartbeat int64
	consumer      kafka.Consumer
	messages      chan *proto.Message

	// Number of heartbeat cycles this claim has been lagging, i.e., consumption is going
	// too slowly (defined as being behind by more than 2 heartbeat cycles)
	cyclesBehind int

	// History arrays used for calculating average velocity for health checking.
	offsetCurrentHistory [10]int64
	offsetLatestHistory  [10]int64
}

// newClaim returns an internal claim object, used by the consumer to manage the
// claim of a single partition.
func newClaim(topic string, partID int, marshal *Marshaler) *claim {
	// Get all available offset information
	offsets, err := marshal.GetPartitionOffsets(topic, partID)
	if err != nil {
		log.Errorf("%s:%d failed to get offsets: %s", topic, partID, err)
		return nil
	}
	log.Debugf("%s:%d consumer offsets: early = %d, cur/comm = %d/%d, late = %d",
		topic, partID, offsets.Earliest, offsets.Current, offsets.Committed, offsets.Latest)

	// Take the greatest of the committed/current offset, this means we will recover from
	// a situation where the last time this partition was claimed has fallen out of our
	// coordination memory.
	if offsets.Committed > 0 && offsets.Committed > offsets.Current {
		if offsets.Current > 0 {
			// This state shouldn't really happen. This implies that someone went back in time
			// and started consuming earlier than the committed offset.
			log.Warningf("%s:%d committed offset is %d but heartbeat offset was %d",
				topic, partID, offsets.Committed, offsets.Current)
		} else {
			log.Infof("%s:%d recovering committed offset of %d",
				topic, partID, offsets.Committed)
		}
		offsets.Current = offsets.Committed
	}

	// Construct object and set it up
	obj := &claim{
		marshal:  marshal,
		topic:    topic,
		partID:   partID,
		claimed:  new(int32),
		offsets:  offsets,
		messages: make(chan *proto.Message, 100),
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	atomic.StoreInt32(obj.claimed, 1)

	// Now try to actually claim it, this can block a while
	log.Infof("%s:%d consumer attempting to claim", topic, partID)
	if !marshal.ClaimPartition(topic, partID) {
		log.Infof("%s:%d consumer failed to claim", topic, partID)
		return nil
	}

	// If that worked, kick off the main setup loop and return
	obj.setup()
	return obj
}

// updateOffsets polls Kafka periodically to get information about the partition's
// state.
func (c *claim) updateOffsetsLoop() {
	ctr := 0
	time.Sleep(<-c.marshal.jitters)
	for c.Claimed() {
		c.updateOffsets(ctr)
		ctr++
		time.Sleep(<-c.marshal.jitters)
	}
	log.Debugf("%s:%d no longer claimed, offset loop exiting", c.topic, c.partID)
}

// setup is the initial worker that initializes the claim structure. Until this is done,
// our internal state is inconsistent.
func (c *claim) setup() {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Of course, if the current offset is greater than the earliest, we must reset
	// to the earliest known
	if c.offsets.Current < c.offsets.Earliest {
		log.Warningf("%s:%d consumer fast-forwarding from %d to %d",
			c.topic, c.partID, c.offsets.Current, c.offsets.Earliest)
		c.offsets.Current = c.offsets.Earliest
	}

	// Since it's claimed, we now want to heartbeat with the last seen offset
	err := c.marshal.Heartbeat(c.topic, c.partID, c.offsets.Current)
	if err != nil {
		log.Errorf("%s:%d consumer failed to heartbeat: %s", c.topic, c.partID, err)
		atomic.StoreInt32(c.claimed, 0)
		return
	}
	c.lastHeartbeat = time.Now().Unix()

	// Set up Kafka consumer
	consumerConf := kafka.NewConsumerConf(c.topic, int32(c.partID))
	consumerConf.StartOffset = c.offsets.Current
	kafkaConsumer, err := c.marshal.kafka.Consumer(consumerConf)
	if err != nil {
		log.Errorf("%s:%d consumer failed to create Kafka Consumer: %s",
			c.topic, c.partID, err)
		// TODO: There is an optimization here where we could release the partition.
		// As it stands, we're not doing anything,
		atomic.StoreInt32(c.claimed, 0)
		return
	}
	c.consumer = kafkaConsumer

	// Start our maintenance goroutines that keep this system healthy
	go c.updateOffsetsLoop()
	go c.healthCheckLoop()
	go c.messagePump()

	// Totally done, let the world know and move on
	log.Infof("%s:%d consumer claimed at offset %d (is %d behind)",
		c.topic, c.partID, c.offsets.Current, c.offsets.Latest-c.offsets.Current)
}

// Given a message offset (which should be our current offset), return whether or
// not that offset is allowed to be consumed (i.e., we're still claimed)
func (c *claim) Consumed(offset int64) bool {
	if atomic.LoadInt32(c.claimed) != 1 {
		return false
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// If our heartbeat has gone at all stale, don't allow consumption from this
	// partition. In fact, trigger an immediate release without waiting for the
	// healthcheck loop.
	if c.lastHeartbeat < time.Now().Unix()-HeartbeatInterval {
		log.Errorf("%s:%d has gone stale during consumption",
			c.topic, c.partID)
		go c.Release()
		return false
	}

	c.offsets.Current = offset + 1
	return true
}

// Claimed returns whether or not this claim structure is alive and well and believes
// that it is still an active claim.
func (c *claim) Claimed() bool {
	return atomic.LoadInt32(c.claimed) == 1
}

// GetCurrentLag returns this partition's cursor lag.
func (c *claim) GetCurrentLag() int64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.offsets.Current < c.offsets.Latest {
		return c.offsets.Latest - c.offsets.Current
	}
	return 0
}

// Release will invoke our release mechanism if and only if we are still claimed.
func (c *claim) Release() bool {
	if !atomic.CompareAndSwapInt32(c.claimed, 1, 0) {
		return false
	}

	// Holds the lock through a Kafka transaction, but since we're releasing I think this
	// is reasonable. Held because of using offsetCurrent below.
	c.lock.RLock()
	defer c.lock.RUnlock()

	log.Infof("%s:%d releasing partition claim", c.topic, c.partID)
	err := c.marshal.ReleasePartition(c.topic, c.partID, c.offsets.Current)
	if err != nil {
		log.Errorf("%s:%d failed to release: %s", c.topic, c.partID, err)
		return false
	}
	return true
}

// messagePump continuously pulls message from Kafka for this partition and makes them
// available for consumption.
func (c *claim) messagePump() {
	// This method MUST NOT make changes to the claim structure. Since we might
	// be running while someone else has the lock, and we can't get it ourselves, we are
	// forbidden to touch anything other than the consumer and the message channel.
	for c.Claimed() {
		msg, err := c.consumer.Consume()
		if err == proto.ErrOffsetOutOfRange {
			// Fell out of range, presumably because we're handling this too slow, so
			// let's abandon this consumer
			log.Warningf("%s:%d error consuming: out of range, abandoning partition",
				c.topic, c.partID)
			go c.Release()
			return
		}
		if err != nil {
			log.Errorf("%s:%d error consuming: %s", c.topic, c.partID, err)

			// Often a consumption error is caused by data going away, such as if we're consuming
			// from the head and Kafka has deleted the data. In that case we need to wait for
			// the next offset update, so let's not go crazy
			time.Sleep(1 * time.Second)
			continue
		}

		c.messages <- msg
	}
	log.Debugf("%s:%d no longer claimed, pump exiting", c.topic, c.partID)
}

// heartbeat is the internal "send a heartbeat" function. Calling this will immediately
// send a heartbeat to Kafka. If we fail to send a heartbeat, we will release the
// partition.
func (c *claim) heartbeat() bool {
	// Unclaimed partitions don't heartbeat.
	if atomic.LoadInt32(c.claimed) != 1 {
		return false
	}

	// TODO: This holds a lock around a Kafka transaction do we really want that?
	// Won't this block consumption pretty hard?
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.marshal.Heartbeat(c.topic, c.partID, c.offsets.Current)
	if err != nil {
		log.Errorf("%s:%d failed to heartbeat, releasing", c.topic, c.partID)
		go c.Release()
	}
	c.lastHeartbeat = time.Now().Unix()
	return true
}

// healthCheck performs a single health check against the claim. If we have failed
// too many times, this will also start a partition release. Returns true if the
// partition is healthy, else false.
func (c *claim) healthCheck() bool {
	// Unclaimed partitions aren't healthy.
	if atomic.LoadInt32(c.claimed) != 1 {
		return false
	}

	// Get velocities; these functions both use the locks so we have to do this before
	// we personally take the lock (to avoid deadlock)
	consumerVelocity := c.ConsumerVelocity()
	partitionVelocity := c.PartitionVelocity()

	c.lock.Lock()
	defer c.lock.Unlock()

	// If our heartbeat is expired, we are definitely unhealthy... don't even bother
	// with checking velocity
	if c.lastHeartbeat < time.Now().Unix()-HeartbeatInterval {
		log.Warningf("%s:%d consumer unhealthy by heartbeat test, releasing",
			c.topic, c.partID)
		go c.Release()
		return false
	}

	// If current has gone forward of the latest (which is possible, but unlikely)
	// then we are by definition caught up
	if c.offsets.Current >= c.offsets.Latest {
		c.cyclesBehind = 0
		return true
	}

	// If velocity is good, reset cycles behind and exit
	if partitionVelocity <= consumerVelocity {
		c.cyclesBehind = 0
		return true
	}

	// Unhealthy, so increase the cycle count so we know when it's been unhealthy for
	// too long and we want to give it up
	c.cyclesBehind++

	// If were behind by too many cycles, then we should try to release the
	// partition. If so, do this in a goroutine since it will involve calling out
	// to Kafka and releasing the partition.
	if c.cyclesBehind >= 3 {
		log.Warningf("%s:%d consumer unhealthy, releasing",
			c.topic, c.partID)
		go c.Release()
		return false
	}

	// Clearly we haven't been behind for long enough, so we're still "healthy"
	log.Warningf("%s:%d consumer unhealthy: CV %0.2f < PV %0.2f (warning #%d)",
		c.topic, c.partID, consumerVelocity, partitionVelocity, c.cyclesBehind)
	return true
}

// healthCheckLoop runs regularly and will perform a health check. Exits when we are no longer
// a claimed partition
func (c *claim) healthCheckLoop() {
	time.Sleep(<-c.marshal.jitters)
	for c.Claimed() {
		if c.healthCheck() {
			go c.heartbeat()
		}
		time.Sleep(<-c.marshal.jitters)
	}
	log.Debugf("%s:%d health check loop exiting", c.topic, c.partID)
}

// average returns the average of a given slice of int64s. It ignores 0s as
// those are "uninitialized" elements.
func average(vals []int64) float64 {
	min, max, ct := int64(0), int64(0), int64(0)
	for _, val := range vals {
		if val <= 0 {
			continue
		}
		if min == 0 || val < min {
			min = val
		}
		if max == 0 || val > max {
			max = val
		}
		ct++
	}
	if ct == 0 {
		return 0
	}
	return float64(max-min) / float64(ct-1)
}

// ConsumerVelocity returns the average of our consumers' velocity
func (c *claim) ConsumerVelocity() float64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return average(c.offsetCurrentHistory[0:])
}

// PartitionVelocity returns the average of the partition's velocity
func (c *claim) PartitionVelocity() float64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return average(c.offsetLatestHistory[0:])
}

// updateOffsets will update the offsets of our current partition.
func (c *claim) updateOffsets(ctr int) error {
	// Slow, hits Kafka. Run in a goroutine.
	offsets, err := c.marshal.GetPartitionOffsets(c.topic, c.partID)
	if err != nil {
		log.Errorf("%s:%d failed to get offsets: %s", c.topic, c.partID, err)
		return err
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Update the earliest/latest offsets that are presently available within the
	// partition
	c.offsets.Earliest = offsets.Earliest
	c.offsets.Latest = offsets.Latest

	// Do update our "history" values, this is used for calculating moving averages
	// in the health checking function
	c.offsetLatestHistory[ctr%10] = offsets.Latest
	c.offsetCurrentHistory[ctr%10] = c.offsets.Current

	return nil
}
