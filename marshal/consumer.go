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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/optiopay/kafka/proto"
)

// ConsumerBehavior is the broad category of behaviors that encapsulate how the Consumer
// will handle claiming/releasing partitions.
// TODO: Turn this into a consumer options struct.
type ConsumerOptions struct {
	// FastReclaim instructs the consumer to attempt to reclaim any partitions
	// that are presently claimed by the ClientID/GroupID we have. This is useful
	// for situations where your ClientID is predictable/stable and you want to
	// minimize churn during restarts. This is dangerous if you have two copies
	// of your application running with the same ClientID/GroupID.
	// TODO: Create an instance ID for Marshaler such that we can detect when
	// someone else has decided to use our Client/Group.
	FastReclaim bool

	// GreedyClaims indicates whether we should attempt to claim all unclaimed
	// partitions on start. This is appropriate in low QPS type environments.
	// Defaults to false/off.
	GreedyClaims bool
}

// Consumer allows you to safely consume data from a given topic in such a way that you
// don't need to worry about partitions and can safely split the load across as many
// processes as might be consuming from this topic. However, you should ONLY create one
// Consumer per topic in your application!
type Consumer struct {
	alive      *int32
	marshal    *Marshaler
	topic      string
	partitions int
	rand       *rand.Rand
	options    ConsumerOptions

	// claims maps partition IDs to claim structures. The lock protects read/write
	// access to this map.
	lock   sync.RWMutex
	claims map[int]*claim
}

// NewConsumer instantiates a consumer object for a given topic. You must create a
// separate consumer for every individual topic that you want to consume from. Please
// see the documentation on ConsumerBehavior.
func NewConsumer(marshal *Marshaler, topicName string,
	options ConsumerOptions) (*Consumer, error) {

	if marshal == nil {
		return nil, errors.New("Must provide a marshaler")
	}

	consumer := &Consumer{
		alive:      new(int32),
		marshal:    marshal,
		topic:      topicName,
		partitions: marshal.Partitions(topicName),
		options:    options,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		claims:     make(map[int]*claim),
	}
	atomic.StoreInt32(consumer.alive, 1)

	go consumer.manageClaims()

	return consumer, nil
}

// NewConsumerOptions returns a default set of options for the Consumer.
func NewConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		FastReclaim:  true,
		GreedyClaims: false,
	}
}

// tryClaimPartition attempts to claim a partition and make it available in the consumption
// flow. If this is called a second time on a partition we already own, it will return
// false. Returns true only if the partition was never claimed and we succeeded in
// claiming it.
func (c *Consumer) tryClaimPartition(partID int) bool {
	// Partition unclaimed by us, see if it's claimed by anybody
	currentClaim := c.marshal.GetPartitionClaim(c.topic, partID)
	if currentClaim.LastHeartbeat > 0 {
		return false
	}

	// Set up internal claim structure we'll track things in, this can block for a while
	// as it talks to Kafka and waits for rationalizers.
	newclaim := newClaim(c.topic, partID, c.marshal)
	if newclaim == nil {
		return false
	}

	// Critical section. Engage the lock here, we hold it until we exit.
	c.lock.Lock()
	defer c.lock.Unlock()

	// Ugh, we managed to claim a partition in our termination state. Don't worry too hard
	// and just release it.
	if c.Terminated() {
		// This can be a long blocking operation so send it to the background. We ultimately
		// don't care if it finishes or not, because the heartbeat will save us if we don't
		// submit a release message. This is just an optimization.
		go func() {
			newclaim.Release()
		}()
		return false
	}

	// Ensure we don't have another valid claim in this slot. This shouldn't happen and if
	// it does we treat it as fatal.
	oldClaim, ok := c.claims[partID]
	if ok && oldClaim != nil {
		if oldClaim.Claimed() {
			log.Fatalf("Internal double-claim for %s:%d.", c.topic, partID)
		}
	}

	// Save the claim, this makes it available for message consumption and status.
	c.claims[partID] = newclaim
	return true
}

// claimPartitions actually attempts to claim partitions. If the current consumer is
// set on aggressive, this will try to claim ALL partitions that are free. Balanced mode
// will claim a single partition.
func (c *Consumer) claimPartitions() {
	offset := rand.Intn(c.partitions)
	for i := 0; i < c.partitions; i++ {
		partID := (i + offset) % c.partitions

		// Get the most recent claim for this partition
		lastClaim := c.marshal.GetLastPartitionClaim(c.topic, partID)
		if lastClaim.isClaimed(time.Now().Unix()) {
			continue
		}

		// If the last claim was by this particular consumer, skip it; this is because
		// we might have become unhealthy and dropped it or we might already be
		// claiming this partition
		if lastClaim.GroupID == c.marshal.groupID &&
			lastClaim.ClientID == c.marshal.clientID {
			continue
		}

		// Unclaimed, so attempt to claim it
		if !c.tryClaimPartition(partID) {
			continue
		}

		// If greedy claims is disabled, finish here
		if !c.options.GreedyClaims {
			break
		}
	}
}

// manageClaims is our internal state machine that handles partitions and claiming new
// ones (or releasing ones).
func (c *Consumer) manageClaims() {
	for !c.Terminated() {
		// Attempt to claim more partitions, this always runs and will keep running until all
		// partitions in the topic are claimed (by somebody).
		c.claimPartitions()

		// Now sleep a bit so we don't pound things
		// TODO: Raise this later, we shouldn't attempt to claim this fast, this is just for
		// development.
		time.Sleep(time.Duration(rand.Intn(3000)) * time.Millisecond)
	}
}

// Terminated returns whether or not this consumer has been terminated.
func (c *Consumer) Terminated() bool {
	return atomic.LoadInt32(c.alive) == 0
}

// Terminate instructs the consumer to release its locks. This will allow other consumers
// to begin consuming. (If you do not call this method before exiting, things will still
// work, but more slowly.)
func (c *Consumer) Terminate() bool {
	if !atomic.CompareAndSwapInt32(c.alive, 1, 0) {
		return false
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	for _, claim := range c.claims {
		if claim != nil {
			claim.Release()
		}
	}
	return true
}

// GetCurrentLag returns the number of messages that this consumer is lagging by. Note that
// this value can be unstable in the beginning of a run, as we might not have claimed all of
// partitions we will end up claiming, or we might have overclaimed and need to back off.
// Ideally this will settle towards 0. If it continues to rise, that implies there isn't
// enough consumer capacity.
func (c *Consumer) GetCurrentLag() int64 {
	c.lock.RLock()
	defer c.lock.RUnlock()

	var lag int64
	for _, cl := range c.claims {
		if cl.Claimed() {
			lag += cl.GetCurrentLag()
		}
	}
	return lag
}

// GetCurrentLoad returns a number representing the "load" of this consumer. Think of this
// like a load average in Unix systems: the numbers are kind of related to how much work
// the system is doing, but by itself they don't tell you much.
func (c *Consumer) GetCurrentLoad() int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	ct := 0
	for _, cl := range c.claims {
		if cl.Claimed() {
			ct++
		}
	}
	return ct
}

// Consume returns the next available message from the topic. If no messages are available,
// it will block until one is.
func (c *Consumer) Consume() []byte {
	// TODO: This is almost certainly a slow implementation as we have to scan everything
	// every time.
	for {
		var cl *claim
		var msg *proto.Message

		// TODO: This implementation also can lead to queue starvation since we start at the
		// front every time. This might be OK, since we'll still consume as many messages
		// as we can and the unconsumed ones will get released.
		// TODO: Rethink this locking.
		c.lock.RLock()
		for _, trycl := range c.claims {
			select {
			case msg = <-trycl.messages:
				cl = trycl
			default:
				// Do nothing.
			}
			if msg != nil {
				break
			}
		}
		c.lock.RUnlock()

		// TODO: This is braindead.
		if msg == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Inform the claimholder that we've consumed this message offset so that it
		// can update its internal structures.
		if cl.Consumed(msg.Offset) {
			return msg.Value
		}

		// If we get here, then we retrieved a message for a partition that is not
		// presently claimed, so let's just pretend we didn't and continue back to the top
		// for the next message
	}
}
