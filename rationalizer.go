/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"sync/atomic"
	"time"

	"github.com/optiopay/kafka"
)

// kafkaConsumerChannel creates a consumer that continuously attempts to consume messages from
// Kafka for the given partition.
func (w *Marshaler) kafkaConsumerChannel(partID int) <-chan message {
	out := make(chan message, 1000)
	go func() {
		// TODO: Technically we don't have to start at the beginning, we just need to start back
		// a couple heartbeat intervals to get a full state of the world. But this is easiest
		// for right now...
		// TODO: Think about the above. Is it actually true?
		consumerConf := kafka.NewConsumerConf(MarshalTopic, int32(partID))
		consumerConf.StartOffset = kafka.StartOffsetOldest
		consumerConf.RequestTimeout = 1 * time.Second

		consumer, err := w.kafka.Consumer(consumerConf)
		if err != nil {
			// Unfortunately this is a fatal error, as without being able to consume this partition
			// we can't effectively rationalize.
			log.Fatalf("rationalize[%d]: Failed to create consumer: %s", partID, err)
		}

		// Consume messages forever, or until told to quit.
		for {
			if atomic.LoadInt32(w.quit) == 1 {
				log.Debugf("rationalize[%d]: terminating.", partID)
				close(out)
				return
			}

			msgb, err := consumer.Consume()
			if err != nil {
				// The internal consumer will do a number of retries. If we get an error here,
				// for now let's consider it fatal.
				log.Fatalf("rationalize[%d]: failed to consume: %s", partID, err)
			}

			msg, err := decode(msgb.Value)
			if err != nil {
				// Invalid message in the stream. This should never happen, but if it does, just
				// continue on.
				// TODO: We should probably think about this. If we end up in a situation where
				// one version of this software has a bug that writes invalid messages, it could
				// be doing things we don't anticipate. Of course, crashing all consumers
				// reading that partition is also bad.
				log.Errorf("rationalize[%d]: %s", partID, err)
				continue
			}

			log.Debugf("Got message at offset %d: [%s]", msgb.Offset, msg.Encode())
			out <- msg
		}
	}()
	return out
}

// updateClaim is called whenever we need to adjust a claim structure.
func (w *Marshaler) updateClaim(msg *msgHeartbeat) {
	topic := w.getTopicState(msg.Topic, msg.PartID)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// Note that a heartbeat will just set the claim structure. It's not valid to heartbeat
	// for something you don't own (which is why we have ClaimPartition as a separate
	// message), so we can only assume it's valid.
	topic.partitions[msg.PartID].ClientID = msg.ClientID
	topic.partitions[msg.PartID].GroupID = msg.GroupID
	topic.partitions[msg.PartID].LastOffset = msg.LastOffset
	topic.partitions[msg.PartID].LastHeartbeat = int64(msg.Time)
}

// releaseClaim is called whenever someone has released their claim on a partition.
func (w *Marshaler) releaseClaim(msg *msgReleasingPartition) {
	topic := w.getTopicState(msg.Topic, msg.PartID)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// The partition must be claimed by the person releasing it
	if topic.partitions[msg.PartID].ClientID != msg.ClientID ||
		topic.partitions[msg.PartID].GroupID != msg.GroupID {
		log.Warningf("ReleasePartition message from client that doesn't own it. Dropping.")
		return
	}

	// Record the offset they told us they last processed, and then set the heartbeat to 0
	// which means this is no longer claimed
	topic.partitions[msg.PartID].LastOffset = msg.LastOffset
	topic.partitions[msg.PartID].LastHeartbeat = 0
}

// handleClaim is called whenever we see a ClaimPartition message.
func (w *Marshaler) handleClaim(msg *msgClaimingPartition) {
	topic := w.getTopicState(msg.Topic, msg.PartID)

	topic.lock.Lock()
	defer topic.lock.Unlock()

	// Send message to all pending consumers then clear the list (it is a violation of the
	// protocol to send two responses)
	fireEvents := func(evt bool) {
		for _, out := range topic.partitions[msg.PartID].pendingClaims {
			out <- evt
		}
		topic.partitions[msg.PartID].pendingClaims = nil
	}

	// Claim logic: if the claim is for an already claimed partition, we can end now and decide
	// whether or not to fire.
	if topic.partitions[msg.PartID].isClaimed(w.ts) {
		// The ClaimPartition message needs to be from us, or we should just return
		if msg.ClientID == w.clientID && msg.GroupID == w.groupID {
			// Now determine if we own the partition claim and let us know whether we do
			// or not
			if topic.partitions[msg.PartID].ClientID == w.clientID &&
				topic.partitions[msg.PartID].GroupID == w.groupID {
				fireEvents(true)
			} else {
				fireEvents(false)
			}
		}
		return
	}

	// At this point, the partition is unclaimed, which means we know we have the first
	// ClaimPartition message. As soon as we get it, we fill in the structure which makes
	// us think it's claimed (it is).
	topic.partitions[msg.PartID].ClientID = msg.ClientID
	topic.partitions[msg.PartID].GroupID = msg.GroupID
	topic.partitions[msg.PartID].LastOffset = 0 // not present in this message, reset.
	topic.partitions[msg.PartID].LastHeartbeat = int64(msg.Time)

	// Now we can advise that this partition has been handled
	if msg.ClientID == w.clientID && msg.GroupID == w.groupID {
		fireEvents(true)
	} else {
		fireEvents(true)
	}
}

// rationalize is a goroutine that constantly consumes from a given partition of the marshal
// topic and makes changes to the world state whenever something happens.
func (w *Marshaler) rationalize(partID int, in <-chan message) { // Might be in over my head.
	for {
		msg, ok := <-in
		if !ok {
			log.Debugf("rationalize[%d]: channel closed.", partID)
			return
		}
		log.Debugf("rationalize[%d]: %s", partID, msg.Encode())

		switch msg.Type() {
		case msgTypeHeartbeat:
			w.updateClaim(msg.(*msgHeartbeat))
		case msgTypeClaimingPartition:
			w.handleClaim(msg.(*msgClaimingPartition))
		case msgTypeReleasingPartition:
			w.releaseClaim(msg.(*msgReleasingPartition))
		case msgTypeClaimingMessages:
			// TODO: Implement.
		}
	}
}
