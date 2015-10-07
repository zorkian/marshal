package marshal

import (
	"math/rand"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	. "gopkg.in/check.v1"

	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
)

var _ = Suite(&ConsumerSuite{})

type ConsumerSuite struct {
	c  *C
	s  *kafkatest.Server
	m  *Marshaler
	cn *Consumer
}

func (s *ConsumerSuite) SetUpTest(c *C) {
	s.c = c
	s.s = StartServer()

	var err error
	s.m, err = NewMarshaler("cl", "gr", []string{s.s.Addr()})
	c.Assert(err, IsNil)

	s.cn = &Consumer{
		alive:      new(int32),
		marshal:    s.m,
		topic:      "test16",
		partitions: s.m.Partitions("test16"),
		options:    NewConsumerOptions(),
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		claims:     make(map[int]*claim),
	}
	atomic.StoreInt32(s.cn.alive, 1)
}

func (s *ConsumerSuite) TearDownTest(c *C) {
	s.cn.Terminate()
	s.m.Terminate()
	s.s.Close()
}

func (s *ConsumerSuite) Produce(topicName string, partID int, msgs ...string) int64 {
	var protos []*proto.Message
	for _, msg := range msgs {
		protos = append(protos, &proto.Message{Value: []byte(msg)})
	}
	offset, err := s.m.producer.Produce(topicName, int32(partID), protos...)
	s.c.Assert(err, IsNil)
	return offset
}

func (s *ConsumerSuite) TestNewConsumer(c *C) {
	options := NewConsumerOptions()
	options.GreedyClaims = true

	cn, err := NewConsumer(s.m, "test1", options)
	c.Assert(err, IsNil)
	defer cn.Terminate()

	// Wait for 2 messages to be processed
	c.Assert(s.m.waitForRsteps(2), Equals, 2)
	c.Assert(s.m.GetPartitionClaim("test1", 0).LastHeartbeat, Not(Equals), int64(0))

	// Test basic consumption
	s.Produce("test1", 0, "m1", "m2", "m3")
	c.Assert(cn.Consume(), DeepEquals, []byte("m1"))
	c.Assert(cn.Consume(), DeepEquals, []byte("m2"))
	c.Assert(cn.Consume(), DeepEquals, []byte("m3"))

	// TODO: flesh out test, can create a second consumer and then see if it gets any
	// partitions, etc.
	// lots of things can be tested.
}

func (s *ConsumerSuite) TestTerminate(c *C) {
	// Termination is supposed to release active claims that we have, ensure that
	// this happens
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	c.Assert(s.cn.Terminate(), Equals, true)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim(s.cn.topic, 0).LastHeartbeat, Equals, int64(0))
}

func (s *ConsumerSuite) TestMultiClaim(c *C) {
	// Set up claims on two partitions, we'll put messages in both and then ensure that
	// we get all of the messages out
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	c.Assert(s.cn.tryClaimPartition(1), Equals, true)

	// Produce 1000 messages to the two partitions
	for i := 0; i < 1000; i++ {
		s.Produce("test16", i%2, strconv.Itoa(i))
	}

	// Now consume 1000 times and ensure we get exactly 1000 unique messages
	results := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		results[string(s.cn.Consume())] = true
	}
	c.Assert(len(results), Equals, 1000)
}

func (s *ConsumerSuite) TestUnhealthyPartition(c *C) {
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	cl := s.cn.claims[0]

	// We just claimed, nothing should be unhealthy
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 0)

	// Put in one message and consume it making sure things work, and then update offsets.
	s.Produce("test16", 0, "m1")
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m1"))
	c.Assert(cl.updateOffsets(0), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 0)

	// Produce 5, consume 3... at this point we are "unhealthy" since we are falling
	// behind for this period
	s.Produce("test16", 0, "m2", "m3", "m4", "m5", "m6")
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m2"))
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m3"))
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m4"))
	c.Assert(cl.updateOffsets(1), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 1)

	// Produce nothing and consume the last two, we become healthy again because
	// we are caught up and our velocity is equal
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m5"))
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m6"))
	c.Assert(cl.updateOffsets(2), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.ConsumerVelocity() == cl.PartitionVelocity(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 0)

	// Produce again, falls behind slightly
	s.Produce("test16", 0, "m7")
	c.Assert(cl.updateOffsets(3), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 1)

	// Still behind
	c.Assert(cl.updateOffsets(4), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 2)

	// Consume the last message, which will fix our velocity and let us
	// pass as healthy again
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m7"))
	c.Assert(cl.updateOffsets(5), IsNil)
	c.Assert(cl.healthCheck(), Equals, true)
	c.Assert(cl.cyclesBehind, Equals, 0)
}

func (s *ConsumerSuite) TestConsumerHeartbeat(c *C) {
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	c.Assert(s.m.waitForRsteps(1), Equals, 1)

	// Newly claimed partition should have heartbeated
	c.Assert(s.cn.claims[0].lastHeartbeat, Not(Equals), 0)

	// Now reset the heartbeat to some other value
	s.cn.claims[0].lastHeartbeat -= HeartbeatInterval
	hb := s.cn.claims[0].lastHeartbeat

	// Manual heartbeat, ensure lastHeartbeat is updated
	s.cn.claims[0].heartbeat()
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.cn.claims[0].lastHeartbeat, Not(Equals), hb)
}

func (s *ConsumerSuite) TestCommittedOffset(c *C) {
	// Test that we save/load committed offsets properly and that when we load them we will
	// prefer them over the heartbeated values (if they're higher)
	s.Produce("test16", 0, "m1", "m2", "m3", "m4")
	c.Assert(s.m.offsets.Commit("test16", 0, 2), IsNil)
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	c.Assert(s.m.waitForRsteps(1), Equals, 1)
	c.Assert(s.cn.claims[0].offsets.Current, Equals, int64(2))

	// Since the committed offset was 2, the first consumption should be the third message
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m3"))

	// Heartbeat should succeed and update the committed offset
	c.Assert(s.cn.claims[0].heartbeat(), Equals, true)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	offset, _, err := s.m.offsets.Offset("test16", 0)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(3))
	c.Assert(s.cn.claims[0].Release(), Equals, true)
	c.Assert(s.m.waitForRsteps(4), Equals, 4)
	c.Assert(s.cn.claims[0].Claimed(), Equals, false)
	s.cn.claims[0] = nil

	// Now let's "downcommit" the offset back to an earlier value, and then re-claim the
	// partition to verify that it sets the offset to the heartbeated value rather than
	// the committed value
	c.Assert(s.m.offsets.Commit("test16", 0, 2), IsNil)
	offset, _, err = s.m.offsets.Offset("test16", 0)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(2))
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	c.Assert(s.m.waitForRsteps(5), Equals, 5)
	c.Assert(s.cn.claims[0].offsets.Current, Equals, int64(3))
}

func (s *ConsumerSuite) TestTryClaimPartition(c *C) {
	// Should work
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	// Should fail (can't claim a second time)
	c.Assert(s.cn.tryClaimPartition(0), Equals, false)
}

func (s *ConsumerSuite) TestAggressiveClaim(c *C) {
	// Ensure aggressive mode claims all partitions in a single call to claim
	s.cn.options.GreedyClaims = true
	c.Assert(s.cn.GetCurrentLoad(), Equals, 0)
	s.cn.claimPartitions()
	c.Assert(s.cn.GetCurrentLoad(), Equals, 16)
}

func (s *ConsumerSuite) TestBalancedClaim(c *C) {
	// Ensure balanced mode only claims one partition
	c.Assert(s.cn.GetCurrentLoad(), Equals, 0)
	s.cn.claimPartitions()
	c.Assert(s.cn.GetCurrentLoad(), Equals, 1)
}

func (s *ConsumerSuite) TestFastReclaim(c *C) {
	// Claim some partitions then create a new consumer with fast reclaim on; this
	// should "reclaim" the partitions automatically at the offset they were last
	// reported at
	cn1, err := NewConsumer(s.m, "test2", NewConsumerOptions())
	c.Assert(err, IsNil)
	defer cn1.Terminate()
	s.Produce("test2", 0, "m1", "m2", "m3")

	// By default the consumer will claim all partitions so let's wait for that
	c.Assert(s.m.waitForRsteps(4), Equals, 4)

	// Consume the first two messages from 0, then heartbeat to set the offset to 2
	c.Assert(cn1.Consume(), DeepEquals, []byte("m1"))
	c.Assert(cn1.Consume(), DeepEquals, []byte("m2"))
	c.Assert(cn1.claims[0].heartbeat(), Equals, true)
	c.Assert(s.m.waitForRsteps(5), Equals, 5)

	// Now add some messages to the next, but only consume some
	s.Produce("test2", 1, "p1", "p2", "p3", "p4")
	c.Assert(cn1.Consume(), DeepEquals, []byte("m3"))
	c.Assert(cn1.Consume(), DeepEquals, []byte("p1"))
	c.Assert(cn1.Consume(), DeepEquals, []byte("p2"))
	c.Assert(cn1.Consume(), DeepEquals, []byte("p3"))

	// Now we "reclaim" by creating a new consumer here; this is actually bogus
	// usage as it would normally lead to stepping on the prior consumer, but it
	// is useful for this test.
	cn, err := NewConsumer(s.m, "test2", NewConsumerOptions())
	c.Assert(err, IsNil)
	defer cn.Terminate()

	// We expect the two partitions to be reclaimed with a simple heartbeat
	// and no claim message sent
	c.Assert(s.m.waitForRsteps(7), Equals, 7)
	c.Assert(len(cn.claims), Equals, 2)
	c.Assert(cn.claims[0].offsets.Current, Equals, int64(2))
	c.Assert(cn.claims[1].offsets.Current, Equals, int64(0))

	// There should be four messages left, but they can come in any order depending
	// on how things get scheduled. Let's get them all and sort and verify. This
	// does indicate we've double-consumed, but that's expected in this particular
	// failure scenario.
	var msgs []string
	for i := 0; i < 4; i++ {
		msgs = append(msgs, string(cn.Consume()))
	}
	sort.Strings(msgs)
	c.Assert(msgs, DeepEquals, []string{"m3", "p1", "p2", "p3"})
}
