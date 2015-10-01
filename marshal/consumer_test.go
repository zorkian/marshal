package marshal

import (
	"math/rand"
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
		behavior:   CbAggressive,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		claims:     make(map[int]*claim),
	}
	atomic.StoreInt32(s.cn.alive, 1)
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
	cn, err := NewConsumer(s.m, "test1", CbAggressive)
	c.Assert(err, IsNil)
	defer cn.Terminate()

	// A new consumer will immediately start to claim partitions, so let's give it a
	// second and then see if it has
	time.Sleep(500 * time.Millisecond)
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
	// Claim partition 0, update our offsets, produce, update offsets again, ensure everything
	// is consistent (offsets got updated, etc)
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3"), Equals, int64(2))

	// Consume once, heartbeat unchanged
	hb := s.cn.claims[0].lastHeartbeat
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m1"))
	c.Assert(s.cn.claims[0].lastHeartbeat, Equals, hb)

	// Now "force" the heartbeat backwards, should cause consume to heartbeat
	s.cn.claims[0].lastHeartbeat -= HeartbeatInterval
	hb = s.cn.claims[0].lastHeartbeat
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m2"))
	c.Assert(s.cn.claims[0].lastHeartbeat, Not(Equals), hb)

	// Now consume the last, again no heartbeat
	hb = s.cn.claims[0].lastHeartbeat
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m3"))
	c.Assert(s.cn.claims[0].lastHeartbeat, Equals, hb)
}

func (s *ConsumerSuite) TestTryClaimPartition(c *C) {
	// Should work
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	// Should fail (can't claim a second time)
	c.Assert(s.cn.tryClaimPartition(0), Equals, false)
}

func (s *ConsumerSuite) TestOffsetUpdates(c *C) {
	// Claim partition 0, update our offsets, produce, update offsets again, ensure everything
	// is consistent (offsets got updated, etc)
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	c.Assert(s.cn.claims[0].updateOffsets(0), IsNil)
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3"), Equals, int64(2))
	c.Assert(s.cn.claims[0].updateOffsets(1), IsNil)
	c.Assert(s.cn.claims[0].offsetLatest, Equals, int64(3))
}

func (s *ConsumerSuite) TestAggressiveClaim(c *C) {
	// Ensure aggressive mode claims all partitions in a single call to claim
	c.Assert(s.cn.GetCurrentLoad(), Equals, 0)
	s.cn.claimPartitions()
	c.Assert(s.cn.GetCurrentLoad(), Equals, 16)
}

func (s *ConsumerSuite) TestBalancedClaim(c *C) {
	// Ensure balanced mode only claims one partition
	s.cn.behavior = CbBalanced
	c.Assert(s.cn.GetCurrentLoad(), Equals, 0)
	s.cn.claimPartitions()
	c.Assert(s.cn.GetCurrentLoad(), Equals, 1)
}
