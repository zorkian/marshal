package marshal

import (
	"math/rand"
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
		marshal:    s.m,
		topic:      "test16",
		partitions: s.m.Partitions("test16"),
		behavior:   CbAggressive,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		claims:     make(map[int]*consumerClaim),
	}
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

func (s *ConsumerSuite) TestUnhealthyPartition(c *C) {
	// Claim partition 0, update our offsets, produce, update offsets again, ensure everything
	// is consistent (offsets got updated, etc)
	c.Assert(s.cn.tryClaimPartition(0), Equals, true)
	c.Assert(s.cn.tryClaimPartition(1), Equals, true)

	// We just claimed, nothing should be unhealthy
	c.Assert(len(s.cn.getUnhealthyClaims()), Equals, 0)

	// Now put some messages in and update offsets, then we will still be "healthy" since we
	// haven't consumed
	s.Produce("test16", 0, "m1", "m2", "m3")
	c.Assert(s.cn.updateOffsets(), IsNil)
	c.Assert(len(s.cn.getUnhealthyClaims()), Equals, 0)

	// Now consume some messages, and we'll still be healthy
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m1"))
	c.Assert(len(s.cn.getUnhealthyClaims()), Equals, 0)

	// Update the claim structure so it looks like it's behind, but it's not yet in the danger
	// of being released
	s.cn.claims[0].startTime -= HeartbeatInterval * 2
	c.Assert(len(s.cn.getUnhealthyClaims()), Equals, 0)
	c.Assert(s.cn.claims[0].cyclesBehind, Equals, 1)

	// A second call will increase the cycles behind again, but still not release
	c.Assert(len(s.cn.getUnhealthyClaims()), Equals, 0)
	c.Assert(s.cn.claims[0].cyclesBehind, Equals, 2)

	// And a third call, this time the partition shows up in our list
	c.Assert(len(s.cn.getUnhealthyClaims()), Equals, 1)
	c.Assert(s.cn.claims[0].cyclesBehind, Equals, 3)

	// Now let's consume a second message and double our velocity, which will take us under
	// the threshold and reset our behind count
	c.Assert(s.cn.Consume(), DeepEquals, []byte("m2"))
	c.Assert(len(s.cn.getUnhealthyClaims()), Equals, 0)
	c.Assert(s.cn.claims[0].cyclesBehind, Equals, 0)
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
	c.Assert(s.cn.updateOffsets(), IsNil)
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3"), Equals, int64(2))
	c.Assert(s.cn.updateOffsets(), IsNil)
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
