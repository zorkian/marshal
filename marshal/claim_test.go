package marshal

import (
	. "gopkg.in/check.v1"

	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
)

var _ = Suite(&ClaimSuite{})

type ClaimSuite struct {
	c  *C
	s  *kafkatest.Server
	m  *Marshaler
	cl *claim
}

func (s *ClaimSuite) SetUpTest(c *C) {
	s.c = c
	s.s = StartServer()

	var err error
	s.m, err = NewMarshaler("cl", "gr", []string{s.s.Addr()})
	c.Assert(err, IsNil)

	s.cl = newClaim("test16", 0, s.m)
}

func (s *ClaimSuite) TearDownTest(c *C) {
	s.cl.Release()
	s.m.Terminate()
	s.s.Close()
}

func (s *ClaimSuite) Produce(topicName string, partID int, msgs ...string) int64 {
	var protos []*proto.Message
	for _, msg := range msgs {
		protos = append(protos, &proto.Message{Value: []byte(msg)})
	}
	offset, err := s.m.producer.Produce(topicName, int32(partID), protos...)
	s.c.Assert(err, IsNil)
	return offset
}

func (s *ClaimSuite) TestOffsetUpdates(c *C) {
	// Test that the updateOffsets function works and updates offsets from Kafka
	c.Assert(s.cl.updateOffsets(0), IsNil)
	c.Assert(s.Produce("test16", 0, "m1", "m2", "m3"), Equals, int64(2))
	c.Assert(s.cl.updateOffsets(1), IsNil)
	c.Assert(s.cl.offsetLatest, Equals, int64(3))
}

func (s *ClaimSuite) TestRelease(c *C) {
	// Test that calling Release on a claim properly sets the flag and releases the
	// partition
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Not(Equals), int64(0))
	c.Assert(s.cl.Claimed(), Equals, true)
	c.Assert(s.cl.Release(), Equals, true)
	c.Assert(s.cl.Claimed(), Equals, false)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.cl.Release(), Equals, false)
}

func (s *ClaimSuite) TestCurrentLag(c *C) {
	// Test that GetCurrentLag returns the correct numbers in various cases
	s.cl.offsetCurrent = 0
	s.cl.offsetLatest = 0
	c.Assert(s.cl.GetCurrentLag(), Equals, int64(0))

	s.cl.offsetCurrent = 1
	s.cl.offsetLatest = 0
	c.Assert(s.cl.GetCurrentLag(), Equals, int64(0))

	s.cl.offsetCurrent = 0
	s.cl.offsetLatest = 1
	c.Assert(s.cl.GetCurrentLag(), Equals, int64(1))

	s.cl.offsetCurrent = 1
	s.cl.offsetLatest = 2
	c.Assert(s.cl.GetCurrentLag(), Equals, int64(1))
}

func (s *ClaimSuite) TestHeartbeat(c *C) {
	// Ensure that our heartbeats are updating the marshal structures appropriately
	// (makes sure clients are seeing the right values)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(0))
	s.cl.offsetCurrent = 10
	c.Assert(s.cl.heartbeat(), Equals, true)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(10))

	// And test that releasing means we can't update heartbeat anymore
	c.Assert(s.cl.Release(), Equals, true)
	c.Assert(s.m.waitForRsteps(4), Equals, 4)
	s.cl.offsetCurrent = 20
	c.Assert(s.cl.heartbeat(), Equals, false)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(0))
	c.Assert(s.m.GetLastPartitionClaim("test16", 0).LastOffset, Equals, int64(10))
}

func (s *ClaimSuite) TestVelocity(c *C) {
	// Test that the velocity functions perform as expected given the expected inputs
	s.cl.offsetCurrentHistory = [10]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(0))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 2, 0, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(1))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 2, 3, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(1))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(1))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 21, 21, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(10))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{1, 21, 21, 21, 21, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(5))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())

	s.cl.offsetCurrentHistory = [10]int64{21, 21, 1, 21, 21, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity(), Equals, float64(5))
	s.cl.offsetLatestHistory = s.cl.offsetCurrentHistory
	c.Assert(s.cl.PartitionVelocity(), Equals, s.cl.ConsumerVelocity())
}

func (s *ClaimSuite) TestHealthCheck(c *C) {
	// Ensure that the health check system returns expected values for given states
	s.cl.offsetCurrent = 0
	s.cl.offsetCurrentHistory = [10]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s.cl.offsetLatest = 0
	s.cl.offsetLatestHistory = [10]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 0)

	// Put us in an "unhealthy" state, PV is high and we aren't caught up
	s.cl.offsetLatest = 10
	s.cl.offsetLatestHistory = [10]int64{1, 10, 0, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 1)

	// Now we're "caught up" even PV>CV we're healthy
	s.cl.offsetCurrent = 21
	s.cl.offsetCurrentHistory = [10]int64{1, 6, 11, 16, 21, 0, 0, 0, 0, 0}
	s.cl.offsetLatest = 21
	s.cl.offsetLatestHistory = [10]int64{1, 11, 21, 0, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity() < s.cl.PartitionVelocity(), Equals, true)
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 0)

	// Now we're behind and fail health checks 3 times, this will release
	s.cl.offsetLatest = 31
	s.cl.offsetLatestHistory = [10]int64{1, 11, 21, 31, 0, 0, 0, 0, 0, 0}
	c.Assert(s.cl.ConsumerVelocity() < s.cl.PartitionVelocity(), Equals, true)
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 1)
	c.Assert(s.cl.healthCheck(), Equals, true)
	c.Assert(s.cl.cyclesBehind, Equals, 2)
	c.Assert(s.cl.healthCheck(), Equals, false)
	c.Assert(s.cl.cyclesBehind, Equals, 3)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(0))
	c.Assert(s.m.GetLastPartitionClaim("test16", 0).LastOffset, Equals, int64(21))
}

func (s *ClaimSuite) TestHealthCheckRelease(c *C) {
	// Test that an expired heartbeat causes the partition to get immediately released
	s.cl.lastHeartbeat -= HeartbeatInterval * 2
	s.cl.offsetCurrent = 5
	c.Assert(s.cl.healthCheck(), Equals, false)
	c.Assert(s.m.waitForRsteps(3), Equals, 3)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.cl.Claimed(), Equals, false)
	c.Assert(s.cl.healthCheck(), Equals, false)
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastHeartbeat, Equals, int64(0))
	c.Assert(s.m.GetPartitionClaim("test16", 0).LastOffset, Equals, int64(0))
	c.Assert(s.m.GetLastPartitionClaim("test16", 0).LastOffset, Equals, int64(5))
}
