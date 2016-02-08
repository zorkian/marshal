package marshal

import (
	. "gopkg.in/check.v1"

	"github.com/dropbox/kafka/kafkatest"
)

var _ = Suite(&WorldSuite{})

type WorldSuite struct {
	s *kafkatest.Server
	m *Marshaler
}

func (s *WorldSuite) SetUpTest(c *C) {
	s.s = StartServer()

	var err error
	s.m, err = NewMarshaler("cl", "gr", []string{s.s.Addr()})
	if err != nil {
		c.Errorf("New Marshaler failed: %s", err)
	}
}

func (s *WorldSuite) TearDownTest(c *C) {
	s.m.Terminate()
	s.s.Close()
}

func (s *WorldSuite) TestGetTopicState(c *C) {
	// Always works
	c.Assert(s.m.getPartitionState("gr", "test2", 0), NotNil)

	// Should error (not claimed)
	topic, err := s.m.getClaimedPartitionState("gr", "test2", 0)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// Now claim this partition
	c.Assert(s.m.ClaimPartition("test2", 0), Equals, true)
	c.Assert(s.m.waitForRsteps(1), Equals, 1)

	// getClaimed should now work for our group
	topic, err = s.m.getClaimedPartitionState("gr", "test2", 0)
	c.Assert(topic, NotNil)
	c.Assert(err, IsNil)

	// And fail here
	topic, err = s.m.getClaimedPartitionState("gr2", "test2", 0)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// And fail here (our group, diff partition)
	topic, err = s.m.getClaimedPartitionState("gr", "test2", 1)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// Release partition now
	c.Assert(s.m.ReleasePartition("test2", 0, 0), IsNil)
	c.Assert(s.m.waitForRsteps(2), Equals, 2)

	// getClaimed should now fail again for our group
	topic, err = s.m.getClaimedPartitionState("gr", "test2", 0)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// And fail here
	topic, err = s.m.getClaimedPartitionState("gr2", "test2", 0)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)

	// And fail here (our group, diff partition)
	topic, err = s.m.getClaimedPartitionState("gr", "test2", 1)
	c.Assert(topic, IsNil)
	c.Assert(err, NotNil)
}
