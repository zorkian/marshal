package marshal

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"github.com/op/go-logging"
)

func init() {
	// TODO: This changes logging for the whole suite. Is that what we want?
	logging.SetLevel(logging.ERROR, "PortalMarshal")
}

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&RationalizerSuite{})

type RationalizerSuite struct {
	m   *Marshaler
	out chan message
	ret chan bool
}

func (s *RationalizerSuite) SetUpTest(c *C) {
	s.m = NewWorld()
	s.out = make(chan message)
	go s.m.rationalize(0, s.out)

	// Build our return channel and insert it (simulating what the marshal does for
	// actually trying to claim)
	s.ret = make(chan bool, 1)
	topic := s.m.getTopicState("test1", 0)
	topic.lock.Lock()
	topic.partitions[0].pendingClaims = append(topic.partitions[0].pendingClaims, s.ret)
	topic.lock.Unlock()
}

func (s *RationalizerSuite) TearDownTest(c *C) {
	s.m.Terminate()
	close(s.ret)
	close(s.out)
}

func NewWorld() *Marshaler {
	return &Marshaler{
		quit:     new(int32),
		clientID: "cl",
		groupID:  "gr",
		groups:   make(map[string]map[string]*topicState),
	}
}

func heartbeat(ts int, cl, gr, t string, id int, lo int64) *msgHeartbeat {
	return &msgHeartbeat{
		msgBase: msgBase{
			Time:     ts,
			ClientID: cl,
			GroupID:  gr,
			Topic:    t,
			PartID:   id,
		},
		LastOffset: lo,
	}
}

func claimingPartition(ts int, cl, gr, t string, id int) *msgClaimingPartition {
	return &msgClaimingPartition{
		msgBase: msgBase{
			Time:     ts,
			ClientID: cl,
			GroupID:  gr,
			Topic:    t,
			PartID:   id,
		},
	}
}

func releasingPartition(ts int, cl, gr, t string, id int, lo int64) *msgReleasingPartition {
	return &msgReleasingPartition{
		msgBase: msgBase{
			Time:     ts,
			ClientID: cl,
			GroupID:  gr,
			Topic:    t,
			PartID:   id,
		},
		LastOffset: lo,
	}
}

func (s *RationalizerSuite) TestIsClaimed(c *C) {
	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	s.out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	s.m.ts = 1
	c.Assert(s.m.IsClaimed("test1", 0), Equals, true)

	// Should still be claimed immediately after the interval
	s.m.ts = HeartbeatInterval + 2
	c.Assert(s.m.IsClaimed("test1", 0), Equals, true)

	// And still claimed right at the last second of the cutoff
	s.m.ts = HeartbeatInterval * 2
	c.Assert(s.m.IsClaimed("test1", 0), Equals, true)

	// Should NOT be claimed >2x the heartbeat interval
	s.m.ts = HeartbeatInterval*2 + 1
	c.Assert(s.m.IsClaimed("test1", 0), Equals, false)
}

func (s *RationalizerSuite) TestClaimNotMutable(c *C) {
	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	s.out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	s.m.ts = 1
	cl := s.m.GetPartitionClaim("test1", 0)
	c.Assert(cl.LastHeartbeat, Not(Equals), 0)

	// Modify structure, then refetch and make sure it hasn't been mutated
	cl.ClientID = "invalid"
	cl2 := s.m.GetPartitionClaim("test1", 0)
	c.Assert(cl2.LastHeartbeat, Not(Equals), 0)
	c.Assert(cl2.ClientID, Equals, "cl")
}

func (s *RationalizerSuite) TestClaimPartition(c *C) {
	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	s.m.ts = 30
	s.out <- claimingPartition(1, "cl", "gr", "test1", 0)

	select {
	case resp := <-s.ret:
		c.Assert(resp, Equals, true)
	case <-time.After(1 * time.Second):
		c.Error("Timed out claiming partition")
	}
}

func (s *RationalizerSuite) TestReclaimPartition(c *C) {
	// This log is us having the partition (HB) + a CP from someone else + a CP from us,
	// this should only fire a single 'true' into the out channel
	s.m.ts = 30
	s.out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	s.out <- claimingPartition(2, "clother", "gr", "test1", 0)
	s.out <- claimingPartition(3, "cl", "gr", "test1", 0)

	select {
	case resp := <-s.ret:
		c.Assert(resp, Equals, true)
	case <-time.After(1 * time.Second):
		c.Error("Timed out claiming partition")
	}
}

func (s *RationalizerSuite) TestReleaseClaim(c *C) {
	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	s.out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	s.m.ts = 1
	c.Assert(s.m.IsClaimed("test1", 0), Equals, true)

	// Someone else attempts to release the claim, this shouldn't work
	s.out <- releasingPartition(20, "cl-bad", "gr", "test1", 0, 5)
	time.Sleep(5 * time.Millisecond)

	// Must be unclaimed, invalid release
	s.m.ts = 25
	c.Assert(s.m.IsClaimed("test1", 0), Equals, true)

	// Now they release it at position 10
	s.out <- releasingPartition(30, "cl", "gr", "test1", 0, 10)
	time.Sleep(5 * time.Millisecond)

	// They released at 30, should be free as of 31
	s.m.ts = 31
	c.Assert(s.m.IsClaimed("test1", 0), Equals, false)
}

func (s *RationalizerSuite) TestClaimHandoff(c *C) {
	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	s.out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// They heartbeated at 1, should be claimed as of 1.
	s.m.ts = 1
	c.Assert(s.m.IsClaimed("test1", 0), Equals, true)

	// Now they hand this off to someone else who picks up the heartbeat
	s.out <- heartbeat(10, "cl2", "gr", "test1", 0, 10)
	time.Sleep(5 * time.Millisecond)

	// Must be claimed, and claimed by cl2
	s.m.ts = 25
	c.Assert(s.m.IsClaimed("test1", 0), Equals, true)
	c.Assert(s.m.GetPartitionClaim("test1", 0).ClientID, Equals, "cl2")

	// Now we change the group ID of our world state (which client's can't do) and validate
	// that these partitions are NOT claimed
	s.m.ts = 25
	s.m.groupID = "gr2"
	c.Assert(s.m.IsClaimed("test1", 0), Equals, false)
	c.Assert(s.m.GetPartitionClaim("test1", 0).ClientID, Equals, "")
}

func (s *RationalizerSuite) TestPartitionExtend(c *C) {
	// This log, a single heartbeat at t=0, indicates that this topic/partition are claimed
	// by the client/group given.
	s.out <- heartbeat(1, "cl", "gr", "test1", 0, 0)
	time.Sleep(5 * time.Millisecond)

	// Ensure len is 1
	s.m.lock.RLock()
	s.m.groups["gr"]["test1"].lock.RLock()
	c.Assert(len(s.m.groups["gr"]["test1"].partitions), Equals, 1)
	s.m.groups["gr"]["test1"].lock.RUnlock()
	s.m.lock.RUnlock()

	// Extend by 4
	s.out <- heartbeat(2, "cl2", "gr", "test1", 4, 0)
	time.Sleep(5 * time.Millisecond)

	// Ensure len is 5
	s.m.lock.RLock()
	defer s.m.lock.RUnlock()
	s.m.groups["gr"]["test1"].lock.RLock()
	defer s.m.groups["gr"]["test1"].lock.RUnlock()
	c.Assert(len(s.m.groups["gr"]["test1"].partitions), Equals, 5)

	// Ensure 0 and 4 are claimed by us
	p1 := s.m.groups["gr"]["test1"].partitions[0]
	c.Assert(p1.ClientID, Equals, "cl")
	c.Assert(p1.GroupID, Equals, "gr")
	c.Assert(p1.LastHeartbeat, Equals, int64(1))
	p2 := s.m.groups["gr"]["test1"].partitions[4]
	c.Assert(p2.ClientID, Equals, "cl2")
	c.Assert(p2.GroupID, Equals, "gr")
	c.Assert(p2.LastHeartbeat, Equals, int64(2))
}
