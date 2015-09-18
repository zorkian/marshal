package marshal

import (
	"time"

	. "gopkg.in/check.v1"

	"github.com/optiopay/kafka/kafkatest"
)

var _ = Suite(&MarshalSuite{})

type MarshalSuite struct {
	s *kafkatest.Server
	m *Marshaler
}

func (s *MarshalSuite) SetUpTest(c *C) {
	s.s = StartServer()

	var err error
	s.m, err = NewMarshaler("cl", "gr", []string{s.s.Addr()})
	if err != nil {
		c.Errorf("New Marshaler failed: %s", err)
	}
}

func (s *MarshalSuite) TearDownTest(c *C) {
	s.m.Terminate()
	s.s.Close()
}

func MakeTopic(srv *kafkatest.Server, topic string, numPartitions int) {
	for i := 0; i < numPartitions; i++ {
		srv.AddMessages(topic, int32(i))
	}
}

func StartServer() *kafkatest.Server {
	srv := kafkatest.NewServer()
	srv.MustSpawn()
	MakeTopic(srv, MarshalTopic, 4)
	MakeTopic(srv, "test1", 1)
	MakeTopic(srv, "test16", 16)
	return srv
}

func (s *MarshalSuite) TestNewMarshaler(c *C) {
	c.Assert(s.m.Partitions(MarshalTopic), Equals, 4)
	c.Assert(s.m.Partitions("test1"), Equals, 1)
	c.Assert(s.m.Partitions("test16"), Equals, 16)
	c.Assert(s.m.Partitions("unknown"), Equals, 0)
}

// This is a full integration test of claiming including writing to Kafka via the marshaler
// and waiting for responses
func (s *MarshalSuite) TestClaimPartitionIntegration(c *C) {
	resp := make(chan bool)
	go func() {
		resp <- s.m.ClaimPartition("test1", 0) // true
		resp <- s.m.ClaimPartition("test1", 0) // true (no-op)
		s.m.lock.Lock()
		s.m.clientID = "cl-other"
		s.m.lock.Unlock()
		resp <- s.m.ClaimPartition("test1", 0) // false (collission)
		resp <- s.m.ClaimPartition("test1", 1) // true (new client)
	}()

	select {
	case out := <-resp:
		c.Assert(out, Equals, true)
	case <-time.After(1 * time.Second):
		c.Error("Timed out claiming partition")
	}

	select {
	case out := <-resp:
		c.Assert(out, Equals, true)
	case <-time.After(1 * time.Second):
		c.Error("Timed out claiming partition")
	}

	select {
	case out := <-resp:
		c.Assert(out, Equals, false)
	case <-time.After(1 * time.Second):
		c.Error("Timed out claiming partition")
	}

	select {
	case out := <-resp:
		c.Assert(out, Equals, true)
	case <-time.After(1 * time.Second):
		c.Error("Timed out claiming partition")
	}
}

// This is a full integration test of a claim, heartbeat, and release cycle
func (s *MarshalSuite) TestPartitionLifecycleIntegration(c *C) {
	// Claim partition (this is synchronous, will only return when)
	// it has succeeded
	c.Assert(s.m.ClaimPartition("test1", 0), Equals, true)

	// Ensure we have claimed it
	cl := s.m.GetPartitionClaim("test1", 0)
	if cl.LastHeartbeat <= 0 || cl.ClientID != "cl" || cl.GroupID != "gr" {
		c.Error("PartitionClaim values unexpected")
	}
	if cl.LastOffset != 0 {
		c.Error("LastOffset is not 0")
	}

	// Now heartbeat on it to update the last offset
	c.Assert(s.m.Heartbeat("test1", 0, 10), IsNil)

	// Now we have to wait for the rationalizer to update, so let's pause
	time.Sleep(500 * time.Millisecond)

	// Get the claim again, validate it's updated
	cl = s.m.GetPartitionClaim("test1", 0)
	if cl.LastHeartbeat <= 0 || cl.ClientID != "cl" || cl.GroupID != "gr" {
		c.Error("PartitionClaim values unexpected")
	}
	if cl.LastOffset != 10 {
		c.Error("LastOffset is not 10")
	}

	// Release
	c.Assert(s.m.ReleasePartition("test1", 0, 20), IsNil)

	// Now we have to wait for the rationalizer to update, so let's pause
	time.Sleep(500 * time.Millisecond)

	// Get the claim again, validate it's empty
	cl = s.m.GetPartitionClaim("test1", 0)
	if cl.LastHeartbeat > 0 || cl.ClientID != "" || cl.GroupID != "" {
		c.Errorf("PartitionClaim values unexpected %s", cl)
	}
	if cl.LastOffset != 0 {
		c.Error("LastOffset is not 20")
	}

	// Get the last known claim data
	cl = s.m.GetLastPartitionClaim("test1", 0)
	if cl.LastHeartbeat > 0 || cl.ClientID != "cl" || cl.GroupID != "gr" {
		c.Errorf("PartitionClaim values unexpected %s", cl)
	}
	if cl.LastOffset != 20 {
		c.Error("LastOffset is not 20")
	}
}
