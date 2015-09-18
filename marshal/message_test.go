package marshal

import . "gopkg.in/check.v1"

var _ = Suite(&MessageSuite{})

type MessageSuite struct{}

func (s *MessageSuite) TestMessageEncode(c *C) {
	base := msgBase{
		Time:     2,
		ClientID: "cl",
		GroupID:  "gr",
		Topic:    "t",
		PartID:   3,
	}
	c.Assert(base.Encode(), Equals, "2/cl/gr/t/3")

	hb := msgHeartbeat{
		msgBase:    base,
		LastOffset: 5,
	}
	c.Assert(hb.Encode(), Equals, "Heartbeat/2/cl/gr/t/3/5")

	cp := msgClaimingPartition{
		msgBase: base,
	}
	c.Assert(cp.Encode(), Equals, "ClaimingPartition/2/cl/gr/t/3")

	rp := msgReleasingPartition{
		msgBase:    base,
		LastOffset: 7,
	}
	c.Assert(rp.Encode(), Equals, "ReleasingPartition/2/cl/gr/t/3/7")

	cm := msgClaimingMessages{
		msgBase:            base,
		ProposedLastOffset: 9,
	}
	c.Assert(cm.Encode(), Equals, "ClaimingMessages/2/cl/gr/t/3/9")
}

func (s *MessageSuite) TestMessageDecode(c *C) {
	msg, err := decode([]byte("banana"))
	c.Assert(msg, IsNil)
	c.Assert(err, NotNil)

	msg, err = decode([]byte("Heartbeat/2/cl/gr/t/1/2"))
	c.Assert(msg, NotNil)
	c.Assert(err, IsNil)

	mhb, ok := msg.(*msgHeartbeat)
	if !ok || msg.Type() != msgTypeHeartbeat || mhb.ClientID != "cl" || mhb.GroupID != "gr" ||
		mhb.Topic != "t" || mhb.PartID != 1 || mhb.LastOffset != 2 || mhb.Time != 2 {
		c.Error("Heartbeat message contents invalid")
	}

	msg, err = decode([]byte("ClaimingPartition/2/cl/gr/t/1"))
	if msg == nil || err != nil {
		c.Error("Expected msg, got error", err)
	}
	mcp, ok := msg.(*msgClaimingPartition)
	if !ok || msg.Type() != msgTypeClaimingPartition || mcp.ClientID != "cl" ||
		mcp.GroupID != "gr" || mcp.Topic != "t" || mcp.PartID != 1 || mcp.Time != 2 {
		c.Error("ClaimingPartition message contents invalid")
	}

	msg, err = decode([]byte("ReleasingPartition/2/cl/gr/t/1/9"))
	if msg == nil || err != nil {
		c.Error("Expected msg, got error", err)
	}
	mrp, ok := msg.(*msgReleasingPartition)
	if !ok || msg.Type() != msgTypeReleasingPartition || mrp.ClientID != "cl" ||
		mrp.GroupID != "gr" || mrp.Topic != "t" || mrp.PartID != 1 || mrp.Time != 2 ||
		mrp.LastOffset != 9 {
		c.Error("ReleasingPartition message contents invalid")
	}

	msg, err = decode([]byte("ClaimingMessages/2/cl/gr/t/1/2"))
	if msg == nil || err != nil {
		c.Error("Expected msg, got error", err)
	}
	mcm, ok := msg.(*msgClaimingMessages)
	if !ok || msg.Type() != msgTypeClaimingMessages || mcm.ClientID != "cl" || mcm.GroupID != "gr" ||
		mcm.Topic != "t" || mcm.PartID != 1 || mcm.ProposedLastOffset != 2 || mcm.Time != 2 {
		c.Error("ClaimingMessages message contents invalid")
	}
}
