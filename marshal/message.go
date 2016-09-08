/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"fmt"
	"strconv"
	"strings"
)

// TODO: This all uses a dumb string representation format which is very bytes-intensive.
// A binary protocol would be nice.

type msgType int

const (
	msgLengthBase int = 8
	idxType       int = 0
	idxVersion    int = 1
	idxTimestamp  int = 2
	idxInstanceID int = 3
	idxClientID   int = 4
	idxGroupID    int = 5
	idxTopic      int = 6
	idxPartID     int = 7
	idxBaseEnd    int = 7 // Index of last element in base message.

	msgTypeHeartbeat   msgType = 0
	msgLengthHeartbeat int     = msgLengthBase + 1
	idxHBCurrentOffset int     = idxBaseEnd + 1

	msgTypeClaimingPartition   msgType = 1
	msgLengthClaimingPartition int     = msgLengthBase

	msgTypeReleasingPartition   msgType = 2
	msgLengthReleasingPartition int     = msgLengthBase + 1
	idxRPCurrentOffset          int     = idxBaseEnd + 1

	msgTypeClaimingMessages    msgType = 3
	msgLengthClaimingMessages  int     = msgLengthBase + 1
	idxCMProposedCurrentOffset int     = idxBaseEnd + 1

	msgTypeReleaseGroup   msgType = 4
	msgLengthReleaseGroup int     = msgLengthBase + 1
	idxRGMsgExpireTime    int     = idxBaseEnd + 1
)

type message interface {
	Encode() string
	Timestamp() int
	Type() msgType
	Ownership() (string, string, string)
}

// decode takes a slice of bytes that should constitute a single message and attempts to
// decode it into one of our message structs.
func decode(inp []byte) (message, error) {
	parts := strings.Split(string(inp), "/")
	if len(parts) < msgLengthBase {
		return nil, fmt.Errorf("Invalid message (length): [%s]", string(inp))
	}

	version, err := strconv.Atoi(parts[idxVersion])
	if err != nil {
		return nil, fmt.Errorf("Invalid message (version): [%s]", string(inp))
	}

	// Get out the base message which is always present as it identifies the sender.
	partID, err := strconv.Atoi(parts[idxPartID])
	if err != nil {
		return nil, fmt.Errorf("Invalid message (partID): [%s]", string(inp))
	}
	ts, err := strconv.Atoi(parts[idxTimestamp])
	if err != nil {
		return nil, fmt.Errorf("Invalid message (timestamp): [%s]", string(inp))
	}
	base := msgBase{
		Version:    version,
		Time:       ts,
		InstanceID: parts[idxInstanceID],
		ClientID:   parts[idxClientID],
		GroupID:    parts[idxGroupID],
		Topic:      parts[idxTopic],
		PartID:     partID,
	}

	switch parts[0] {
	case "Heartbeat":
		if len(parts) != msgLengthHeartbeat {
			return nil, fmt.Errorf("Invalid message (hb length): [%s]", string(inp))
		}
		offset, err := strconv.ParseInt(parts[idxHBCurrentOffset], 10, 0)
		if err != nil {
			return nil, fmt.Errorf("Invalid message (hb offset): [%s]", string(inp))
		}
		return &msgHeartbeat{msgBase: base, CurrentOffset: int64(offset)}, nil
	case "ClaimingPartition":
		if len(parts) != msgLengthClaimingPartition {
			return nil, fmt.Errorf("Invalid message (cp length): [%s]", string(inp))
		}
		return &msgClaimingPartition{msgBase: base}, nil
	case "ReleasingPartition":
		if len(parts) != msgLengthReleasingPartition {
			return nil, fmt.Errorf("Invalid message (rp length): [%s]", string(inp))
		}
		offset, err := strconv.ParseInt(parts[idxRPCurrentOffset], 10, 0)
		if err != nil {
			return nil, fmt.Errorf("Invalid message (rp offset): [%s]", string(inp))
		}
		return &msgReleasingPartition{msgBase: base, CurrentOffset: offset}, nil
	case "ClaimingMessages":
		if len(parts) != msgLengthClaimingMessages {
			return nil, fmt.Errorf("Invalid message (cm length): [%s]", string(inp))
		}
		offset, err := strconv.ParseInt(parts[idxCMProposedCurrentOffset], 10, 0)
		if err != nil {
			return nil, fmt.Errorf("Invalid message (cm offset): [%s]", string(inp))
		}
		return &msgClaimingMessages{msgBase: base, ProposedCurrentOffset: offset}, nil
	case "ReleaseGroup":
		if len(parts) != msgLengthReleaseGroup {
			return nil, fmt.Errorf("Invalid message (rg length): [%s]", string(inp))
		}
		if base.Topic != "" || base.PartID != 0 {
			return nil, fmt.Errorf("Invalid ReleaseGroup message (Topic, PartID must be empty)")
		}
		expiry, err := strconv.Atoi(parts[idxRGMsgExpireTime])
		if err != nil {
			return nil, fmt.Errorf("Invalid message (rg message expire time): [%s]", string(inp))
		}

		return &msgReleaseGroup{msgBase: base, MsgExpireTime: expiry}, nil
	}
	return nil, fmt.Errorf("Invalid message: [%s]", string(inp))
}

type msgBase struct {
	Version    int
	Time       int
	InstanceID string
	ClientID   string
	GroupID    string
	Topic      string
	PartID     int
}

// Encode returns a string representation of the message.
func (m *msgBase) Encode() string {
	return fmt.Sprintf("%d/%d/%s/%s/%s/%s/%d",
		m.Version, m.Time, m.InstanceID, m.ClientID, m.GroupID, m.Topic, m.PartID)
}

// Type returns the type of this message.
func (m *msgBase) Type() msgType {
	panic("Attempted to type the base message. This should never happen.")
}

// Timestamp returns the timestamp of the message
func (m *msgBase) Timestamp() int {
	return m.Time
}

// Ownership returns InstanceID, ClientID, GroupID for message
func (m *msgBase) Ownership() (string, string, string) {
	return m.InstanceID, m.ClientID, m.GroupID
}

// msgHeartbeat is sent regularly by all consumers to re-up their claim to the partition that
// they're consuming.
type msgHeartbeat struct {
	msgBase
	CurrentOffset int64
}

// Encode returns a string representation of the message.
func (m *msgHeartbeat) Encode() string {
	return "Heartbeat/" + m.msgBase.Encode() + fmt.Sprintf("/%d", m.CurrentOffset)
}

// Type returns the type of this message.
func (m *msgHeartbeat) Type() msgType {
	return msgTypeHeartbeat
}

// Timestamp returns the timestamp of the message
func (m *msgHeartbeat) Timestamp() int {
	return m.Time
}

// Ownership returns InstanceID, ClientID, GroupID for message
func (m *msgHeartbeat) Ownership() (string, string, string) {
	return m.InstanceID, m.ClientID, m.GroupID
}

// msgClaimingPartition is used in the claim flow.
type msgClaimingPartition struct {
	msgBase
}

// Encode returns a string representation of the message.
func (m *msgClaimingPartition) Encode() string {
	return "ClaimingPartition/" + m.msgBase.Encode()
}

// Type returns the type of this message.
func (m *msgClaimingPartition) Type() msgType {
	return msgTypeClaimingPartition
}

// Timestamp returns the timestamp of the message
func (m *msgClaimingPartition) Timestamp() int {
	return m.Time
}

// Ownership returns InstanceID, ClientID, GroupID for message
func (m *msgClaimingPartition) Ownership() (string, string, string) {
	return m.InstanceID, m.ClientID, m.GroupID
}

// msgReleasingPartition is used in a controlled shutdown to indicate that you are done with
// a partition.
type msgReleasingPartition struct {
	msgBase
	CurrentOffset int64
}

// Encode returns a string representation of the message.
func (m *msgReleasingPartition) Encode() string {
	return "ReleasingPartition/" + m.msgBase.Encode() + fmt.Sprintf("/%d", m.CurrentOffset)
}

// Type returns the type of this message.
func (m *msgReleasingPartition) Type() msgType {
	return msgTypeReleasingPartition
}

// Timestamp returns the timestamp of the message
func (m *msgReleasingPartition) Timestamp() int {
	return m.Time
}

// Ownership returns InstanceID, ClientID, GroupID for message
func (m *msgReleasingPartition) Ownership() (string, string, string) {
	return m.InstanceID, m.ClientID, m.GroupID
}

// msgClaimingMessages is used for at-most-once consumption semantics, this is a pre-commit
// advisory message.
type msgClaimingMessages struct {
	msgBase
	ProposedCurrentOffset int64
}

// Encode returns a string representation of the message.
func (m *msgClaimingMessages) Encode() string {
	return "ClaimingMessages/" + m.msgBase.Encode() + fmt.Sprintf("/%d", m.ProposedCurrentOffset)
}

// Type returns the type of this message.
func (m *msgClaimingMessages) Type() msgType {
	return msgTypeClaimingMessages
}

// Timestamp returns the timestamp of the message
func (m *msgClaimingMessages) Timestamp() int {
	return m.Time
}

// Ownership returns InstanceID, ClientID, GroupID for message
func (m *msgClaimingMessages) Ownership() (string, string, string) {
	return m.InstanceID, m.ClientID, m.GroupID
}

// msgReleaseGroup is used by the Admin to pause a consumer group,
// identified by groupID, until MsgExpireTime.
type msgReleaseGroup struct {
	msgBase
	MsgExpireTime int
}

// Encode returns a string representation of the message.
func (m *msgReleaseGroup) Encode() string {
	if m.msgBase.Topic != "" || m.msgBase.PartID != 0 {
		panic("ReleaseGroup message must have non-empty topic and partition id.")
	}
	return "ReleaseGroup/" + m.msgBase.Encode() + fmt.Sprintf("/%d", m.MsgExpireTime)
}

// Type returns the type of this message.
func (m *msgReleaseGroup) Type() msgType {
	return msgTypeReleaseGroup
}

// Timestamp returns the timestamp of the message
func (m *msgReleaseGroup) Timestamp() int {
	return m.Time
}

// Ownership returns InstanceID, ClientID, GroupID for message
func (m *msgReleaseGroup) Ownership() (string, string, string) {
	return m.InstanceID, m.ClientID, m.GroupID
}
