# Kafka Only Consumer Coordination

This is a description of the consumer coordination protocol implemented by the Marshal
library.

## Synopsis

It is possible to coordinate N consumers without any shared state other than what Kafka
provides. Without using Zookeeper or any other such coordination system, and still provide
similar guarantees/functionality.

The essence of this approach is that we can use a new topic in Kafka such as `__marshal`
as a write-ahead log/transaction log and use it for constructing a race-safe consumer
coordination protocol. Since Kafka guarantees ordering within a partition, outside of an
unclean leader election we can safely coordinate consumers.

The goal is for this protocol to be robust to all failure cases. The goal is *not* for the
protocol to be the absolute fastest thing out there.

## Protocol Messages

This section defines the messages used in the protocol.

In the following definitions, certain bolded words are used to define parameters to
the message:

- **client_id** is an arbitrary string.
  - This value should be unique within a **group_id** (see below).
  - This can be random, but, you might want to make it predictable for your set of consumers.
    If you do, you gain the property that your consumer can restart where it left off if
    you restart it (since it can resume its own heartbeats as long as the **client_id** is
    stable).
- **group_id** is a namespaced opaque string. I.e., if you are the foo team,
  you should use a value such as `foo.bar_consumer`.
- **topic**, **desired_topic** is a string of the topic name, this is also namespaced
  by your team.
- **partition**, **desired_partition** is an integer as provided by Kafka.
- **last_offset**, **proposed_last_offset** is an integer representing a message offset as
  provided by Kafka. This should never be generated on your own.

Some constants defined in the protocol

- *HeartbeatInterval* is the maximum allowed time between two heartbeats. Consumers are expected
  to send heartbeat messages once per interval. The smaller this number is, the busier the
  coordination topic will be, but the faster failure recovery will be.

The protocol is defined with several simple messages:

1. `Heartbeat` which includes **client_id**, **group_id**, **topic**, **partition**,
   **last_offset**. These are sent at most every **HeartbeatInterval** seconds apart.
1. `ClaimingPartition` which includes **client_id**, **group_id**, **topic**, **partition**
   and is used as the initial request stating that you wish to claim a partition.
1. `ReleasingPartition` which includes **client_id**, **group_id**, **topic**,
   **partition**, **last_offset** and is used when a consumer wants to proactively release
   a partition.
1. `ClaimingMessages` which includes **client_id**, **group_id**, **topic**,
   **partition**, **proposed_last_offset** is used for the At Most Once consumption flow.
1. `ReleaseGroup` which includes **client_id**, **group_id**, **msg_expire_time**. This message
   is sent by a special Admin actor, which can pause an entire consumer group identified
   by the **group_id**, until **msg_expire_time**.

## Determining World State

This is the primary engine of Marshal. The "rationalizer" will read the messages in the
coordination topic and calculate a "world state" given the sequence of messages in the log.
The algorithm works based on everybody coming to the same conclusion about the world state
given the same log, i.e., every state transition is determined solely by the messages in
the logs and their relative ordering.

The state is calculated, for a given **topic**/**partition** you wish to know about,
by fully consuming the data from the coordination topic (which should be relatively
minimal and fast to process) and constructing a current state of the world (as of the
last message you have).

You can know what consumers exist (actively) based on the heartbeats and partition messages.

### Heartbeats

Every consumer is required to heartbeat every **HeartbeatInterval** seconds.

A client is considered fresh when less than **HeartbeatInterval** seconds have elapsed
since the last heartbeat.

A client is considered to be in an unknown state when **HeartbeatInterval** to *twice that
value* seconds have elapsed.

A client is considered stale when *more than twice* **HeartbeatInterval** seconds have
elapsed and no further heartbeat has been received.

## Partition Assignment for Consumption

This is the meat of the system and the reason for such an algorithm. Being able to safely
assign partitions to consumers such that they can process messages with the desired
properties is non-trivial and requires this coordination.

### Consuming a New Partition

This assumes that you want to start consuming a new partition.

1. Pick a partition to try to claim
  1. This is done by whatever method you choose. Random, round-robin, etc.
1. Pick coordinating partition based on
   `hash(desired_topic, desired_partition) % number of partitions in coordinating topic`
1. Determine state of world on chosen coordinating partition
  1. If the partition you wish to claim is already claimed, and the heartbeat for
     that partition is not stale, return to step 1 (remember, "stale" is defined as
     twice the **HeartbeatInterval**)
  1. Since the heartbeat is stale, this consumer may continue to step 4 and attempt to claim
1. Send a `ClaimingPartition` message
1. Re-determine the state of the world (read up to the end)
  1. Look for the earliest `ClaimingPartition` message associated with the desired
     topic/partition, if it was ours (message from step 4) then continue
  1. If somebody else won the race, return to step 1
1. If the current client wins (has the earliest claim), send an immediate `Heartbeat`
   message and consumption on desired topic/partition can begin

This process, assuming no Kafka data loss (we'll have to carefully make sure to produce with the right options), should guarantee safe partition assignment.

### Consuming Recently Used Partitions on Restart

This is an optimization to help prevent churn of consumption. If you define your consumer
such that you have a predictable **client_id** and it is unique within your consumer
group, you can use that to determine what partitions your client was previously consuming.

1. Determine the complete state of the world
  1. This requires scanning "recent" events for the entire coordination topic (all partitions)
1. If heartbeats are found for the current **client_id**+**group_id**, and if those
   heartbeats are fresh (only), then send a new heartbeat and recover state
  1. Note: The previous heartbeats should contain enough information to continue where you
     left off (modulo the guarantees of ALO/AMO consumption)

## Consumption

There are two main algorithms for message processing. Both of these assume that your client
*already has a valid claim to a partition* that you are going to be consuming from.

### At Most Once (AMO)

The semantics of at-most-once consumption are that you would prefer to consume a message
zero (0) times (never see it) than to consume it more than once.

To do safely, we use the same linear nature of the Kafka partitions to make a
transactional guarantee:

1. Determine that we still have the claim to this partition
1. Fetch a batch of messages
  1. Batch size should be adjusted for the QPS of your category, the smaller your batches
     the more traffic against the coordination topic, but the fewer you lose in the failure
     case
1. Produce a `ClaimingMessages` message with the last offset from our batch of messages
1. Re-determine the state of the world
  1. Validate we still hold the claim on this partition
  1. Validate that our claim of the messages from step 2 is in the log
1. Send `Heartbeat` with the offset to "commit" the transaction
1. Process the messages in this batch

Assuming again that Kafka is durable and we use the right settings, this should provide the
guarantees we want for at-most-once. Any failure along the way will be handled by either a
normal heartbeat-expire-retry loop (steps 1-4 fail) and if we fail during step 5 then that
batch of messages will be dropped per AMO semantics.

### At Least Once (ALO)

Much easier than at-most-once, still assuming we have the claim to a partition:

1. Determine that we still have the claim to this partition
1. Fetch a batch of messages
1. Process batch of messages
1. Every **HeartbeatInterval**, send a `Heartbeat` message with the last processed offset

As long as you heartbeat every interval, failure is constrained to only re-process at most
one single **HeartbeatInterval** of messages.

### Consumer Failure

If a consumer stops reporting heartbeats, other consumers can pick up that partition.
In essence, if no `Heartbeat` messages have arrived on a partition for twice the
**HeartbeatInterval**, then whichever consumers are looking for partitions will attempt
to claim that partition, starting that whole process.

In the ALO consumption case, this can lead to two consumers running on a single batch
of messages at the same time, but it is constrained to one batch. The AMO consumer cannot
have that failure case, at worst it will never process some messages.

# TODO

I believe a Kafka-service system would want to consume messages off of a partition but
not necessarily take a whole lock on the partition. I.e. just saying "I claim message
offsets X-Y". You can also then fix latency issues by pre-claiming ranges so that the
instant they become used you've already negotiated the lock on that range and can
start processing them/handing them out?
