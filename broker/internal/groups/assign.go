// Package groups implements broker-side consumer-group coordination:
// membership, range-based partition assignment, heartbeat-driven liveness,
// and durable committed offsets.
//
// The data model is intentionally thin: groups exist only as long as
// their members do. Member state lives in memory and is rebuilt from
// rejoins on broker restart; only **committed offsets** are durable
// (via OffsetStore). This is the Kafka design with one simplification —
// real Kafka stores commits in a special compacted topic
// (`__consumer_offsets`); holocron's Stage 4 uses a JSON file in the data
// directory and revisits the topic-based approach in a later stage.
package groups

import (
	"sort"

	"github.com/jedi-knights/holocron/proto"
)

// PartitionsForFunc looks up the partition count for a topic. Injected
// by the broker so this package does not depend on the topic registry.
type PartitionsForFunc func(topic string) (int32, error)

// rangeAssign distributes partitions of one topic across members using
// the **range** strategy: each member gets a contiguous slice. Members
// are addressed in lexical order so a given (members, topics) pair always
// produces the same assignment — important so a rejoin does not shuffle
// every consumer.
//
// Example: 7 partitions across 3 members → [3, 2, 2].
func rangeAssign(memberIDs []string, topic string, partitionCount int32) map[string][]proto.PartitionRef {
	out := make(map[string][]proto.PartitionRef, len(memberIDs))
	if partitionCount <= 0 || len(memberIDs) == 0 {
		return out
	}

	sorted := append([]string(nil), memberIDs...)
	sort.Strings(sorted)

	n := int32(len(sorted))
	base := partitionCount / n
	extra := partitionCount % n

	cursor := int32(0)
	for i, id := range sorted {
		count := base
		if int32(i) < extra {
			count++
		}
		parts := make([]proto.PartitionRef, 0, count)
		for j := int32(0); j < count; j++ {
			parts = append(parts, proto.PartitionRef{Topic: topic, Index: cursor + j})
		}
		out[id] = parts
		cursor += count
	}
	return out
}

// assign computes the full assignment for a group: every member gets a
// (possibly empty) slice of PartitionRefs covering each subscribed topic.
func assign(memberIDs, topics []string, partitionsFor PartitionsForFunc) (map[string][]proto.PartitionRef, error) {
	out := make(map[string][]proto.PartitionRef, len(memberIDs))
	for _, id := range memberIDs {
		out[id] = nil
	}
	for _, topic := range topics {
		n, err := partitionsFor(topic)
		if err != nil {
			return nil, err
		}
		perMember := rangeAssign(memberIDs, topic, n)
		for id, parts := range perMember {
			out[id] = append(out[id], parts...)
		}
	}
	return out, nil
}

// roundRobinAssign distributes partitions of one topic across members
// using the **round-robin** strategy: partition i goes to member
// (i % memberCount). When partitionCount does not divide evenly, the
// extras land on the first members in lexical order — like range, but
// the partitions a member owns are strided rather than contiguous.
//
// Round-robin spreads load more evenly than range when partition count
// per topic is small relative to member count, or when several topics
// share a group with mismatched partition counts. Range groups
// contiguous partitions, which can concentrate hot keys (typically
// hashed to early indices) on a single consumer.
//
// Members are sorted lexically so the same (members, topic) pair always
// produces the same assignment — same determinism guarantee as range.
func roundRobinAssign(memberIDs []string, topic string, partitionCount int32) map[string][]proto.PartitionRef {
	out := make(map[string][]proto.PartitionRef, len(memberIDs))
	if partitionCount <= 0 || len(memberIDs) == 0 {
		return out
	}

	sorted := append([]string(nil), memberIDs...)
	sort.Strings(sorted)
	for _, id := range sorted {
		out[id] = nil
	}

	n := int32(len(sorted))
	for j := range partitionCount {
		owner := sorted[j%n]
		out[owner] = append(out[owner], proto.PartitionRef{Topic: topic, Index: j})
	}
	return out
}

// assignRoundRobin is the round-robin counterpart to assign: it computes
// the full per-member assignment across every subscribed topic.
func assignRoundRobin(memberIDs, topics []string, partitionsFor PartitionsForFunc) (map[string][]proto.PartitionRef, error) {
	out := make(map[string][]proto.PartitionRef, len(memberIDs))
	for _, id := range memberIDs {
		out[id] = nil
	}
	for _, topic := range topics {
		n, err := partitionsFor(topic)
		if err != nil {
			return nil, err
		}
		perMember := roundRobinAssign(memberIDs, topic, n)
		for id, parts := range perMember {
			out[id] = append(out[id], parts...)
		}
	}
	return out, nil
}

// assignSticky preserves as much of the prior assignment as possible
// while keeping per-member counts balanced. The algorithm:
//
//  1. Compute target counts per member: floor(total/members) for most,
//     ceil() for the lex-first `extras` members. (Same target shape as
//     range / round-robin so cluster-wide load matches.)
//  2. For each member that's still in the new member set, retain the
//     first `target` partitions from their prior assignment that still
//     exist on a subscribed topic. These records are never moved.
//  3. Collect every partition not retained in step 2 — this is the
//     "free pool" of partitions that need a new owner (because their
//     prior member departed, or the prior owner had more than target).
//  4. Distribute the free pool to members below their target,
//     lex-ordered, so successive rebalances with the same input always
//     produce the same answer.
//
// Sticky's whole value is in step 2: a returning member with a
// well-matched prior assignment keeps every partition it owned. New
// members and departures cause minimal churn, never a wholesale
// reshuffle.
func assignSticky(memberIDs, topics []string, partitionsFor PartitionsForFunc, prior map[string][]proto.PartitionRef) (map[string][]proto.PartitionRef, error) {
	all, err := allPartitions(topics, partitionsFor)
	if err != nil {
		return nil, err
	}

	out := make(map[string][]proto.PartitionRef, len(memberIDs))
	for _, id := range memberIDs {
		out[id] = nil
	}
	if len(memberIDs) == 0 || len(all) == 0 {
		return out, nil
	}

	sorted := append([]string(nil), memberIDs...)
	sort.Strings(sorted)

	total := int32(len(all))
	n := int32(len(sorted))
	base := total / n
	extras := total % n

	// Step 1: compute target counts.
	target := make(map[string]int32, len(sorted))
	for i, id := range sorted {
		t := base
		if int32(i) < extras {
			t++
		}
		target[id] = t
	}

	// Step 2: retain prior partitions, capped at target.
	taken := make(map[proto.PartitionRef]bool, len(all))
	valid := make(map[proto.PartitionRef]bool, len(all))
	for _, p := range all {
		valid[p] = true
	}
	for _, id := range sorted {
		for _, p := range prior[id] {
			if int32(len(out[id])) >= target[id] {
				break
			}
			if !valid[p] || taken[p] {
				continue
			}
			out[id] = append(out[id], p)
			taken[p] = true
		}
	}

	// Step 3: collect free partitions (those not retained), in stable
	// order so the output is deterministic across runs.
	var free []proto.PartitionRef
	for _, p := range all {
		if !taken[p] {
			free = append(free, p)
		}
	}

	// Step 4: distribute free partitions, lex-ordered by member.
	for _, p := range free {
		for _, id := range sorted {
			if int32(len(out[id])) < target[id] {
				out[id] = append(out[id], p)
				break
			}
		}
	}
	return out, nil
}

// allPartitions returns every (topic, partition) pair across the
// subscribed topics, in stable order: topics in the input order, then
// partitions ascending.
func allPartitions(topics []string, partitionsFor PartitionsForFunc) ([]proto.PartitionRef, error) {
	var out []proto.PartitionRef
	for _, topic := range topics {
		n, err := partitionsFor(topic)
		if err != nil {
			return nil, err
		}
		for i := int32(0); i < n; i++ {
			out = append(out, proto.PartitionRef{Topic: topic, Index: i})
		}
	}
	return out, nil
}
