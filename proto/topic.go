package proto

// TopicConfig describes the per-topic configuration knobs the broker honors.
// The values here are what producers and operators see; internal broker state
// (segments, indexes, partition runtime data) lives in the broker module.
type TopicConfig struct {
	Name           string
	PartitionCount int32
	RetentionMs    int64
	SegmentBytes   int64
}

// PartitionRef identifies a partition within a topic. It is a stable address;
// nothing in proto holds the partition's runtime state.
type PartitionRef struct {
	Topic string
	Index int32
}
