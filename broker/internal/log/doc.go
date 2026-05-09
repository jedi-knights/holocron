// Package log implements the on-disk append-only log: a sequence of segment
// files with companion sparse indexes. Each partition owns one PartitionLog;
// each PartitionLog contains an ordered list of segments. The active
// segment is the only file open for writing. Older segments are immutable
// and may be deleted in their entirety by retention policy.
//
// The on-disk format is described in docs/stage-2.md and docs/data-model.md.
package log
