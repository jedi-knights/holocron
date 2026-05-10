package cluster

import (
	"context"
	"fmt"

	"github.com/jedi-knights/holocron/broker/internal/storage"
	"github.com/jedi-knights/holocron/proto"
)

// SyncPeer is the minimal capability SyncPartitionFromPeer needs:
// stream records from a partition starting at an offset, and
// query the partition's high-water. Both inproc.Transport and
// sdk/net.Transport satisfy this — Stage 9's join hook can sync
// against the cluster's leader over either transport.
type SyncPeer interface {
	Subscribe(ctx context.Context, p proto.PartitionRef, fromOffset int64) (<-chan proto.Record, <-chan error, error)
	HighWater(ctx context.Context, p proto.PartitionRef) (int64, error)
}

// SyncPartitionFromPeer streams records from peer's partition p
// into local, starting at local's current high-water and stopping
// when local catches up to peer's high-water at call time. Returns
// the number of records appended.
//
// Bounded by peer.HighWater at call time. Records produced on the
// peer after this call don't transfer; the Stage 9 caller follows
// up with the steady-state Raft Apply path for ongoing
// replication. The dedup guard in FSM.applyAppend (milestone 2)
// drops Apply entries whose Offset is already covered by the sync.
//
// Goes directly through storage.Store, bypassing the FSM. The
// caller must ensure local hasn't been populated past peer's
// high-water; a fresh follower in the M4 join path satisfies this
// by construction (the local store starts empty / at the snapshot
// state).
func SyncPartitionFromPeer(
	ctx context.Context,
	peer SyncPeer,
	local storage.Store,
	p proto.PartitionRef,
) (int, error) {
	target, err := peer.HighWater(ctx, p)
	if err != nil {
		return 0, fmt.Errorf("cluster: peer high-water for %v: %w", p, err)
	}
	localHW, err := local.HighWater(ctx, p)
	if err != nil {
		return 0, fmt.Errorf("cluster: local high-water for %v: %w", p, err)
	}
	if localHW >= target {
		return 0, nil
	}

	// Subscribe is open-ended (the channel keeps streaming as the
	// peer produces more); cancel the sub-context as soon as we
	// reach target so the peer-side pump exits cleanly.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	records, errCh, err := peer.Subscribe(subCtx, p, localHW)
	if err != nil {
		return 0, fmt.Errorf("cluster: subscribe peer %v: %w", p, err)
	}

	appended := 0
	for {
		select {
		case r, ok := <-records:
			if !ok {
				return appended, nil
			}
			if _, err := local.Append(ctx, p, r); err != nil {
				return appended, fmt.Errorf("cluster: local append %v: %w", p, err)
			}
			appended++
			// Re-check after every append. Subscribe doesn't tell
			// us "you've reached the original target"; the local
			// store does.
			hw, err := local.HighWater(ctx, p)
			if err != nil {
				return appended, fmt.Errorf("cluster: local high-water for %v: %w", p, err)
			}
			if hw >= target {
				return appended, nil
			}
		case err, ok := <-errCh:
			if ok && err != nil {
				return appended, fmt.Errorf("cluster: peer subscribe error %v: %w", p, err)
			}
		case <-ctx.Done():
			return appended, ctx.Err()
		}
	}
}
