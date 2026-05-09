package connect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// coordGroupPrefix namespaces consumer groups created for source-task
// coordination so they don't collide with the connector's own
// group-mode usage (e.g., a sink connector and source connector that
// happen to share a name).
const coordGroupPrefix = "connect-source-coord-"

// coordHeartbeatInterval is how often the coord consumer pings the
// broker. Tighter than the SDK default because rebalance latency
// directly bounds the duplicate-production window: a stale "I own
// this partition" lasts at most one heartbeat after a peer joins or
// leaves.
const coordHeartbeatInterval = 200 * time.Millisecond

// topicEnsurer is the duck-typed interface a Transport must satisfy for
// the coord topic to be auto-created on Start. The inproc transport
// implements it via the broker's topic registry; transports without
// topic-management semantics fall through to a "topic must exist"
// requirement that surfaces as a Subscribe error.
type topicEnsurer interface {
	EnsureTopic(ctx context.Context, name string, partitions int32) error
}

// runCoordinatedSource is the lifetime of one source mount whose
// AddSource was given WithSourceCoordTopic. It joins a consumer group
// on the coord topic; partition assignments map directly to task
// indices. On rebalance the runner stops tasks for revoked indices and
// starts tasks for newly-assigned ones.
//
// The coord topic carries no real payload — it exists only to lend its
// partition-assignment machinery to source coordination. A
// single-partition coord topic gives leader-election (one owner runs
// every task slot); a maxTasks-partition coord topic distributes task
// slots across the worker pool.
func (w *Worker) runCoordinatedSource(ctx context.Context, mount sourceMount, producer *sdk.Producer) {
	defer w.wg.Done()

	// Auto-create the coord topic if the transport supports it.
	// Operators running against transports without topic management
	// must pre-create the topic externally; the subsequent Subscribe
	// will surface that case as an error.
	if et, ok := w.transport.(topicEnsurer); ok {
		if err := et.EnsureTopic(ctx, mount.coordTopic, int32(mount.maxTasks)); err != nil && !errors.Is(err, context.Canceled) {
			w.recordErr(fmt.Errorf("source %q ensure coord topic %q: %w", mount.connector.Name(), mount.coordTopic, err))
			return
		}
	}

	state := &coordState{
		worker:   w,
		mount:    mount,
		producer: producer,
		running:  make(map[int]context.CancelFunc),
	}

	// onAssign is the single source of truth for which task indices run
	// on this worker. We deliberately do not register a revoke listener:
	// the SDK rejoin path fires revoke→assign back-to-back, and acting
	// on revoke would needlessly tear down tasks that the upcoming
	// assign restores. Letting onAssign diff the new and current sets
	// avoids that thrash and keeps source state (open files, cursors)
	// continuous when this worker keeps the same partitions across a
	// rebalance.
	consumer, err := sdk.NewConsumer(
		w.transport,
		sdk.WithGroup(coordGroupPrefix+mount.connector.Name()),
		sdk.WithAssignListener(state.onAssign),
		sdk.WithHeartbeatInterval(coordHeartbeatInterval),
	)
	if err != nil {
		w.recordErr(fmt.Errorf("source %q coord consumer: %w", mount.connector.Name(), err))
		return
	}
	defer consumer.Close()

	if err := consumer.Subscribe(ctx, mount.coordTopic, 0); err != nil {
		if !errors.Is(err, context.Canceled) {
			w.recordErr(fmt.Errorf("source %q coord subscribe %q: %w", mount.connector.Name(), mount.coordTopic, err))
		}
		return
	}

	// Park until the worker stops. The coord consumer's heartbeat
	// goroutine handles rebalances via the assign/revoke callbacks; we
	// just need to keep the consumer alive and stop assigned tasks
	// when ctx cancels.
	<-ctx.Done()
	state.stopAll()
}

// coordState tracks the per-mount running source tasks under
// coordination. running[i] is the cancel func for task index i; absence
// means "not currently assigned to this worker."
type coordState struct {
	worker   *Worker
	mount    sourceMount
	producer *sdk.Producer

	mu      sync.Mutex
	running map[int]context.CancelFunc
}

// onAssign reconciles the running task set with the new assignment.
// It diffs assigned-vs-running, starting tasks for newly-arrived
// indices and stopping tasks whose index is no longer assigned. Tasks
// that survive the rebalance — same index in both old and new — keep
// running with their existing source state intact.
func (s *coordState) onAssign(ctx context.Context, assigned []proto.PartitionRef) error {
	wanted := make(map[int]struct{}, len(assigned))
	for _, p := range assigned {
		wanted[int(p.Index)] = struct{}{}
	}

	s.mu.Lock()
	// Stop indices that were running but are no longer assigned.
	for idx, cancel := range s.running {
		if _, keep := wanted[idx]; keep {
			continue
		}
		cancel()
		delete(s.running, idx)
	}
	// Identify newly-assigned indices we need to start.
	var toStart []int
	for idx := range wanted {
		if _, already := s.running[idx]; already {
			continue
		}
		toStart = append(toStart, idx)
	}
	s.mu.Unlock()

	if len(toStart) == 0 {
		return nil
	}
	tasks, err := s.mount.connector.Tasks(s.mount.maxTasks)
	if err != nil {
		return fmt.Errorf("connect: source %q tasks: %w", s.mount.connector.Name(), err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, idx := range toStart {
		// Re-check after re-acquiring the lock: a concurrent revoke
		// could have removed the index from `wanted` semantics, though
		// our flow does not currently produce that race.
		if _, already := s.running[idx]; already {
			continue
		}
		if idx >= len(tasks) {
			return fmt.Errorf("connect: source %q assigned task index %d but connector returned %d tasks", s.mount.connector.Name(), idx, len(tasks))
		}
		task := tasks[idx]
		var stored []map[string]any
		if s.worker.offsetStore != nil {
			loaded, err := s.worker.offsetStore.Load(ctx, s.mount.connector.Name(), idx)
			if err != nil {
				return fmt.Errorf("connect: source %q task %d offset load: %w", s.mount.connector.Name(), idx, err)
			}
			stored = loaded
		}
		if err := task.Init(ctx, stored); err != nil {
			return fmt.Errorf("connect: source %q task %d init: %w", s.mount.connector.Name(), idx, err)
		}
		taskCtx, cancel := context.WithCancel(ctx)
		s.running[idx] = cancel
		s.worker.wg.Add(1)
		go s.worker.runSourceTask(taskCtx, s.mount.connector.Name(), idx, task, s.producer)
	}
	return nil
}

// stopAll cancels every running task. Called when the Worker shuts
// down so the source goroutines exit promptly.
func (s *coordState) stopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for idx, cancel := range s.running {
		cancel()
		delete(s.running, idx)
	}
}
