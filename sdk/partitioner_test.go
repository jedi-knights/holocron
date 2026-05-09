package sdk

import (
	"sync"
	"testing"

	"github.com/jedi-knights/holocron/proto"
)

func TestDefaultPartitioner_KeyHashIsStable(t *testing.T) {
	p := &DefaultPartitioner{}
	r := proto.Record{Key: []byte("user-42")}
	first := p.Partition(r, 8)
	for range 100 {
		if got := p.Partition(r, 8); got != first {
			t.Fatalf("key routing not stable: got %d then %d", first, got)
		}
	}
}

func TestDefaultPartitioner_NoKeyRoundRobins(t *testing.T) {
	p := &DefaultPartitioner{}
	const n = 4
	counts := make(map[int32]int)
	for range 4 * n {
		counts[p.Partition(proto.Record{}, n)]++
	}
	if len(counts) != n {
		t.Fatalf("expected all %d partitions hit, got %d", n, len(counts))
	}
}

func TestDefaultPartitioner_ZeroPartitionsReturnsZero(t *testing.T) {
	p := &DefaultPartitioner{}
	if got := p.Partition(proto.Record{Key: []byte("k")}, 0); got != 0 {
		t.Fatalf("expected 0 for 0 partitions, got %d", got)
	}
}

func TestDefaultPartitioner_ConcurrentSafe(t *testing.T) {
	p := &DefaultPartitioner{}
	var wg sync.WaitGroup
	const goroutines = 8
	const each = 1000
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range each {
				p.Partition(proto.Record{}, 16)
			}
		}()
	}
	wg.Wait()
}
