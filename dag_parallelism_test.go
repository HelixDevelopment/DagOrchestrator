package dag

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestParallelismOne_ChainCompletes is the regression guard for the
// Parallelism:1 deadlock. A worker used to hold its single semaphore slot
// through the recursive dispatch() that schedules its own successor, so a
// dependency chain could never advance at par=1 (the successor waited forever
// on a slot the completed worker still held). The scheduler must complete a
// 3-node chain well within a deadline.
//
// RED (pre-fix): this Run hangs and the test fails on the 5s timeout.
// GREEN (post-fix): all three nodes complete.
func TestParallelismOne_ChainCompletes(t *testing.T) {
	nodes := []Node{
		&FuncNode{NodeID: "a", Fn: func(ctx context.Context, in Inputs) (Output, error) { return "a", nil }},
		&FuncNode{NodeID: "b", Deps: []string{"a"}, Fn: func(ctx context.Context, in Inputs) (Output, error) { return "b", nil }},
		&FuncNode{NodeID: "c", Deps: []string{"b"}, Fn: func(ctx context.Context, in Inputs) (Output, error) { return "c", nil }},
	}
	d, err := Build(nodes)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	done := make(chan *Result, 1)
	go func() {
		res, _ := NewScheduler().Run(context.Background(), d, Options{Parallelism: 1})
		done <- res
	}()
	select {
	case res := <-done:
		if len(res.Outputs) != 3 {
			t.Fatalf("par=1 chain: want 3 outputs, got %d (skipped=%v failed=%v)",
				len(res.Outputs), res.Skipped, res.Failed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("par=1 chain DEADLOCK: scheduler did not complete within 5s")
	}
}

// TestParallelismCap_Honored locks the invariant that the number of nodes
// executing concurrently never exceeds Options.Parallelism, while still
// achieving real parallelism (>1) for independent nodes. An atomic in-flight
// counter is the unforgeable oracle (no filesystem/timing heuristics).
//
// §1.1: if the semaphore cap were removed, peak would reach n (16) and the
// peak<=cap assertion would FAIL — it is not a tautology.
func TestParallelismCap_Honored(t *testing.T) {
	const n, cap = 16, 4
	var inflight, peak int64
	nodes := make([]Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = &FuncNode{NodeID: strconv.Itoa(i), Fn: func(ctx context.Context, in Inputs) (Output, error) {
			cur := atomic.AddInt64(&inflight, 1)
			for {
				p := atomic.LoadInt64(&peak)
				if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
					break
				}
			}
			time.Sleep(25 * time.Millisecond) // hold so peers coexist
			atomic.AddInt64(&inflight, -1)
			return nil, nil
		}}
	}
	d, err := Build(nodes)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := NewScheduler().Run(context.Background(), d, Options{Parallelism: cap}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if peak < 2 {
		t.Fatalf("expected real parallelism (peak>1), got %d", peak)
	}
	if peak > cap {
		t.Fatalf("cap violated: peak %d > Parallelism %d", peak, cap)
	}
}
