package dag

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDiamond_ConcurrentThenOrdered proves the core scheduler contract on a
// diamond A -> {B,C} -> D: B and C run concurrently (overlapping wall-clock
// windows), and D runs strictly after BOTH B and C complete.
func TestDiamond_ConcurrentThenOrdered(t *testing.T) {
	var mu sync.Mutex
	type span struct{ start, end time.Time }
	spans := map[string]span{}

	mark := func(id string, d time.Duration) func(context.Context, Inputs) (Output, error) {
		return func(ctx context.Context, in Inputs) (Output, error) {
			s := time.Now()
			time.Sleep(d)
			e := time.Now()
			mu.Lock()
			spans[id] = span{s, e}
			mu.Unlock()
			return id, nil
		}
	}

	nodes := []Node{
		&FuncNode{NodeID: "A", Fn: mark("A", 10*time.Millisecond)},
		&FuncNode{NodeID: "B", Deps: []string{"A"}, Fn: mark("B", 60*time.Millisecond)},
		&FuncNode{NodeID: "C", Deps: []string{"A"}, Fn: mark("C", 10*time.Millisecond)},
		&FuncNode{NodeID: "D", Deps: []string{"B", "C"}, Fn: mark("D", 10*time.Millisecond)},
	}
	d, err := Build(nodes)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	res, err := NewScheduler().Run(context.Background(), d, Options{Parallelism: 4})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Outputs) != 4 {
		t.Fatalf("expected 4 outputs, got %d (%v)", len(res.Outputs), res.Outputs)
	}

	// B and C overlap in time (ran concurrently).
	b, c := spans["B"], spans["C"]
	if c.start.After(b.end) || b.start.After(c.end) {
		t.Fatalf("B and C did NOT run concurrently: B[%v,%v] C[%v,%v]", b.start, b.end, c.start, c.end)
	}
	// D starts strictly after both B and C ended.
	dd := spans["D"]
	if dd.start.Before(b.end) {
		t.Fatalf("D started before B finished: D.start=%v B.end=%v", dd.start, b.end)
	}
	if dd.start.Before(c.end) {
		t.Fatalf("D started before C finished: D.start=%v C.end=%v", dd.start, c.end)
	}
}

// TestFailFast_CancelsAndGates proves a planted failure in B prevents D and
// cancels in-flight C under FailFast.
func TestFailFast_CancelsAndGates(t *testing.T) {
	var cCancelled atomic.Bool
	nodes := []Node{
		&FuncNode{NodeID: "A", Fn: func(ctx context.Context, in Inputs) (Output, error) { return nil, nil }},
		&FuncNode{NodeID: "B", Deps: []string{"A"}, Fn: func(ctx context.Context, in Inputs) (Output, error) {
			return nil, errors.New("planted failure in B")
		}},
		&FuncNode{NodeID: "C", Deps: []string{"A"}, Fn: func(ctx context.Context, in Inputs) (Output, error) {
			select {
			case <-time.After(500 * time.Millisecond):
				return "C", nil
			case <-ctx.Done():
				cCancelled.Store(true)
				return nil, ctx.Err()
			}
		}},
		&FuncNode{NodeID: "D", Deps: []string{"B", "C"}, Fn: func(ctx context.Context, in Inputs) (Output, error) {
			t.Error("D MUST NOT run under FailFast when B fails")
			return "D", nil
		}},
	}
	d, _ := Build(nodes)
	res, err := NewScheduler().Run(context.Background(), d, Options{Parallelism: 4, Failure: FailFast})
	if err == nil {
		t.Fatal("expected fail-fast error, got nil")
	}
	if _, ok := res.Failed["B"]; !ok {
		t.Fatalf("expected B in Failed, got %v", res.Failed)
	}
	if _, ran := res.Outputs["D"]; ran {
		t.Fatal("D ran despite FailFast")
	}
	if !cCancelled.Load() {
		t.Fatal("C was not cancelled under FailFast")
	}
}

// TestContinueOnError_RunsIndependentPath proves ContinueOnError keeps running
// the independent branch and populates Failed.
func TestContinueOnError_RunsIndependentPath(t *testing.T) {
	nodes := []Node{
		&FuncNode{NodeID: "bad", Fn: func(ctx context.Context, in Inputs) (Output, error) {
			return nil, errors.New("boom")
		}},
		&FuncNode{NodeID: "downstream", Deps: []string{"bad"}, Fn: func(ctx context.Context, in Inputs) (Output, error) {
			t.Error("downstream of failed node MUST be skipped")
			return nil, nil
		}},
		&FuncNode{NodeID: "independent", Fn: func(ctx context.Context, in Inputs) (Output, error) {
			return "ok", nil
		}},
	}
	d, _ := Build(nodes)
	res, _ := NewScheduler().Run(context.Background(), d, Options{Parallelism: 4, Failure: ContinueOnError})
	if _, ok := res.Failed["bad"]; !ok {
		t.Fatalf("expected bad in Failed, got %v", res.Failed)
	}
	if res.Outputs["independent"] != "ok" {
		t.Fatalf("independent branch did not run: %v", res.Outputs)
	}
	if _, ran := res.Outputs["downstream"]; ran {
		t.Fatal("downstream of failed node ran")
	}
}

// TestInputsPropagate proves a node receives its dependency outputs.
func TestInputsPropagate(t *testing.T) {
	nodes := []Node{
		&FuncNode{NodeID: "x", Fn: func(ctx context.Context, in Inputs) (Output, error) { return 7, nil }},
		&FuncNode{NodeID: "y", Deps: []string{"x"}, Fn: func(ctx context.Context, in Inputs) (Output, error) {
			v, ok := in["x"].(int)
			if !ok || v != 7 {
				return nil, errors.New("missing dep input")
			}
			return v * 2, nil
		}},
	}
	d, _ := Build(nodes)
	res, err := NewScheduler().Run(context.Background(), d, Options{Parallelism: 2})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outputs["y"] != 14 {
		t.Fatalf("expected y=14, got %v", res.Outputs["y"])
	}
}

// TestRetry proves a flaky node succeeds within MaxAttempts.
func TestRetry(t *testing.T) {
	var n int32
	nodes := []Node{
		&FuncNode{NodeID: "flaky", Fn: func(ctx context.Context, in Inputs) (Output, error) {
			if atomic.AddInt32(&n, 1) < 3 {
				return nil, errors.New("transient")
			}
			return "stable", nil
		}},
	}
	d, _ := Build(nodes)
	res, err := NewScheduler().Run(context.Background(), d, Options{Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outputs["flaky"] != "stable" {
		t.Fatalf("retry did not succeed: %v", res.Outputs)
	}
}

// TestCycleRejected proves Build rejects a cyclic graph.
func TestCycleRejected(t *testing.T) {
	nodes := []Node{
		&FuncNode{NodeID: "p", Deps: []string{"q"}, Fn: func(ctx context.Context, in Inputs) (Output, error) { return nil, nil }},
		&FuncNode{NodeID: "q", Deps: []string{"p"}, Fn: func(ctx context.Context, in Inputs) (Output, error) { return nil, nil }},
	}
	if _, err := Build(nodes); err == nil {
		t.Fatal("expected cycle error, got nil")
	}
}

// TestUnknownDepRejected proves Build rejects a dangling dependency.
func TestUnknownDepRejected(t *testing.T) {
	nodes := []Node{
		&FuncNode{NodeID: "a", Deps: []string{"ghost"}, Fn: func(ctx context.Context, in Inputs) (Output, error) { return nil, nil }},
	}
	if _, err := Build(nodes); err == nil {
		t.Fatal("expected unknown-dep error, got nil")
	}
}

// TestExpand proves a completed node can dynamically emit a new node that runs.
func TestExpand(t *testing.T) {
	var childRan atomic.Bool
	nodes := []Node{
		&FuncNode{
			NodeID: "seed",
			Fn:     func(ctx context.Context, in Inputs) (Output, error) { return "seed-out", nil },
			OnExpand: func(ctx context.Context, out Output) ([]Node, error) {
				return []Node{
					&FuncNode{NodeID: "child", Deps: []string{"seed"}, Fn: func(ctx context.Context, in Inputs) (Output, error) {
						childRan.Store(true)
						return "child-out", nil
					}},
				}, nil
			},
		},
	}
	d, _ := Build(nodes)
	res, err := NewScheduler().Run(context.Background(), d, Options{Parallelism: 2})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !childRan.Load() {
		t.Fatal("dynamically-expanded child did not run")
	}
	if res.Outputs["child"] != "child-out" {
		t.Fatalf("child output missing: %v", res.Outputs)
	}
}
