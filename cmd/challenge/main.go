// Command challenge is the anti-bluff runtime driver for dev.helix.dag.
// It builds a real diamond DAG A -> {B,C} -> D and emits captured runtime
// evidence (start/end timestamps + ordering) so an external script can assert
// real concurrency + ordering — not a single timestamp, not "it ran".
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	dag "dev.helix.dag"
)

func main() {
	mode := "happy"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	var mu sync.Mutex
	events := map[string][2]time.Time{} // id -> {start,end}
	var cCancelled bool
	t0 := time.Now()

	mark := func(id string, d time.Duration) func(context.Context, dag.Inputs) (dag.Output, error) {
		return func(ctx context.Context, in dag.Inputs) (dag.Output, error) {
			s := time.Now()
			select {
			case <-time.After(d):
			case <-ctx.Done():
				if id == "C" {
					mu.Lock()
					cCancelled = true
					mu.Unlock()
				}
				return nil, ctx.Err()
			}
			e := time.Now()
			mu.Lock()
			events[id] = [2]time.Time{s, e}
			mu.Unlock()
			return id, nil
		}
	}

	bFn := mark("B", 80*time.Millisecond)
	if mode == "fail" {
		bFn = func(ctx context.Context, in dag.Inputs) (dag.Output, error) {
			s := time.Now()
			mu.Lock()
			events["B"] = [2]time.Time{s, s}
			mu.Unlock()
			return nil, errors.New("planted failure in B")
		}
	}

	nodes := []dag.Node{
		&dag.FuncNode{NodeID: "A", Fn: mark("A", 10*time.Millisecond)},
		&dag.FuncNode{NodeID: "B", Deps: []string{"A"}, Fn: bFn},
		&dag.FuncNode{NodeID: "C", Deps: []string{"A"}, Fn: mark("C", 400*time.Millisecond)},
		&dag.FuncNode{NodeID: "D", Deps: []string{"B", "C"}, Fn: mark("D", 10*time.Millisecond)},
	}

	d, err := dag.Build(nodes)
	if err != nil {
		fmt.Printf("BUILD_ERROR %v\n", err)
		os.Exit(2)
	}

	failure := dag.FailFast
	res, runErr := dag.NewScheduler().Run(context.Background(), d, dag.Options{Parallelism: 4, Failure: failure})

	rel := func(t time.Time) int64 { return t.Sub(t0).Milliseconds() }
	mu.Lock()
	defer mu.Unlock()
	for _, id := range []string{"A", "B", "C", "D"} {
		if ev, ok := events[id]; ok {
			fmt.Printf("EVENT %s start_ms=%d end_ms=%d\n", id, rel(ev[0]), rel(ev[1]))
		} else {
			fmt.Printf("EVENT %s not_run\n", id)
		}
	}
	for id := range res.Outputs {
		fmt.Printf("OUTPUT %s\n", id)
	}
	for id := range res.Failed {
		fmt.Printf("FAILED %s\n", id)
	}
	fmt.Printf("C_CANCELLED %v\n", cCancelled)
	if runErr != nil {
		fmt.Printf("RUN_ERR %v\n", runErr)
	}
	fmt.Println("DONE")
}
