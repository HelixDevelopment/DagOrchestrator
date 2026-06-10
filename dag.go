// Package dag is a pure-data DAG scheduler: it consumes a directed-acyclic
// graph of nodes with declared dependencies, computes the ready-set in
// topological order, dispatches ready nodes onto a bounded worker pool honoring
// a parallelism cap, applies a per-node retry policy plus a failure policy
// (fail-fast vs continue-on-error), and records per-node output and lineage.
//
// It is agent-free (operates on opaque node payloads) so it is reusable by any
// project, not just agentic flows. Module path dev.helix.dag.
package dag

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Inputs maps an upstream node ID to that node's Output, supplied to a node's
// Execute once all its dependencies have completed successfully.
type Inputs map[string]Output

// Output is the opaque value a node produces.
type Output interface{}

// Node is any unit of work with an ID and a dependency list. Execute receives
// the outputs of every dependency.
type Node interface {
	ID() string
	DependsOn() []string
	Execute(ctx context.Context, in Inputs) (Output, error)
}

// Expandable is an optional interface a Node may implement: a completed node may
// emit additional nodes that are scheduled into the same run (dynamic DAG —
// bridges to flow-engine Send-style fan-out). Emitted nodes may depend on the
// expanding node or on any already-known node.
type Expandable interface {
	Expand(ctx context.Context, out Output) ([]Node, error)
}

// FailurePolicy governs what happens to the rest of the DAG when a node fails.
type FailurePolicy int

const (
	// FailFast cancels in-flight and not-yet-started nodes on the first failure.
	FailFast FailurePolicy = iota
	// ContinueOnError keeps scheduling nodes whose dependencies all succeeded;
	// any node with a failed (transitive) dependency is skipped.
	ContinueOnError
)

// RetryPolicy controls per-node retry with linear backoff.
type RetryPolicy struct {
	MaxAttempts int           // total attempts (1 = no retry)
	Backoff     time.Duration // delay between attempts
}

func (r RetryPolicy) attempts() int {
	if r.MaxAttempts < 1 {
		return 1
	}
	return r.MaxAttempts
}

// Options configures a scheduler Run.
type Options struct {
	Parallelism int           // max nodes executing concurrently (<=0 => 1)
	Failure     FailurePolicy // FailFast or ContinueOnError
	Retry       RetryPolicy   // per-node retry
}

// Edge records that From ran before To (lineage).
type Edge struct {
	From string
	To   string
}

// Result is the outcome of a Run.
type Result struct {
	Outputs map[string]Output // per successfully-completed node
	Lineage []Edge            // dependency edges actually traversed
	Failed  map[string]error  // per failed node
	Skipped []string          // nodes never run (cancelled or unmet deps)
}

// DAG is a validated acyclic graph of nodes.
type DAG struct {
	nodes map[string]Node
	order []string // deterministic node id order (for stable iteration)
}

// Build validates that the supplied nodes form a DAG (unique IDs, every
// declared dependency exists, no cycle) and returns it. A cycle or a dangling
// dependency is an error.
func Build(nodes []Node) (*DAG, error) {
	m := make(map[string]Node, len(nodes))
	order := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n == nil {
			return nil, errors.New("dag: nil node")
		}
		id := n.ID()
		if id == "" {
			return nil, errors.New("dag: node with empty ID")
		}
		if _, dup := m[id]; dup {
			return nil, fmt.Errorf("dag: duplicate node ID %q", id)
		}
		m[id] = n
		order = append(order, id)
	}
	sort.Strings(order)
	for id, n := range m {
		for _, dep := range n.DependsOn() {
			if _, ok := m[dep]; !ok {
				return nil, fmt.Errorf("dag: node %q depends on unknown node %q", id, dep)
			}
		}
	}
	if cyc := findCycle(m, order); cyc != "" {
		return nil, fmt.Errorf("dag: cycle detected involving %s", cyc)
	}
	return &DAG{nodes: m, order: order}, nil
}

// findCycle returns a non-empty description if the graph has a cycle.
func findCycle(m map[string]Node, order []string) string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(m))
	var stack []string
	var visit func(id string) string
	visit = func(id string) string {
		color[id] = gray
		stack = append(stack, id)
		for _, dep := range m[id].DependsOn() {
			switch color[dep] {
			case gray:
				return fmt.Sprintf("%v -> %s", append(append([]string{}, stack...), dep), dep)
			case white:
				if c := visit(dep); c != "" {
					return c
				}
			}
		}
		color[id] = black
		stack = stack[:len(stack)-1]
		return ""
	}
	for _, id := range order {
		if color[id] == white {
			if c := visit(id); c != "" {
				return c
			}
		}
	}
	return ""
}

// Scheduler runs a DAG.
type Scheduler interface {
	Run(ctx context.Context, d *DAG, opts Options) (*Result, error)
}

// NewScheduler returns the default in-memory scheduler.
func NewScheduler() Scheduler { return &scheduler{} }

type scheduler struct{}

// nodeState tracks runtime status of a node.
type nodeState struct {
	node      Node
	deps      map[string]struct{} // remaining unmet deps
	done      bool
	failed    bool
	out       Output
	tainted   bool // a dependency failed (ContinueOnError skip)
}

func (s *scheduler) Run(ctx context.Context, d *DAG, opts Options) (*Result, error) {
	if d == nil {
		return nil, errors.New("dag: nil DAG")
	}
	par := opts.Parallelism
	if par <= 0 {
		par = 1
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	res := &Result{
		Outputs: make(map[string]Output),
		Failed:  make(map[string]error),
	}

	mu := &sync.Mutex{}
	states := make(map[string]*nodeState, len(d.nodes))
	// dependents[x] = nodes that depend on x
	dependents := make(map[string][]string)
	for _, id := range d.order {
		n := d.nodes[id]
		deps := make(map[string]struct{})
		for _, dep := range n.DependsOn() {
			deps[dep] = struct{}{}
			dependents[dep] = append(dependents[dep], id)
		}
		states[id] = &nodeState{node: n, deps: deps}
	}

	sem := make(chan struct{}, par)
	var wg sync.WaitGroup
	var aborted bool // FailFast tripped

	// readyLocked returns ids whose deps are all satisfied and which are not
	// yet started/skipped. Caller holds mu.
	var started map[string]struct{} = make(map[string]struct{})
	readyLocked := func() []string {
		var r []string
		for _, id := range d.order {
			st := states[id]
			if st.done || st.failed || st.tainted {
				continue
			}
			if _, ok := started[id]; ok {
				continue
			}
			if len(st.deps) == 0 {
				r = append(r, id)
			}
		}
		return r
	}

	var dispatch func()
	dispatch = func() {
		mu.Lock()
		if aborted {
			mu.Unlock()
			return
		}
		ready := readyLocked()
		for _, id := range ready {
			started[id] = struct{}{}
		}
		mu.Unlock()

		for _, id := range ready {
			st := states[id]
			// Gather inputs (deps already completed -> outputs present).
			mu.Lock()
			in := make(Inputs)
			for _, dep := range st.node.DependsOn() {
				if o, ok := res.Outputs[dep]; ok {
					in[dep] = o
				}
			}
			mu.Unlock()

			wg.Add(1)
			select {
			case sem <- struct{}{}:
			case <-runCtx.Done():
				wg.Done()
				continue
			}
			go func(id string, st *nodeState, in Inputs) {
				defer wg.Done()
				defer func() { <-sem }()

				out, err := runWithRetry(runCtx, st.node, in, opts.Retry)

				mu.Lock()
				if err != nil {
					st.failed = true
					res.Failed[id] = err
					if opts.Failure == FailFast {
						aborted = true
						cancel()
						mu.Unlock()
						return
					}
					// ContinueOnError: taint transitive dependents.
					taintDependents(states, dependents, id)
					mu.Unlock()
				} else {
					st.done = true
					st.out = out
					res.Outputs[id] = out
					// Decrement deps of dependents + record lineage.
					for _, dep := range dependents[id] {
						delete(states[dep].deps, id)
						res.Lineage = append(res.Lineage, Edge{From: id, To: dep})
					}
					// Dynamic expansion.
					if exp, ok := st.node.(Expandable); ok {
						newNodes, eerr := exp.Expand(runCtx, out)
						if eerr != nil {
							st.failed = true
							res.Failed[id] = fmt.Errorf("expand: %w", eerr)
						} else {
							addNodes(d, states, dependents, started, res, newNodes)
						}
					}
					mu.Unlock()
				}
				dispatch()
			}(id, st, in)
		}
	}

	dispatch()
	wg.Wait()

	// Collect skipped (never completed and never failed).
	mu.Lock()
	for _, id := range d.order {
		st := states[id]
		if !st.done && !st.failed {
			res.Skipped = append(res.Skipped, id)
		}
	}
	sort.Strings(res.Skipped)
	mu.Unlock()

	if len(res.Failed) > 0 && opts.Failure == FailFast {
		return res, fmt.Errorf("dag: run aborted (fail-fast); %d node(s) failed", len(res.Failed))
	}
	if len(res.Failed) > 0 {
		return res, fmt.Errorf("dag: %d node(s) failed (continue-on-error)", len(res.Failed))
	}
	return res, nil
}

// addNodes wires dynamically-expanded nodes into the live run. Caller holds mu.
func addNodes(d *DAG, states map[string]*nodeState, dependents map[string][]string,
	started map[string]struct{}, res *Result, newNodes []Node) {
	for _, n := range newNodes {
		if n == nil {
			continue
		}
		id := n.ID()
		if _, exists := states[id]; exists {
			continue
		}
		d.nodes[id] = n
		d.order = append(d.order, id)
		sort.Strings(d.order)
		deps := make(map[string]struct{})
		for _, dep := range n.DependsOn() {
			// Only keep deps that are not already completed.
			if _, done := res.Outputs[dep]; !done {
				deps[dep] = struct{}{}
				dependents[dep] = append(dependents[dep], id)
			}
		}
		states[id] = &nodeState{node: n, deps: deps}
	}
}

// taintDependents marks every transitive dependent of a failed node as tainted
// (skipped under ContinueOnError). Caller holds mu.
func taintDependents(states map[string]*nodeState, dependents map[string][]string, failedID string) {
	queue := append([]string{}, dependents[failedID]...)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		st := states[id]
		if st.tainted {
			continue
		}
		st.tainted = true
		queue = append(queue, dependents[id]...)
	}
}

// runWithRetry runs a node up to RetryPolicy.MaxAttempts times.
func runWithRetry(ctx context.Context, n Node, in Inputs, rp RetryPolicy) (Output, error) {
	var lastErr error
	for attempt := 0; attempt < rp.attempts(); attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		out, err := n.Execute(ctx, in)
		if err == nil {
			return out, nil
		}
		lastErr = err
		if attempt+1 < rp.attempts() && rp.Backoff > 0 {
			select {
			case <-time.After(rp.Backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return nil, lastErr
}
