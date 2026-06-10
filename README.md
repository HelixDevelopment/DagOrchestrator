# DagOrchestrator — `dev.helix.dag`

| Field | Value |
|-------|-------|
| Module | `dev.helix.dag` |
| Revision | 1 |
| Created | 2026-06-10 |
| Last modified | 2026-06-10 |
| Status | active |
| Status summary | Initial scaffold: working in-memory DAG scheduler + 8 passing race tests + anti-bluff Challenge. |
| Org | HelixDevelopment |
| Visibility | public |
| Go | 1.26 |

A generic, reusable, **agent-free** pure-data **DAG scheduler** for Go. Give it
a directed-acyclic graph of nodes with declared dependencies; it dispatches the
ready-set in topological order onto a bounded worker pool, honors a parallelism
cap, applies per-node retry/backoff and a failure policy, records per-node
output + lineage, and supports dynamic node expansion.

## Table of contents

- [Why](#why)
- [Install](#install)
- [Quick start](#quick-start)
- [Core interfaces](#core-interfaces)
- [Failure policies](#failure-policies)
- [Dynamic expansion](#dynamic-expansion)
- [Testing & anti-bluff Challenge](#testing--anti-bluff-challenge)
- [Governance](#governance)

## Why

Many projects have dependency *data* and a *linear* step runner but no
**scheduler** that turns "these N tasks with these deps" into "dispatch the
ready ones in parallel, gate the rest, propagate failure, record lineage." No
single dominant Go-native general-purpose DAG scheduler exists; projects either
embed a heavyweight workflow engine or hand-roll topo-sort over a pool. This
module is the small, reusable, standalone answer.

## Install

```bash
go get dev.helix.dag
```

## Quick start

```go
package main

import (
	"context"
	"fmt"

	dag "dev.helix.dag"
)

func main() {
	nodes := []dag.Node{
		&dag.FuncNode{NodeID: "fetch", Fn: func(ctx context.Context, in dag.Inputs) (dag.Output, error) {
			return "data", nil
		}},
		&dag.FuncNode{NodeID: "parse", Deps: []string{"fetch"}, Fn: func(ctx context.Context, in dag.Inputs) (dag.Output, error) {
			return fmt.Sprintf("parsed(%v)", in["fetch"]), nil
		}},
		&dag.FuncNode{NodeID: "validate", Deps: []string{"fetch"}, Fn: func(ctx context.Context, in dag.Inputs) (dag.Output, error) {
			return "valid", nil
		}},
		&dag.FuncNode{NodeID: "write", Deps: []string{"parse", "validate"}, Fn: func(ctx context.Context, in dag.Inputs) (dag.Output, error) {
			return "written", nil
		}},
	}

	d, err := dag.Build(nodes) // validates acyclicity + dep existence
	if err != nil {
		panic(err)
	}
	res, err := dag.NewScheduler().Run(context.Background(), d, dag.Options{
		Parallelism: 4,
		Failure:     dag.FailFast,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(res.Outputs["write"]) // -> written
}
```

`parse` and `validate` run concurrently (both depend only on `fetch`); `write`
runs strictly after both complete.

## Core interfaces

```go
type Node interface {
	ID() string
	DependsOn() []string
	Execute(ctx context.Context, in Inputs) (Output, error)
}

type Scheduler interface {
	Run(ctx context.Context, d *DAG, opts Options) (*Result, error)
}

func Build(nodes []Node) (*DAG, error)
func NewScheduler() Scheduler
```

`Result` carries `Outputs map[string]Output`, `Lineage []Edge`,
`Failed map[string]error`, and `Skipped []string`.

## Failure policies

- `FailFast` — on the first node failure, cancel in-flight nodes and stop
  scheduling. The run returns an error; `Result.Failed` names the failing node.
- `ContinueOnError` — keep running nodes whose dependencies all succeeded; any
  node with a failed (transitive) dependency is skipped; independent branches
  still complete.

`RetryPolicy{MaxAttempts, Backoff}` retries a flaky node with linear backoff.

## Dynamic expansion

A `Node` may implement `Expandable`:

```go
type Expandable interface {
	Expand(ctx context.Context, out Output) ([]Node, error)
}
```

After a node completes, the nodes it returns from `Expand` are scheduled into
the same run — enabling runtime fan-out / dynamic DAGs.

## Testing & anti-bluff Challenge

```bash
go test -race -count=1 -v ./...
./challenges/dag_orchestrator_challenge.sh
```

The Challenge builds a real diamond DAG `A → {B,C} → D` and asserts from
captured runtime evidence that B and C ran concurrently, D ran strictly after
both, and FailFast gates D + cancels C on a planted failure.

## Governance

Inherits the consuming project's `constitution/` canonical root (CONST-059).
Anti-bluff §11.4 family, CONST-047/050/051/052/053/054, §11.4.113 (no
force-push), §11.4.135 (regression guards) all bind. See `CLAUDE.md`,
`CONSTITUTION.md`, `AGENTS.md`.
