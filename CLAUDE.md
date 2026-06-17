## INHERITED FROM Helix Constitution

This submodule includes the Helix Constitution submodule at the parent
project's `constitution/` path. All rules in `constitution/CLAUDE.md` and
the `constitution/Constitution.md` it references — the universal anti-bluff
covenant (§11.4), host-safety (§12), and data-safety (§9) — apply
unconditionally. This submodule stays fully decoupled and project-not-aware
(§11.4.28): this pointer is generic governance inheritance only, never
project-specific context. Use `constitution/find_constitution.sh` from the
parent project root to resolve the constitution from any nested location.

---

# CLAUDE.md — DagOrchestrator (`dev.helix.dag`)

## INHERITED FROM constitution/CLAUDE.md

All rules in the parent project's `constitution/CLAUDE.md` (and the
`constitution/Constitution.md` it references) apply unconditionally to this
submodule. The rules below extend them — they MUST NOT weaken any inherited
rule. When this file disagrees with the constitution submodule, the
constitution wins. Per CONST-059 (Canonical-Root Inheritance Clarity), the
canonical root is the constitution submodule; this file is a consumer
extension.

This module is **fully decoupled and project-not-aware** (CONST-051(B)): it
contains NO HelixCode-specific paths, hostnames, or asset names. Any consuming
project injects context via constructor parameters / options — never a
hardcoded reach into a parent tree.

## Overview

`dev.helix.dag` is a generic, reusable Go module: a **pure-data DAG
scheduler**. Given a directed-acyclic graph of nodes with declared
dependencies, it computes the ready-set in topological order, dispatches ready
nodes onto a bounded worker pool honoring a parallelism cap, applies per-node
retry/backoff and a failure policy (fail-fast vs continue-on-error), records
per-node output + lineage, and supports dynamic expansion (a completed node may
emit new nodes).

It is **agent-free** — it operates on opaque node payloads — so any project can
reuse it, not just agentic flows.

- **Module**: `dev.helix.dag` (Go 1.26)
- **Org**: HelixDevelopment
- **Public interfaces**: `Node`, `Expandable`, `DAG`, `Build`, `Scheduler`,
  `NewScheduler`, `Options`, `FailurePolicy`, `RetryPolicy`, `Result`, `Edge`,
  `FuncNode`.

## Anti-bluff Definition of Done

No task is done without pasted output from a real run of the real code in the
same session as the change (parent CLAUDE.md Rule 8/9, §11.4.5). Coverage and
green summaries are not evidence.

```bash
# Acceptance demo for this module:
go test -race -count=1 -v ./...
# Run the anti-bluff Challenge (real diamond DAG, concurrency + ordering proof):
./challenges/dag_orchestrator_challenge.sh
```

## Build & Test

```bash
go build ./...
go vet ./...
go test -race -count=1 ./...
```
