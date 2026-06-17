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

# AGENTS.md — DagOrchestrator (`dev.helix.dag`)

## Inheritance (CONST-059)

This file inherits the canonical-root agent manual from the consuming
project's `constitution/AGENTS.md` (and the `constitution/Constitution.md` it
references). Rules below extend — never weaken — the inherited rules. When this
file conflicts with the constitution submodule, the constitution wins.

## What this module is

`dev.helix.dag` — a generic, agent-free, pure-data **DAG scheduler** in Go.
Ready-set topological dispatch onto a bounded worker pool, parallelism cap,
per-node retry/backoff, fail-fast / continue-on-error policy, per-node lineage,
and dynamic node expansion. Fully decoupled and project-not-aware (CONST-051).

## Rules for agents working here

1. **No bluff (CONST-035 / §11.4).** Every claim of "works/passing/fixed"
   requires pasted output from a real run in the same session. No simulations,
   no placeholders, no `TODO`/`for now` in non-test code.
2. **No mocks outside unit tests (CONST-050).** This module's tests use only
   real in-process execution.
3. **Decoupled (CONST-051(B)).** Never inject consuming-project context into
   this module; expose configuration via options/constructor parameters.
4. **No force-push (§11.4.113).** Merge onto the latest `main` of every
   upstream; pushes are fast-forward only.
5. **Regression guard on every fix (§11.4.135).** A closed defect ships a
   permanent regression test in the same commit.

## Acceptance demo

```bash
go test -race -count=1 -v ./...
./challenges/dag_orchestrator_challenge.sh
```
