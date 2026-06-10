# CONSTITUTION.md — DagOrchestrator

## Inheritance (CONST-059)

This submodule inherits the **canonical root** governance from the consuming
project's `constitution/` submodule (`constitution/Constitution.md`,
`constitution/CLAUDE.md`, `constitution/AGENTS.md`). Those are the source of
truth for all universal mandates. This file records only the submodule-scoped
extensions; where it conflicts with the canonical root, the canonical root
wins.

## Binding constitutional anchors

The following anchors (cascaded from the constitution submodule) bind this
repository in full. They are referenced by ID per the cascade requirement; the
full text lives in the canonical `constitution/Constitution.md`:

- **CONST-035 / Article XI §11.9** — Zero-Bluff / Anti-Bluff Forensic Anchor:
  every PASS carries positive runtime evidence captured during execution.
- **CONST-047** — Recursive Submodule Application: this module receives the
  same anti-bluff posture, documentation, tests, and Challenges as the parent.
- **CONST-050** — No fakes beyond unit tests; 100% test-type coverage warranted
  by the domain. Production code never imports test mocks.
- **CONST-051** — Submodules are equal codebase, fully decoupled, project-
  not-aware, reachable from the consuming project's root (no nested own-org
  chains).
- **CONST-052** — lowercase snake_case for directories/files where the
  technology allows.
- **CONST-053** — proper `.gitignore`; no versioned build artefacts/secrets.
- **CONST-054** — ships `helix-deps.yaml` declaring own-org deps.
- **§11.4.113** — absolute no-force-push; merge onto latest main.
- **§11.4.135** — every closed defect registers a permanent regression guard.

## Definition of Done

A change is done only when accompanied by pasted terminal output from a real
run of the real code in the same session (CONST-035). `go test -race` PASS plus
the anti-bluff Challenge output is the floor.
