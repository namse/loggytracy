# Loggytracy development workflow

## Project goal

Complete the milestones in `docs/PROJECT_PLAN.md` in order. Treat
`docs/ARCHITECTURE.md` as the product and architecture source of truth. If the
two documents disagree, stop before changing scope and surface the conflict.

## Required validation

Run these commands before a milestone can be committed:

```text
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
git diff --check
```

Add focused regression tests for every bug fixed during review. Run narrower
tests while iterating, then run the complete validation set.

## Milestone loop

1. Inspect the working tree and preserve all pre-existing user changes.
2. Select only the next incomplete milestone from `docs/PROJECT_PLAN.md`.
3. Delegate implementation to one `milestone_worker` at a time. Do not run
   multiple code-writing agents concurrently in the shared working tree.
4. After implementation and validation, spawn a brand-new
   `milestone_reviewer` with no prior conversation turns when the client
   supports it. Give it the milestone acceptance criteria, base commit, diff,
   and validation commands, but not the implementation rationale.
5. Fix every correctness, durability, security, regression, and material test
   coverage finding. Close that reviewer and use another fresh reviewer for
   the next pass. Never ask the fixing agent to certify its own work.
6. A milestone passes only when a fresh review reports no blocking findings
   and the full validation set passes.
7. Update `docs/PROJECT_PLAN.md`, explicitly stage only files belonging to the
   milestone, inspect the staged diff, and create one milestone commit.
8. Do not amend, rebase, merge, push, or rewrite history unless the user asks.

## Review standard

Prioritize observable defects over style. Review error paths, crash recovery,
concurrency, persistence ordering, unbounded resource use, and missing tests.
Every finding must name the relevant file and explain impact and a concrete
failure scenario. A clean review must explicitly say that no blocking findings
were found.

## Scope and safety

Normal local reads, edits, builds, and tests are authorized. Ask before external
writes, destructive operations, handling secrets, or materially changing the
documented product scope. Never stage the entire working tree with `git add .`
or rely on `git commit -a`; list milestone files explicitly so untracked and
unrelated files cannot be lost or mixed into a commit.
