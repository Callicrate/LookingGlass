# Rookery project status

Updated: 2026-08-28 20:38 ET

## Goal

Deliver a cleaner, more reliable, better-tested, and git-committed version of this local remote-state dashboard by Sunday at 22:00 ET.

## Current state

- Project: `async-api-view`, the local directory requested as the improved Rookery working copy.
- Latest checks: 145 tests passed warning-free; Ruff format, Ruff lint, `uv lock --check`, and package build passed.
- Runtime surface: 4 CLI commands and 5 HTTP routes, verified from source.
- Version control: local `main` history has four coherent commits ending at `e251856`.
- Active review round: configuration desired-state reconciliation, runtime supervision, bounded dashboard reads, and migration concurrency.
- Next progress report due: 2026-08-28 21:00 ET.
- Remote validation: intentionally not run; no credentials or live Databricks profile will be guessed.

## TODO

- [x] Initialize local Git history and commit the verified baseline.
- [x] Finish the first critical review ledger across contracts, storage, adapter boundary, web surface, and documentation.
- [x] Remove or ignore generated artifacts and tighten repository hygiene.
- [x] Improve dashboard wording, status hierarchy, and refresh interaction without weakening the trust boundary.
- [x] Add regression tests for the first lifecycle, error, and security fixes.
- [x] Add reproducible CI, editor, and line-ending conventions.
- [x] Re-run format, lint, tests, lock validation, package build, and a rendered UI smoke check.
- [x] Review the final diff critically and commit coherent changes.
- [x] Make orderly shutdown cancel and reap in-flight CLI work promptly.
- [ ] Reconcile configuration identity and refresh authority across restarts.
- [x] Eliminate repeated full action/object reads from per-facet dashboard rendering.
- [ ] Bound dashboard reads for large object and action histories.
- [ ] Serialize concurrent migration decisions safely.
- [ ] Reconcile documentation claims with the current implemented surface.
- [ ] Independently review and verify the next change set before committing.

## Evidence and decisions

- Existing implementation is intentionally observation-only and loopback-bound; preserve those constraints.
- Stale cached state must remain visible and must not be represented as live truth.
- A live Databricks smoke test remains out of scope because selecting a profile would require credentials or user input.
- The production composition now passes an explicit host allowlist, so the test-only `testserver` host is not accepted by the real runtime.
- The public web-app default also rejects `testserver`; tests opt into that host explicitly for their in-process client.
- Invalid bootstrap capabilities are validated before any database write, and reserved profile/root settings cannot be overridden by extension settings.
- Final critical-review report is schema-valid in `.local/review/findings.json`; the local review workspace is ignored and does not pollute Git history.
- Locked runtime dependencies have no known vulnerabilities according to `pip-audit`; the local package itself was correctly skipped as unpublished.
- The independent diff review found no blocker in the shutdown, dashboard, config, packaging, or test-transport changes.

## Risks / watch list

- The directory was not a Git worktree when inspected.
- The architecture document describes deferred capabilities beyond the implemented slice; documentation must keep current behavior distinct from roadmap behavior.
- Local browser/debug data is present under `.local/` and must remain untracked.
- The remaining live-validation limitation is explicit: no remote inventory was run without a user-selected profile.
