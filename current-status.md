# Rookery project status

Updated: 2026-08-28 20:06 ET

## Goal

Deliver a cleaner, more reliable, better-tested, and git-committed version of this local remote-state dashboard by Sunday at 22:00 ET.

## Current state

- Project: `async-api-view`, the local directory requested as the improved Rookery working copy.
- Final checks: 130 tests passed; Ruff format, Ruff lint, `uv lock --check`, package build, and workflow YAML validation passed.
- Runtime surface: 4 CLI commands and 5 HTTP routes, verified from source.
- Version control: local `main` history has three coherent commits ending at `3149059`.
- Remote validation: intentionally not run; no credentials or live Databricks profile will be guessed.

## TODO

- [x] Initialize local Git history and commit the verified baseline.
- [ ] Finish the critical review ledger across contracts, storage, adapter boundary, web surface, and documentation.
- [x] Remove or ignore generated artifacts and tighten repository hygiene.
- [x] Improve dashboard wording, status hierarchy, and refresh interaction without weakening the trust boundary.
- [x] Add regression tests for the first lifecycle, error, and security fixes.
- [x] Add reproducible CI, editor, and line-ending conventions.
- [ ] Re-run format, lint, tests, lock validation, and a rendered UI smoke check.
- [x] Re-run format, lint, tests, lock validation, package build, and a rendered UI smoke check.
- [x] Review the final diff critically and commit coherent changes.

## Evidence and decisions

- Existing implementation is intentionally observation-only and loopback-bound; preserve those constraints.
- Stale cached state must remain visible and must not be represented as live truth.
- A live Databricks smoke test remains out of scope because selecting a profile would require credentials or user input.
- The production composition now passes an explicit host allowlist, so the test-only `testserver` host is not accepted by the real runtime.
- The public web-app default also rejects `testserver`; tests opt into that host explicitly for their in-process client.
- Invalid bootstrap capabilities are validated before any database write, and reserved profile/root settings cannot be overridden by extension settings.
- Final critical-review report is schema-valid in `.local/review/findings.json`; the local review workspace is ignored and does not pollute Git history.

## Risks / watch list

- The directory was not a Git worktree when inspected.
- The architecture document describes deferred capabilities beyond the implemented slice; documentation must keep current behavior distinct from roadmap behavior.
- Local browser/debug data is present under `.local/` and must remain untracked.
- The remaining live-validation limitation is explicit: no remote inventory was run without a user-selected profile.
