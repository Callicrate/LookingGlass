# Rookery project status

Updated: 2026-08-28 21:25 ET

## Goal

Deliver a cleaner, more reliable, better-tested, and git-committed version of this local remote-state dashboard by Sunday at 22:00 ET.

## Current state

- Project: `async-api-view`, the local directory requested as the improved Rookery working copy.
- Latest checks: 168 tests passed warning-free; Ruff format, lint, lock validation, and the 80% branch-coverage gate passed.
- Runtime surface: 4 CLI commands and 5 HTTP routes, verified from source.
- Version control: local `main` is clean; every completed implementation batch is committed with focused messages.
- Active review round: fresh residual-risk audit, package smoke validation, and low-coverage boundary triage.
- Next progress report due: 2026-08-28 22:07 ET.
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
- [x] Supervise and recover transient worker startup, coordinator, and worker-loop failures.
- [x] Reconcile configuration identity and refresh authority across restarts.
- [x] Eliminate repeated full action/object reads from per-facet dashboard rendering.
- [x] Bound dashboard reads for large object and action histories.
- [x] Serialize concurrent migration decisions safely.
- [x] Reconcile documentation claims with the current implemented surface.
- [x] Independently review and verify each consequential change set before committing.
- [x] Surface a bounded recent projection of durable alertable failures.
- [x] Enforce an aggregate branch-coverage floor in CI.
- [ ] Add full alert history filtering only if it remains higher value than deeper object navigation.

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
- A durable configuration identity mapping adopts existing cached system UUIDs before desired-state reconciliation.
- Removing a config entry now disables stale refresh authority; queued stale work is cancelled by the pre-dispatch guard without a CLI call.
- Eight concurrent store constructors now complete the migration ledger safely in the regression test.
- Runtime retries use a 0.1 to 30 second bounded exponential backoff, remain wakeable, and emit one event per uninterrupted outage.
- Successful component recovery clears the dashboard error automatically; the independent follow-up review confirmed the retry-floor blocker is resolved.
- Dashboard inventory now uses 50-object pages, 128-character literal filters, a one-million-page input ceiling, and active-plus-latest action summaries.
- A 502-object regression case keeps the first-page SELECT budget at 70 or fewer while later pages and filtered objects remain refreshable.
- Desktop and 390px mobile QA passed with no console errors; Lighthouse accessibility, best practices, and agentic browsing scored 100 on both device profiles.
- The dashboard now renders the latest ten durable alertable failures with system, time, canonical class, and escaped redacted summary.
- Alert recency uses a dedicated SQLite index verified by `EXPLAIN QUERY PLAN`; the exact full-history count was removed to preserve bounded dashboard work.
- Aggregate branch coverage is 82% with an enforced 80% CI floor; statement coverage remains about 90%.
- Composition failures now close their opened SQLite store before propagating the original startup error.
- `serve` now closes its runtime store even when Uvicorn fails before lifespan startup; the independent review found no blocker.
- The latest locked runtime dependency audit reports no known vulnerabilities; the unpublished local package is the only expected skip.
- A fresh isolated Python 3.12 environment installs the built wheel, runs the CLI entry point, and loads the packaged FastAPI routes successfully.
- The fresh current-HEAD review found no remaining medium-or-higher local defect; demonstrated residual work is low severity or intentionally deferred.

## Risks / watch list

- The directory was not a Git worktree when inspected.
- The architecture document describes deferred capabilities beyond the implemented slice; documentation must keep current behavior distinct from roadmap behavior.
- Local browser/debug data is present under `.local/` and must remain untracked.
- The remaining live-validation limitation is explicit: no remote inventory was run without a user-selected profile.
