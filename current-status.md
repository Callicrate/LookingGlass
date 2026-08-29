# Rookery project status

Updated: 2026-08-28 23:22 ET

## Goal

Deliver a cleaner, more reliable, better-tested, and git-committed version of this local remote-state dashboard by Sunday at 22:00 ET.

## Current state

- Project: `async-api-view`, the local directory requested as the improved Rookery working copy.
- Latest checks: 204 tests passed warning-free; Ruff format, standard/security lint, lock validation, package build, source secret scan, and the branch-coverage gate passed.
- Runtime surface: 4 CLI commands and 9 HTTP routes, verified from source.
- Version control: local `main` includes the verified local-caller authorization and direct Workspace metadata fixes; completed implementation batches are committed with focused messages.
- Active review round: repair out-of-order relationship reconciliation, then doctor cancellation.
- Next progress report due: 2026-08-29 00:12 ET.
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
- [x] Choose bounded object/containment navigation over lower-value full alert history filtering.
- [x] Add bounded, filterable full alert history without expanding dashboard query cost.
- [x] Require an ephemeral, process-local browser session before exposing cached data or refresh authority.
- [x] Repair direct Workspace metadata coverage so its normalized observation is accepted and credited.
- [ ] Prevent older complete collection omissions from overwriting newer relationships.
- [ ] Make Databricks compatibility checks reap their subprocess on cancellation.

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
- Aggregate branch coverage is 83% with an enforced 80% CI floor; statement coverage remains about 91%.
- Composition failures now close their opened SQLite store before propagating the original startup error.
- `serve` now closes its runtime store even when Uvicorn fails before lifespan startup; the independent review found no blocker.
- The latest locked runtime dependency audit reports no known vulnerabilities; the unpublished local package is the only expected skip.
- A fresh isolated Python 3.12 environment installs the built wheel, runs the CLI entry point, and loads the packaged FastAPI routes successfully.
- The fresh current-HEAD review found no remaining medium-or-higher local defect; demonstrated residual work is low severity or intentionally deferred.
- CLI doctor, bounded run-once draining, and store cleanup now have direct command-level tests; CLI branch coverage increased from 72% to 93%.
- Remote display text now neutralizes ANSI/control and bidi-override characters while preserving ordinary Unicode and ZWJ emoji.
- The ignored local database upgraded through migration `0005`; `PRAGMA integrity_check` is `ok` with zero foreign-key violations.
- The installed Databricks CLI compatibility doctor still passes without authentication or inventory probing.
- CI now runs Ruff's source security rules in addition to the normal lint suite; all SQL construction is static and all values remain bound parameters.
- The second structured critical-review report is schema-valid in `.local/review-round2/` and records four resolved findings plus the live-validation gate.
- Direct runner tests prove manually assembled wrong subcommands, extra flags, unsafe profiles, missing fixed flags, and traversal paths are rejected before subprocess creation; adapter branch coverage rose from 69% to 74%.
- The tracked-file secret scan is clean; its sole redaction-fixture false positive is explicitly allowlisted and asserted not to persist.
- Storage tests now document idempotent close behavior and literal backslash search, covering two operational assumptions used by CLI cleanup and filtering.
- The paused example-config QA system was removed from local state after proving it contained no actions, observations, facets, relationships, or alerts; a recoverable pre-cleanup backup remains under `.local/`.
- Dashboard objects now link to a bounded detail route with canonical identity, facets, provenance, current direct children, type filtering, and object-specific refresh actions.
- Containment pages include only present relationships, show relationship and child-object presence separately, and use one indexed join without temporary sorting or per-child lookups.
- The object page passed desktop/mobile interaction and screenshot checks with no console or network errors; Lighthouse scored 100 in every audited category on both profiles.
- The exact locked CI sequence passes on native Windows and Ubuntu/WSL with Python 3.12; CI now runs both operating systems with fail-fast disabled.
- A fresh post-commit review of the object route, containment join/index, refresh authorization, CI matrix, migrations, and docs found no remaining medium-or-higher local defect.
- Full alert history is an explicit 50-row page with a 10,000-page ceiling, exact filtered totals, type/severity filters, stable ordering, and preserved filter state across pagination.
- Alert counts and all four page-query shapes use static parameterized SQL; dedicated recency indexes avoid temporary sorting for unfiltered, type-only, severity-only, and combined filters.
- The history route reports invalid or duplicate query input as 400 and backend failure as a generic 503; the backend-less fallback was corrected so missing state cannot be misrepresented as an empty history.
- The alert-history page passed desktop/mobile browser checks without console or network errors; Lighthouse scored 100 in every audited category on both profiles.
- Independent review found no release blocker in the alert-history diff; its sole low-severity fallback concern was fixed and covered directly.
- Aggregate branch coverage is now 84% with 203 passing tests.
- The real ignored local database is migrated through `0007_operational_event_filters`; `PRAGMA integrity_check` is `ok` with zero foreign-key violations.
- An unauthenticated local HTTP client previously could fetch the process CSRF token and enqueue a registered read under the service owner's CLI profile; an independent reproduction rated this medium severity.
- `serve` now issues one 256-bit, ten-minute, single-use activation capability in a URL fragment and exchanges it through a bounded same-origin POST body for one memory-only browser session.
- Bootstrap and session values are CSPRNG-generated, stored server-side only as SHA-256 digests, compared in constant time, invalidated on restart, and excluded from rendered HTML, request targets, redirects, application logs, and access logs.
- Deny-by-default middleware gates every cached-data and mutation route before parsing or backend work; Trusted Host runs before authorization and security headers wrap both denials.
- Refresh CSRF nonces are per-session, and manual intents now durably record the non-secret UI session UUID without persisting authentication material.
- Redirected activation output fails closed unless `serve --allow-redirected-activation` explicitly opts in; the denial path closes the runtime store and exposes no capability.
- Actual Chrome QA over plain `http://127.0.0.1` proved fragment scrubbing, one-time exchange, subsequent authorization, an `HttpOnly` cookie hidden from JavaScript, no console errors, and token-free access-log targets.
- The bootstrap page scored 100 for Lighthouse accessibility, best practices, SEO, and agentic browsing; the full suite now passes 204 tests at 84% branch coverage.
- Independent security follow-up found no release blocker after tracing bootstrap expiry/replay, cookie/session fixation, middleware order, route coverage, CSRF, attribution, and logging.
- A separate storage review demonstrated that direct Workspace metadata normalization emits no coverage declaration, so ingestion rejects its only observation after spending the remote call.
- Direct Workspace metadata reads now declare complete exact-scope coverage with no absence authority while retaining partial field coverage; the path updates SQLite, records refresh credit, succeeds its action, and completes its intent in the vertical regression.
- A second exact metadata request is now satisfied from fresh evidence without another CLI call; independent review confirmed the metadata-only diff is commit-ready.
- Workspace content intentionally receives no freshness coverage because artifact bytes still have no persistence consumer; a critical-review blocker prevented that unsupported claim from entering the fix.
- The same review demonstrated that delayed older complete collection evidence can mark a newer relationship absent; both medium defects are queued ahead of feature work.
- A runtime review reproduced that cancelling a Databricks compatibility check can leave its CLI subprocess alive; the bounded cleanup fix remains queued.

## Risks / watch list

- The directory was not a Git worktree when inspected.
- The architecture document describes deferred capabilities beyond the implemented slice; documentation must keep current behavior distinct from roadmap behavior.
- Local browser/debug data is present under `.local/` and must remain untracked.
- The remaining live-validation limitation is explicit: no remote inventory was run without a user-selected profile.
