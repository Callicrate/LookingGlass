# Rookery project status

Updated: 2026-08-29 01:56 ET

## Goal

Deliver a cleaner, more reliable, better-tested, and git-committed version of this local remote-state dashboard by Sunday at 22:00 ET.

## Current state

- Project: `async-api-view`, the local directory requested as the improved Rookery working copy.
- Latest checks: 241 tests passed warning-free; Ruff format, standard/security lint, lock validation, package build, source secret scan, and the branch-coverage gate passed.
- Runtime surface: 4 CLI commands and 11 HTTP routes, verified from source.
- Version control: local `main` contains the verified action/activity, facet-truth, authorization, lifecycle, poison-item, and bidirectional presence-monotonicity slices plus all prior correctness fixes; completed batches are committed with focused messages.
- Active review round: continue post-fix residual-risk review.
- Next progress report due: 2026-08-29 02:12 ET.
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
- [x] Prevent older complete collection omissions from overwriting newer relationships.
- [x] Make Databricks compatibility checks reap their subprocess on cancellation.
- [x] Roll back rejected ingestion-item identity and journal mutations without discarding valid siblings.
- [ ] Tend Murmuration about hourly; the 01:43 pass remained read-only because the native project profile is not provisioned.
- [x] Commit bounded durable action activity with alert links and truthful dashboard labeling.
- [x] Prevent the browser session cookie from reaching ordinary loopback services on other ports.
- [x] Persist bounded retry delays instead of immediately repeating transient CLI failures.
- [x] Terminalize expired action deadlines before any remote dispatch.
- [x] Expose bounded, redacted per-action attempt detail from alerts, activity, and intent receipts.
- [x] Terminalize malformed persisted action timestamps once instead of repeatedly faulting the worker.
- [x] Keep object presence monotonic across delayed present and absence observations.
- [x] Distinguish due, refreshing, failed-last-attempt, and current facet state truthfully.
- [x] Terminalize incompatible persisted intents once so valid coordinator work can progress.
- [x] Let newer canonical-ID observations revive presence without allowing delayed resurrection.

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
- The same review demonstrated that delayed older complete collection evidence could mark a newer relationship absent.
- Complete-membership omission reconciliation now applies the same observed-time guard as explicit relationship updates; delayed older evidence preserves the newer edge while genuinely newer omission still marks it absent.
- The relationship guard adds no query and runs inside the existing immediate transaction; independent review found no blocker and confirmed equal-time last-ingested-wins behavior remains consistent with the existing merge contract.
- A runtime review reproduced that cancelling a Databricks compatibility check could leave its CLI subprocess alive.
- Compatibility checks now create explicit reader/wait tasks and share the normal runner's kill, exit-wait, and task-settlement path for cancellation, timeout, and output-limit failures while preserving the original exception.
- Fake hanging-process regressions cover all three exceptional paths without launching a subprocess; independent review found no blocker and adapter branch coverage increased from 74% to 77%.
- Late-rejected facet absence and relationship items now use transaction-local savepoints; rollback removes locator, journal, facet, and relationship residue before the durable ingestion issue is written.
- The atomicity regression combines a valid facet sibling, an unauthorized external absence, a relationship whose second locator is invalid, and complete relationship-absence coverage; only the valid sibling persists, both issues remain durable, and no freshness credit is granted.
- Savepoint use is limited to late-rejection paths so the 502-object regression remains fast; the full 209-test suite completes in about nine seconds and storage branch coverage is 84%.
- Independent storage review found no blocker, confirmed nested savepoints remain atomic under `BEGIN IMMEDIATE`, and verified coverage/reconciliation stay suppressed when any item is rejected.
- Murmuration project identity is now intentionally tracked in `.murmuration/project.toml`; the 23:46 tending pass found no project-specific forum context and could not write because the native identity profile is not provisioned. BookStack remained credential-gated.
- Bounded action activity now uses 50-row pages, a 10,000-page ceiling, exact action lookup, state/system filters, static SQL, four dedicated recency indexes, and no per-action scope reads.
- Alerts with an action ID now link into exact local action discovery; diagnostics stay redacted and escaped, while connection bindings, profiles, payloads, and raw commands remain absent.
- The dashboard's former `Actions` tile counted current refresh controls; it is now truthfully labeled `Refresh options`.
- The action slice passes 221 tests at 86% branch coverage; migration `0008` is applied locally with SQLite integrity `ok` and zero foreign-key violations, and both migration and template are packaged.
- Desktop and 390px mobile action-page QA passed with real failed/retry rows, functional state filtering, no console or network errors, and Lighthouse 100 in every audited category.
- A fresh review demonstrated that the host-only `rookery_session` cookie is shared across ports for `127.0.0.1`; a local service receiving it can replay the bearer to Rookery. A process-unique `*.localhost` browser host is the selected repair.
- Every runtime now generates a 128-bit `rookery-….localhost` browser host, places it in the activation URL, and accepts only that production Host while Uvicorn remains bound to `127.0.0.1` or `localhost`.
- Cookie-jar regressions prove the session is absent from ordinary `127.0.0.1`, `localhost`, and unrelated `*.localhost` requests; actual Chrome resolved the generated host, activated successfully, and sent no Cookie header on the rejected direct-IP request.
- Configuration now rejects alternate `127.x` bind addresses that cannot reliably match `.localhost` resolution; the full suite passes 223 tests at 86% branch coverage.
- A fresh residual review also reproduced immediate retry of transient CLI failures with no `retry_at`, and dispatch after an action deadline has expired; both medium defects are next in the queue.
- Retryable failures now write one durable attempt with a future `retry_at` and return; the worker performs at most one CLI call per lease instead of looping immediately.
- Retry delays use bounded exponential policy: one second for timeout/transient failures, five seconds for rate limits, and a 30-second ceiling.
- `ActionLease` now carries the transactionally elected next durable ordinal; reopen tests prove early leasing fails and ordinal two becomes eligible exactly at `retry_at`.
- Independent review found no blocker; strict positive-integer ordinal validation was added, the suite passes 228 tests, and adapter/storage branch coverage increased to 78%/85%.
- The pre-dispatch guard now cancels `deadline <= now` actions before binding resolution or command construction; a vertical regression proves zero CLI calls.
- Truly expired attached scopes become `expired`; still-live coalesced scopes detach, return to `queued`, and immediately admit a replacement action after the stale action releases dedupe authority.
- Deadline splitting/cancellation is atomic under `BEGIN IMMEDIATE`, expected expiry remains non-alertable, and independent review found no blocker at 230 tests.
- The second hourly Murmuration pass found no Rookery-specific shared context; forum health remained public/read-only while native notifications, writes, and BookStack stayed credential-gated.
- Protected `/actions/{uuid}` detail now shows the durable action plus the latest 100 attempts in chronological order, including outcomes, retry times, canonical errors, and escaped redacted diagnostics.
- Activity rows, dashboard/full alerts, initial intent receipts, and polling updates now link to action detail without exposing bindings, command text, profiles, or payloads.
- Attempt overflow reports `Showing latest 100 of N`; successful lifecycle writes persist `NULL` rather than a false `no diagnostic supplied` failure message.
- Isolated desktop browser QA rendered a failed-retry-then-success sequence with no console/network errors and Lighthouse 100 in every audited category; the packaged template and full 233-test gate pass at 86% branch coverage.
- Malformed queued action contracts now terminalize once as `adapter_contract_mismatch`, reject active attached scopes, emit one redacted idempotent failure event, and release dedupe authority before lease mutation.
- `lease_next` continues selecting inside the same immediate transaction after poison terminalization, so the first call returns the next healthy lease instead of making `run-once` falsely report a drained queue.
- Storage and composition regressions prove queue progress and that only the healthy capability reaches the runner; independent follow-up cleared the blocker at 235 tests.
- Authorized object absence now updates only when it is at least as new as `last_seen_at`, and records its observation time as the presence watermark.
- Regressions prove delayed absence cannot hide newer presence, delayed presence cannot resurrect newer absence, and genuinely newer presence restores the object; independent review found no blocker at 236 tests.
- Dashboard and object detail now derive active-preferred/latest terminal action disposition for every visible facet through two batched, target-ID-indexed queries capped at 100 object IDs.
- Configured-scope and object-target actions share the same mapping; active work renders `refreshing`, later failed work renders `failed` with a redacted action link, elapsed facts render `due`, and newer evidence restores `current`.
- Browser QA kept the cached value visible beside its failed badge and diagnostic with no console/network errors; migration `0009` is applied locally with integrity `ok`, and independent follow-up cleared both query/state blockers at 238 tests.
- Coordinator leasing now reconstructs a persisted intent before lease mutation, rejects only contract/data decoding failures, records one redacted idempotent mismatch event, and continues selecting valid work in the same immediate transaction.
- Coordinator and CLI run-once regressions prove the valid intent admits and only its capability reaches the runner; injected SQLite operational failure still propagates and rolls back.
- Strict parent-envelope decoding remains intact, while a tolerant requested-time read lets the protected receipt page show durable rejected scopes plus an explicit unsupported-contract notice; independent follow-up marked the 241-test diff commit-ready.
- The 01:43 Murmuration pass again found no Rookery-specific public context; native writes, notifications, and BookStack remained unavailable without the project profile.
- Canonical-ID and external-locator non-absence observations now share the same `last_seen_at` watermark; only newer/equal evidence can mark an object present.
- Absence resolution no longer creates or transiently revives unknown objects, and the existing guarded absence update remains timestamp-monotonic.
- Regression coverage proves older canonical evidence cannot resurrect a newer absence while later canonical evidence restores operational presence; independent review found no blocker at 241 tests.

## Risks / watch list

- The directory was not a Git worktree when inspected.
- The architecture document describes deferred capabilities beyond the implemented slice; documentation must keep current behavior distinct from roadmap behavior.
- Local browser/debug data is present under `.local/` and must remain untracked.
- The remaining live-validation limitation is explicit: no remote inventory was run without a user-selected profile.
