# Remote System State and Refresh Service Specification

> Status: Draft v0.5, 2026-08-24.
> This document incorporates the accepted version 1 product decisions and isolates the remaining implementation questions.

## Table of contents

- [Summary](#summary)
- [Implementation status](#implementation-status)
- [First usable workflow](#first-usable-workflow)
- [Goals and non-goals](#goals-and-non-goals)
- [Terminology and normative language](#terminology-and-normative-language)
- [Core design decisions](#core-design-decisions)
- [System invariants](#system-invariants)
- [Logical architecture](#logical-architecture)
- [Canonical data model](#canonical-data-model)
- [Canonical object types and facets](#canonical-object-types-and-facets)
- [Observation and projection semantics](#observation-and-projection-semantics)
- [Freshness and refresh policy](#freshness-and-refresh-policy)
- [Queue and action lifecycle](#queue-and-action-lifecycle)
- [Generic coordinator contract](#generic-coordinator-contract)
- [Adapter and API worker contract](#adapter-and-api-worker-contract)
- [Observation ingestion and reconciliation](#observation-ingestion-and-reconciliation)
- [Connection and capability model](#connection-and-capability-model)
- [UI and TUI requirements](#ui-and-tui-requirements)
- [Initial target systems](#initial-target-systems)
- [Failure, concurrency, and recovery behavior](#failure-concurrency-and-recovery-behavior)
- [Security and trust boundaries](#security-and-trust-boundaries)
- [Observability and operations](#observability-and-operations)
- [Configuration and compatibility](#configuration-and-compatibility)
- [Testing and acceptance criteria](#testing-and-acceptance-criteria)
- [Phased delivery](#phased-delivery)
- [Alternatives considered](#alternatives-considered)
- [Open questions](#open-questions)
- [References](#references)

## Summary

This service is a local knowledge system for remote state.
It shows what is currently known about a remote system, when each fact was observed, and how that knowledge was obtained.
It does not claim that cached state is live remote truth.

Users inspect canonical local state through a UI or TUI.
The current slice exposes manual refreshes through one generic refresh intent.
Planned Phase 4 automatic refreshes will create that same intent rather than introducing a second execution path.
A generic coordinator validates, coalesces, defers, or admits that intent using only canonical local data.
An API-specific adapter worker is the only component allowed to resolve credentials, understand a downstream API or CLI, and contact a remote system.
The worker returns normalized observations, which are accepted even when an equivalent targeted refresh would have been throttled.

The pivotal modeling choice is **facet-level state and freshness**.
A remote file is one canonical object with separate metadata and content facets.
A directory listing can refresh the file's metadata without falsely claiming that its content was read.
The same pattern applies to a service's runtime and configuration, and a job's definition, status, and run summary.

Version 1 is a local, single-user, remote-observation-only service.
API workers read remote systems and submit normalized observations for local canonical storage through the observation-ingestion contract.
The product exposes no operation intended to modify a remote system.
Observation calls may still cause unavoidable collateral effects, such as updating a file access time or creating a remote audit record, and adapters must declare and minimize those effects.
The recommended initial implementation is a modular monolith with explicit internal contracts.
The boundaries in this document are logical boundaries, not a requirement to deploy separate services or select a particular language, database, queue product, or UI framework.

## Implementation status

The Databricks-first modular-monolith slice is implemented in Python 3.12 with SQLite, FastAPI, and the existing Databricks CLI.

Implemented and covered by automated tests:

- versioned canonical, refresh, action, and observation contracts;
- object-over-system-over-type refresh policy and failed-action cooldown;
- durable SQLite intents, actions, leases, observations, projections, and alertable failures;
- generic local-only coordination and idempotent observation ingestion;
- closed Databricks CLI command mapping, bounded execution, error redaction, and Workspace/Unity Catalog metadata normalizers;
- loopback operational UI with bounded inventory, containment, alert-history, action-activity, and per-action attempt pages, object facet/provenance detail, registered refresh controls, intent polling, ephemeral local-caller authorization, per-session CSRF, Origin/Host checks, stale/error states, and output escaping;
- local configuration, initialization, doctor, one-shot worker, online backup, and serve commands;
- an end-to-end fake-CLI test covering request, admission, execution, ingestion, display, and duplicate suppression.

Not yet verified or intentionally deferred:

- the live inventory smoke test, pending an explicit choice of existing Databricks CLI profile;
- automatic refresh subscriptions and UI-session heartbeat scheduling;
- Workspace content reads, persistent artifacts, and retention deletion; the current worker rejects content actions before CLI execution;
- the Linux-over-SSH adapter;
- evidence compaction and multi-process deployment.

## First usable workflow

The first usable slice uses Databricks as the first real adapter and API-worker test case.
It proves one complete refresh path before broad object coverage, Linux support, or automatic scheduling is added.

### Inputs

- One configured Databricks workspace system.
- One non-secret Databricks connection binding referencing an existing CLI profile.
- The enabled `databricks.workspace.children.read` capability.
- One preconfigured Workspace directory scope.

### User action

1. The user opens the local state view.
2. The view displays the target as unobserved or shows its last-known state.
3. The user requests a refresh.
4. The refresh command service records a generic intent and returns its local request ID immediately.
5. The generic coordinator validates the intent from local schema, capability, policy, object, and queue data.
6. The Databricks API worker invokes the existing CLI through its fixed Workspace-list command mapping and returns a normalized observation batch.
7. The ingestion service records the evidence and updates the current projection.

### Expected output

The state view shows:

- the canonical value or explicit unknown state;
- the time it was observed;
- any remote modification time reported by the source;
- the adapter and action that produced it;
- its computed freshness;
- the most recent refresh activity and failure, if any.

### First-slice proof

The slice is complete only when the Workspace folder and returned child metadata can be viewed from local state after Databricks becomes unavailable.
A second refresh requested inside the minimum interval must not cause a second logical downstream action.

## Goals and non-goals

### Goals

- Maintain a canonical, source-independent representation of known remote objects.
- Preserve enough provenance to distinguish remote facts, local bookkeeping, derived state, and unknown state.
- Let users browse cached state without waiting for or requiring a remote connection.
- Let users selectively request refreshes by canonical object and facet.
- Use one refresh path for manual and automatic activity.
- Prevent duplicate or too-frequent targeted downstream calls.
- Accept useful newer observations regardless of whether a targeted call would currently be allowed.
- Keep remote API, CLI, authentication, pagination, rate-limit, retry, and concurrency behavior inside API-specific adapters.
- Add new object kinds and adapters without changing the generic coordinator.
- Make stale but useful information visible rather than replacing it with a generic failure state.

### Non-goals for version 1

- Real-time or strongly consistent mirroring of a remote system.
- Remote mutation, administration, arbitrary API calls, or arbitrary command execution.
- Replacing native Databricks, operating-system, or filesystem administration tools.
- Exactly-once downstream execution.
- A universal strongly typed schema for every possible vendor field.
- Automatic capture of arbitrary file content.
- A distributed-service topology before scale or tenancy requires it.
- Selecting between a UI and TUI, or selecting an implementation framework.

Remote mutation is intentionally excluded.
Writing observations, queue state, refresh results, retained artifacts, and operational events into local storage is expected and is not a remote mutation.
An observation capability is not required to be perfectly side-effect-free, but every known collateral remote effect must be declared.

## Terminology and normative language

The words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** describe requirement strength.

| Term | Meaning |
|---|---|
| System | One configured remote target, such as a Databricks workspace or a server. |
| Adapter | The versioned integration that knows one downstream API, CLI, or remote protocol. |
| API worker | A runtime executing one adapter's downstream actions. |
| Object | A stable local identity for one remote resource. |
| Facet | An independently observed and refreshed aspect of an object, such as file metadata or file content. |
| Observation | Provenance-bearing evidence reported by an adapter at a particular time. |
| Current projection | The best-known canonical state derived from accepted observations. |
| Refresh intent | A user's or scheduler's request for newer evidence about a canonical target. |
| Adapter action | A validated unit of downstream observation work created from one or more refresh intents. |
| Refresh scope | The tuple of system, target, facet, and requested coverage to which freshness applies. |
| Capability | A locally stored declaration that an adapter binding can perform a canonical read and which observations it can produce. |
| Qualifying observation | An accepted observation whose declared coverage satisfies a refresh scope. |
| Complete collection | An adapter assertion that a successful result fully enumerates one bounded collection. |
| Partial collection | A result that must not be used to infer absence for omitted members. |

The document uses **system** as the canonical term.
UI labels MAY use site, workspace, host, or another source-appropriate word.

## Core design decisions

| Decision | Rationale | Status |
|---|---|---|
| One object identity with independently fresh facets | Prevents metadata, content, status, and configuration from being falsely treated as equally current. | Specified |
| Bounded observation journal plus current projection | Preserves provenance and recovery without requiring full event sourcing. | Specified |
| Generic intents and adapter-owned actions | Keeps UI and generic scheduling independent from vendor commands and credentials. | Specified |
| Local-only admission decisions | Makes refresh eligibility deterministic and testable without a remote call. | Required by brief |
| Version 1 exposes only remote observation capabilities | Remote API workers update local knowledge but never offer intentional remote state changes. | Accepted |
| Version 1 is local and single-user | Avoids premature tenancy, network-service authorization, and distributed deployment requirements. | Accepted |
| Manual requests obey the same interval as automatic requests | Preserves one policy path and prevents a manual button from becoming a rate-limit bypass. | Accepted |
| Auto-refresh exists only during its originating UI session | Prevents background polling after the user closes the client. | Accepted |
| Stored content defaults to 365-day retention with normal override precedence | Makes content lifecycle explicit and locally configurable. | Accepted |
| Databricks is the first adapter and end-to-end test case | Proves the generic contracts against a real CLI-backed system before Linux support is added. | Accepted |
| Trusted local time drives freshness and cooldown | Remote clocks and timestamps are facts to display, not safe scheduling clocks. | Specified |
| Logical modules can initially share one process | Proves the contracts without committing to distributed infrastructure. | Accepted |

The file metadata and file content concepts from the brief are modeled as facets of one `file` object.
If content later needs independent identity or version history, the model can add content-version child objects without changing the canonical file identity.

## System invariants

1. The UI and TUI MUST read remote state only through the canonical state query contract.
2. The UI, TUI, scheduler, generic coordinator, queue infrastructure, and ingester MUST NOT contact a downstream system.
3. Only the adapter worker bound to a system MAY resolve its credentials and perform downstream calls.
4. Manual and automatic refreshes MUST enter through the same refresh-intent interface.
5. Refresh admission MUST use only canonical local schema, system, object, capability, policy, observation, authorization, and queue data.
6. Refresh throttling MUST control targeted downstream observation actions only.
7. The ingester MUST accept a valid newer observation even if an equivalent targeted refresh is currently deferred or suppressed.
8. A failed refresh MUST preserve the last-known remote facts and MUST NOT translate them into inactive, deleted, empty, or unknown values.
9. An omitted object MUST NOT be marked absent unless the adapter declared a successful, complete coverage boundary with object-presence authority.
10. Remote facts, local bookkeeping, and derived state MUST remain distinguishable in storage and query responses.
11. Missing fields in a partial observation MUST mean not observed, not null, false, empty, or deleted.
12. Credentials and executable downstream commands MUST NOT appear in canonical object state, refresh intents, queue messages, observations, UI responses, or ordinary logs.
13. All canonical contracts and adapter bindings MUST be versioned.
14. Request creation, action creation, action claiming, and observation ingestion MUST be idempotent.
15. Remote values, paths, names, messages, and content MUST be treated as untrusted data.
16. The system MUST remain able to display cached state while a downstream system or adapter is unavailable.
17. Every terminal queue-worker or adapter-action failure MUST produce a durable, redacted operational event that a later UI alert feature can consume.
18. Closing or losing the originating UI session MUST stop new automatic refresh work for that session.
19. Every enabled version 1 capability MUST be classified as remote observation only.
20. No UI, queue, coordinator, adapter, or configuration path may expose an operation whose intended effect is remote state change.
21. Every known collateral effect of an observation call MUST be declared in its capability contract and minimized by its adapter.
22. API workers MAY and normally DO cause local canonical updates, but only by submitting action outcomes and normalized observations through their owning local ports.

## Logical architecture

### Components

| Component | Owns | Must not own |
|---|---|---|
| State query service | Canonical reads, relationship traversal, provenance, freshness calculation, and activity overlays | Remote calls or adapter-specific response parsing |
| UI or TUI | Navigation, display, selection, refresh submission, and auto-refresh preferences | Credentials, arbitrary remote paths, API syntax, or direct worker invocation |
| Refresh command service | Refresh-intent validation at the public boundary, durable recording, and idempotency receipt | Downstream execution or vendor API behavior |
| Auto-refresh scheduler | Finding selected due scopes and submitting normal refresh intents | Direct dispatch or separate refresh semantics |
| Durable intent queue | Ordering, visibility, leases, and delivery of generic work | Remote commands or secrets |
| Generic coordinator | Local refresh eligibility, capability selection, coalescing, deferral, and action creation | Credentials, network access, vendor syntax, or vendor error interpretation |
| Adapter action queue | Delivery of admitted, versioned adapter actions | Canonical merge logic |
| Adapter API worker | Credential resolution, downstream protocol, request construction, pagination, rate limits, concurrency, retries, and normalization | Arbitrary canonical database writes |
| Observation ingester | Schema validation, identity resolution, evidence storage, ordering, merge, reconciliation, and freshness credit | Remote calls or source-specific command construction |
| Canonical state store | Systems, bindings, types, objects, relationships, observations, projections, policies, intents, and action records | Secret material |
| Secret provider | Credential values and rotation | Canonical state |

These components MAY share a process and database initially.
Their interfaces and ownership MUST remain explicit so that an adapter worker can later move to a separate process without changing product semantics.

### Local write authority

Adapter workers do not have general write authority over canonical storage.
They may:

- update their lease, heartbeat, attempt, and redacted outcome through the action-lifecycle port;
- submit normalized observation batches through the observation-ingestion port.

The observation ingester is the sole owner of writes to canonical objects, facets, relationships, observation evidence, freshness credit, and current projections.
It performs schema validation, identity resolution, ordering, quarantine, merge, and reconciliation before those writes.
An adapter worker MUST NOT bypass either port or write canonical tables directly, even when all components share one process or database.

This boundary permits expected local persistence while preserving the rule that downstream workers never intentionally modify the remote system.

### Dependency direction

- The UI depends on state-query and refresh-intent contracts.
- The scheduler depends only on state-query and refresh-intent contracts.
- The generic coordinator depends on canonical data contracts and queue ports.
- An adapter depends on the adapter-action and observation contracts plus its own secret and remote clients.
- The ingester depends on the canonical observation contract, never on adapter-specific persistence tables.
- Core domain code MUST NOT import Databricks-, SSH-, operating-system-, UI-, queue-, or database-specific behavior.

### Read flow

1. A client queries systems, objects, facets, relationships, freshness, and refresh activity.
2. The state query service reads only the current projection and local activity records.
3. It calculates freshness from the trusted local clock and effective refresh policy.
4. It returns exact observation and activity times so the client can explain every freshness label.

### Refresh flow

1. A user or scheduler submits a refresh intent.
2. The command service durably records the intent and returns its ID.
3. The generic coordinator leases the intent and validates it using local data.
4. It records an independent rejected, satisfied, coalesced, deferred, or admitted outcome for each normalized intent scope.
5. Admission creates a versioned adapter action through an atomic transaction or durable outbox.
6. The appropriate adapter worker leases the action and invokes the local pre-dispatch guard before resolving command inputs.
7. Immediately before process creation, the guard revalidates mutable local authority, deadline, scope, target, policy, and evidence.
8. If still valid, that final guard transaction records the logical action start atomically; otherwise it satisfies or cancels without a remote call.
9. The adapter performs one or more source-specific attempts according to its own retry and rate policy.
10. The adapter emits normalized observation batches and a redacted action outcome.
11. The ingester validates and applies all safe observations.
12. The state query service immediately exposes the new projection and activity.

## Canonical data model

The schema below is logical.
An implementation MAY use relational tables, documents, or another durable representation, but MUST preserve the listed identities, constraints, transactions, and distinctions.

### Field conventions

- Canonical IDs MUST be locally generated stable identifiers, preferably UUIDs.
- Source-native identifiers MUST be stored as opaque adapter-owned keys.
- Every timestamp MUST include a timezone and be normalized to UTC at the contract boundary.
- A bare `created_at` or `modified_at` field is prohibited where its clock or meaning is ambiguous.
- Remote timestamps MUST use names such as `remote_created_at`, `remote_modified_at`, `remote_accessed_at`, or `remote_as_of`.
- Local record timestamps MUST use names such as `record_created_at`, `first_seen_at`, `observed_at`, `received_at`, and `state_changed_at`.
- Remote timestamp precision and source SHOULD be retained when they affect ordering or display.
- Unknown, unsupported, explicitly null, false, empty, and absent MUST be representable as different states.
- Flexible attribute payloads MUST carry a namespace and schema version.
- Durations MUST be positive and stored in an unambiguous unit, such as integer seconds.

### Core records

| Record | Required fields | Contract |
|---|---|---|
| `System` | `system_id`, display name, `system_kind`, enabled state, local timestamps | One configured remote target and its system-wide refresh override. |
| `AdapterDefinition` | adapter key, adapter version, supported core contract range, manifest digest | Identifies adapter code and the contracts it implements. |
| `ConnectionBinding` | binding ID, system ID, adapter key/version, non-secret settings, secret reference, allowed scope, enabled state | Associates a system with an adapter without copying secrets into canonical storage. A system may have multiple bindings. |
| `CapabilityBinding` | capability-binding ID, connection-binding ID, capability key/version, target types, input schema, produced facets, coverage rules, collateral effects/mitigations, local priority, enabled state | Lets the generic coordinator plan an observation without knowing a vendor API. |
| `ObjectTypeDefinition` | namespaced type key, schema version, allowed facets, facet schemas, default refresh intervals, default artifact retention | Defines canonical validation and type-level refresh/retention policy. |
| `RemoteObject` | object ID, system ID, type key/version, source kind, opaque external key, display name, presence state, first/last seen times | Holds stable identity and lifecycle, not all source data. |
| `RelationshipState` | relationship ID, system ID, subject ID, predicate, object ID, observation provenance, presence state | Represents containment and other observed relationships. |
| `FacetState` | object ID, facet key/version, knowledge state, canonical payload, field provenance, qualifying observation time, source revision | Current best-known values for one independently fresh facet. |
| `ObservationBatch` | batch ID, contract version, system and binding IDs, optional action ID, adapter version, observed/received times, coverage declarations | One provenance envelope of normalized adapter evidence. |
| `Observation` | observation ID, batch ID, target locator, facet, update mode, field coverage/mask, payload, ordering metadata, item-authority scopes, satisfaction scopes | Append-only normalized evidence for an object, system, or relationship. |
| `ProjectionCheckpoint` | checkpoint ID, contract version, through-observation watermark, facet/relationship baseline, provenance summary, digest, local timestamp | Preserves a rebuildable canonical baseline when supporting observations are compacted. |
| `RefreshPolicyOverride` | scope level, scope ID, optional facet, interval, local timestamps | Stores object or system overrides. Type defaults remain in type definitions. |
| `RetentionPolicyOverride` | scope level, scope ID, optional facet/artifact kind, retention duration, local timestamps | Stores object or system artifact-retention overrides. Type defaults remain in type definitions. |
| `StoredArtifact` | artifact ID, object/facet and observation IDs, storage reference, digest, byte count, captured time, effective retention, expiry time, retention source, deletion time | Tracks retained content without placing bytes in ordinary facet rows. |
| `AutoRefreshSubscription` | subscription ID, UI session ID, owner, target scope, facets, enabled state, heartbeat/expiry times, local timestamps | Describes session-scoped automatic refresh and expires when its UI session ends. |
| `RefreshIntent` | intent ID, idempotency key, source, actor, target, requested facets, priority, requested/expiry times, aggregate state | Preserves every manual or automatic request and its derived overall result. |
| `RefreshIntentScope` | intent-scope ID, intent ID, normalized refresh scope, state, disposition, `eligible_at`, linked action IDs, satisfying observation IDs | Preserves the independent outcome of every atomic facet/coverage scope in an intent. |
| `AdapterAction` | action ID, dedupe key, binding and capability versions, canonical target, requested facets, state, lease, eligibility and lifecycle times | Represents one admitted logical downstream observation shared by coalesced refresh intents. |
| `ActionAttempt` | attempt ID, action ID, ordinal, start/end times, outcome class, retry time, redacted diagnostic | Records adapter-owned retries without exposing secrets or command strings. |
| `OperationalEvent` | event ID, event type/version, severity, alertable flag, system/intent/action/attempt links, canonical error class, redacted summary, occurred time, idempotency key | Provides a durable source for diagnostics and a later local UI alert projection. |
| `IngestionIssue` | issue ID, batch/action IDs, item locator, error class, redacted detail, local timestamp | Quarantines malformed items and supports partial-success diagnostics. |

### System record

A system MUST store:

- a stable local ID and human-readable name;
- a namespaced kind such as `databricks.workspace` or `server.host`;
- an enabled, disabled, or paused local state;
- zero or more connection bindings, referenced through their separate records;
- an optional system-wide refresh interval override;
- local record creation and update times.

A system MUST NOT imply that its connection currently works.
Connection health is time-bound observed state and belongs in a system facet or action result.
Each capability selects a specific enabled connection binding.
The core MUST NOT assume that one system has only one downstream API, credential, or adapter.

### Connection binding

Non-secret settings MAY include:

- a Databricks profile name or workspace/account selector;
- a server connection alias;
- allowlisted filesystem roots;
- allowlisted service identifiers;
- an adapter-specific non-secret mode;
- secret-provider reference names.

The binding MUST NOT store tokens, passwords, private keys, secret values, shell fragments, or arbitrary user-supplied command arguments.

### Remote object identity

The tuple of system, adapter identity namespace, source kind, and opaque external key MUST uniquely identify a current remote object.
Adapters own the rules that create and normalize the external key.

Paths and display names SHOULD be mutable attributes rather than identity when a source exposes a stable ID.
When no stable remote ID exists, the adapter MUST document whether a path/name change creates a new object or can be correlated as a move.
The core MUST NOT guess rename identity from similarity alone.

### Object lifecycle

Object presence uses these canonical states:

| State | Meaning |
|---|---|
| `unknown` | No authoritative presence evidence has been accepted. |
| `present` | A positive observation currently establishes that the object exists. |
| `absent` | An explicit not-found result or complete coverage with object-presence authority establishes absence. |

Authentication failure, permission denial, connection failure, timeout, unsupported capability, and partial omission are action or knowledge conditions.
They MUST NOT be converted to `absent`.

An absent object SHOULD remain as a tombstone with prior facts and provenance until retention policy permits removal.

## Canonical object types and facets

The type registry is extensible, but every type uses the common identity, observation, knowledge, and freshness envelope.

| Type | Facet | Representative canonical fields |
|---|---|---|
| `folder` | `metadata` | name, path or locator, remote creation/modification times, permissions when available |
| `folder` | `membership` | observed child relationships plus collection completeness |
| `file` | `metadata` | name, path or locator, byte size, media/type hint, permissions, digest or revision, remote creation/modification/access times |
| `file` | `content` | content reference, digest, byte count, media type, encoding, complete/truncated state, source revision |
| `service` | `runtime` | canonical active state, source substate, health detail when reported |
| `service` | `configuration` | canonical enabled state, startup mode, source-specific versioned attributes |
| `job` | `metadata` | name, description, schedule summary, source kind and attributes |
| `job` | `status` | enabled/paused state and source lifecycle state |
| `job` | `run_summary` | last run ID, start/end time, outcome, and selected summary fields |
| `generic_object` | `attributes` | namespaced and versioned source-specific fields |

Canonical service values MUST preserve unknown and unsupported:

- active state: `active`, `inactive`, `transitioning`, `failed`, `unknown`, or `unsupported`;
- enabled state: `enabled`, `disabled`, `masked`, `static`, `unknown`, or `unsupported`.

Adapters MAY retain a source-native state alongside the canonical enum.
They MUST NOT force an unknown vendor value into the nearest canonical value.

The initial job model intentionally stores a run summary rather than assuming every run is a first-class object.
An implementation MAY later add `job_run` objects when users need run navigation, history, or independent refresh.

`generic_object` is a compatibility mechanism, not a schema escape hatch.
Its namespace, source kind, attribute schema version, identity, provenance, knowledge state, and refresh facets remain mandatory.

### File content

File content SHOULD be stored outside the ordinary facet row behind a `content_ref`.
The facet MUST store digest, byte count, media type or encoding when known, capture completeness, source revision, and access metadata.

Local file and Databricks Workspace file/notebook content storage is allowed in version 1.
Automatic content capture MUST remain disabled until size, sensitivity, encryption, and access policies are configured.
If a worker reads only part of a file, the result MUST be labeled truncated and MUST NOT be presented as complete content.

### Content retention

Every stored content artifact MUST have a schema-visible effective retention duration, policy source, capture time, and expiry time.
The version 1 type default for `file.content`, including Databricks Workspace file and notebook content, is 365 days.

Resolve retention in the same precedence order as refresh intervals:

1. object override for the facet or artifact kind;
2. object-wide override;
3. system override for the facet or artifact kind;
4. system-wide override;
5. object-type default for the facet or artifact kind;
6. object-type-wide default.

Retention durations MUST be positive.
An implementation MAY expose a separate capture-disabled policy rather than overloading a zero duration.

`expires_at` is calculated from the artifact's local capture time and the effective policy in force when it is stored.
A later policy change MUST recalculate expiry for retained artifacts in its affected scope, record the policy change time, and schedule safe deletion when needed.
Increasing retention cannot restore content that has already been deleted; a normal eligible refresh is required to capture it again.

When content expires:

- the content bytes and storage reference MUST be deleted according to the retention worker's policy;
- `StoredArtifact.deleted_at` and a non-sensitive deletion outcome MUST be recorded;
- the facet MAY retain digest, byte count, source revision, capture provenance, and `content_availability=expired`;
- no checkpoint or compacted observation may retain the expired bytes;
- expiry MUST NOT be presented as remote deletion or as evidence that the remote file changed;
- expiry MUST NOT bypass refresh admission or itself trigger a downstream call.

Physical content-addressed deduplication is allowed.
Shared bytes MUST NOT be deleted until every `StoredArtifact` reference to them has expired or been deleted.

## Observation and projection semantics

### Four kinds of state

Every state query MUST make these distinctions available:

1. **Remote fact:** a value a source reported, such as file size, service active state, or job last-run time.
2. **Local bookkeeping:** first seen, last accepted observation, queue state, action start, or adapter version.
3. **Derived state:** current, due, age, queued, refreshing, or failed-last-attempt.
4. **Unknown or unsupported:** the system does not possess evidence for a value or the source cannot provide it.

Derived freshness MUST NOT be stored as if the remote source reported it.
Remote modification time MUST NOT be labeled as observation time.

### Observation batch

An observation batch MUST include:

- canonical contract version;
- batch ID and optional originating action ID;
- system, connection binding, adapter key, and adapter version;
- trusted-local observed and received times;
- zero or more normalized observation items;
- explicit coverage declarations, including member completeness, field completeness, and absence authority;
- action outcome metadata or a link to it;
- redacted diagnostics only.

An adapter MAY emit an observation batch not caused by a targeted refresh, such as information learned incidentally during a broader request or received from a future push source.
The ingester applies the same validation and ordering rules.

### Observation item

An observation item MUST declare:

- its canonical target or an adapter-owned identity locator used to create a target;
- object type, facet, and schema version;
- update mode: `snapshot`, `patch`, explicit `absence`, or relationship update;
- field coverage: `complete` or `partial`;
- an explicit field mask for every partial field set, including a partial snapshot;
- canonical payload;
- optional comparable source revision and remote as-of time;
- the refresh scopes it satisfies;
- provenance back to the batch and adapter.

An explicit absence item or collection coverage declaration MUST name its authority:

- `relationship`: it may change only membership in the declared relationship scope;
- `object_presence`: it may change object presence inside the declared identity scope;
- `facet_fields`: it may clear fields only inside a declared field-complete facet.

Missing fields in a patch or partial snapshot mean not observed.
An explicit null requires inclusion in the field mask and must be valid for that field.
A full-facet snapshot may clear an omitted field only when its registered capability declares field-complete coverage for that facet.
Otherwise the ingester MUST treat it as a patch or reject it as a contract violation.

### Observation journal

The service MUST retain a bounded append-only journal of accepted normalized observations and action outcomes.
The current projection remains the ordinary read path.
This is not full event sourcing: UI commands, every local calculation, and every internal transition need not be reconstructible as domain events.

Retention MAY compact or expire old evidence only after the current projection, required audit window, and rebuild guarantee are safe.
Raw downstream responses are not required and SHOULD be disabled by default.

For every current field and relationship, the store MUST either retain its supporting observation or include that value and a provenance summary in a versioned projection checkpoint.
A rebuild starts from the newest valid checkpoint and replays later observations.
Compaction MUST NOT leave a current value that cannot be reconstructed under this rule.

### Current projection

The projection MUST retain:

- current best-known values per facet;
- per-field provenance when patches can come from different observations;
- knowledge state;
- last qualifying observation time;
- source revision and ordering basis when available;
- local time at which the canonical value last changed;
- the observation ID supporting each current value.

A projection write and the durable recording of its accepted observation MUST be atomic, or recoverable through an idempotent inbox/outbox.
The projection MUST be rebuildable from the newest retained checkpoint plus subsequent observations.

## Freshness and refresh policy

### Refresh scope

Freshness belongs to a canonical scope:

```text
(system_id, target_kind, target_id_or_configured_scope, facet, coverage)
```

A whole-object freshness timestamp MUST NOT be the only admission signal.
UI summaries MAY calculate an object's aggregate age, but MUST expose the facet-level basis.

### Interval precedence

For each scope, resolve the effective minimum interval in this exact order:

1. object override for the facet;
2. object-wide override;
3. system override for the facet;
4. system-wide override;
5. object-type default for the facet;
6. object-type-wide default.

This is the required object over system over type-default precedence from the brief, extended consistently to facets.
An adapter's downstream rate or concurrency policy may impose a later execution time but MUST NOT make the canonical interval shorter.

### Version 1 default refresh intervals

Version 1 uses these accepted conservative defaults:

| Type and facet | Minimum interval |
|---|---:|
| `folder.metadata` | 24 hours |
| `folder.membership` | 24 hours |
| `file.metadata` | 24 hours |
| `file.content` | 7 days |
| `service.runtime` | 24 hours |
| `service.configuration` | 7 days |
| `job.metadata` | 24 hours |
| `job.status` | 24 hours |
| `job.run_summary` | 24 hours |
| `generic_object.attributes` | 7 days |

All intervals MUST be positive.
Systems and individual objects MAY override them with shorter or longer positive durations.

### Eligibility

For one atomic refresh scope:

```text
fresh_until =
  latest_qualifying_observation_at + effective_minimum_interval

policy_anchor = max(
  latest_qualifying_observation_at,
  latest_targeted_logical_action_started_at
)

eligible_at = policy_anchor + effective_minimum_interval
```

If neither anchor exists, the scope is immediately eligible.
All calculations use trusted local timestamps.

`fresh_until` describes the evidence.
`eligible_at` describes when another targeted downstream action may start.
A failed action may move `eligible_at`, but it MUST NOT move `fresh_until` or make cached facts appear current.

Starting one logical targeted action consumes the interval even if the action ultimately fails.
Adapter-internal retries are attempts within that same logical action and follow the adapter's backoff and rate policy.
They do not create new generic refresh actions.

### Evidence satisfaction

Current evidence satisfies an intent scope only when:

1. its declared target, facet, and coverage meet the normalized requested scope; and
2. either the observation arrived at or after the intent's requested time, or the current time is before `fresh_until`.

Evidence that merely has the right facet but is already due does not satisfy a new request.
An active action is coalescing, not evidence satisfaction.
An observation that arrives after the intent may satisfy it even when another action originally produced that observation.

### Incidental and broader observations

Cooldown never blocks ingestion.
A valid newer observation MUST be stored and MAY advance freshness for every scope its coverage declaration truthfully satisfies.

Examples:

- A directory listing normally refreshes the folder's membership and each returned file's metadata.
- It does not normally refresh file content.
- It MAY corroborate cached content without reading it only when the adapter contract declares a reliable comparable revision invariant, such as an authoritative content version that has not changed.
- A Unity Catalog schema listing may refresh metadata for many tables and views while a table-specific metadata query refreshes a richer metadata facet.
- A connection check may refresh connection health without refreshing any object.

The core trusts a coverage declaration only from a registered adapter contract.
It does not infer cross-facet freshness from matching field names.

### Manual refresh and planned automatic refresh

The current implementation supports manual requests.
Phase 4 automatic requests will have identical eligibility, coalescing, deferral, and adapter behavior, while their origin remains recorded for display and metrics.

A manual request made before `eligible_at` is deferred or satisfied by existing evidence, not silently discarded.
Version 1 has no one-shot force or bypass capability.
To make a targeted refresh eligible sooner, the local user must lower the applicable object or system refresh interval through the ordinary policy-override contract.
That policy change applies normally to subsequent and deferred work, is recorded as local configuration, and does not bypass adapter rate limits.
Changing a policy MUST wake affected deferred scopes for immediate local re-evaluation.

### Planned auto-refresh scheduling

The scheduler, subscription UI, and UI-session heartbeat in this subsection are Phase 4 contracts and are not implemented in the current slice.
The planned scheduler will:

1. read enabled auto-refresh subscriptions;
2. expand each subscription into supported facet scopes;
3. compute due scopes from canonical state and policy;
4. submit ordinary refresh intents;
5. rely on normal queue deduplication and admission;
6. never contact an adapter directly.

The scheduler MAY scan ahead and submit a future intent with `not_before=eligible_at`.
The coordinator MUST re-evaluate it when claimed because newer incidental evidence may have satisfied it.

Auto-refresh subscriptions are session-scoped in version 1.
The scheduler MUST accept a subscription only while its originating UI session has a valid local heartbeat.
When the UI closes, explicitly stops auto-refresh, crashes, or exceeds the heartbeat timeout:

- the subscription expires and MUST NOT be restored automatically on the next UI start;
- the scheduler MUST stop creating intents for it;
- every scope originating from that subscription MUST be detached or cancelled;
- work shared with a manual intent or another live UI-session scope MUST remain attached;
- a `ready` or pre-running `leased` observation action MUST be cancelled when no live originating scope remains;
- an auto-only action in `retry_wait` MUST schedule no further attempt;
- an action already in `running` MAY finish its current in-flight attempt because a downstream read may already have occurred, but MUST NOT retry after that attempt when no live originating scope remains.

## Queue and action lifecycle

### Delivery model

Queues MUST be durable.
Delivery MAY be at least once.
Exactly-once downstream execution is not promised.

Every claimed record MUST use an atomic lease with an expiry.
An expired lease MAY be reclaimed, but the item MUST be revalidated before downstream work.

If intent/action state and queue delivery cannot share a transaction, the implementation MUST use a durable outbox/inbox pattern.
The service MUST durably mark a logical action as started before making a downstream call.

### Intent-scope states

| State | Meaning |
|---|---|
| `queued` | Recorded and waiting for local admission. |
| `leased` | Temporarily owned by a coordinator. |
| `deferred` | Not yet eligible; includes a visible next-evaluation time. |
| `coalesced` | Attached to an equivalent or declared broader active action. |
| `admitted` | Produced or attached to an adapter action. |
| `satisfied` | Existing or incidental evidence met the request without a new action. |
| `rejected` | Permanently invalid, unsupported, unauthorized, or outside configured scope. |
| `expired` | No longer useful by its requested expiry time. |
| `cancelled` | Cancelled locally before an irreversible dispatch boundary. |

These states belong to `RefreshIntentScope`.
One parent intent may contain mixed outcomes, such as satisfied metadata and deferred content.
Every terminal intent scope MUST retain a machine-readable disposition reason and, when applicable, linked action and observation IDs.

The parent `RefreshIntent` aggregate state MUST be derived from its scope records.
It is complete only when every scope is terminal or has reached its linked action's terminal outcome.
The query contract MUST return per-scope state rather than hiding mixed outcomes behind the aggregate.
The parent aggregate MAY use only `open`, `complete`, or `cancelled`; success, partial failure, and mixed detail come from the scope and action records.

### Adapter action states

| State | Meaning |
|---|---|
| `ready` | Admitted and available to the bound adapter. |
| `leased` | Temporarily owned by an adapter worker. |
| `running` | Logical start is durable and downstream work may occur. |
| `retry_wait` | The adapter plans another attempt within the same logical action. |
| `satisfied` | New qualifying evidence made the action unnecessary before downstream execution. |
| `succeeded` | Required coverage was ingested successfully. |
| `partial` | Some valid observations were ingested but required coverage was incomplete or some items failed. |
| `failed` | No further adapter attempts will occur and required coverage was not produced. |
| `cancelled` | Cancelled before downstream execution or according to adapter-safe semantics. |

### Alertable failure events

A transition to terminal `failed` MUST atomically, or through an idempotent outbox, create one alertable `OperationalEvent`.
An unexpected queue-worker processing failure MUST create a redacted operational event on every failed attempt and an alertable terminal event when retry or poison-item policy is exhausted.

At minimum, event types include:

- `refresh.action.attempt_failed` (non-alertable attempt history);
- `refresh.action.failed`;
- `queue.coordinator.failed`;
- `queue.adapter_worker.failed`;
- `observation.ingestion.failed`;
- `retention.deletion.failed`.

Expected local outcomes such as satisfied, coalesced, deferred, cancelled-on-session-close, or validation rejection are observable dispositions but are not alertable failures by default.

Operational events MUST:

- use an idempotency key tied to the state transition or worker attempt;
- link the affected system, intent scope, action, attempt, or artifact when available;
- contain a stable error class, severity, occurrence time, and redacted summary;
- remain queryable from local state after process restart;
- contain no credentials, raw content, or unsafe downstream payload.

Version 1 need not render pop-up alerts.
It MUST expose alertable events through the local state-query contract so a later UI alert feature does not depend on scraping process logs.

### Deduplication and coalescing

An observation action dedupe key MUST include:

- system and connection binding;
- capability and capability version;
- canonical target;
- requested facet and coverage;
- normalized non-secret parameters that change semantics.

Equivalent refresh intents within an active observation action attach to that action.
A broader action MAY absorb a narrower intent only when its registered capability declares that it produces a superset of the requested coverage.
Every requester retains its own intent receipt and disposition.

The active states for dedupe are `ready`, `leased`, `running`, and `retry_wait`.
Creation MUST use an implementation-independent atomic compare-and-create or uniqueness rule over the normalized dedupe key across those states.
If concurrent coordinators race, exactly one action becomes the active winner.
Every loser MUST attach its intent scope to that winner and record `coalesced`.

Deduplication is a cost and concurrency control, not the correctness mechanism.
Revalidation and idempotent observation ingestion remain required if a crash or external queue behavior causes redelivery.

## Generic coordinator contract

The coordinator has no network client, downstream credentials, CLI command builder, or vendor response model.

### Local inputs

The coordinator MAY read:

- refresh intent and actor/session;
- system state and connection binding identity;
- canonical target, object presence, type, and facets;
- capability bindings and their versioned input/output declarations;
- effective refresh policy;
- qualifying observation credit;
- active and prior logical actions;
- configured target allowlists and subscription scope;
- current trusted local time.

It MUST NOT inspect remote API data or fetch current downstream capabilities during admission.

### Refresh admission algorithm

```text
claim intent with a lease
validate contract version and request schema
validate actor, system state, target, and configured scope
resolve a compatible enabled capability from local bindings
normalize requested facets into atomic refresh scopes

for each scope:
    attach to an equivalent or declared broader active action when present
    otherwise calculate effective interval and eligible_at
    if current evidence satisfies the formal coverage and time rule:
        mark scope satisfied
    else if now is before eligible_at:
        defer scope until eligible_at
    else:
        atomically claim the active-action dedupe key
        create the action only when this coordinator wins
        otherwise attach the scope to the winning action

record an explainable disposition for every requested scope
```

Capability choice MUST be deterministic.
Version 1 SHOULD require one preferred capability per target/facet combination or an explicit local priority.
The coordinator SHOULD NOT become a cost optimizer until real overlapping capabilities require it.

### Validation outcomes

The coordinator MUST distinguish:

- invalid request schema;
- unknown or disabled system;
- unknown target;
- known-absent target;
- target outside configured scope;
- unsupported facet;
- missing, disabled, or incompatible capability;
- unauthorized actor;
- duplicate or coalesced work;
- existing evidence already sufficient;
- minimum interval not elapsed;
- action admitted.

These outcomes are local decisions and SHOULD be visible in the UI.

### Pre-dispatch local guard

Immediately before an adapter action enters `running`, a local guard MUST revalidate:

- the action lease and active-action dedupe winner;
- system, connection binding, capability, and adapter enabled state;
- pinned adapter and capability compatibility;
- canonical target presence and configured allowlist;
- cancellation or pause state;
- at least one live manual or active-session originating scope;
- whether a qualifying observation accepted after admission now satisfies the action.

This guard uses local canonical data only and remains generic coordinator policy.
The adapter worker invokes the guard but does not reimplement or weaken it.

If the system, binding, capability, or scope is no longer enabled, the guard cancels the action with a stable local reason and no downstream call.
If an observation action has no remaining live originating scope, the guard cancels it before dispatch.
If newer evidence now satisfies every action scope, the guard marks the action `satisfied`, links the evidence, updates attached intent scopes, and makes no downstream call.
An incompatible pinned contract fails explicitly.
Only a successful guard result permits the durable transition to `running`.

### Discovery scopes

Initial discovery cannot always target a known object.
A capability MAY declare a preconfigured system or collection scope, such as an allowlisted workspace root, job collection, server directory root, or service allowlist.

The UI may select only registered scopes.
It MUST NOT put an arbitrary remote path, endpoint, query, or command into a generic queue message.

## Adapter and API worker contract

### Ownership

An adapter worker exclusively owns:

- secret resolution;
- authentication and connection setup;
- downstream API, SDK, CLI, SSH, agent, or operating-system details;
- command and request construction using structured arguments;
- pagination and complete/partial result determination;
- downstream rate limits, concurrency groups, quotas, retries, and backoff;
- mapping downstream errors to canonical error classes;
- external identity construction;
- canonical normalization and truthful coverage declarations.

The adapter MUST enforce configured target allowlists again before a downstream call.
Local coordinator validation is not a substitute for enforcement at the trust boundary.

### Collateral effects of observation

Remote-observation-only describes the intended operation, not a guarantee of zero remote side effects.
A read may update file access time, write an access or audit record, create a short-lived session, warm a cache, or consume a remote quota.

Every capability MUST declare:

- known or plausible collateral effects;
- the remote resources or metadata they may affect;
- whether the effect is unavoidable, configurable, or adapter-mitigated;
- any user-visible warning required before enabling the capability.

Adapters MUST choose the least invasive supported observation method.
Collateral effects MUST NOT be exposed as selectable state-change operations.
If a collateral effect exceeds configured tolerance, the capability MUST remain disabled.
A later observation of collateral metadata, such as an updated access time, is ingested as an ordinary remote fact.

### Dispatch envelope

A generic action sent to an adapter contains only canonical and registered data:

```json
{
  "contract_version": "1",
  "action_id": "uuid",
  "correlation_id": "uuid",
  "system_id": "uuid",
  "connection_binding_id": "uuid",
  "adapter": {
    "key": "databricks",
    "version": "adapter-version"
  },
  "capability": {
    "key": "databricks.workspace.children.read",
    "version": "1"
  },
  "target": {
    "kind": "configured_scope",
    "id": "uuid"
  },
  "requested_facets": ["folder.membership", "file.metadata"],
  "deadline": "timestamp-or-null"
}
```

The envelope MUST NOT contain a token, password, private key, arbitrary URL, shell string, executable command, or unregistered source-native parameter.

### Adapter output

An adapter returns:

- action outcome and canonical error class;
- zero or more versioned observation batches;
- explicit coverage and completeness;
- adapter-provided retry guidance;
- redacted diagnostics;
- operational counters such as pages, calls, or items when safe.

The action may succeed with zero observed objects when an empty complete collection has the declared authority required by the capability contract.
That case is distinct from failure and from an incomplete response.

### Canonical error classes

At minimum, adapters MUST classify:

- authentication;
- authorization;
- connection or timeout;
- downstream rate limit;
- transient downstream failure;
- not found;
- unsupported;
- invalid downstream response;
- adapter contract mismatch;
- local cancellation;
- unknown adapter failure.

Source-native codes MAY be retained in redacted diagnostics.
They MUST NOT be interpreted by the generic coordinator.

### Retry and rate behavior

Adapter workers own retry count, retryable error classification, backoff, concurrency, and downstream rate groups.
They MUST respect any syntactically valid downstream retry-after signal within the adapter's bounded scheduling contract. The Databricks worker accepts delta-seconds or HTTP-date guidance up to 24 hours and performs no automatic retry when a larger delay is requested.
They MAY delay an admitted action beyond canonical `eligible_at`.

Adapter retries remain part of one logical action.
They MUST NOT emit duplicate state changes when an observation batch is redelivered.


## Observation ingestion and reconciliation

### Ingestion order

For each batch, the ingester:

1. validates batch, adapter, connection, and contract versions;
2. verifies that every item belongs to an exact action or enabled incidental capability/scope;
3. validates canonical type and facet schemas;
4. resolves or creates object identities through adapter-owned external keys;
5. orders each item against current evidence;
6. records accepted observations idempotently;
7. updates current facets and relationships;
8. applies absence reconciliation only across fully valid complete boundaries;
9. grants refresh credit only to declared supported scopes;
10. records item-level issues and the final action outcome.

Every facet and relationship item MUST identify the scope that authorized the downstream read.
This `authorized_by` scope is distinct from any `satisfies` claim: authority permits an item to enter canonical evidence, while satisfaction grants freshness credit.
Action-linked authority MUST match the action's pinned capability/version and stored requested scope.
Incidental authority MUST resolve to one unambiguous enabled capability and one supported scope.
A non-root collection fact MUST be linked by an authorized same-batch `contains` edge from the exact collection subject; direct facet evidence MUST target the exact authorized object.
Identity, journal, and projection writes occur only after this check.

Lease authority for a new action-linked batch linearizes when the ingester, inside its `BEGIN IMMEDIATE` transaction, verifies the matching unexpired `running` lease. That writer reservation prevents another worker from reclaiming or reassigning the action until the whole ingestion transaction commits or rolls back. Wall-clock expiry during the bounded transaction does not retroactively invalidate evidence admitted at that linearization point; every later attempt, completion, or new batch still performs its own current lease check. Exact replay of an already recorded batch digest remains idempotent and does not require the original lease.

Every JSON-bearing shared contract defensively copies its value and rejects active-ancestor cycles, depth above 32, more than 10,000 items in one container, or more than one million traversed nodes. Canonical digest failure rejects the batch before opening its ingestion transaction, leaving no batch, journal, issue, identity, or projection residue.

### Merge rules

- A patch updates only its explicit field mask.
- A partial snapshot behaves like a patch and updates only its explicit field mask.
- A field-complete snapshot may replace only the facet and field scope its capability declares complete.
- A missing field in a patch or partial snapshot never clears a known value.
- Explicit null, unknown, unsupported, and absent remain distinct.
- Remote modification time is data and does not by itself order receipt of two observations.
- Comparable decimal adapter source revisions decide facet ordering before timestamps.
- Otherwise, trusted `observed_at` and then `received_at` order comparable evidence.
- If revision and both timestamps tie exactly, the first accepted projection remains current.
- When observations cannot be safely ordered, the system SHOULD preserve evidence and surface uncertainty rather than invent stronger consistency.
- A late older observation MUST NOT overwrite a newer comparable current value.

### Collection completeness

Coverage MUST be one of:

- `complete`: every member in one declared bounded scope was considered;
- `partial`: returned members are valid, but omissions prove nothing;
- `unknown`: completeness cannot be established.

Completeness alone does not define what becomes absent.
Every coverage declaration MUST also state whether it is authoritative for a relationship, object presence, or facet fields.

A complete folder membership listing normally marks an omitted containment relationship absent.
It MUST NOT tombstone the child object unless the registered capability separately declares object-presence authority for that exact identity scope.
An adapter MAY declare both authorities when the downstream source actually supports both claims.

Only a successful and fully validated complete boundary may infer absence within its exact declared authority.
If any item invalidates the completeness boundary, valid positive items MAY be ingested, but omission reconciliation MUST be skipped and the action marked partial.

### Partial success

One malformed item SHOULD NOT discard unrelated valid items.
The ingester SHOULD quarantine the bad item, ingest safe items, and mark the action partial.
No partial result may accidentally widen its coverage or tombstone omitted objects.

### Freshness credit

An accepted observation advances `latest_qualifying_observation_at` for each declared scope it satisfies.
The ingester MUST validate each satisfaction claim against the registered capability contract.

A fresh metadata observation cannot credit content merely because both belong to one file.
It can credit content validity only if the capability contract records a reliable source-specific revision invariant and the observation proves it.

## Connection and capability model

### Configuration versus observation

The schema distinguishes:

- configured capability: local intent that an adapter may perform an action;
- supported capability: compatibility declared by the registered adapter version;
- enabled capability: locally allowed for this system;
- last verified availability: a time-bound observation that a remote call worked.

The statement “this Databricks binding can list Unity Catalog schemas” is local configuration and adapter compatibility.
The statement “these credentials successfully listed schemas at 10:00” is an observation.
One MUST NOT silently replace the other.

### Capability declaration

A capability binding MUST declare:

- namespaced key and version;
- compatible adapter and core contract versions;
- valid system kinds;
- valid target kinds and object types;
- canonical input schema;
- facets and relationships it may produce;
- possible collection coverage;
- which refresh scopes its observations may satisfy;
- operation class, which MUST be `observe` in version 1;
- declared collateral effects and mitigations;
- local selection priority;
- downstream concurrency or rate group name when needed;
- enabled state and configured allowlist.

Vendor endpoints, CLI syntax, and raw command arguments remain adapter code, not generic capability data.
The coordinator MUST reject any capability not classified as `observe`.
Configuration cannot convert an observing capability into a remote state-change operation.

### Capability evolution

Capability versions are immutable contracts.
An additive adapter upgrade MAY register a new version alongside the old one.
Actions pin the binding and capability version used at admission.
Changing permitted target/facet coverage, maximum completeness, or absence authority MUST register a new capability version rather than mutating the existing version's policy.
An adapter worker MUST fail explicitly rather than reinterpret an action created for an incompatible version.

## UI and TUI requirements

The state-query and refresh-intent contracts MUST remain presentation-framework independent.

### Required views

The first useful client SHOULD support:

- systems and their enabled/connection-observation state;
- object navigation by containment and type;
- facet values and knowledge state;
- exact observed and remote modification times;
- freshness and effective interval;
- queued, deferred, coalesced, running, partial, failed, and satisfied refresh activity;
- per-scope outcomes when one request selects multiple facets;
- manual refresh for supported selected facets;
- planned Phase 4 auto-refresh selection;
- adapter and provenance detail sufficient to explain a value;
- declared collateral effects for the selected observation capability;
- content availability and retention expiry when content is stored;
- redacted failure detail and next eligible or retry time;
- a queryable history of alertable operational failures, even if active alert presentation is implemented later.

Object inventory views MUST use bounded pages and keep every cached object reachable through pagination or filtering.
Refresh controls for objects SHOULD follow the displayed page; configured-scope refreshes remain visible independently.
Object detail views MUST bound current outgoing containment pages and SHOULD expose facets, provenance, presence, type filtering, and object-specific refresh controls.

### Freshness presentation

The client MUST distinguish:

| Display state | Meaning |
|---|---|
| Unobserved | No accepted evidence exists for the facet. |
| Current | The latest qualifying evidence is inside the effective interval. |
| Due | The interval has elapsed and no equivalent work is active. |
| Queued or refreshing | Local work exists; cached facts remain visible. |
| Deferred | A request exists but cannot yet dispatch; show `eligible_at`. |
| Failed last attempt | The most recent action failed; retain and age the last-known facts. |
| Unsupported | The bound adapter cannot provide this facet. |

Stale or due does not mean wrong.
The UI SHOULD show the exact age and “known as of” time rather than only a color.
An object-level indicator SHOULD be derived from the facets relevant to the current view and allow the user to inspect them.

### Interaction safety

- A refresh control MUST submit a generic intent and return a local receipt.
- A receipt for multiple facets MUST expose every `RefreshIntentScope`, its disposition, evidence, action, and next eligible time.
- A too-soon manual request MUST show the effective interval and the object, system, or type policy that supplied it.
- To request an earlier call, the user changes the ordinary object or system interval override and lets the deferred scope re-evaluate; the client MUST NOT offer a force bypass.
- The client MUST NOT accept or construct arbitrary API endpoints, CLI arguments, or shell commands.
- Large-scope refreshes SHOULD show the selected canonical scope, expected capability, and declared collateral effects before submission.
- Remote strings and content MUST be escaped for terminal and browser control sequences.
- File content MUST respect separate authorization and display policies.
- The client SHOULD make locally rejected, coalesced, deferred, and satisfied-without-call outcomes understandable.

### Planned Phase 4 auto-refresh selection

When implemented, the auto-refresh UI MUST make clear:

- which system, object subtree, or facets are selected;
- the effective interval and source of that interval;
- that the selection does not survive the UI session;
- the next due time;
- whether an adapter or system is paused.

The client MUST stop the session heartbeat during orderly shutdown and SHOULD explicitly expire its subscriptions.
The service heartbeat timeout is the fallback for crashes or lost clients.

## Initial target systems

### Databricks adapter

Databricks is the first version 1 adapter, API worker, and live end-to-end test case.
The worker MUST use the installed Databricks CLI for all downstream access.
Direct Databricks REST or SDK access and the CLI's arbitrary `databricks api` command are outside the version 1 adapter boundary.

> The Databricks API worker is a queue worker that invokes the existing `databricks` executable.
> It is not a new Databricks API client, REST wrapper, or replacement CLI.

The adapter binds a configured workspace scope without exposing Databricks command syntax to the generic core.
It MUST reuse the user's existing supported Databricks authentication configuration through a named profile or equivalent CLI-owned binding.
The canonical store keeps only the profile/binding reference and non-secret scope.

The accepted version 1 inventory is:

| Databricks concept | Canonical mapping |
|---|---|
| Workspace directories | `folder.metadata` and `folder.membership` |
| Workspace files and notebooks | Current: `file.metadata`; deferred target: retained `file.content` artifacts |
| Unity Catalog catalogs | `generic_object` with source kind `databricks.uc.catalog` |
| Unity Catalog schemas | `generic_object` with source kind `databricks.uc.schema` |
| Unity Catalog tables and views | Metadata-only `generic_object` records, including schema/column metadata when returned by the metadata API |
| Unity Catalog volumes | Metadata-only `generic_object` records for the volume object |

Catalog-to-schema and schema-to-table/view/volume containment are canonical relationships.
When implemented, Workspace content artifacts use the normal 365-day type default and retention override rules.

The versioned capability contract includes the following keys. Registration preserves the future contract; it does not make a deferred capability executable.

| Capability key | Canonical target and output |
|---|---|
| `databricks.workspace.children.read` | Configured Workspace folder to membership plus child folder/file metadata |
| `databricks.workspace.metadata.read` | Known Workspace object to folder/file metadata |
| `databricks.workspace.content.read` | Deferred: known Workspace file/notebook to one retained `file.content` artifact; the current worker rejects this key before CLI execution |
| `databricks.uc.catalogs.read` | Configured metastore/workspace scope to catalog metadata and relationships |
| `databricks.uc.schemas.read` | Known catalog to schema metadata and relationships |
| `databricks.uc.relations.read` | Known schema to table/view metadata and relationships |
| `databricks.uc.volumes.read` | Known schema to volume-object metadata and relationships |

Version 1 MUST NOT:

- query, sample, preview, export, or store table or view rows;
- execute SQL to infer table content;
- list files inside Unity Catalog volumes;
- read or store Unity Catalog volume file content;
- follow table or volume storage locations to underlying object storage;
- treat Unity Catalog metadata permission failure as object absence.

Jobs, clusters, pipelines, warehouses, serving endpoints, and other Databricks surfaces are outside the initial inventory.
Their canonical types remain available for later adapter capabilities.

The installed Databricks CLI on the design host exposes the relevant `workspace`, `catalogs`, `schemas`, `tables`, and `volumes` metadata command groups, structured JSON output, and named profiles.
The adapter MUST verify the available command and output contract at runtime or startup rather than assuming one globally fixed CLI version.

#### CLI-backed worker boundary

The Databricks integration contains two small adapter-owned logical parts:

1. A thin CLI command map/runner that maps only registered capability keys to fixed Databricks CLI command groups and structured argument arrays.
2. A Databricks API worker that consumes admitted `AdapterAction` records, applies Databricks-specific concurrency/retry policy, calls that runner, and emits normalized observations and action outcomes.

No other component may invoke the Databricks CLI.
The CLI runner MUST:

- resolve and invoke the existing `databricks` executable rather than install, vendor, or reimplement it;
- invoke the executable without a shell command string;
- pass the profile and JSON-output selection as separate structured arguments;
- resolve the executable only through explicit absolute `PATH` entries and run from a fresh empty directory beneath a private per-user root whose full ancestor chain contains no CLI-recognized bundle configuration;
- remove inherited Databricks and bundle override variables so the named profile remains authoritative while ordinary home, trust-store, proxy, and cloud SDK settings remain available;
- validate the installed CLI version and required command groups at startup;
- capture stdout, stderr, exit code, timeout, and correlation ID;
- parse only the expected JSON contract for the pinned capability version;
- redact profile, host, authentication, and diagnostic secrets before persistence;
- reject every unregistered command group, subcommand, flag, endpoint, and free-form argument.

The API worker MUST:

- support every registered Workspace and Unity Catalog capability in the table above before the Databricks adapter is considered version 1 complete;
- own pagination and declare complete, partial, or unknown coverage truthfully;
- map CLI failures to canonical error classes;
- create stable external keys and canonical relationships;
- submit observation batches through the observation-ingestion port;
- persist attempt and redacted outcome state only through the action-lifecycle port;
- never write canonical object tables directly.

The first live worker test uses only `databricks.workspace.children.read` against one configured Workspace directory.
After that thin slice passes, the same worker expands to the remaining Workspace and Unity Catalog capabilities before Linux adapter work begins.

The adapter owns:

- structured process invocation without a shell command string;
- profile selection and secret resolution;
- pagination and source-specific completeness;
- mapping Workspace and Unity Catalog objects to stable external keys;
- enforcing the Unity Catalog metadata-only boundary before invocation and during normalization;
- Databricks-specific error and rate behavior;
- redaction of CLI diagnostics.

Unity Catalog schemas, relations, and volumes use their typed source UUIDs when the CLI supplies them. Qualified names remain mutable display/dispatch facts and do not define those objects' identity. Historical name-key objects and responses without a source UUID remain separate: Rookery does not auto-merge them into typed identities because rename versus delete/recreate cannot be proven retrospectively.

Databricks observations may consume API quota and create authentication, access, or audit records.
The adapter spike MUST verify and record those collateral effects for each capability before it is enabled.

### Remote server adapter

The version 1 server target is a Linux host reached over SSH.
The server adapter supports only configured hosts, filesystem roots, and service identifiers.
SSH details do not enter the canonical refresh request.

The connection binding stores non-secret SSH settings such as:

- configured host alias or hostname and port;
- remote user;
- known-hosts entry or pinned host-key fingerprint;
- credential or SSH-agent reference;
- allowlisted filesystem roots and service identifiers.

Initial capabilities SHOULD be limited to:

- `linux.ssh.folder.children.read`: list one allowlisted folder and return folder membership plus file metadata;
- `linux.ssh.file.metadata.read`: read metadata for one known allowlisted file;
- `linux.ssh.file.content.read`: read content for one known allowlisted file when content policy permits it;
- `linux.ssh.service.status.read`: read runtime and enablement state for one allowlisted service.

The adapter MUST use registered observation command templates with validated positional values.
It MUST NOT accept arbitrary remote commands, command fragments, environment assignments, or user-controlled shell syntax from the UI or queue.
Filesystem paths MUST resolve within an allowlisted root before execution and be checked again when results are normalized.

`linux.ssh.file.content.read` MUST declare that reading content may update file access time depending on the filesystem and mount policy.
Opening an SSH session may also create authentication, login, or audit records.
The adapter MUST minimize these effects where supported but MUST NOT claim that a remote read is side-effect-free.

The adapter MUST map operating-system-specific service states into canonical enums without losing the source-native value.
The first Linux service-manager backend remains adapter configuration until the target distribution and init system are known.
SSH authentication, host-key, timeout, connection, and remote-command failures use the canonical failure classes and alertable operational-event contract.

## Failure, concurrency, and recovery behavior

| Scenario | Required behavior |
|---|---|
| Invalid request or target | Reject before adapter dispatch with a stable reason. |
| Equivalent active request | Coalesce and preserve a receipt for each requester. |
| Request before minimum interval | Defer until `eligible_at`, then re-evaluate. |
| New incidental evidence while deferred | Ingest it; mark the intent satisfied if coverage is sufficient. |
| Adapter unavailable or version mismatch | Fail or defer explicitly; never select an unrelated adapter. |
| Transient coordinator or worker runtime failure | Keep cached reads available, emit one event per outage transition, and retry with bounded backoff until recovery or orderly shutdown. |
| Authentication or authorization failure | Preserve cached facts; record the action failure and redacted class. |
| Timeout or rate limit | Preserve cached facts; let the adapter apply retry/backoff and guidance. |
| Terminal worker or action failure | Preserve cached facts, record the final state, and emit one idempotent alertable operational event. |
| Partial page or listing | Ingest positive facts; never infer omission. |
| Empty complete listing | Reconcile only the relationship, object-presence, or facet-field absence authorized for the exact declared scope. |
| Invalid observation item | Quarantine the item; ingest unrelated safe items; disable affected absence reconciliation. |
| Store unavailable before durable action start | Do not perform the remote call. |
| Backup file or publication synchronization fails | Preserve the primary database, report failure, and remove only the matching unpublished snapshot. |
| Crash after remote read before ingestion | The read may repeat; ingestion remains idempotent. |
| Orderly process shutdown during a remote read | Cancel the local worker task and subprocess promptly; leave durable action recovery to the ordinary lease path. |
| Expired queue lease | Reclaim and revalidate before work. |
| Out-of-order result | Use comparable source revision or trusted local observation order; do not overwrite known newer evidence. |
| Projection failure after journal write | Replay the durable observation into the projection. |
| File content exceeds policy | Store no content, or an explicitly labeled allowed partial artifact; never silently claim completeness. |
| Observation causes a declared collateral effect | Record any observed result normally; do not present the effect as a requested remote action. |
| Adapter discovers an undeclared material collateral effect | Pause the capability, emit an alertable operational event, and require its contract and tolerance policy to be reviewed. |
| Content retention expires | Delete bytes safely, retain allowed metadata/provenance, and never infer a remote change. |
| Retention deletion fails | Preserve the storage reference for retry, record the failure, and emit an alertable operational event. |
| UI session ends | Expire its subscription, detach its scopes, cancel pre-running auto-only actions, stop retries, and preserve shared or already-ingested work. |
| System or capability paused | Stop new admissions and leave cached state readable. |

### Consistency guarantees

- State queries are locally consistent with committed projections.
- The service provides no point-in-time consistency across different remote calls unless an adapter explicitly supplies it.
- Refresh requests are durable once acknowledged.
- Downstream observation calls may repeat.
- Accepted observation IDs are idempotent.
- An action's final status may be partial even when some facts were successfully updated.

### Cancellation

Cancellation is best effort.
An intent can be cancelled while queued or deferred.
A coalesced intent can detach without cancelling the shared action needed by other intents.
After an adapter action starts, cancellation follows adapter-safe semantics and MUST NOT imply that no remote call occurred.

## Security and trust boundaries

### Credentials

- Credentials MUST live in a secret provider or existing secure client configuration.
- Canonical records and queue payloads contain references only.
- Only the bound adapter worker may resolve a reference.
- Diagnostic output MUST be redacted before persistence.
- Credential health is an observed result and MUST expire like other knowledge.

### Remote input

Remote object names, paths, IDs, timestamps, service states, errors, and file contents are untrusted.
They require:

- schema and length validation;
- canonical encoding;
- terminal and browser escaping;
- size and resource limits;
- safe path handling;
- log redaction;
- explicit content access control.

### Command and network execution

- Generic requests MUST NOT carry arbitrary endpoints or executable text.
- Adapters MUST use typed SDK calls, structured process argument arrays, or a fixed protocol.
- Observation requests MUST carry only capability-validated targets and parameters and MUST NOT be synthesized into free-form remote commands.
- Server roots and services MUST be allowlisted in local configuration and enforced again at execution.
- SSH host keys or equivalent remote identity MUST be verified.
- Redirects or endpoint changes MUST remain inside adapter-owned configured policy.
- A plugin or adapter is trusted code and SHOULD run with only the credentials and network access required for its bound systems.

### Authorization

Version 1 is local and single-user.
Contracts MUST still record the local actor and UI session so actions, policy changes, content access, and failures remain attributable.

The service MUST bind only to local inter-process or loopback interfaces.
For the browser UI, each runtime creates a high-entropy `*.localhost` browser hostname and trusts only that Host.
Before disclosing an activation capability, startup MUST exclusively reserve and listen on both `127.0.0.1` and `::1` at the configured port; failure to own either address family MUST close any partial reservation and stop without disclosure.
The launching terminal discloses one short-lived, single-use activation capability in a URL fragment on that unique host.
The browser removes the fragment before exchanging the capability in a same-origin request body for a host-only, process-local session cookie.
The capability and session MUST remain memory-only, rotate on restart, stay out of request targets and application/access logs, and gate every cached-data or mutation route by default.
The session cookie MUST be host-only so ordinary `127.0.0.1`, `localhost`, and unrelated `*.localhost` services do not receive it.
Operating-system account and terminal permissions protect this bootstrap channel; loopback location or port alone is not caller authentication.
It MUST NOT expose a network-accessible multi-user interface without a new authentication, authorization, tenancy, and content-access design.

The configured SQLite parent and each backup destination parent are dedicated current-user state directories, not shared folders or Git worktree roots.
Rookery MUST reject redirects and multiply hard-linked state files, establish current-user ownership, and restrict directories/files to `0700`/`0600` on POSIX or protected current-user DACLs on Windows.
Before any write-capable open, WAL transition, sidecar creation, migration, repair, or backup publication for an existing database, Rookery MUST validate its application identity and minimum schema through an immutable read-only connection that does not open WAL shared state.
New and markerless stores MUST migrate and persist the `ROOK` application ID in rollback-journal mode before enabling WAL; an unmarked store with WAL/SHM sidecars MUST fail closed until its owning version cleanly checkpoints it.
If that preflight identifies foreign SQLite state, startup and backup MUST leave its bytes, journal mode, and sidecar set unchanged.
A successful backup MUST synchronize the validated snapshot file and final publication metadata before returning; a failed or unsupported durability barrier MUST publish no claimed recovery copy.

There is no force-refresh permission in version 1.
Refresh and retention policy changes are ordinary attributable local configuration changes.
Single-user operation does not weaken the remote-observation-only boundary, credential isolation, local audit, or collateral-effect requirements.

### Sensitive content

File content, notebook content, configuration, and remote error text may contain secrets or regulated data.
Content retention defaults to 365 days.
Size, encryption, redaction, indexing, searchability, backup, and local access policies MUST be configured before general automatic capture is enabled.

## Observability and operations

### Structured logs

Logs SHOULD include:

- intent, intent-scope, action, attempt, batch, artifact, operational-event, system, adapter, and correlation IDs;
- state transitions and duration;
- capability version and declared collateral-effect classification;
- local disposition and canonical error class;
- item and coverage counts;
- retry and rate-limit decisions;
- retention-policy resolution and deletion outcomes.

Logs MUST NOT include credentials, raw command strings containing secrets, raw file content, or unredacted downstream payloads.
Creating a process log entry does not replace the durable `OperationalEvent` required for alertable failures.

### Metrics

At minimum, expose:

- queue depth and age by state, adapter, and capability;
- admitted, coalesced, deferred, satisfied-without-call, rejected, succeeded, partial, and failed counts by intent scope;
- downstream calls, latency, and error classes by adapter capability;
- current object/facet counts and age distribution;
- observation ingestion and quarantine counts;
- alertable operational-event counts by type and severity;
- lease expirations and redeliveries;
- auto-refresh due-to-dispatch delay;
- rate-limit and retry delay;
- retained artifact bytes, upcoming expirations, deletion successes, and deletion failures.

### Operator controls

Operators MUST be able to:

- pause a system, adapter binding, capability, or auto-refresh subscription;
- inspect queued and running work without revealing secrets;
- inspect alertable operational events and their linked local actions;
- see why an intent was deferred or rejected;
- retry an eligible failed refresh through the ordinary intent path;
- inspect effective retention policy and upcoming content expiry;
- rebuild a projection from the newest retained checkpoint and later observations;
- disable a faulty adapter version while preserving cached state.

## Configuration and compatibility

### Configuration boundaries

Canonical configuration includes:

- systems and their enabled state;
- non-secret connection settings and secret references;
- configured discovery scopes and allowlists;
- capability bindings;
- capability collateral-effect declarations, mitigations, and configured tolerances;
- type defaults and refresh overrides;
- retention defaults and overrides;
- content capture and local-access policies.

The local TOML file is desired enabled state for its Databricks resources.
Each new entry has an explicit stable configuration ID.
Legacy entries without an ID remain compatible; adding an ID before changing other identity inputs records a durable mapping to the existing system and cached history.
Display-name and profile changes under the same ID and Workspace root update one local system.
Removing an entry disables its system, binding, capabilities, and configured scopes without deleting cached facts.
Changing the Workspace root creates a new local system authority boundary and disables the predecessor.

Session runtime state includes:

- active UI sessions and heartbeats;
- session-scoped auto-refresh subscriptions;
- the automatic intents linked to each subscription.

Session runtime state MUST NOT be restored as active after process restart without a new live UI session.

Adapter implementation configuration includes:

- downstream client and CLI details;
- page sizes within source constraints;
- retry policy;
- rate and concurrency groups;
- source-specific mappings and completeness rules;
- error redaction.

### Build and dependency integrity

The committed `uv.lock` is the dependency authority for source-checkout verification and reproducible release smoke.
Installed-wheel verification MUST export the hash-pinned runtime-only graph from that lock, install it before the wheel, install the wheel without dependency resolution, check installed compatibility, and audit the exact installed versions.
A separate newest-allowed or range-resolution compatibility job MAY exist, but it MUST be labeled separately and MUST NOT substitute for locked release evidence.
Automated `uv` and GitHub Actions update proposals MUST run the complete cross-platform gate before merge.

### Versioning

- Core request, action, observation, type, facet, and capability contracts MUST have explicit versions.
- Schema changes SHOULD be additive when possible.
- Before an upgrade's first stateful invocation, preserve a verified pre-migration backup. In-place database downgrade is unsupported until a validated restore or down-migration path exists.
- A reader encountering an unsupported facet MUST preserve it as unsupported rather than delete it.
- An action pins the exact adapter and capability version admitted.
- Adapter upgrades SHOULD support side-by-side contract validation before activation.
- Rollback MUST be possible by pausing a capability or selecting the prior compatible adapter version.

### Time and clock behavior

Trusted local service time drives leases, cooldowns, observation receipt, and scheduling.
Remote times are nullable facts.
Clock skew between local processes must remain within a documented operational tolerance, or one authoritative persistence clock MUST be used.

### Scale assumptions

No hard throughput or latency SLO is specified because object counts, system counts, and desired freshness are unknown.
The initial design SHOULD optimize for correctness and inspectability at single-user local scale.
The contracts MUST avoid requiring full object scans for every UI read, but a simple indexed scheduler scan is acceptable until measured otherwise.
The current web view uses 50-object pages, bounded query text, active-plus-latest action summaries, and literal wildcard escaping for filters.

## Testing and acceptance criteria

### Contract tests

Every adapter MUST pass a shared conformance suite proving:

- dispatch envelope version validation;
- no arbitrary command or secret field is accepted;
- external identity stability for representative fixtures;
- canonical type and facet schema validation;
- explicit field coverage and masks for every partial field set;
- correct complete, partial, and unknown member coverage;
- correct relationship, object-presence, and facet-field absence authority;
- truthful refresh satisfaction scopes;
- unknown source values remain unknown or source-native;
- redacted error classification;
- duplicate observation ingestion is idempotent.

### Remote observation boundary tests

- The coordinator rejects every capability not classified as `observe`.
- No UI, queue, configuration, or adapter path can request a remote state-change operation.
- Free-form commands, arbitrary endpoints, shell fragments, and unregistered source-native parameters cannot enter a refresh intent or dispatch envelope.
- Every capability contract declares known collateral effects and mitigations.
- A file-content fixture may update remote access time, and the later access-time observation is ingested as a remote fact rather than represented as a requested action.
- Discovering an undeclared material collateral effect pauses the capability and emits an alertable event.
- API-worker results update local action outcomes through the action-lifecycle port and canonical objects/facets/relationships through the observation-ingestion port.
- An adapter worker cannot write canonical object, facet, relationship, evidence, freshness, or projection tables directly.

### Refresh-policy tests

- Object facet override beats object-wide, system facet, system-wide, and type defaults.
- System override beats type default when no object override exists.
- An unobserved scope is immediately eligible.
- Two equivalent intents create at most one logical adapter action.
- Concurrent coordinators racing on equivalent intents produce one active action, and every loser attaches to it.
- A broader action coalesces a narrower request only with declared coverage.
- A request inside the interval is deferred and reports `eligible_at`.
- Existing evidence satisfies a scope only when its coverage matches and it meets the formal observation-time rule.
- A deferred request is re-evaluated before dispatch.
- A qualifying incidental observation satisfies or delays queued work without being discarded.
- One intent may report metadata as satisfied and content as deferred without losing either disposition.
- Starting a logical action consumes the interval.
- Adapter retries remain within the same action.
- Manual and automatic intents traverse the same coordinator.
- No request field, UI action, or worker path can force a refresh inside the effective interval.
- Lowering an ordinary object or system override wakes and re-evaluates affected deferred scopes.
- The accepted version 1 type-default intervals match the default table.

### State and reconciliation tests

- A directory listing updates folder membership and file metadata but not file content by default.
- A directory-listing partial metadata snapshot does not clear digest, permissions, or other fields learned by a fuller direct read.
- A declared reliable unchanged content revision may corroborate content only under its registered invariant.
- A partial listing cannot mark an unseen relationship or object absent.
- A fully valid empty complete membership listing can mark relationships absent within its exact authority.
- Membership-only coverage cannot tombstone an object; object-presence authority can do so only within its exact scope.
- Authentication failure does not change object presence or service active state.
- Unknown service state is never rendered as inactive.
- Missing patch or partial-snapshot fields do not clear current values.
- Explicit null remains distinguishable from missing and unknown.
- An older comparable result cannot overwrite newer current evidence.
- A malformed item does not discard unrelated valid facts.
- A malformed item disables absence reconciliation for its affected completeness boundary.
- After journal compaction, a projection can be rebuilt from the newest checkpoint plus retained later observations.

### Retention tests

- Stored file and Databricks Workspace content defaults to 365-day retention.
- Object retention override beats object-wide, system facet, system-wide, and type defaults.
- A policy change recalculates affected artifact expiry idempotently.
- Expiry deletes bytes and storage references but preserves permitted digest, provenance, and explicit expired availability.
- Expiry is not represented as remote deletion or modification and does not trigger or bypass a refresh.
- Projection checkpoints and compacted observations cannot restore content bytes after retention deletion.

### Queue and crash tests

- Intent acknowledgement is durable.
- Lease expiration causes safe revalidation.
- A queued action observes a later system pause, capability disable, or satisfying observation at the pre-dispatch guard and makes no downstream call.
- A crash before durable action start causes no downstream call.
- A crash after a downstream read may repeat the read but cannot duplicate the canonical observation.
- Coalesced request cancellation does not cancel work still needed by another intent.
- Pausing a system prevents new admissions and leaves cached data readable.
- Closing or losing the UI session detaches all of its scopes and cancels auto-only `queued`, `deferred`, `ready`, and pre-running `leased` work.
- An auto-only action in `retry_wait` performs no further attempt after its UI session ends.
- An already `running` auto-only observation may finish its current attempt but performs no retry after session end.
- Shared work remains active when a manual intent or another live UI-session scope still needs it.
- The pre-dispatch guard rejects an observation action with no live originating scope.
- Session-scoped auto-refresh is not restored after restart without a new live UI session.
- Every terminal action failure creates one idempotent alertable operational event.
- A queue worker that exhausts retry or poison-item handling creates an alertable operational event without leaking its payload.

### Databricks worker tests

- The Databricks API worker is the first live adapter tested through the complete generic refresh path.
- Every downstream invocation uses the Databricks CLI with structured arguments and no shell command string.
- The pinned CLI's iterator renderer exhausts list pagination before emitting JSON; any continuation-token envelope is rejected as incomplete rather than credited.
- CLI JSON is capped at 4 MiB and depth 32; registered collections are capped at 10,000 items and relation schemas at 1,000 columns per item, then normalized evidence is packed into canonical parts no larger than 1 MiB or 250 linked units.
- The CLI runner rejects `databricks api`, unregistered command groups/subcommands, arbitrary endpoints, and free-form flags.
- `doctor` fails clearly when the CLI version or a required `workspace`, `catalogs`, `schemas`, `tables`, or `volumes` command group is incompatible.
- `serve` keeps cached pages available, marks refresh authority unavailable, and retries compatibility checks in its supervised background loop.
- Profile selection and JSON output are passed as structured arguments and are redacted from persisted diagnostics.
- Sanitized fixture output covers every registered parser contract; content remains parser-only and is not executed by the current worker.
- A live `databricks.workspace.children.read` action is the first external end-to-end test.
- Two equivalent refresh intents inside the interval produce at most one Databricks CLI action.
- Worker results reach canonical state only through the action-lifecycle and observation-ingestion ports.
- Workspace content actions are rejected before CLI execution until the artifact/retention persistence port and policy are complete.
- Unity Catalog table/view results contain metadata only, and volume results contain volume-object metadata only.
- CLI authentication, permission, pagination, timeout, rate, malformed-output, and partial-result cases map to canonical outcomes without erasing cached facts.

### Security tests

- Credentials and secret values never enter canonical records, queue messages, logs, or UI output.
- User-controlled paths and names cannot become shell fragments.
- Server roots and service allowlists are enforced at admission and execution.
- Terminal control sequences and browser markup from remote data are escaped.
- File-content size, access, and truncation policy is fail-closed.
- An incompatible adapter or capability version cannot reinterpret queued work.
- The Databricks adapter cannot query table/view rows, list volume files, read volume content, or follow storage locations.
- The Linux SSH adapter rejects unregistered commands, non-allowlisted paths/services, and unverified host keys.

### Version 1 acceptance criteria

Version 1 is acceptable when:

1. The CLI-backed Databricks worker completes Workspace and Unity Catalog metadata inventory through the generic refresh path and is validated before Linux adapter implementation begins.
2. The UI or TUI can browse cached state while both remote targets are unavailable.
3. Every displayed fact exposes a known-as-of time and knowledge state.
4. Manual and automatic refreshes use one queue and coordinator.
5. Duplicate and too-soon refreshes cause no extra logical downstream action.
6. Broader observations refresh only the facets their adapter contract declares.
7. Failed refreshes preserve and visibly age last-known state and emit a durable alertable operational event.
8. Complete and partial collection semantics prevent false deletion.
9. Adapter workers are the only components with downstream credentials and network/CLI access.
10. A new namespaced generic object facet can be added without vendor logic in the coordinator or client.
11. Stored file and Workspace content has an effective 365-day default retention policy with object-over-system-over-type overrides.
12. Auto-refresh stops when its originating UI session ends and is not restored automatically.
13. Unity Catalog state contains metadata only, with no table rows or volume file listings/content.
14. Every enabled downstream capability is observation-only and no product interface can request an intentional remote state change.
15. Known collateral effects are declared and minimized, and undeclared material effects pause the affected capability.

## Phased delivery

### Phase 0: contract and fixture spike

- Define versioned system, object, facet, observation, refresh/retention policy, intent, action, artifact, operational-event, and capability contracts.
- Build an in-memory or fixture adapter, not a production remote integration.
- Define sanitized fixture contracts for the registered Databricks CLI JSON outputs without coupling the generic core to the CLI.
- Prove duplicate coalescing, interval precedence, incidental refresh credit, partial listing safety, failed-refresh preservation, and out-of-order behavior.
- Prove the remote-observation-only boundary and collateral-effect handling with fixture capabilities.
- Use file metadata versus file content as the decisive contract test.

Exit condition: the core tests pass without importing any Databricks, SSH, operating-system, UI, or queue implementation.

### Phase 1: first live Databricks worker slice

- Implement the thin Databricks CLI command runner and Databricks API worker.
- Configure one Databricks system, named CLI profile binding, Workspace directory scope, and `databricks.workspace.children.read` capability.
- Support manual refresh only.
- Persist an intent, validate locally, dispatch to the Databricks worker, invoke the CLI, ingest its normalized folder/file metadata observation, and query the projection.
- Provide the smallest state view and request-status view.

This Databricks Workspace directory listing is the required first live test, not an optional example.

Exit condition: cached Workspace state remains readable after Databricks becomes unavailable, a duplicate refresh creates no second CLI action, and the worker has no direct canonical-table write path.

### Phase 2: complete the Databricks worker

- Complete Workspace file/notebook content support; Workspace object metadata is already implemented.
- Store Workspace content through the artifact contract with 365-day default retention; keep capture disabled until its size/security policy is configured.
- Add Unity Catalog catalog, schema, table/view metadata, and volume-object metadata capabilities.
- Enforce the prohibition on table/view rows, SQL content inspection, volume file listing/content, and storage-location traversal.
- Prove complete versus partial collection reconciliation with adapter fixtures and live observation checks.
- Add relationship navigation and object/facet provenance.

Exit condition: the same CLI-backed worker passes contract and live tests for every registered Databricks capability, and Unity Catalog content-boundary tests pass.

### Phase 3: second adapter, Linux over SSH

- Add Linux-over-SSH folder/file metadata and selected service-status observations.
- Reuse the same generic intent, coordinator, queue, action, ingestion, projection, and UI-read contracts.
- Use this second adapter to prove that no Databricks semantics leaked into the generic core.

### Phase 4: automatic refresh

- Add the session heartbeat and session-scoped scheduler using the ordinary refresh-intent path.
- Detach expired-session scopes, cancel auto-only pre-running work, and stop retries when no live origin remains.
- Add stale, due, queued, deferred, and failed activity indicators.
- Add pause controls and core metrics.

### Phase 5: operational hardening

- Complete size, encryption, redaction, backup, and local-access controls for stored content.
- Add alert retention, compaction, and operator policy around the implemented durable UI projection.
- Add projection replay, adapter compatibility rollout, restore, and broader operator tooling.

### Rollback

Operational rollback is:

1. pause the affected subscription, capability, system, or adapter binding;
2. prevent new actions and allow or cancel running work according to adapter-safe semantics;
3. select a prior compatible adapter version if appropriate;
4. retain cached projections, refresh records, outcomes, and observation provenance;
5. repair or replay local projections.

Version 1 exposes no intentional remote mutation to roll back.
Pausing or rolling back a capability stops future observations but cannot necessarily undo collateral effects already caused by reads, such as file access-time or audit-log updates.
Those effects must be declared and minimized rather than presented as reversible product actions.

## Alternatives considered

### One last-refreshed time per object

Rejected.
It makes a directory listing either lie about file content freshness or fail to credit useful metadata.
It also conflates service runtime with enablement and job definition with run state.

### File metadata and content as unrelated root objects

Rejected for the default model.
They share one remote identity and must remain correlated across rename, modification, and provenance.
Independent facets preserve separate cost and freshness without duplicating identity.

### Current-state rows without observations

Rejected.
They cannot safely explain partial merges, freshness credit, negative evidence, out-of-order responses, or projection recovery.

### Full event sourcing

Rejected for version 1.
No requirement justifies making every UI command and internal state transition replayable.
A bounded evidence journal plus ordinary current projection is sufficient.

### One table per source or object type

Rejected as the canonical core.
It forces generic query, UI, and coordinator changes for every new source type.
Typed versioned facets provide validation without vendor-specific core tables.

### One schemaless object blob

Rejected.
It cannot reliably enforce identity, knowledge state, partial field updates, coverage, facet freshness, or safe generic display.

### Raw API operations in the generic queue

Rejected.
It leaks vendor semantics, arbitrary parameters, credentials, and trust decisions into the UI and coordinator.

### UI or scheduler calls adapters directly

Rejected.
It creates two refresh paths, bypasses common policy, and prevents consistent coalescing and observability.

### Adapter-specific stores behind a federated UI

Rejected unless live adapter spikes prove canonical facets cannot preserve required source semantics.
It duplicates refresh and staleness logic and violates the canonical local view objective.

### Separate deployed microservices immediately

Deferred.
Logical ownership is required, but no known scale or tenancy requirement justifies distributed transactions and operations in the first slice.

## Open questions

The major version 1 product choices are now accepted.
These narrower implementation questions remain.

| Priority | Question | Current assumption | What changes if the answer differs |
|---:|---|---|---|
| 1 | What size, encryption, redaction, indexing, backup, and local-access policies apply to stored file and Workspace content? | Storage is allowed, automatic capture is off until configured, and retained artifacts default to 365 days. | Determines the artifact store and when automatic content capture can be enabled. |
| 2 | What Linux distribution, service manager, SSH authentication method, filesystem roots, and service allowlist apply to the first host? | Linux over SSH with pinned host identity and registered observation templates; no service manager is assumed yet. | Determines the first SSH worker backend and canonical service mapping fixtures. |
| 3 | May an unchanged reliable directory-listing revision corroborate cached file content? | Metadata only unless the adapter proves a registered revision invariant. | Determines content satisfaction rules and adapter conformance tests. |
| 4 | How long must normalized observations, action history, tombstones, and operational events be retained? | Bounded provenance with checkpoints, exact durations undecided; stored content alone defaults to 365 days. | Changes replay window, storage cost, audit history, and deletion behavior. |
| 5 | Do initial targets expose stable object IDs, or must some paths and names define identity? | Use stable source IDs where available; otherwise a new path/name means new identity unless an adapter proves a move. | Affects rename/move history and deduplication. |
| 6 | What are the expected system count, object count, queue concurrency, and freshness targets? | Single-user local scale with no hard SLO. | May require indexed scheduling, partitioning, push events, or stronger process isolation. |

## References

- [Databricks CLI command reference](https://docs.databricks.com/aws/en/dev-tools/cli/commands)
- [Databricks CLI profiles](https://docs.databricks.com/aws/en/dev-tools/cli/profiles)
- [Databricks unified authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth)

The Databricks references establish the external command and authentication surface only.
Exact commands, fields, pagination, and compatibility remain adapter-owned and must be validated against the installed CLI version during implementation.
