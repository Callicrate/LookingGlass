# Rookery

Rookery keeps a local, inspectable cache of remote-system state and refreshes it through controlled API-specific workers.
The installable package and compatibility CLI remain named `async-api-view`.
The first adapter uses the existing Databricks CLI.

## Contents

- [First usable workflow](#first-usable-workflow)
- [Standalone wheel install](#standalone-wheel-install)
- [Source checkout setup](#source-checkout-setup)
- [Databricks scope](#databricks-scope)
- [Commands](#commands)
- [Recovery status](#recovery-status)
- [Verify](#verify)
- [Project structure](#project-structure)
- [Documentation](#documentation)

## First usable workflow

The current runnable slice can:

- register a Databricks workspace by named CLI profile;
- filter and page through cached Workspace and Unity Catalog metadata in a loopback dashboard;
- drill into one object's facets, provenance, current direct children, and registered refreshes;
- browse and filter bounded pages of durable redacted operational alerts;
- browse bounded durable action activity by state, system, or exact local action ID, then inspect its redacted attempts;
- distinguish current, due, refreshing, and failed-last-attempt facet state while keeping cached values visible;
- submit a generic refresh request;
- validate, queue, and execute it through the Databricks worker;
- ingest normalized observations into SQLite;
- keep cached state visible when Databricks or the worker is unavailable;
- enforce duplicate and minimum-interval refresh controls.

### Prerequisites

- Windows PowerShell for the commands below.
- Python 3.12.
- [`uv`](https://docs.astral.sh/uv/).
- Databricks CLI 0.298 or newer on `PATH`.
- An existing, valid named Databricks CLI profile.

Check available profiles without printing credential values:

```powershell
databricks auth profiles
```

Rookery resolves the CLI only from explicit absolute `PATH` entries, launches it from a fresh empty directory beneath a private per-user root, and rejects that root if any ancestor contains a bundle configuration recognized by the supported CLI contract. It removes inherited `DATABRICKS_*` and `BUNDLE_*` variables so ambient authentication or bundle settings cannot override the named profile. Configure the profile in the standard Databricks configuration location; ambient-only credentials and `DATABRICKS_CONFIG_FILE` overrides are intentionally ignored.

`doctor` verifies only the installed CLI version and required command groups. It does not query workspace inventory or verify that the selected profile exists or can authenticate; the first mapped refresh performs that live check.

## Standalone wheel install

This path needs the wheel, Python 3.12, `uv`, and the Databricks CLI, but no source checkout or `config.example.toml`.

```powershell
uv tool install 'C:\path\to\async_api_view-0.1.0-py3-none-any.whl'
New-Item -ItemType Directory -Path '.\rookery' -Force
Set-Location -LiteralPath '.\rookery'
async-api-view init-config --output '.\rookery.toml'
```

Edit `rookery.toml`: replace `YOUR_PROFILE`, choose a stable workspace `id` and display `name`, and narrow `workspace_root` if `/` is broader than intended. Then initialize and verify compatibility:

```powershell
async-api-view --config '.\rookery.toml' init
async-api-view --config '.\rookery.toml' doctor
async-api-view --config '.\rookery.toml' serve
```

`init-config` writes UTF-8 TOML, creates parent directories, and refuses to overwrite any existing path. The generated SQLite path is `.local/rookery.sqlite3` relative to the configuration file.
Rookery treats the database parent as a dedicated current-user state directory; do not point `database_path` at a Git worktree root or a shared directory. Existing configurations that place `rookery.sqlite3` beside the configuration file should move it into a dedicated directory and update the path before starting this version.
Use `uv tool install --force '<path-to-new-wheel>'` to upgrade and `uv tool uninstall async-api-view` to remove the installed command; configuration and cached SQLite state remain operator-owned files.

## Source checkout setup

```powershell
Set-Location -LiteralPath '<path-to-your-rookery-checkout>'
Copy-Item -LiteralPath '.\config.example.toml' -Destination '.\config.local.toml'
```

Edit `config.local.toml` and replace `YOUR_PROFILE` with the intended named profile.
The file stores only the profile name and configured Workspace root, not Databricks credentials or host secrets.
Keep each `databricks.id` stable when renaming a display name or rotating its profile.
Legacy entries without `id` remain supported; add the ID before changing the name, profile, or root so startup can adopt the existing cached identity.
Removing an entry disables its refresh authority but preserves cached facts; changing `workspace_root` creates a new authority boundary and pauses the predecessor.

```powershell
uv sync --locked --group dev
uv run async-api-view --config config.local.toml init
uv run async-api-view --config config.local.toml doctor
```

`doctor` is the fail-fast compatibility check: `serve` performs the same check in its background supervisor so cached pages remain available while compatibility is pending or failing.

### Run

```powershell
uv run async-api-view --config config.local.toml serve
```

`serve` prints a one-time browser activation link to its controlling terminal.
It does not hold the local UI behind Databricks CLI readiness; refresh controls stay disabled with a worker-unavailable explanation while compatibility is checked and retried.
Open that complete link, including its `#` fragment, within ten minutes.
The browser removes the fragment before exchanging it, so the capability does not enter HTTP request targets, redirects, or access logs.
The link uses a process-unique, high-entropy `rookery-….localhost` hostname.
Before printing that link, Rookery reserves and listens on both `127.0.0.1` and `::1` at the configured port; startup fails without disclosing the capability if either loopback address is unavailable.
The configured bind host remains restricted to `127.0.0.1` or `localhost` for compatibility, but it cannot weaken this dual-stack reservation.
The resulting session cookie is scoped to that unique host, process-local, `HttpOnly`, and `SameSite=Strict`, so ordinary `127.0.0.1` and `localhost` services do not receive it.
If the link expires, was already used by another browser profile, or the browser session is lost, restart `serve` to rotate it.
Redeemed browser access expires after two idle hours or twelve total hours, whichever comes first; restart `serve` to issue a new activation link.
When stdout is redirected, `serve` refuses to disclose the capability unless the operator explicitly passes `--allow-redirected-activation` and protects the destination.

Keep using the generated hostname after activation; direct `127.0.0.1` and `localhost` Host headers are rejected.
Stop it with `Ctrl+C`; an in-flight local CLI process is cancelled and its durable lease remains recoverable.

## Databricks scope

The closed worker registry supports:

- Workspace folder membership and object metadata;
- Workspace file/notebook metadata;
- Unity Catalog catalog, schema, table/view metadata, and volume-object metadata.

It does not use the Databricks SDK, direct REST calls, SQL inspection, or `databricks api`.
It never reads table/view rows, lists files inside Unity Catalog volumes, reads volume file content, or follows storage locations.

Workspace content reads and retained artifacts are not part of the current executable product.
The worker rejects content actions before target resolution or CLI execution until artifact storage, encryption, local access, and retention are implemented; parser and artifact contracts remain only as deferred implementation seams.

## Commands

| Command | Purpose |
|---|---|
| `async-api-view init-config --output <path>` | Create a no-overwrite starter TOML without loading configuration or opening SQLite. |
| `async-api-view init` | Apply SQLite migrations and idempotently register configured systems/scopes. |
| `async-api-view doctor` | Verify the existing Databricks CLI compatibility surface. |
| `async-api-view run-once` | Drain currently eligible local coordinator and worker activity, then stop. |
| `async-api-view backup --output <path>` | Create a consistent standalone SQLite snapshot without overwriting an existing path. |
| `async-api-view serve` | Run the loopback UI, coordinator, and Databricks worker in one process. |

Pass `--config <path>` before the subcommand.
Use `--log-level DEBUG`, `INFO`, `WARNING`, or `ERROR` when needed.
`backup` first proves through an immutable read-only connection that the source is a recognized Rookery database without opening or changing WAL shared state. It then uses one WAL-aware read connection for validation and SQLite's online snapshot operation, so committed WAL state is included while `serve` is running. The completed copy is identity- and integrity-checked before it appears at the requested path.
Database, WAL, SHM, backup, and temporary snapshot files are restricted to the current user. On POSIX, dedicated directories use mode `0700` and files use `0600`; on Windows, Rookery establishes current-user ownership and a protected current-user DACL. Redirected and multiply hard-linked state files are rejected.
The backup destination parent is also treated as a dedicated private directory and must not be a Git worktree root or shared folder.

## Recovery status

`backup` creates a consistent, validated snapshot, but Rookery does not yet provide or verify a restore workflow. Treat a snapshot as protected recovery input, not as proof that end-to-end recovery has been tested.

Do not replace the configured database while any Rookery process is running, and do not copy or combine `-wal` or `-shm` sidecar files from a different database state. If the primary database is damaged, stop Rookery, preserve the database and sidecars, and work only from copies until a supported restore command can validate and publish a replacement atomically. Restore tooling and regression-tested recovery remain deferred operational work.

For an existing database, startup performs an immutable identity and minimum-schema preflight before opening it for writes or changing journal mode. New and markerless stores migrate and persist the `ROOK` application ID in rollback-journal mode before WAL is enabled. An unmarked database with WAL/SHM sidecars must be cleanly closed and checkpointed by its owning version before adoption.
A foreign SQLite file is rejected without changing its bytes, permissions, journal mode, or sidecar set and without creating a backup destination.

## Verify

```powershell
uv run ruff format --check src tests scripts
uv run ruff check src tests scripts
uv run ruff check src scripts --select S
uv run coverage run -m pytest -q
uv run coverage report
uv lock --check
uv audit --locked --preview-features audit-command
uv build --build-constraint build-constraints.txt --require-hashes
uv run python scripts/verify_distribution.py
```

The default test suite uses fake CLI results and does not contact Databricks.
A live smoke test requires an explicit named profile and Workspace root.
The same formatting, lint, test, and locked-dependency checks run on Windows and Ubuntu in CI for pushes and pull requests.

## Project structure

- `src/async_api_view/contracts/`: versioned canonical and worker ports.
- `src/async_api_view/core/`: pure refresh policy.
- `src/async_api_view/storage/`: SQLite schema, queues, leases, and projections.
- `src/async_api_view/ingestion/`: canonical observation-write boundary.
- `src/async_api_view/adapters/`: closed Databricks CLI runner, normalizers, and worker.
- `src/async_api_view/web/`: loopback operational UI.
- `src/async_api_view/composition.py`: concrete one-process wiring.
- `docs/architecture.md`: full product and architecture contract.

## Documentation

See the [architecture specification](docs/architecture.md) for schema, freshness, queue, worker, failure, security, and phased-delivery contracts.
