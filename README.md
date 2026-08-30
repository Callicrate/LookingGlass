# Rookery

Rookery keeps a local, inspectable cache of remote-system state and refreshes it through controlled API-specific workers.
The installable package and compatibility CLI remain named `async-api-view`.
The first adapter uses the existing Databricks CLI.

Wheel-only operators can export the complete architecture and safety contract locally with
`async-api-view export-docs --output rookery-architecture.md`; no source checkout or database is required.

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
- filter by cached-name prefix and move through metadata with bounded forward cursors; restart at the first page to include concurrent refresh commits;
- drill into one object's facets, provenance, last-observed cached children, and registered refreshes;
- browse and filter forward-cursor pages of durable redacted operational alerts without full-history counts;
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
- Certified Databricks CLI 0.298.0 on `PATH`. Other releases require an explicit Rookery compatibility review before use.
- An existing, valid named Databricks CLI profile.

Check available profiles without printing credential values:

```powershell
databricks auth profiles --profile 'YOUR_PROFILE' --skip-validate
```

Generate the non-secret fingerprint of the profile's resolved workspace route, including any workspace, account, or Azure routing selectors, then copy the digest into `authority_fingerprint`:

```powershell
async-api-view fingerprint-profile --profile 'YOUR_PROFILE'
```

Rookery checks that fingerprint again before every remote command. Retargeting the same profile name, including changing a `workspace_id` behind a shared unified host, therefore fails before dispatch until configuration explicitly names the new authority, which creates or restores a separate cache.

Rookery resolves the CLI only from explicit absolute `PATH` entries, launches it from a fresh empty directory beneath a private per-user root, and rejects that root if any ancestor contains a bundle configuration recognized by the supported CLI contract. It removes inherited `DATABRICKS_*` and `BUNDLE_*` variables so ambient authentication or bundle settings cannot override the named profile. Configure the profile in the standard Databricks configuration location; ambient-only credentials and `DATABRICKS_CONFIG_FILE` overrides are intentionally ignored. For each mapped command, Rookery derives a minimal verified snapshot containing only the selected profile's own keys, writes it to that command's private temporary directory, and supplies it through a child-only `DATABRICKS_CONFIG_FILE`; changing the source profile after verification cannot retarget that process. `DEFAULT` is an ordinary profile and never supplies inherited credentials to another profile. Profiles that enable `skip_verify` are rejected before fingerprinting or dispatch. Each CLI runs in an owned process group on POSIX or kill-on-close Job Object on Windows, so timeout, cancellation, output overflow, and normal cleanup terminate supported auth helpers and other descendants before releasing local work. A held per-command lock protects active snapshots, normal exit removes them before releasing the lock, and every later command performs a bounded locked scan that removes crash-retained snapshots without touching live invocations.

`doctor` verifies the exact certified CLI version plus every reachable leaf command's usage and required profile/output/format flags. It records a full executable identity and SHA-256 witness; every mapped dispatch verifies that witness and reruns certification before execution if the CLI path changes. It does not query workspace inventory or verify that the selected profile exists or can authenticate; the first mapped refresh performs that live check.
Before running `--version`, doctor requires the executable bytes to match Databricks' official 0.298.0 x86-64 release digest for Windows or Linux. Compatibility-shaped mimic executables and unsupported platforms fail before receiving a profile snapshot or command authority.

## Standalone wheel install

This path needs the wheel, its matching `runtime-constraints.txt`, Python 3.12, `uv`, the Databricks CLI, and access to a Python package index for the constrained runtime dependencies, but no source checkout or `config.example.toml`.
The Rookery wheel plus constraints file is not a self-contained offline bundle. In a restricted environment, pre-populate uv's cache with the complete constrained runtime graph or provide an internally reviewed wheelhouse before installing or upgrading; this repository does not currently publish an offline wheelhouse.
Download the wheel, constraints file, and commit-qualified `rookery-*-SHA256SUMS.txt` from the same `rookery-distributions-<commit>-<os>` workflow artifact. Verify both install inputs before use:

```powershell
$manifest = '.\rookery-0.1.0-<commit>-SHA256SUMS.txt'
foreach ($artifact in @('.\async_api_view-0.1.0-py3-none-any.whl', '.\runtime-constraints.txt')) {
  $name = Split-Path -Leaf $artifact
  $line = @(Get-Content -LiteralPath $manifest | Where-Object { $_ -match "  $([regex]::Escape($name))$" })
  if ($line.Count -ne 1) { throw "Missing or duplicate checksum for $name" }
  $expected = ($line[0] -split '\s+')[0]
  $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $artifact).Hash.ToLowerInvariant()
  if ($actual -ne $expected) { throw "Checksum mismatch for $name" }
}
```

```powershell
uv venv --relocatable --python 3.12 '.\rookery-tool'
if ($LASTEXITCODE -ne 0) { throw 'Could not create the Rookery environment' }
uv pip install --python '.\rookery-tool\Scripts\python.exe' --require-hashes --no-build -r 'C:\path\to\runtime-constraints.txt'
if ($LASTEXITCODE -ne 0) { throw 'Could not install the hash-verified runtime graph' }
uv pip install --python '.\rookery-tool\Scripts\python.exe' --no-deps 'C:\path\to\async_api_view-0.1.0-py3-none-any.whl'
if ($LASTEXITCODE -ne 0) { throw 'Could not install the verified Rookery wheel' }
$env:PATH = "$((Resolve-Path -LiteralPath '.\rookery-tool\Scripts').Path);$env:PATH"
New-Item -ItemType Directory -Path '.\rookery' -Force
Set-Location -LiteralPath '.\rookery'
async-api-view init-config --output '.\rookery.toml'
```

Edit `rookery.toml`: replace `YOUR_PROFILE`, run `async-api-view fingerprint-profile --profile 'YOUR_PROFILE'`, replace the all-zero `authority_fingerprint` with its output, choose a stable workspace `id` and display `name`, and narrow `workspace_root` if `/` is broader than intended. The zero sentinel is rejected before database creation. Then initialize and verify compatibility:

```powershell
async-api-view --config '.\rookery.toml' init
async-api-view --config '.\rookery.toml' doctor
async-api-view --config '.\rookery.toml' serve
```

`init-config` writes UTF-8 TOML, creates parent directories, and refuses to overwrite any existing path. The generated SQLite path is `.local/rookery.sqlite3` relative to the configuration file.
Rookery treats the database parent as a dedicated current-user state directory; do not point `database_path` at a Git worktree root or a shared directory. Existing configurations that place `rookery.sqlite3` beside the configuration file should move it into a dedicated directory and update the path before starting this version.
Before upgrading, stop `serve` and use the currently installed version to create a no-overwrite backup in a dedicated private directory. Verify that command succeeds before replacing the package.
To upgrade, stop `serve`, create and smoke a fresh relocatable sibling, then swap it into place while retaining the prior environment for rollback:

```powershell
$active = (Resolve-Path -LiteralPath '.\rookery-tool').Path
$next = "$active-next"
$previous = "$active-previous"
if ((Test-Path -LiteralPath $next) -or (Test-Path -LiteralPath $previous)) { throw 'Upgrade staging paths already exist' }
uv venv --relocatable --python 3.12 $next
if ($LASTEXITCODE -ne 0) { throw 'Could not create the staged Rookery environment' }
uv pip install --python "$next\Scripts\python.exe" --require-hashes --no-build -r '<path-to-matching-runtime-constraints.txt>'
if ($LASTEXITCODE -ne 0) { throw 'Could not stage the hash-verified runtime graph' }
uv pip install --python "$next\Scripts\python.exe" --no-deps '<path-to-new-wheel>'
if ($LASTEXITCODE -ne 0) { throw 'Could not stage the verified Rookery wheel' }
& "$next\Scripts\async-api-view.exe" --help
if ($LASTEXITCODE -ne 0) { throw 'Staged Rookery command failed its smoke check' }
$oldMoved = $false
$newMoved = $false
try {
  Move-Item -LiteralPath $active -Destination $previous -ErrorAction Stop
  $oldMoved = $true
  Move-Item -LiteralPath $next -Destination $active -ErrorAction Stop
  $newMoved = $true
  & "$active\Scripts\async-api-view.exe" --help
  if ($LASTEXITCODE -ne 0) { throw 'Relocated Rookery command failed its smoke check' }
} catch {
  $upgradeFailure = $_
  try {
    if ($newMoved -and (Test-Path -LiteralPath $active)) {
      Move-Item -LiteralPath $active -Destination $next -ErrorAction Stop
      $newMoved = $false
    }
    if ($oldMoved -and (Test-Path -LiteralPath $previous)) {
      Move-Item -LiteralPath $previous -Destination $active -ErrorAction Stop
      $oldMoved = $false
    }
  } catch {
    throw "Upgrade failed and rollback also failed: $($_.Exception.Message)"
  }
  throw $upgradeFailure
}
```

Keep `$previous` until the upgraded command and local state have passed validation. Remove the private environment to uninstall the command; configuration and cached SQLite state remain operator-owned files. A fresh sibling prevents packages removed from the new lock, including executable `.pth` hooks, from surviving an upgrade. Keep the matching constraints, relocatable Python 3.12 environment, `--require-hashes`, `--no-build`, and `--no-deps` separation on every install. Installation and upgrade still require the configured package index or a complete populated cache; a staging failure leaves the active environment untouched.
The first later `init`, `run-once`, or `serve` invocation automatically applies pending migrations. Rookery binds the ordered migration inventory to packaged SQL bytes and rejects a database whose recorded provenance disagrees with the installed build before applying new changes. A recognized pre-provenance database records older versions as `ledger_adopted`, not `executed`; that label confirms only the accepted ledger/schema bridge and does not retroactively prove which historical DML bytes ran. In-place downgrade is unsupported because an older binary rejects migration-ledger versions it does not know. Keep the pre-upgrade backup until the upgraded version has passed local validation; restore remains a separate unsupported workflow.

## Source checkout setup

```powershell
Set-Location -LiteralPath '<path-to-your-rookery-checkout>'
Copy-Item -LiteralPath '.\config.example.toml' -Destination '.\config.local.toml'
```

Edit `config.local.toml` and replace `YOUR_PROFILE` with the intended named profile.
The file stores only the profile name, SHA-256 route-authority fingerprint, and configured Workspace root, not Databricks credentials, raw host, or raw selectors.
Keep each `databricks.id` and `authority_fingerprint` stable when renaming a display name or changing to another profile for the same verified workspace. Rotate credentials inside the same named profile normally.
Legacy entries without `id` remain supported, but adding an explicit ID/fingerprint creates a new verified authority rather than silently blessing legacy cache whose remote host was never recorded.
Removing an entry disables its refresh authority but preserves cached facts; changing `authority_fingerprint` or `workspace_root` creates a new authority boundary and pauses the predecessor. Returning to the prior fingerprint/root re-enables that authority's original cache.
Removal also terminalizes pre-running work so change-back cannot revive old requests. Use `authority-retire` when an authority must remain readable but must not reactivate automatically; `authority-unretire` is explicit and does not itself enable the authority.

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
It also acquires one private database-scoped runtime lock before applying desired configuration. `init`, `run-once`, and `serve` share that ownership boundary, so no second stateful runner can rotate profiles, disable resources, or compete for leases while another owns the database. Online `backup` remains available.
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
List results record positive child evidence and an explicit `unknown` collection-completeness value. The object view therefore labels containment as last-observed cached children: an omitted prior child is not marked absent until a future bounded multipart contract can commit one authoritative complete boundary.

## Commands

| Command | Purpose |
|---|---|
| `async-api-view init-config --output <path>` | Create a no-overwrite starter TOML without loading configuration or opening SQLite. |
| `async-api-view export-docs --output <path>` | Export the packaged architecture and safety contract without loading configuration or opening SQLite. |
| `async-api-view fingerprint-profile --profile <name>` | Print the non-secret SHA-256 workspace-route authority fingerprint for a standard CLI profile. |
| `async-api-view authority-list` | List enabled, historical, and retired local authority identities without credentials. |
| `async-api-view authority-retire --system-id <uuid>` | Retire one authority, cancel its pending work, and preserve its cache. |
| `async-api-view authority-unretire --system-id <uuid>` | Remove retirement; run `init` afterward to re-enable if still configured. |
| `async-api-view init` | Apply SQLite migrations and idempotently register configured systems/scopes. |
| `async-api-view doctor` | Verify the existing Databricks CLI compatibility surface. |
| `async-api-view run-once [--max-cycles N]` | Process up to `N` eligible coordinator/worker cycles; exit 3 means work may remain and the command should be rerun. |
| `async-api-view backup --output <path>` | Create a consistent standalone SQLite snapshot without overwriting an existing path. |
| `async-api-view serve` | Run the loopback UI, coordinator, and Databricks worker in one process. |

Pass `--config <path>` before the subcommand.
Use `--log-level DEBUG`, `INFO`, `WARNING`, or `ERROR` when needed.
`run-once` defaults to 10,000 cycles and accepts at most 1,000,000. Exit 0 means the eligible queue became idle; exit 3 is bounded incompletion, not a worker fault.
`backup`, `authority-list`, `authority-retire`, and `authority-unretire` validate only the bounded top-level and `[app]` configuration needed to identify local state. A syntactically valid but semantically broken Databricks entry therefore cannot block local preservation, inspection, or retirement. `init`, `doctor`, `run-once`, and `serve` continue to require the complete remote configuration contract.
`backup` first proves through an immutable read-only connection that the source is a recognized Rookery database without opening or changing WAL shared state. It then uses one WAL-aware read connection for validation and SQLite's online snapshot operation, so committed WAL state is included while `serve` is running. The completed copy is identity- and integrity-checked, then its file and final publication metadata are synchronized before success is reported.
Database, WAL, SHM, backup, and temporary snapshot files are restricted to the current user. On POSIX, dedicated directories use mode `0700` and files use `0600`; on Windows, Rookery establishes current-user ownership and a protected current-user DACL. Redirected and multiply hard-linked state files are rejected.
The backup destination parent is also treated as a dedicated private directory and must not be a Git worktree root or shared folder.

## Recovery status

`backup` creates a consistent, validated snapshot, but Rookery does not yet provide or verify a restore workflow. Treat a snapshot as protected recovery input, not as proof that end-to-end recovery has been tested.

Before creating its temporary copy, `backup` measures the SQLite snapshot as `page_count × page_size` and requires that amount plus 64 MiB available to the current caller on the destination volume. If capacity cannot be confirmed, it publishes nothing. This is an admission guard, not an operating-system reservation; another process, quota change, or thin-provisioned volume can still exhaust space after the check.

Do not replace the configured database while any Rookery process is running, and do not copy or combine `-wal` or `-shm` sidecar files from a different database state. If the primary database is damaged, stop Rookery, preserve the database and sidecars, and work only from copies until a supported restore command can validate and publish a replacement atomically. Restore tooling and regression-tested recovery remain deferred operational work.

For an existing database, startup performs an immutable identity and minimum-schema preflight before opening it for writes or changing journal mode. New and markerless stores migrate and persist the `ROOK` application ID in rollback-journal mode before WAL is enabled. An unmarked database with WAL/SHM sidecars must be cleanly closed and checkpointed by its owning version before adoption.
A foreign SQLite file is rejected without changing its bytes, permissions, journal mode, or sidecar set and without creating a backup destination.
Observation, action, attempt, issue, and operational-event history is currently uncompacted, and backup files are never pruned automatically. Monitor the private state and backup directories for disk growth and manage backup retention explicitly. Do not delete or combine the active database's WAL/SHM files.

Rookery also protects write headroom before accepting a new refresh, admitting its action, and authorizing remote dispatch. The required threshold preserves at least 64 MiB or twice the configured CLI output limit, whichever is larger, after one output-sized planned write. Low or unprovable headroom leaves cached state readable, disables refresh controls with an explanation, defers queued work, and rechecks later. Terminalization and failure-reporting writes remain allowed to consume the reserve. The check uses capacity available to the current caller but cannot prove quota or storage availability after the measurement.

## Verify

```powershell
uv sync --locked --group dev --no-install-project
uv run --no-sync ruff format --check src tests scripts
uv run --no-sync ruff check src tests scripts
uv run --no-sync ruff check src scripts --select S
uv run --no-sync coverage run -m pytest -q
uv run --no-sync coverage report
uv run --no-sync coverage json -o coverage-report.json
uv run --no-sync python scripts/check_coverage.py coverage-report.json
uv lock --check
uv audit --locked --preview-features audit-command
uv export --locked --no-dev --no-emit-project --no-header --format requirements.txt --output-file runtime-constraints.txt
uv build --build-constraint build-constraints.txt --require-hashes
uv run --no-sync python scripts/verify_distribution.py
```

The default test suite uses fake CLI results and does not contact Databricks.
A live smoke test requires an explicit named profile and Workspace root.
The same formatting, lint, test, and locked-dependency checks run on Windows and Ubuntu in CI for pushes and pull requests.
`check_coverage.py` reports and enforces statement (85%), branch-only (75%), and combined (80%) floors separately.
`verify_distribution.py` binds every packaged module and runtime asset to current source bytes, validates wheel metadata and RECORD digests, rebuilds the wheel from the sdist under the hash-constrained build graph, requires byte identity, installs the locked runtime graph into a private wheel environment, installs the wheel with `--no-deps`, runs checkout-free behavior checks, and audits the exact installed versions. It verifies the tracked hash-bearing runtime constraints against `uv.lock`, exercises documented hash-required private-environment installation and fresh-sibling upgrade, rejects corrupted dependency hashes, proves prior-only startup hooks cannot survive, publishes the constraints into `dist/`, and writes a commit-qualified SHA-256 manifest only from a completely clean checkout. This keeps release smoke, standalone execution, vulnerability evidence, and source provenance on one dependency graph.
Dependabot opens weekly update proposals for both the `uv` lock and pinned GitHub Actions; proposals must pass the same gates.

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

Source checkouts and sdists include the [architecture specification](docs/architecture.md) for schema, freshness, queue, worker, failure, security, and phased-delivery contracts. Wheel-only installs expose the same bytes through `export-docs`.
