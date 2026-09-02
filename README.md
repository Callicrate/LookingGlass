# LookingGlass

**LookingGlass** turns approved remote APIs into inspectable local views.
It keeps a durable cache of normalized metadata and lets each API-specific adapter refresh only the facts it declares safe to observe.

## What it does

- Browses cached objects, relationships, facets, provenance, and freshness locally, even when a remote system is unavailable.
- Lets an adapter declare its API capabilities, supported resource kinds, produced facets, collection coverage, and collateral effects.
- Lets the operator choose an offered **Add to cache** capability for a known object or configured scope.
- Queues that request through the same bounded refresh path that later maintains its normal cadence.

The first adapter uses the certified Databricks CLI.
It can cache Workspace and Unity Catalog metadata, but never table or view rows, Unity Catalog volume-file contents, or storage-location contents.

## Quick start

The current configured adapter requires Windows PowerShell, Python 3.12, [`uv`](https://docs.astral.sh/uv/), and Databricks CLI 0.298.0 on `PATH` with a valid named profile.

```powershell
Copy-Item -LiteralPath '.\config.example.toml' -Destination '.\config.local.toml'
uv sync --locked --group dev
uv run lookingglass fingerprint-profile --profile 'YOUR_PROFILE'
```

In the copied configuration, replace `YOUR_PROFILE` and the all-zero `authority_fingerprint` with the command output.
Choose stable values for the workspace `id` and `name`, and narrow `workspace_root` if `/` is broader than intended.

```powershell
uv run lookingglass --config config.local.toml init
uv run lookingglass --config config.local.toml doctor
uv run lookingglass --config config.local.toml serve
```

`serve` prints a one-time activation link to its controlling terminal.
Open that complete link within ten minutes, then keep using its unique `lookingglass-….localhost` hostname.

## Common operations

| Command | Purpose |
| --- | --- |
| `lookingglass init-config --output <path>` | Create a starter configuration without opening SQLite. |
| `lookingglass fingerprint-profile --profile <name>` | Print the non-secret route-authority fingerprint for a Databricks profile. |
| `lookingglass init` | Apply migrations and register configured systems. |
| `lookingglass doctor` | Verify the certified Databricks CLI surface. |
| `lookingglass serve` | Run the local UI, coordinator, and worker. |
| `lookingglass run-once` | Process a bounded batch of eligible refresh work. |
| `lookingglass backup --output <path>` | Create a consistent, no-overwrite SQLite snapshot. |
| `lookingglass authority-list` | List local authority identities without credentials. |

Use `lookingglass --help` for the full command list and `--config <path>` before a subcommand when not using `config.local.toml`.

## Operational boundaries

Each adapter owns downstream details, such as authentication, endpoint or command selection, pagination, rate limits, and source-specific safety limits.
The generic local model never accepts raw endpoints, query strings, or command fragments from the UI.
It accepts only registered capabilities, then stores the resulting normalized facts and uses their declared refresh policy thereafter.

For Databricks, the profile name and verified route fingerprint form the remote-authority boundary.
Changing the profile's effective route, `authority_fingerprint`, or `workspace_root` creates a separate cache boundary rather than silently reusing state.

LookingGlass runs the Databricks CLI from a private directory, removes ambient `DATABRICKS_*` and `BUNDLE_*` variables, and creates a minimal per-command profile snapshot.
The local UI binds only to loopback and protects its activation capability from request URLs and access logs.

The default database location is `.local/lookingglass.sqlite3` next to the configuration file.
Keep its parent directory private and outside a Git worktree or shared directory.

New adapters use the same canonical objects, facets, observations, refresh intents, capability declarations, and cadence rules.
For example, a future SSH adapter can expose an allowlisted Linux file-read capability without changing the cache model or granting an arbitrary shell.

For wheel-only use, install the matching release wheel and `runtime-constraints.txt` from the verified release bundle with hash verification and no source builds.
The bundle is not an offline dependency bundle.

## Renamed from Outpost

LookingGlass is a rename of the project formerly published as Outpost (and, earlier, as Rookery and `async-api-view`).
The command and Python module are now `lookingglass`; use `lookingglass` for both after upgrading.

This rename is a clean break: databases created under the earlier names are **not** compatible and are rejected on open.
Re-run `lookingglass init` against a fresh database path and re-observe your configured systems.

## Documentation

The full design, security boundaries, recovery contract, and release verification process are in the [architecture specification](./docs/architecture.md).
Start from [`config.example.toml`](./config.example.toml) when creating a configuration.
