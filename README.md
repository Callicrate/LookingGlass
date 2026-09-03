# LookingGlass

LookingGlass represents any remote API in a common local data model and presents its
state through a local UI.

```text
Databricks CLI ─┐
OpenSSH         ├── adapters ──> one canonical model ──> SQLite ──> one local UI
your API        ┘                 objects · facets · provenance · freshness
```

Adapters own the source-specific mess: authentication, requests, pagination, rate
limits, command construction, response parsing, and safety limits.

## What the model keeps

LookingGlass stores what was known, when it was observed, how it was obtained, and
whether the evidence is still fresh.

- A folder listing can refresh membership and file metadata. It does not pretend file
  content was read.
- A failed refresh does not replace the last-known facts with an error. The facts stay
  visible, age honestly, and keep the failure beside them.
- A missing item is not a deletion unless the adapter had complete coverage and the
  authority to make that claim.
- Equivalent requests share one action. A request made too soon waits instead of
  becoming a rate-limit bypass.

## Current implementation

**Databricks CLI 0.298.0.** LookingGlass observes Workspace directory, file, and notebook
metadata plus Unity Catalog catalog, schema, table, view, and volume metadata. It does
not read Workspace content, execute SQL, query table rows, list volume files, follow
storage locations, or expose the CLI's arbitrary API surface.

**OpenSSH.** LookingGlass observes immediate directory membership and file or folder
metadata beneath a configured POSIX root. It uses pinned host identity, strict host-key
checking, and two fixed remote command shapes. No content reads. No recursive scan. No
writes. No arbitrary shell.

Cache expansion is explicit today. Cadence controls eligibility; it does not imply a
hidden automatic poller.

## Quick start

The source checkout needs Python 3.12, [`uv`](https://docs.astral.sh/uv/), and the
certified Databricks CLI 0.298.0 on `PATH`. The current runtime checks that CLI before
any refresh worker starts, including an SSH-only configuration. SSH also needs OpenSSH,
a configured host alias, a pinned host key, and GNU `find` and `stat` on the remote
Linux host. These examples use PowerShell.

**Create the environment and starter config.**

```powershell
uv sync --locked --group dev
uv run lookingglass init-config
```

The starter contains one `[[databricks]]` block and one `[[ssh]]` block. Delete any block
you will not use. LookingGlass deliberately rejects every retained block whose
`authority_fingerprint` is still the all-zero placeholder.

**Bind the remote authority.** Choose stable `id` and `name` values, then narrow
`workspace_root` or `path_root` to the smallest useful scope. For SSH, set
`app.ssh_config_path` and `app.ssh_known_hosts_path` before fingerprinting.

```powershell
# Databricks
uv run lookingglass fingerprint-profile --profile 'YOUR_PROFILE'

# SSH
uv run lookingglass --config '.\config.local.toml' fingerprint-host `
  --alias 'YOUR_SSH_HOST_ALIAS'
```

Neither command queries remote inventory. Paste each digest into its matching
`authority_fingerprint` field.

**Open the view.**

```powershell
uv run lookingglass --config '.\config.local.toml' init
uv run lookingglass --config '.\config.local.toml' doctor
uv run lookingglass --config '.\config.local.toml' serve
```

`serve` prints a private, single-use browser link valid for ten minutes. Open the whole
link. Choose an offered **Add to cache** action. Follow its receipt, open an object, and
inspect the facts, freshness, and provenance that came back.

See `lookingglass --help` for one-shot runs, backups, and authority management.

## Operating boundaries

- **Credentials stay with the existing clients.** Canonical state keeps non-secret
  binding references and route fingerprints, not credentials.
- **Remote reads still have effects.** They may consume quota, create authentication or
  audit records, or execute a fixed observation command. LookingGlass shows those
  declared effects with the capability.
- **Authority is part of identity.** The verified route fingerprint and configured root
  form a cache boundary. Retargeting either does not silently inherit old state.
- **Access is local and single-user.** The UI reserves both loopbacks before revealing its
  one-time activation capability, which stays out of request URLs and access logs.
- **State belongs in a private directory.** Move lasting state out of repositories and shared
  folders into a private directory, then take no-overwrite backups.

The draft [architecture specification](./docs/architecture.md) covers the data model,
security, and recovery. Its roadmap predates the
[approved SSH adapter design](./docs/superpowers/specs/2026-09-01-ssh-adapter-design.md).
Start from [`config.example.toml`](./config.example.toml), or run
`lookingglass export-docs`.
