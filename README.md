# LookingGlass

LookingGlass represents any remote API in a common local data model and presents its
state through a local UI.

An adapter translates source-native resources and responses into systems, objects,
relationships, facets, observations, provenance, and freshness. LookingGlass keeps that
model in SQLite. Add a source and the adapter changes. The core does not.

> **The trick is refusal.** The UI never learns how to call the remote system.
> It can ask for a capability an adapter declared in advance. It cannot invent an
> endpoint, a query, a flag, or a command.

## One shape, however many sources

```text
Databricks CLI ─┐
OpenSSH         ├── adapters ──> one canonical model ──> SQLite ──> one local UI
your API        ┘                 objects · facets · provenance · freshness
```

Adapters own the source-specific mess: authentication, requests, pagination, rate
limits, command construction, response parsing, and safety limits.

On the other side of that boundary, LookingGlass sees only versioned capabilities and
normalized evidence. The coordinator can decide whether work is valid, duplicate, or
too early without touching the remote system. Storage does not need a vendor table for
every vendor object. The UI does not need a vendor screen for every vendor API.

That is the whole seam. Teach an adapter how to observe a source, and the rest of the
system already knows how to remember and show what it found.

## Memory with receipts

A live response tells you something now. Then the terminal scrolls, the token expires,
the service goes down, or the next response says something different. LookingGlass
keeps the useful part: what was known, when it was observed, how it was obtained, and
whether that evidence is still fresh.

The distinctions are deliberate:

- A folder listing can refresh membership and file metadata. It does not pretend file
  content was read.
- A failed refresh does not replace the last-known facts with an error. The facts stay
  visible, age honestly, and keep the failure beside them.
- A missing item is not a deletion unless the adapter had complete coverage and the
  authority to make that claim.
- Equivalent requests share one action. A request made too soon waits instead of
  becoming a rate-limit bypass.

Cache expansion is explicit today. Cadence controls eligibility; it does not imply a
hidden automatic poller.

## What ships today

**Databricks CLI 0.298.0.** LookingGlass observes Workspace directory, file, and notebook
metadata plus Unity Catalog catalog, schema, table, view, and volume metadata. It does
not read Workspace content, execute SQL, query table rows, list volume files, follow
storage locations, or expose the CLI's arbitrary API surface.

**OpenSSH.** LookingGlass observes immediate directory membership and file or folder
metadata beneath a configured POSIX root. It uses pinned host identity, strict host-key
checking, and two fixed remote command shapes. No content reads. No recursive scan. No
writes. No arbitrary shell.

These are the first two adapters, not the definition of the product. A new adapter
declares the resource kinds it understands, the capabilities it offers, the facets and
relationships it produces, how complete its observations are, and what collateral
effects they may have. The same coordinator, database, refresh path, and UI take it from
there.

The adapters reuse client configuration you already own. Persistent canonical state
keeps non-secret binding references and route fingerprints, never credentials.

> Observation-only does not mean invisible. A read may consume API quota, create an
> authentication or audit record, or execute a fixed remote observation command.
> LookingGlass puts those effects next to the capability before you ask for it.

## Get to the first useful screen

The source checkout requires Python 3.12, [`uv`](https://docs.astral.sh/uv/), and the
certified Databricks CLI 0.298.0 on `PATH`. The current runtime still certifies that CLI
before refresh workers become ready. For SSH, you also need OpenSSH, a configured host
alias, a pinned host key, and GNU `find` and `stat` on the remote Linux host. CI covers
Windows and Ubuntu; these examples use PowerShell.

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

For headless use, `run-once` drains a bounded batch, `backup` writes a no-overwrite SQLite
snapshot, and `authority-list` identifies cached authorities. See `lookingglass --help`
for the rest.

## The hard edges

- **Cached is not live.** LookingGlass shows when evidence was observed and whether it
  is due. It never passes a snapshot off as current remote truth.
- **The browser is not a console.** Raw endpoints, SQL, CLI flags, query strings, shell
  fragments, and credentials never enter a refresh intent.
- **Authority is part of identity.** The verified route fingerprint and configured root
  form a cache boundary. Retargeting either does not silently inherit old state.
- **Local means local.** The single-user UI reserves both loopbacks before revealing its
  one-time activation capability, which stays out of request URLs and access logs.
- **State is yours to protect.** Move lasting state out of repositories and shared
  folders into a private directory, then take no-overwrite backups.

The draft [architecture specification](./docs/architecture.md) carries the data model,
security boundaries, recovery behavior, and release verification. Its dated roadmap
predates the [approved SSH adapter design](./docs/superpowers/specs/2026-09-01-ssh-adapter-design.md).
Start from [`config.example.toml`](./config.example.toml), or export the packaged design
with `lookingglass export-docs`.

The remote call is temporary. What you learned from it does not have to be.
