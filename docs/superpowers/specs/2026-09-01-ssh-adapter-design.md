# SSH Adapter Design

**Status:** approved (design walked through and approved section-by-section; implementation authorized)
**Date:** 2026-09-01
**Author:** LookingGlass

## 1. Goal and scope

LookingGlass currently ships one adapter (Databricks). This adds a second concrete
adapter, **SSH**, that reads files and folders on a remote host over OpenSSH. It is a
remote-**observation-only** capability: no remote mutation, no file-content capture in V1.

First slice — two capabilities:

- `ssh.fs.children.read` — list the immediate children of one directory: emit a child
  object per entry, per-child metadata, and `contains` relationships from the directory.
- `ssh.fs.metadata.read` — stat one known folder or file (type, size, mtime, mode).

**Explicit non-goals (V1):** file-content reads, recursive walks, writes/renames/deletes,
symlink target resolution, sftp/scp transfer, and any library-based SSH client.

## 2. Constraints (inherited, non-negotiable)

- No secret ever enters LookingGlass — not in canonical state, intents, the queue,
  observations, the UI, or logs.
- Remote values are untrusted. Allowlists are enforced at the trust boundary (the worker).
- No shell command strings — structured `argv` only.
- The core domain must not import SSH/OS-specific behavior; only the adapter does.
- Commit logically; TDD (test-first).

## 3. Transport and authentication

- **Transport:** the OpenSSH `ssh` client executing fixed remote commands. Not sftp, not
  a Python SSH library.
- **Authentication:** an OpenSSH **Host alias**. A binding references a named `Host` entry
  in an operator-managed ssh config; `ssh` resolves HostName/User/Port/IdentityFile/agent
  from it. No credential is ever passed through or stored by LookingGlass. The agent socket
  (`SSH_AUTH_SOCK`) is the only auth-relevant environment variable allowed through.

### Command-safety model (5 layers)

1. Structured `argv` only — never a shell string.
2. Two fixed remote command templates drawn from a closed capability registry.
3. `_remote_path` validation of the single path argument at the trust boundary.
4. `shlex.quote` the validated path, guarded with `--`, before it reaches the remote shell.
5. ssh hardening flags on every invocation:
   `-T -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=<known_hosts>
   -o GlobalKnownHostsFile=none -o ClearAllForwardings=yes -o PermitLocalCommand=no
   -o ConnectTimeout=<n> -F <ssh_config_path>`, plus `<alias>` and the remote command.

### Remote command templates (GNU coreutils on remote Linux)

- `ssh.fs.metadata.read`:
  `stat --printf '%F\x1f%s\x1f%Y\x1f%f\x1f%n' -- <qpath>`
- `ssh.fs.children.read`:
  `find <qpath> -maxdepth 1 -mindepth 1 -printf '%y\x1f%s\x1f%T@\x1f%m\x1f%f\x00'`

`\x1f` (unit separator) delimits fields; `\x00` (NUL) delimits child records — neither can
appear in a POSIX filename, so parsing is unambiguous.

### Authority fingerprint

Mirrors the Databricks profile-authority witness: `ssh_host_authority_fingerprint(alias, *,
ssh_config_path, known_hosts_path)` resolves the effective host via `ssh -G <alias>` (no
connection) and the pinned host key via `ssh-keygen -F <host> -f <known_hosts>` (no
connection), then returns a SHA-256 hexdigest over the normalized (host, port, key) witness.
No secret material is included; the digest is a non-secret binding witness stored in config.

## 4. Normalization and data model

Reuses the generic contracts unchanged.

- `ssh.fs.children.read` on success emits:
  - a root **membership** `FacetObservation` (`UpdateMode.PATCH`, `FieldCoverage.PARTIAL`,
    `member_count` set) with a `CoverageDeclaration`;
  - one child `RemoteObject` per entry with a **metadata** facet (type, size, mtime, mode);
  - one `contains` `RelationshipObservation` (`PresenceState.PRESENT`) from directory to child.
  - **Collection completeness:** `find -maxdepth 1` yields the complete child set, so
    `CollectionCoverage.COMPLETE` is declared **unless** stdout hit the byte cap (then the
    membership coverage degrades to `UNKNOWN` / partial).
- `ssh.fs.metadata.read` on success emits a single **metadata** `FacetObservation` for the
  target and `CollectionCoverage.COMPLETE`.
- `observed_at_is_local=True`; batches are chunked with the same batch-size/byte limits as
  the Databricks adapter.

Source kinds: `ssh.fs.folder`, `ssh.fs.file`. Object types: `folder`, `file`.

### Error classification (`classify_ssh_failure`)

- ssh exit `255` → transport failure. Parse stderr:
  authentication tokens → `ADAPTER_CONTRACT_MISMATCH`/authorization; timeout / connection
  refused / unreachable → `CONNECTION_TIMEOUT` or transient (retryable).
- other nonzero exit → remote command failure. Parse stderr:
  `No such file` → `NOT_FOUND`; `Permission denied` → authorization; else downstream failure.
- timeout / output-limit / invalid-response mirror the Databricks mapping.

## 5. Architecture, composition, CLI, testing

**Approach A (approved):** add a second concrete `SshWorker` mirroring `DatabricksWorker`;
generalize `ApplicationRuntime` to own a tuple of workers. The generic coordinator, queue,
storage, ingestion, guard, and UI are unchanged. The queue's `lease_next(*, adapter_key, ...)`
is per-adapter-key, so the two workers never contend.

**Shared process primitive (Decision 2A, approved):** extract the low-level async subprocess
machinery (`_ProcessTree`, `_terminate_process`, `_read_limited`, `_absolute_path_entries`)
from `CliRunner` into `adapters/_process.py`, used by both `CliRunner` and `SshRunner`. This
is the one intentional, mechanical touch into `databricks.py`; its control flow is unchanged.

Implementation surface:

- `config.py` — `ssh_config_path` / `ssh_known_hosts_path` on `AppSettings`;
  `SshSystemSettings(name, host_alias, path_root, config_id, authority_fingerprint)`;
  `ssh_systems` on `ProjectSettings`; `"ssh"` top-level table; `host_alias` validator
  `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`.
- `adapters/_process.py` — extracted shared primitive.
- `adapters/ssh.py` — `SSH_ADAPTER_KEY="ssh"`, `SSH_ADAPTER_VERSION="1"`, `CAPABILITIES`
  ({`ssh.fs.children.read`, `ssh.fs.metadata.read`}), error hierarchy, `SshCommandRegistry`,
  `_remote_path`, `SshRunner` (doctor / verify_host_authority / run), `normalize`,
  `classify_ssh_failure`, `SshWorker`.
- `application/bootstrap.py` — `configure_ssh_host` + SSH `_CAPABILITIES`/`_coverage_policies`.
- `composition.py` — `ApplicationRuntime.workers: tuple[...]`, per-worker `_run_background`
  loop, `SQLiteSshTargetResolver`, seed ssh systems in `_apply_local_configuration`, extend
  placeholder-fingerprint guard.
- `cli.py` — `fingerprint-host --alias`; iterate `runtime.workers`.
- `config.example.toml` — `[[ssh]]` example.

**Testing:** mirror `test_databricks_adapter.py` (`test_ssh_adapter.py`) with fake ports and
fake `SshRunner` subclasses; extend `test_composition.py`, `test_config.py`, `test_cli.py`.
Registry closure, path rejection, hardening-flag presence, normalization, error
classification, and multi-worker background loop are each covered.
