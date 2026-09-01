# Repository Guidance

## Scope

This file applies to the entire repository; there are no nested `AGENTS.md` files.
See [README.md](README.md) for the product overview and
[docs/architecture.md](docs/architecture.md) for the design and security boundaries.

## Context

- Purpose: LookingGlass keeps a durable local cache of normalized remote-API metadata
  and refreshes only facts each adapter declares safe to observe.
- Primary stack: Python 3.12 only (`requires-python = ">=3.12,<3.13"`), managed with
  `uv`; FastAPI for the local UI. CI runs on Ubuntu and Windows.
- Entry point: the `lookingglass` CLI in [src/lookingglass/cli.py](src/lookingglass/cli.py).

## Local Commands

Run from the repo root. Ruff line length is 100 (E501 enforced).

```bash
uv sync --locked --group dev --no-install-project
uv run --no-sync ruff format --check src tests scripts
uv run --no-sync ruff check src tests scripts
uv run --no-sync ruff check src scripts --select S
uv run --no-sync coverage run -m pytest -q
uv run --no-sync coverage report
uv run --no-sync python scripts/check_coverage.py coverage-report.json
uv build --build-constraint build-constraints.txt --require-hashes
uv run --no-sync python scripts/verify_distribution.py
```

## Project Rules

### Name casing

Match the project name to its context:

- `lookingglass` (all lowercase, one word): the Python package and imports
  (`import lookingglass`), the CLI command, the distribution name, configuration keys,
  and database/file paths.
- `LookingGlass` (PascalCase): the product name in prose, documentation, and UI text,
  and Python class names (for example `_LookingGlassServer` in
  [src/lookingglass/cli.py](src/lookingglass/cli.py)).
- `LOOKINGGLASS_` (upper snake case): the prefix for any environment variable (none are
  read today; this is the convention if one is introduced).

Do not reintroduce the former names `outpost`, `rookery`, or `async_api_view` in any
casing. Application-defined SQLite functions use the `lookingglass_` prefix.

### Migration ledger contract

SQLite migrations in [src/lookingglass/storage/migrations/](src/lookingglass/storage/migrations/)
are integrity-checked when the store opens. Adding or editing a migration means updating
all of these together, or the store refuses to open:

- Bump `_MIGRATION_HEAD` in [src/lookingglass/storage/sqlite.py](src/lookingglass/storage/sqlite.py)
  to the new head file's stem; the applied set must stay a contiguous prefix.
- Regenerate [src/lookingglass/storage/migrations/MANIFEST.sha256](src/lookingglass/storage/migrations/MANIFEST.sha256)
  (per-file SHA-256, two-space separator, sorted by filename, LF endings, trailing
  newline) and set `_MIGRATION_MANIFEST_SHA256` in the same file to the SHA-256 of the
  whole manifest file.
- Keep migration SQL files LF-only with a trailing newline; CRLF is rejected.
- Update the ledger-rewind test helpers `_rewind_projection_order_migration`
  ([tests/test_ingestion.py](tests/test_ingestion.py)) and `_rewind_nonnull_queue_id_migration`
  ([tests/test_coordinator.py](tests/test_coordinator.py)) to delete every migration
  after the rewind target (including the new one) and drop the columns/objects it
  created, so `_migrate()` can re-apply it cleanly.

Application-defined SQL functions (prefixed `lookingglass_`) are registered in
[src/lookingglass/storage/sqlite.py](src/lookingglass/storage/sqlite.py) and called from
migration SQL; keep the registered names and SQL call sites matched.

## Testing

`pytest` (with `coverage`) lives in [tests/](tests/); config is in
[pyproject.toml](pyproject.toml). Run the full suite with
`uv run --no-sync coverage run -m pytest -q`.
