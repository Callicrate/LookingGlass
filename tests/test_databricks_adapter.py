from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from async_api_view.adapters import databricks as databricks_adapter
from async_api_view.adapters.databricks import (
    DATABRICKS_ADAPTER_KEY,
    DATABRICKS_ADAPTER_VERSION,
    MAX_COLLECTION_ITEMS,
    MAX_TABLE_COLUMNS,
    CliExecution,
    CliIncompatible,
    CliInvocation,
    CliOutputLimit,
    CliRunner,
    CliTimeout,
    CliUnavailable,
    CommandRejected,
    DatabricksCommandRegistry,
    DatabricksWorker,
    DownstreamFailure,
    InvalidDownstreamResponse,
    LifecyclePersistenceFailure,
    ResolvedTarget,
    _downstream_retry_after,
    classify_failure,
    redact_diagnostic,
)
from async_api_view.contracts import (
    ActionLease,
    ActionLeaseLost,
    ActionOutcome,
    AdapterAction,
    CapabilityBinding,
    CollectionCoverage,
    ConnectionBinding,
    ErrorClass,
    GuardDecision,
    GuardDisposition,
    IngestionResult,
    IngestionStatus,
    ObservationBatch,
    OperationClass,
    RefreshScope,
    TargetKind,
    TargetRef,
)

_DELIVERY_ID = str(uuid4())


def normalize(**kwargs):
    return databricks_adapter.normalize(delivery_id=_DELIVERY_ID, **kwargs)


def _binding() -> ConnectionBinding:
    return ConnectionBinding(
        uuid4(),
        uuid4(),
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local", "content_capture_enabled": True, "content_max_bytes": 1024},
    )


def _catalog_root_target() -> ResolvedTarget:
    return ResolvedTarget(
        canonical_object_id=uuid4(),
        canonical_object_type="generic_object",
    )


def _action(capability: str, facet: str = "attributes") -> AdapterAction:
    system_id = uuid4()
    return AdapterAction(
        uuid4(),
        uuid4(),
        system_id,
        uuid4(),
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        capability,
        "1",
        TargetRef(TargetKind.CONFIGURED_SCOPE, uuid4()),
        (
            RefreshScope(
                system_id, TargetRef(TargetKind.CONFIGURED_SCOPE, uuid4()), "generic_object", facet
            ),
        ),
    )


@pytest.mark.parametrize(
    ("capability", "target", "expected"),
    [
        (
            "databricks.workspace.children.read",
            ResolvedTarget(workspace_path="/Shared/root", workspace_root="/Shared"),
            (
                "databricks",
                "workspace",
                "list",
                "/Shared/root",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        (
            "databricks.workspace.metadata.read",
            ResolvedTarget(workspace_path="/Shared/root", workspace_root="/Shared"),
            (
                "databricks",
                "workspace",
                "get-status",
                "/Shared/root",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        (
            "databricks.workspace.content.read",
            ResolvedTarget(workspace_path="/Shared/root", workspace_root="/Shared"),
            (
                "databricks",
                "workspace",
                "export",
                "/Shared/root",
                "--format",
                "SOURCE",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        (
            "databricks.uc.catalogs.read",
            ResolvedTarget(),
            ("databricks", "catalogs", "list", "--profile", "local", "--output", "json"),
        ),
        (
            "databricks.uc.schemas.read",
            ResolvedTarget(catalog_name="main"),
            ("databricks", "schemas", "list", "main", "--profile", "local", "--output", "json"),
        ),
        (
            "databricks.uc.relations.read",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            (
                "databricks",
                "tables",
                "list",
                "main",
                "sales",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        (
            "databricks.uc.volumes.read",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            (
                "databricks",
                "volumes",
                "list",
                "main",
                "sales",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
    ],
)
def test_registry_is_closed_and_exact(
    capability: str, target: ResolvedTarget, expected: tuple[str, ...]
) -> None:
    assert (
        DatabricksCommandRegistry.build(
            capability_key=capability, profile="local", target=target
        ).argv
        == expected
    )
    with pytest.raises(CommandRejected):
        DatabricksCommandRegistry.build(
            capability_key="databricks.api.read", profile="local", target=target
        )
    with pytest.raises(CommandRejected):
        DatabricksCommandRegistry.build(capability_key=capability, profile="--debug", target=target)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "invocation",
    [
        CliInvocation(
            "databricks.uc.catalogs.read",
            ("databricks", "api", "get", "--profile", "local", "--output", "json"),
        ),
        CliInvocation(
            "databricks.uc.catalogs.read",
            ("databricks", "catalogs", "list", "--debug", "--profile", "local", "--output", "json"),
        ),
        CliInvocation(
            "databricks.uc.catalogs.read",
            ("databricks", "catalogs", "list", "--profile", "--debug", "--output", "json"),
        ),
        CliInvocation(
            "databricks.workspace.children.read",
            (
                "databricks",
                "workspace",
                "list",
                "/Shared/../secret",
                "--profile",
                "local",
                "--output",
                "json",
            ),
        ),
        CliInvocation(
            "databricks.workspace.children.read",
            ("databricks", "workspace", "list", "/Shared", "--output", "json"),
        ),
    ],
)
async def test_runner_rejects_manual_invocation_before_process_creation(
    invocation: CliInvocation, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = CliRunner()
    runner._resolved_executable = "C:\\trusted\\databricks.exe"

    async def unexpected_process(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid invocation reached process creation")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", unexpected_process)

    with pytest.raises(CommandRejected):
        await runner.run(invocation, correlation_id="test")


class ScriptedDoctorRunner(CliRunner):
    def __init__(
        self,
        version: bytes,
        *,
        failed_args: tuple[str, ...] | None = None,
    ) -> None:
        super().__init__()
        self.version = version
        self.failed_args = failed_args
        self.calls: list[tuple[str, ...]] = []
        self._resolved_executable = "C:\\trusted\\databricks.exe"

    async def run_unmapped(self, invocation: CliInvocation) -> CliExecution:
        args = invocation.argv[1:]
        self.calls.append(args)
        return CliExecution(
            correlation_id="doctor",
            duration=timedelta(),
            exit_code=int(args == self.failed_args),
            stdout=self.version if args == ("--version",) else b"",
            stderr=b"",
        )


@pytest.mark.parametrize(
    "version",
    [
        b"Databricks CLI v0.298.0",
        b"Databricks CLI v0.299.1",
        b"Databricks CLI v1.0.0",
    ],
)
def test_doctor_accepts_supported_and_newer_cli_versions(version: bytes) -> None:
    runner = ScriptedDoctorRunner(version)

    asyncio.run(runner.doctor())

    assert runner.calls == [
        ("--version",),
        ("workspace", "--help"),
        ("catalogs", "--help"),
        ("schemas", "--help"),
        ("tables", "--help"),
        ("volumes", "--help"),
    ]


@pytest.mark.parametrize("version", [b"Databricks CLI v0.297.9", b"unknown version"])
def test_doctor_rejects_old_or_unparseable_cli_versions(version: bytes) -> None:
    with pytest.raises(CliIncompatible, match=r"0\.298 or newer"):
        asyncio.run(ScriptedDoctorRunner(version).doctor())


def test_doctor_rejects_missing_required_help_surface() -> None:
    runner = ScriptedDoctorRunner(
        b"Databricks CLI v0.298.0",
        failed_args=("tables", "--help"),
    )

    with pytest.raises(CliIncompatible, match="tables --help"):
        asyncio.run(runner.doctor())


def test_cli_processes_scrub_ambient_databricks_auth_and_bundle_workdir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle_root = tmp_path / "hostile-bundle"
    bundle_root.mkdir()
    (bundle_root / "databricks.yml").write_text(
        "bundle:\n  name: hostile\nworkspace:\n  host: https://ambient.invalid\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(bundle_root)
    for name in (
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_CONFIG_FILE",
        "DATABRICKS_BUNDLE_ROOT",
        "BUNDLE_VAR_target",
    ):
        monkeypatch.setenv(name, "ambient-secret-or-target")
    monkeypatch.setenv("ROOKERY_TEST_ENV", "preserved")
    safe_path = tmp_path / "safe-path"
    safe_path.mkdir()
    monkeypatch.setenv("PATH", f".{os.pathsep}{safe_path}")
    work_root = tmp_path / "trusted-home" / ".rookery" / "cli-work"
    work_root.mkdir(parents=True)
    monkeypatch.setattr(databricks_adapter, "_trusted_cli_work_root", lambda: work_root)
    spawned: list[tuple[Path, dict[str, str]]] = []

    class EmptyStream:
        async def read(self, _size: int) -> bytes:
            return b""

    class CompleteProcess:
        returncode = 0
        stdout = EmptyStream()
        stderr = EmptyStream()

        async def wait(self) -> int:
            return 0

    async def create_process(*_args: object, **kwargs: object) -> CompleteProcess:
        working_directory = Path(kwargs["cwd"])  # type: ignore[arg-type]
        environment = kwargs["env"]  # type: ignore[assignment]
        assert working_directory != bundle_root
        assert list(working_directory.iterdir()) == []
        spawned.append((working_directory, environment))
        return CompleteProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    runner = CliRunner(executable="databricks")
    runner._resolved_executable = "C:\\trusted\\databricks.exe"

    async def exercise() -> None:
        await runner.run_unmapped(CliInvocation("doctor", ("databricks", "--version")))
        await runner.run(
            CliInvocation(
                "databricks.workspace.children.read",
                (
                    "databricks",
                    "workspace",
                    "list",
                    "/Shared",
                    "--profile",
                    "configured-profile",
                    "--output",
                    "json",
                ),
            ),
            correlation_id="test",
        )

    asyncio.run(exercise())

    assert len(spawned) == 2
    for working_directory, environment in spawned:
        assert not working_directory.exists()
        assert environment["ROOKERY_TEST_ENV"] == "preserved"
        assert environment["PATH"] == str(safe_path)
        assert not any(name.upper().startswith(("DATABRICKS_", "BUNDLE_")) for name in environment)


@pytest.mark.parametrize("filename", databricks_adapter._BUNDLE_CONFIG_FILENAMES)
def test_cli_work_root_rejects_every_bundle_configuration_ancestor(
    tmp_path: Path,
    filename: str,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    (home / filename).write_text("bundle:\n  name: hostile\n", encoding="utf-8")

    with pytest.raises(CliUnavailable, match="bundle configuration ancestor"):
        databricks_adapter._trusted_cli_work_root(home=home)


def test_cli_work_root_is_private_and_confined_to_home(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()

    root = databricks_adapter._trusted_cli_work_root(home=home)

    assert root == (home / ".rookery" / "cli-work").resolve()
    assert root.is_dir()
    if os.name != "nt":
        assert root.parent.stat().st_mode & 0o777 == 0o700
        assert root.stat().st_mode & 0o777 == 0o700


@pytest.mark.skipif(os.name != "nt", reason="Windows ownership and DACL regression")
def test_cli_work_root_replaces_permissive_windows_security(tmp_path: Path) -> None:
    home = tmp_path / "home"
    state_root = home / ".rookery"
    work_root = state_root / "cli-work"
    work_root.mkdir(parents=True)
    icacls = Path(os.environ["SYSTEMROOT"]) / "System32" / "icacls.exe"
    for directory in (state_root, work_root):
        subprocess.run(  # noqa: S603 - absolute Windows system executable
            (str(icacls), str(directory), "/grant", "*S-1-1-0:(OI)(CI)F", "/Q"),
            check=True,
            capture_output=True,
        )

    root = databricks_adapter._trusted_cli_work_root(home=home)

    assert root == work_root.resolve()
    powershell = (
        Path(os.environ["SYSTEMROOT"])
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    owner_script = (
        "$acl=(New-Object System.IO.DirectoryInfo("
        "$env:ROOKERY_ACL_TEST_PATH)).GetAccessControl();"
        "$ownerSid=$acl.GetOwner([System.Security.Principal.SecurityIdentifier]).Value;"
        "$currentSid=[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value;"
        "if($ownerSid -ne $currentSid){throw 'Rookery directory owner mismatch'}"
    )
    for ordinal, directory in enumerate((state_root, work_root)):
        saved_acl = tmp_path / f"acl-{ordinal}.txt"
        subprocess.run(  # noqa: S603 - absolute Windows system executable
            (str(icacls), str(directory), "/save", str(saved_acl), "/Q"),
            check=True,
            capture_output=True,
        )
        sddl = saved_acl.read_text(encoding="utf-16-le")
        assert "D:P" in sddl
        assert ";;;WD)" not in sddl
        owner_environment = dict(os.environ)
        owner_environment["ROOKERY_ACL_TEST_PATH"] = str(directory)
        owner_result = subprocess.run(  # noqa: S603 - absolute Windows system executable
            (
                str(powershell),
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                owner_script,
            ),
            check=False,
            capture_output=True,
            env=owner_environment,
            text=True,
        )
        assert owner_result.returncode == 0, owner_result.stderr


def test_cli_work_root_rejects_redirected_state_directory(
    tmp_path: Path,
    create_directory_redirect,
) -> None:
    home = tmp_path / "home"
    redirected = home / "redirected"
    home.mkdir()
    redirected.mkdir()
    create_directory_redirect(home / ".rookery", redirected)

    with pytest.raises(CliUnavailable, match="filesystem redirect"):
        databricks_adapter._trusted_cli_work_root(home=home)

    assert list(redirected.iterdir()) == []


def test_cli_resolution_never_searches_current_directory_or_relative_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hostile = tmp_path / "hostile"
    trusted = tmp_path / "trusted"
    hostile.mkdir()
    trusted.mkdir()
    executable_name = "databricks.exe" if os.name == "nt" else "databricks"
    for directory in (hostile, trusted):
        executable = directory / executable_name
        executable.write_text("placeholder", encoding="utf-8")
        executable.chmod(0o700)
    monkeypatch.chdir(hostile)
    monkeypatch.setenv("PATH", f"{os.pathsep}.{os.pathsep}{trusted}")
    if os.name == "nt":
        monkeypatch.setenv("PATHEXT", ".EXE")

    resolved = CliRunner().resolve_executable()

    assert Path(resolved) == (trusted / executable_name).resolve()


def test_cli_resolution_rejects_current_directory_only_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable_name = "databricks.exe" if os.name == "nt" else "databricks"
    executable = tmp_path / executable_name
    executable.write_text("placeholder", encoding="utf-8")
    executable.chmod(0o700)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PATH", f"{os.pathsep}.")
    if os.name == "nt":
        monkeypatch.setenv("PATHEXT", ".EXE")

    with pytest.raises(CliUnavailable, match="absolute PATH"):
        CliRunner().resolve_executable()


def test_cli_process_creation_failure_is_controlled_and_cleans_workdir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work_root = tmp_path / "trusted-home" / ".rookery" / "cli-work"
    work_root.mkdir(parents=True)
    monkeypatch.setattr(databricks_adapter, "_trusted_cli_work_root", lambda: work_root)

    async def fail_process(*_args: object, **_kwargs: object) -> None:
        raise FileNotFoundError("simulated executable race")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fail_process)
    runner = CliRunner()
    runner._resolved_executable = "C:\\trusted\\databricks.exe"

    with pytest.raises(CliUnavailable, match="process could not start"):
        asyncio.run(runner.run_unmapped(CliInvocation("doctor", ("databricks", "--version"))))

    assert list(work_root.iterdir()) == []


@pytest.mark.parametrize("failure", ["cancel", "timeout", "output_limit"])
def test_compatibility_check_failures_reap_process_and_readers(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    tmp_path: Path,
) -> None:
    work_root = tmp_path / "trusted-home" / ".rookery" / "cli-work"
    work_root.mkdir(parents=True)
    monkeypatch.setattr(databricks_adapter, "_trusted_cli_work_root", lambda: work_root)

    async def exercise() -> None:
        stopped = asyncio.Event()
        created = asyncio.Event()

        class BlockingStream:
            def __init__(self, *, overflow: bool = False) -> None:
                self.overflow = overflow
                self.settled = False

            async def read(self, _size: int) -> bytes:
                try:
                    if self.overflow:
                        self.overflow = False
                        return b"xx"
                    await stopped.wait()
                    return b""
                finally:
                    self.settled = True

        class BlockingProcess:
            returncode: int | None = None
            killed = False
            wait_calls = 0
            stdout = BlockingStream(overflow=failure == "output_limit")
            stderr = BlockingStream()

            def kill(self) -> None:
                self.killed = True
                self.returncode = -9
                stopped.set()

            async def wait(self) -> int:
                self.wait_calls += 1
                await stopped.wait()
                return self.returncode or 0

        process = BlockingProcess()

        async def create_process(*_args: object, **_kwargs: object) -> BlockingProcess:
            created.set()
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
        runner = CliRunner(
            timeout_seconds=0.001 if failure == "timeout" else 30,
            stdout_cap=1,
            stderr_cap=1,
        )
        runner._resolved_executable = "C:\\trusted\\databricks.exe"
        task = asyncio.create_task(
            runner.run_unmapped(CliInvocation("doctor", ("databricks", "--version")))
        )
        try:
            await asyncio.wait_for(created.wait(), timeout=1)
        except TimeoutError:
            if task.done():
                await task
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise
        await asyncio.sleep(0)

        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        elif failure == "timeout":
            with pytest.raises(CliTimeout):
                await task
        else:
            with pytest.raises(CliOutputLimit):
                await task

        assert process.killed
        assert process.wait_calls >= 1
        assert process.stdout.settled
        assert process.stderr.settled

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("capability", "facet", "target", "payload"),
    [
        (
            "databricks.workspace.children.read",
            "membership",
            ResolvedTarget(
                workspace_path="/Shared",
                workspace_root="/Shared",
                canonical_object_id=uuid4(),
                canonical_object_type="folder",
            ),
            {"objects": [{"path": "/Shared/n", "object_type": "NOTEBOOK", "object_id": 1}]},
        ),
        (
            "databricks.workspace.metadata.read",
            "metadata",
            ResolvedTarget(
                workspace_path="/Shared/n",
                workspace_root="/Shared",
                canonical_object_id=uuid4(),
                canonical_object_type="file",
            ),
            {"path": "/Shared/n", "object_type": "FILE"},
        ),
        (
            "databricks.workspace.content.read",
            "content",
            ResolvedTarget(
                workspace_path="/Shared/n",
                workspace_root="/Shared",
                canonical_object_id=uuid4(),
                canonical_object_type="file",
            ),
            {"content": base64.b64encode(b"ok").decode()},
        ),
        (
            "databricks.uc.catalogs.read",
            "attributes",
            _catalog_root_target(),
            {"catalogs": [{"name": "main", "storage_location": "s3://secret"}]},
        ),
        (
            "databricks.uc.schemas.read",
            "attributes",
            ResolvedTarget(catalog_name="main"),
            {"schemas": [{"name": "sales"}]},
        ),
        (
            "databricks.uc.relations.read",
            "attributes",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {
                "tables": [
                    {
                        "name": "orders",
                        "table_type": "MANAGED",
                        "storage_location": "s3://secret",
                        "columns": [{"name": "id", "type_name": "INT"}],
                    }
                ]
            },
        ),
        (
            "databricks.uc.volumes.read",
            "attributes",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {"volumes": [{"name": "landing", "storage_location": "s3://secret"}]},
        ),
    ],
)
def test_fixture_normalization_is_deterministic_and_metadata_only(
    capability: str, facet: str, target: ResolvedTarget, payload: dict[str, object]
) -> None:
    action = _action(capability, facet)
    binding = _binding()
    first = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=json.dumps(payload).encode(),
        observed_at=datetime.now(UTC),
    )
    second = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=json.dumps(payload).encode(),
        observed_at=datetime.now(UTC),
    )
    assert first.batch.batch_id == second.batch.batch_id
    assert [item.observation_id for item in first.batch.facet_observations] == [
        item.observation_id for item in second.batch.facet_observations
    ]
    assert all(
        item.facet != "content"
        for item in first.batch.facet_observations
        if capability.endswith("children.read")
    )
    if capability == "databricks.workspace.content.read":
        assert first.batch.coverage == ()
        assert first.batch.facet_observations[0].satisfies == ()
    else:
        expected_coverage = (
            CollectionCoverage.COMPLETE
            if capability == "databricks.workspace.metadata.read"
            else CollectionCoverage.UNKNOWN
        )
        assert len(first.batch.coverage) == 1
        assert first.batch.coverage[0].scope == replace(
            action.requested_scopes[0],
            capability_key=capability,
        )
        assert first.batch.coverage[0].completeness is expected_coverage
        assert first.batch.coverage[0].absence_authority == ()
        if capability == "databricks.workspace.children.read":
            membership = next(
                item for item in first.batch.facet_observations if item.facet == "membership"
            )
            assert membership.payload["collection_completeness"] == "unknown"
    assert "storage_location" not in str(first.batch.to_dict())


def test_legacy_null_action_scope_normalizes_to_selected_capability_authority() -> None:
    action = _action("databricks.workspace.children.read", "membership")
    action = replace(
        action,
        requested_scopes=(replace(action.requested_scopes[0], capability_key=None),),
    )

    result = normalize(
        action=action,
        binding=_binding(),
        target=ResolvedTarget(
            workspace_path="/Shared",
            workspace_root="/Shared",
            canonical_object_id=uuid4(),
            canonical_object_type="folder",
        ),
        stdout=b'{"objects":[{"path":"/Shared/a.py","object_type":"FILE"}]}',
        observed_at=datetime(2026, 8, 29, tzinfo=UTC),
    )

    assert all(
        scope.capability_key == action.capability_key
        for batch in result.batches
        for item in (*batch.facet_observations, *batch.relationship_observations)
        for scope in item.authorized_by
    )
    assert all(
        declaration.scope.capability_key == action.capability_key
        for batch in result.batches
        for declaration in batch.coverage
    )


@pytest.mark.parametrize(
    ("capability", "target", "payload"),
    [
        (
            "databricks.uc.schemas.read",
            ResolvedTarget(catalog_name="main"),
            {"schemas": [{"name": "sales", "full_name": "other.sales"}]},
        ),
        (
            "databricks.uc.schemas.read",
            ResolvedTarget(catalog_name="main"),
            {"schemas": [{"name": "sales", "catalog_name": "other"}]},
        ),
        (
            "databricks.uc.relations.read",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {"tables": [{"name": "orders", "full_name": "main.other.orders"}]},
        ),
        (
            "databricks.uc.volumes.read",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {"volumes": [{"name": "raw", "schema_name": "other"}]},
        ),
    ],
)
def test_uc_normalization_rejects_identity_that_contradicts_target(
    capability: str,
    target: ResolvedTarget,
    payload: dict[str, object],
) -> None:
    with pytest.raises(InvalidDownstreamResponse, match=r"another|contradicts"):
        normalize(
            action=_action(capability),
            binding=_binding(),
            target=target,
            stdout=json.dumps(payload).encode(),
            observed_at=datetime.now(UTC),
        )


def test_uc_normalization_derives_leaf_name_from_valid_full_name() -> None:
    result = normalize(
        action=_action("databricks.uc.schemas.read"),
        binding=_binding(),
        target=ResolvedTarget(catalog_name="main"),
        stdout=b'{"schemas":[{"full_name":"main.sales"}]}',
        observed_at=datetime.now(UTC),
    )

    assert result.batch.facet_observations[0].target.external_key == "schema:main.sales"
    assert result.batch.facet_observations[0].target.display_name == "sales"


@pytest.mark.parametrize(
    (
        "capability",
        "collection_key",
        "source_key",
        "external_prefix",
        "target",
        "first_item",
        "renamed_item",
    ),
    (
        (
            "databricks.uc.schemas.read",
            "schemas",
            "schema_id",
            "schema:schema_id:",
            ResolvedTarget(catalog_name="main"),
            {"name": "sales", "full_name": "main.sales"},
            {"name": "revenue", "full_name": "main.revenue"},
        ),
        (
            "databricks.uc.relations.read",
            "tables",
            "table_id",
            "relation:table_id:",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {
                "name": "orders",
                "full_name": "main.sales.orders",
                "table_type": "MANAGED",
            },
            {
                "name": "purchases",
                "full_name": "main.sales.purchases",
                "table_type": "MANAGED",
            },
        ),
        (
            "databricks.uc.relations.read",
            "tables",
            "table_id",
            "relation:table_id:",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {
                "name": "orders_view",
                "full_name": "main.sales.orders_view",
                "table_type": "VIEW",
            },
            {
                "name": "purchases_view",
                "full_name": "main.sales.purchases_view",
                "table_type": "VIEW",
            },
        ),
        (
            "databricks.uc.volumes.read",
            "volumes",
            "volume_id",
            "volume:volume_id:",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {"name": "raw", "full_name": "main.sales.raw"},
            {"name": "landing", "full_name": "main.sales.landing"},
        ),
    ),
)
def test_uc_source_ids_survive_rename_and_separate_same_name_recreation(
    capability: str,
    collection_key: str,
    source_key: str,
    external_prefix: str,
    target: ResolvedTarget,
    first_item: dict[str, object],
    renamed_item: dict[str, object],
) -> None:
    action = _action(capability)
    binding = _binding()
    source_id = str(uuid4())

    def normalized(item: dict[str, object], identity: str | None):
        value = dict(item)
        if identity is not None:
            value[source_key] = identity
        return databricks_adapter.normalize(
            action=action,
            binding=binding,
            target=target,
            delivery_id=str(uuid4()),
            stdout=json.dumps({collection_key: [value]}).encode(),
            observed_at=datetime(2026, 8, 29, tzinfo=UTC),
        ).batch.facet_observations[0]

    first = normalized(first_item, source_id)
    renamed = normalized(renamed_item, source_id)
    recreated = normalized(renamed_item, str(uuid4()))
    legacy = normalized(renamed_item, None)

    assert first.target.external_key == external_prefix + source_id
    assert renamed.target.external_key == first.target.external_key
    assert renamed.target.display_name == renamed_item["name"]
    assert renamed.payload[source_key] == source_id
    assert renamed.payload["full_name"] == renamed_item["full_name"]
    if renamed_item.get("table_type") == "VIEW":
        assert renamed.target.source_kind == "databricks.uc.view"
    assert recreated.target.external_key != renamed.target.external_key
    assert legacy.target.external_key != renamed.target.external_key


@pytest.mark.parametrize(
    ("capability", "collection_key", "source_key", "target", "item"),
    (
        (
            "databricks.uc.schemas.read",
            "schemas",
            "schema_id",
            ResolvedTarget(catalog_name="main"),
            {"name": "sales"},
        ),
        (
            "databricks.uc.relations.read",
            "tables",
            "table_id",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {"name": "orders"},
        ),
        (
            "databricks.uc.volumes.read",
            "volumes",
            "volume_id",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            {"name": "raw"},
        ),
    ),
)
def test_uc_source_ids_fail_closed_when_present_but_invalid(
    capability: str,
    collection_key: str,
    source_key: str,
    target: ResolvedTarget,
    item: dict[str, object],
) -> None:
    with pytest.raises(InvalidDownstreamResponse, match="is not a UUID"):
        normalize(
            action=_action(capability),
            binding=_binding(),
            target=target,
            stdout=json.dumps({collection_key: [item | {source_key: "not-a-uuid"}]}).encode(),
            observed_at=datetime(2026, 8, 29, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("capability", "facet", "target", "envelope_key", "array_key"),
    [
        (
            "databricks.workspace.children.read",
            "membership",
            ResolvedTarget(
                workspace_path="/Shared",
                workspace_root="/Shared",
                canonical_object_id=uuid4(),
                canonical_object_type="folder",
            ),
            "workspace_children",
            "workspace_children_array",
        ),
        (
            "databricks.uc.catalogs.read",
            "attributes",
            _catalog_root_target(),
            "catalogs",
            "catalogs_array",
        ),
        (
            "databricks.uc.schemas.read",
            "attributes",
            ResolvedTarget(catalog_name="main"),
            "schemas",
            "schemas_array",
        ),
        (
            "databricks.uc.relations.read",
            "attributes",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            "relations",
            "relations_array",
        ),
        (
            "databricks.uc.volumes.read",
            "attributes",
            ResolvedTarget(catalog_name="main", schema_name="sales"),
            "volumes",
            "volumes_array",
        ),
    ],
)
def test_list_normalization_accepts_envelopes_and_top_level_arrays(
    capability: str,
    facet: str,
    target: ResolvedTarget,
    envelope_key: str,
    array_key: str,
) -> None:
    fixture = json.loads(Path("tests/fixtures/databricks/capabilities.json").read_text())
    action = _action(capability, facet)
    binding = _binding()
    envelope = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=json.dumps(fixture[envelope_key]).encode(),
        observed_at=datetime.now(UTC),
    )
    array = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=json.dumps(fixture[array_key]).encode(),
        observed_at=datetime.now(UTC),
    )
    assert len(envelope.batch.facet_observations) == len(array.batch.facet_observations)
    assert len(envelope.batch.relationship_observations) == len(
        array.batch.relationship_observations
    )
    assert all(item.completeness is CollectionCoverage.UNKNOWN for item in array.batch.coverage)
    assert "storage_location" not in str(array.batch.to_dict())


def test_list_normalization_rejects_incomplete_pagination_envelope() -> None:
    with pytest.raises(InvalidDownstreamResponse, match="incomplete paginated collection"):
        normalize(
            action=_action("databricks.uc.catalogs.read"),
            binding=_binding(),
            target=_catalog_root_target(),
            stdout=b'{"catalogs":[{"name":"first"}],"next_page_token":"next"}',
            observed_at=datetime.now(UTC),
        )


@pytest.mark.parametrize(
    ("capability", "facet", "target"),
    [
        (
            "databricks.workspace.metadata.read",
            "metadata",
            ResolvedTarget(workspace_path="/Shared/n", workspace_root="/Shared"),
        ),
        (
            "databricks.workspace.content.read",
            "content",
            ResolvedTarget(workspace_path="/Shared/n", workspace_root="/Shared"),
        ),
    ],
)
def test_metadata_and_content_reject_top_level_arrays(
    capability: str, facet: str, target: ResolvedTarget
) -> None:
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action(capability, facet),
            binding=_binding(),
            target=target,
            stdout=b"[]",
            observed_at=datetime.now(UTC),
        )


@pytest.mark.parametrize("stdout", [b"1", b"[{}, false]", b'["unsafe"]', b"null"])
def test_list_normalization_rejects_scalars_and_mixed_arrays(stdout: bytes) -> None:
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.uc.catalogs.read"),
            binding=_binding(),
            target=_catalog_root_target(),
            stdout=stdout,
            observed_at=datetime.now(UTC),
        )


@pytest.mark.parametrize(
    "child",
    [
        {"path": "/Shared", "object_type": "DIRECTORY"},
        {"path": "/Shared/nested/child", "object_type": "FILE"},
        {"path": "/Users/other/secret", "object_type": "FILE"},
    ],
)
def test_workspace_listing_rejects_children_outside_direct_scope(child: dict[str, str]) -> None:
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
    )
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.workspace.children.read", "membership"),
            binding=_binding(),
            target=target,
            stdout=json.dumps({"objects": [child]}).encode(),
            observed_at=datetime.now(UTC),
        )


def test_workspace_listing_rejects_duplicate_child_locator() -> None:
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
    )
    child = {"path": "/Shared/one", "object_type": "FILE", "object_id": 1}
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.workspace.children.read", "membership"),
            binding=_binding(),
            target=target,
            stdout=json.dumps({"objects": [child, child]}).encode(),
            observed_at=datetime.now(UTC),
        )


def test_workspace_listing_rejects_duplicate_identity_with_different_paths() -> None:
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
    )
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.workspace.children.read", "membership"),
            binding=_binding(),
            target=target,
            stdout=json.dumps(
                {
                    "objects": [
                        {"path": "/Shared/one", "object_type": "FILE", "object_id": 1},
                        {"path": "/Shared/two", "object_type": "FILE", "object_id": 1},
                    ]
                }
            ).encode(),
            observed_at=datetime.now(UTC),
        )


def test_workspace_listing_accepts_distinct_object_and_resource_ids() -> None:
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
    )
    result = normalize(
        action=_action("databricks.workspace.children.read", "membership"),
        binding=_binding(),
        target=target,
        stdout=json.dumps(
            {
                "objects": [
                    {
                        "path": "/Shared/one",
                        "object_type": "FILE",
                        "object_id": 202,
                        "resource_id": 101,
                    }
                ]
            }
        ).encode(),
        observed_at=datetime.now(UTC),
    )
    child = next(
        observation.target
        for observation in result.batch.facet_observations
        if observation.target.external_key is not None
    )
    assert child.external_key == "workspace:object_id:202"


def test_workspace_metadata_requires_canonical_identity_witness() -> None:
    target = ResolvedTarget(
        workspace_path="/Shared/b.py",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="file",
        canonical_parent_external_key="workspace:101",
    )
    action = _action("databricks.workspace.metadata.read", "metadata")
    binding = _binding()
    with pytest.raises(InvalidDownstreamResponse, match="canonical target"):
        normalize(
            action=action,
            binding=binding,
            target=target,
            stdout=b'{"path":"/Shared/b.py","object_type":"FILE","object_id":202}',
            observed_at=datetime.now(UTC),
        )

    accepted = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=b'{"path":"/Shared/b.py","object_type":"FILE","object_id":101}',
        observed_at=datetime.now(UTC),
    )
    assert accepted.batch.facet_observations[0].target.object_id == str(target.canonical_object_id)
    resource_match = normalize(
        action=action,
        binding=binding,
        target=target,
        stdout=(b'{"path":"/Shared/b.py","object_type":"FILE","object_id":202,"resource_id":101}'),
        observed_at=datetime.now(UTC),
    )
    assert resource_match.batch.facet_observations[0].target.object_id == str(
        target.canonical_object_id
    )
    typed_target = ResolvedTarget(
        workspace_path="/Shared/b.py",
        workspace_root="/Shared",
        canonical_object_id=target.canonical_object_id,
        canonical_object_type="file",
        canonical_parent_external_key="workspace:object_id:101",
    )
    with pytest.raises(InvalidDownstreamResponse, match="canonical target"):
        normalize(
            action=action,
            binding=binding,
            target=typed_target,
            stdout=b'{"path":"/Shared/b.py","object_type":"FILE","resource_id":101}',
            observed_at=datetime.now(UTC),
        )
    typed_match = normalize(
        action=action,
        binding=binding,
        target=typed_target,
        stdout=(b'{"path":"/Shared/b.py","object_type":"FILE","object_id":101,"resource_id":202}'),
        observed_at=datetime.now(UTC),
    )
    assert typed_match.batch.facet_observations[0].target.object_id == str(
        target.canonical_object_id
    )


def test_workspace_listing_rejects_child_with_parent_external_identity() -> None:
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
        canonical_parent_external_key="workspace:/Shared",
    )
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=_action("databricks.workspace.children.read", "membership"),
            binding=_binding(),
            target=target,
            stdout=json.dumps(
                {
                    "objects": [
                        {"path": "/Shared/child", "object_type": "FILE", "object_id": "/Shared"}
                    ]
                }
            ).encode(),
            observed_at=datetime.now(UTC),
        )


def test_normalization_rejects_collection_and_column_cap_plus_one() -> None:
    action = _action("databricks.uc.catalogs.read")
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=action,
            binding=_binding(),
            target=_catalog_root_target(),
            stdout=json.dumps(
                {"catalogs": [{"name": str(index)} for index in range(MAX_COLLECTION_ITEMS + 1)]}
            ).encode(),
            observed_at=datetime.now(UTC),
        )
    relation = _action("databricks.uc.relations.read")
    with pytest.raises(InvalidDownstreamResponse):
        normalize(
            action=relation,
            binding=_binding(),
            target=ResolvedTarget(catalog_name="main", schema_name="sales"),
            stdout=json.dumps(
                {
                    "tables": [
                        {"name": "orders", "columns": [{"name": "id"}] * (MAX_TABLE_COLUMNS + 1)}
                    ]
                }
            ).encode(),
            observed_at=datetime.now(UTC),
        )


def test_supported_large_collection_is_deterministically_chunked_with_linked_authority() -> None:
    action = _action("databricks.workspace.children.read", "membership")
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
    )
    items = [
        {
            "path": f"/Shared/f{index:05d}.py",
            "object_type": "FILE",
            "object_id": index,
        }
        for index in range(MAX_COLLECTION_ITEMS)
    ]
    observed_at = datetime(2026, 8, 29, tzinfo=UTC)

    result = normalize(
        action=action,
        binding=_binding(),
        target=target,
        stdout=json.dumps({"objects": items}).encode(),
        observed_at=observed_at,
    )

    assert len(result.batches) > 1
    with pytest.raises(ValueError, match="multipart normalization"):
        _ = result.batch
    assert len({batch.batch_id for batch in result.batches}) == len(result.batches)
    assert sum(len(batch.facet_observations) for batch in result.batches) == 10_001
    assert sum(len(batch.relationship_observations) for batch in result.batches) == 10_000
    assert all(
        databricks_adapter._canonical_batch_size(batch)
        <= databricks_adapter.MAX_INGESTION_BATCH_BYTES
        for batch in result.batches
    )
    assert all(
        len(batch.relationship_observations) <= databricks_adapter.MAX_INGESTION_BATCH_UNITS
        for batch in result.batches
    )
    assert all(
        declaration.completeness is CollectionCoverage.UNKNOWN and not declaration.absence_authority
        for batch in result.batches
        for declaration in batch.coverage
    )
    assert all(batch.coverage for batch in result.batches)
    assert not any(
        observation.facet == "membership"
        for batch in result.batches[:-1]
        for observation in batch.facet_observations
    )
    assert any(
        observation.facet == "membership" for observation in result.batches[-1].facet_observations
    )
    for batch in result.batches:
        linked_targets = {relationship.object for relationship in batch.relationship_observations}
        assert all(
            observation.facet == "membership" or observation.target in linked_targets
            for observation in batch.facet_observations
        )


def test_supported_wide_relations_chunk_before_shared_node_budget() -> None:
    tables = [
        {
            "name": f"table_{table_index}",
            "full_name": f"main.sales.table_{table_index}",
            "columns": [{"name": f"column_{column_index}"} for column_index in range(1_000)],
        }
        for table_index in range(50)
    ]
    stdout = json.dumps({"tables": tables}).encode()
    assert len(stdout) < databricks_adapter.MAX_JSON_BYTES

    result = normalize(
        action=_action("databricks.uc.relations.read"),
        binding=_binding(),
        target=ResolvedTarget(catalog_name="main", schema_name="sales"),
        stdout=stdout,
        observed_at=datetime(2026, 8, 29, tzinfo=UTC),
    )

    assert len(result.batches) > 1
    assert sum(len(batch.relationship_observations) for batch in result.batches) == 50
    assert all(
        databricks_adapter._canonical_batch_size(batch)
        <= databricks_adapter.MAX_INGESTION_BATCH_BYTES
        for batch in result.batches
    )
    assert all(
        len(batch.relationship_observations) <= databricks_adapter.MAX_INGESTION_BATCH_UNITS
        for batch in result.batches
    )


def test_chunk_plan_is_source_order_independent_and_rejects_oversized_units() -> None:
    action = _action("databricks.workspace.children.read", "membership")
    target = ResolvedTarget(
        workspace_path="/Shared",
        workspace_root="/Shared",
        canonical_object_id=uuid4(),
        canonical_object_type="folder",
    )
    items = [
        {
            "path": f"/Shared/f{index:04d}.py",
            "object_type": "FILE",
            "object_id": index,
        }
        for index in range(1200)
    ]
    arguments = {
        "action": action,
        "binding": _binding(),
        "target": target,
        "observed_at": datetime(2026, 8, 29, tzinfo=UTC),
    }
    forward = normalize(stdout=json.dumps({"objects": items}).encode(), **arguments)
    reverse = normalize(stdout=json.dumps({"objects": items[::-1]}).encode(), **arguments)

    assert [batch.to_dict() for batch in forward.batches] == [
        batch.to_dict() for batch in reverse.batches
    ]
    small_forward = normalize(
        stdout=json.dumps({"objects": items[:2]}).encode(),
        **arguments,
    )
    small_reverse = normalize(
        stdout=json.dumps({"objects": items[1::-1]}).encode(),
        **arguments,
    )
    assert small_forward.batch.to_dict() == small_reverse.batch.to_dict()
    boundary = normalize(
        stdout=json.dumps({"objects": items[:251]}).encode(),
        **arguments,
    )
    assert len(boundary.batches) > 1
    assert all(
        len(batch.relationship_observations) <= databricks_adapter.MAX_INGESTION_BATCH_UNITS
        for batch in boundary.batches
    )
    complete_batch = replace(
        forward.batches[0],
        batch_id=uuid4(),
        facet_observations=tuple(
            observation for batch in forward.batches for observation in batch.facet_observations
        ),
        relationship_observations=tuple(
            observation
            for batch in forward.batches
            for observation in batch.relationship_observations
        ),
        coverage=tuple(
            replace(declaration, completeness=CollectionCoverage.COMPLETE)
            for declaration in forward.batches[0].coverage
        ),
    )
    with pytest.raises(InvalidDownstreamResponse, match="complete collection evidence"):
        databricks_adapter._chunk_normalized_batch(
            action=action,
            delivery_id=_DELIVERY_ID,
            batch=complete_batch,
        )

    oversized_relation = {
        "name": "orders",
        "full_name": "main.sales.orders",
        "columns": [{"name": f"c{index}", "type_text": "X" * 1024} for index in range(1000)],
    }
    with pytest.raises(InvalidDownstreamResponse, match="one normalized collection item"):
        normalize(
            action=_action("databricks.uc.relations.read"),
            binding=_binding(),
            target=ResolvedTarget(catalog_name="main", schema_name="sales"),
            stdout=json.dumps({"tables": [oversized_relation]}).encode(),
            observed_at=datetime(2026, 8, 29, tzinfo=UTC),
        )


def test_sanitized_fixture_covers_registered_capabilities() -> None:
    fixture = json.loads(Path("tests/fixtures/databricks/capabilities.json").read_text())
    assert set(fixture) == {
        "workspace_children",
        "workspace_children_array",
        "workspace_metadata",
        "workspace_content",
        "catalogs",
        "catalogs_array",
        "schemas",
        "schemas_array",
        "relations",
        "relations_array",
        "volumes",
        "volumes_array",
    }


def test_redaction_and_import_boundary() -> None:
    diagnostic = redact_diagnostic(
        "token=not-for-storage --profile local\nAuthorization: Bearer abc "
        "bearer lowercase-secret "
        '"client_secret":"json-secret" access_token=token-value '  # pragma: allowlist secret
        "refresh_token=refresh-value api_key=api-value \\\\server\\share\\config "
        "/home/person/.databrickscfg "
        "Config: C:\\Users\\person\\.databrickscfg, "
        "databricks_cli_path=C:\\Program Files\\Databricks\\databricks.exe"
    )
    assert (
        "not-for-storage" not in diagnostic
        and "abc" not in diagnostic
        and "lowercase-secret" not in diagnostic
        and "json-secret" not in diagnostic
        and "token-value" not in diagnostic
        and "refresh-value" not in diagnostic
        and "api-value" not in diagnostic
        and "--profile local" not in diagnostic
        and "Users" not in diagnostic
        and "Program Files" not in diagnostic
        and "databricks.exe" not in diagnostic
        and "/home/person" not in diagnostic
        and "server\\share" not in diagnostic
    )
    source = Path("src/async_api_view/adapters/databricks.py").read_text(encoding="utf-8")
    assert "async_api_view.storage" not in source
    assert "databricks api" not in source


def test_retry_after_parser_accepts_seconds_and_http_date_with_bound() -> None:
    now = datetime(2026, 8, 29, 20, tzinfo=UTC)

    assert _downstream_retry_after(b"HTTP 429\nRetry-After: 120", now=now) == (
        timedelta(seconds=120),
        False,
    )
    assert _downstream_retry_after(
        b"Retry-After: Sat, 29 Aug 2026 20:02:00 GMT",
        now=now,
    ) == (timedelta(seconds=120), False)
    assert _downstream_retry_after(b'{"retry_after": 86401}', now=now) == (None, True)
    assert _downstream_retry_after(b"Retry-After: 10000000000", now=now) == (None, True)
    assert _downstream_retry_after(b"Retry-After: invalid", now=now) == (None, False)
    assert _downstream_retry_after(
        b"Retry-After: Fri, 31 Dec 9999 23:59:59 -2359",
        now=now,
    ) == (None, False)
    assert (
        classify_failure(
            DownstreamFailure(
                "guided retry",
                exit_code=1,
                diagnostic="Retry-After: 120",
                retry_after=timedelta(seconds=120),
            )
        )
        is ErrorClass.DOWNSTREAM_RATE_LIMIT
    )


class _Queue:
    def __init__(self, lease: ActionLease) -> None:
        self.lease = lease

    async def lease_next(self, **_: object) -> ActionLease | None:
        return self.lease


class _Lifecycle:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def mark_running(self, **kwargs: object) -> None:
        self.events.append(("running", kwargs))

    async def record_attempt(self, attempt: object, *, lease_id: str) -> None:
        self.events.append(("attempt", lease_id, attempt))

    async def complete_action(self, completion: object, *, lease_id: str) -> None:
        self.events.append(("complete", lease_id, completion))

    async def heartbeat(self, **kwargs: object) -> None:
        self.events.append(("heartbeat", kwargs))


class _Guard:
    async def evaluate(self, **_: object) -> GuardDecision:
        return GuardDecision(GuardDisposition.DISPATCH, "allowed")

    async def authorize_start(self, **_: object) -> GuardDecision:
        return GuardDecision(GuardDisposition.DISPATCH, "allowed")


class _Bindings:
    def __init__(self, binding: ConnectionBinding) -> None:
        self.binding = binding

    async def get_connection_binding(self, _: str) -> ConnectionBinding:
        return self.binding

    async def get_capability_binding(
        self, binding_id: str, capability: str, _: str
    ) -> CapabilityBinding:
        return CapabilityBinding(
            uuid4(),
            binding_id,
            capability,
            "1",
            OperationClass.OBSERVE,
            (TargetKind.CONFIGURED_SCOPE,),
            ("attributes",),
            True,
        )


class _Targets:
    async def resolve(self, **_: object) -> ResolvedTarget:
        return _catalog_root_target()


class _MetadataTargets:
    async def resolve(self, **_: object) -> ResolvedTarget:
        return ResolvedTarget(
            workspace_path="/Shared/n",
            canonical_object_id=uuid4(),
            canonical_object_type="file",
        )


class _UnexpectedTargets:
    async def resolve(self, **_: object) -> ResolvedTarget:
        raise AssertionError("content target resolution must remain disabled")


class _Runner:
    executable = "databricks"

    async def run(self, *_: object, **__: object) -> CliExecution:
        return CliExecution(str(uuid4()), timedelta(), 0, b'{"catalogs": [{"name": "main"}]}', b"")


class _MetadataRunner(_Runner):
    async def run(self, *_: object, **__: object) -> CliExecution:
        return CliExecution(
            str(uuid4()), timedelta(), 0, b'{"path": "/Shared/n", "object_type": "FILE"}', b""
        )


class _FailRunner(_Runner):
    async def run(self, *_: object, **__: object) -> CliExecution:
        raise DownstreamFailure(
            "raw downstream failure",
            exit_code=1,
            diagnostic="token=worker-secret C:\\Users\\person\\.databrickscfg /home/person/config",
        )


class _RateLimitRunner(_Runner):
    def __init__(
        self,
        *,
        retry_after: timedelta | None = timedelta(seconds=120),
        retry_after_out_of_bounds: bool = False,
    ) -> None:
        self.retry_after = retry_after
        self.retry_after_out_of_bounds = retry_after_out_of_bounds

    async def run(self, *_: object, **__: object) -> CliExecution:
        raise DownstreamFailure(
            "rate limited",
            exit_code=1,
            diagnostic="HTTP 429 Retry-After: 120",
            retry_after=self.retry_after,
            retry_after_out_of_bounds=self.retry_after_out_of_bounds,
        )


class _TimeoutRunner(_Runner):
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, *_: object, **__: object) -> CliExecution:
        self.calls += 1
        raise CliTimeout("timed out")


class _StaleLifecycle(_Lifecycle):
    async def record_attempt(self, attempt: object, *, lease_id: str) -> None:
        raise ActionLeaseLost(f"stale lease {lease_id}")


class _FailGuard(_Guard):
    async def evaluate(self, **_: object) -> GuardDecision:
        return GuardDecision(GuardDisposition.FAIL, "authority_lost")


class _TerminalGuard(_Guard):
    def __init__(self, disposition: GuardDisposition) -> None:
        self.disposition = disposition

    async def evaluate(self, **_: object) -> GuardDecision:
        observations = (uuid4(),) if self.disposition is GuardDisposition.SATISFY else ()
        return GuardDecision(self.disposition, "terminalized_by_guard", observations)


class _HeartbeatLostLifecycle(_Lifecycle):
    async def heartbeat(self, **_: object) -> None:
        raise ActionLeaseLost("lease lost")


class _HeartbeatFailedLifecycle(_Lifecycle):
    async def heartbeat(self, **_: object) -> None:
        raise OSError("lifecycle storage unavailable")


class _BlockingRunner:
    executable = "databricks"

    def __init__(self) -> None:
        self.cancelled = False

    async def run(self, *_: object, **__: object) -> CliExecution:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class _Ingestion:
    def __init__(self, status: IngestionStatus = IngestionStatus.ACCEPTED) -> None:
        self.batches: list[object] = []
        self.lease_ids: list[str | None] = []
        self.status = status

    async def ingest(self, batch: object, *, lease_id: str | None = None) -> IngestionResult:
        self.batches.append(batch)
        self.lease_ids.append(lease_id)
        return IngestionResult(batch.batch_id, self.status)


def test_worker_uses_only_ports() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    ingestion = _Ingestion()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=ingestion,
        targets=_Targets(),
        runner=_Runner(),
    )
    assert asyncio.run(worker.run_once())
    assert ingestion.batches
    assert ingestion.lease_ids == [worker.queue.lease.lease_id]
    assert all(
        event[1] == worker.queue.lease.lease_id
        for event in lifecycle.events
        if isinstance(event, tuple) and event[0] in {"attempt", "complete"}
    )


def test_worker_ingests_every_collection_chunk_and_stops_when_heartbeat_is_lost() -> None:
    action = _action("databricks.workspace.children.read", "membership")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local", "workspace_root": "/Shared"},
    )
    payload = json.dumps(
        {
            "objects": [
                {
                    "path": f"/Shared/f{index:04d}.py",
                    "object_type": "FILE",
                    "object_id": index,
                }
                for index in range(1200)
            ]
        }
    ).encode()

    class WorkspaceTargets:
        async def resolve(self, **_: object) -> ResolvedTarget:
            return ResolvedTarget(
                workspace_path="/Shared",
                workspace_root="/Shared",
                canonical_object_id=uuid4(),
                canonical_object_type="folder",
            )

    class WorkspaceRunner(_Runner):
        async def run(self, *_: object, **__: object) -> CliExecution:
            return CliExecution(str(uuid4()), timedelta(), 0, payload, b"")

    lifecycle = _Lifecycle()
    ingestion = _Ingestion()
    lease = ActionLease(action, uuid4(), datetime.now(UTC))
    worker = DatabricksWorker(
        worker_id="chunk-worker",
        queue=_Queue(lease),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=ingestion,
        targets=WorkspaceTargets(),
        runner=WorkspaceRunner(),
    )

    assert asyncio.run(worker.run_once())
    assert len(ingestion.batches) > 1
    assert ingestion.lease_ids == [lease.lease_id] * len(ingestion.batches)
    assert (
        len([event for event in lifecycle.events if event[0] == "heartbeat"])
        == len(ingestion.batches) - 1
    )
    attempts = [event[2] for event in lifecycle.events if event[0] == "attempt"]
    completions = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert len(attempts) == len(completions) == 1
    assert attempts[0].outcome is ActionOutcome.PARTIAL
    assert completions[0].outcome is ActionOutcome.PARTIAL

    lost_lifecycle = _HeartbeatLostLifecycle()
    interrupted_ingestion = _Ingestion()
    interrupted = DatabricksWorker(
        worker_id="lost-worker",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lost_lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=interrupted_ingestion,
        targets=WorkspaceTargets(),
        runner=WorkspaceRunner(),
    )

    assert asyncio.run(interrupted.run_once())
    assert len(interrupted_ingestion.batches) == 1
    assert not any(event[0] in {"attempt", "complete"} for event in lost_lifecycle.events)

    class RejectSecondIngestion(_Ingestion):
        async def ingest(
            self,
            batch: ObservationBatch,
            *,
            lease_id: str | None = None,
        ) -> IngestionResult:
            self.batches.append(batch)
            self.lease_ids.append(lease_id)
            status = (
                IngestionStatus.REJECTED if len(self.batches) == 2 else IngestionStatus.ACCEPTED
            )
            return IngestionResult(batch.batch_id, status)

    rejected_lifecycle = _Lifecycle()
    rejected_ingestion = RejectSecondIngestion()
    rejected = DatabricksWorker(
        worker_id="rejected-worker",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=rejected_lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=rejected_ingestion,
        targets=WorkspaceTargets(),
        runner=WorkspaceRunner(),
    )

    assert asyncio.run(rejected.run_once())
    assert len(rejected_ingestion.batches) == 2
    rejected_attempts = [event[2] for event in rejected_lifecycle.events if event[0] == "attempt"]
    rejected_completions = [
        event[2] for event in rejected_lifecycle.events if event[0] == "complete"
    ]
    assert len(rejected_attempts) == len(rejected_completions) == 1
    assert rejected_attempts[0].outcome is ActionOutcome.FAILED
    assert rejected_completions[0].outcome is ActionOutcome.FAILED


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (IngestionStatus.ACCEPTED, ActionOutcome.PARTIAL),
        (IngestionStatus.DUPLICATE, ActionOutcome.PARTIAL),
        (IngestionStatus.PARTIAL, ActionOutcome.PARTIAL),
        (IngestionStatus.REJECTED, ActionOutcome.FAILED),
    ],
)
def test_worker_maps_ingestion_status_and_unknown_collection_coverage(
    status: IngestionStatus, expected: ActionOutcome
) -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(status),
        targets=_Targets(),
        runner=_Runner(),
    )
    asyncio.run(worker.run_once())
    completed = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert completed[-1].outcome is expected
    attempts = [event[2] for event in lifecycle.events if event[0] == "attempt"]
    assert attempts[-1].outcome is expected
    if status is IngestionStatus.REJECTED:
        assert attempts[-1].error_class is not None


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (IngestionStatus.ACCEPTED, ActionOutcome.SUCCEEDED),
        (IngestionStatus.DUPLICATE, ActionOutcome.SUCCEEDED),
        (IngestionStatus.PARTIAL, ActionOutcome.PARTIAL),
        (IngestionStatus.REJECTED, ActionOutcome.FAILED),
    ],
)
def test_worker_maps_all_ingestion_statuses_for_complete_object_read(
    status: IngestionStatus, expected: ActionOutcome
) -> None:
    action = _action("databricks.workspace.metadata.read", "metadata")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local", "workspace_root": "/Shared"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(status),
        targets=_MetadataTargets(),
        runner=_MetadataRunner(),
    )
    asyncio.run(worker.run_once())
    completed = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert completed[-1].outcome is expected


def test_worker_rejects_content_before_cli_without_artifact_persistence() -> None:
    action = _action("databricks.workspace.content.read", "content")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {
            "profile": "local",
            "workspace_root": "/Shared",
            "content_capture_enabled": True,
            "content_max_bytes": 1024,
        },
    )
    lifecycle = _Lifecycle()
    ingestion = _Ingestion()
    runner = _TimeoutRunner()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=ingestion,
        targets=_UnexpectedTargets(),
        runner=runner,
    )

    assert asyncio.run(worker.run_once())

    completed = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert runner.calls == 0
    assert len(completed) == 1
    assert completed[0].outcome is ActionOutcome.FAILED
    assert completed[0].error_class is ErrorClass.ADAPTER_CONTRACT_MISMATCH
    assert ingestion.batches == []
    assert not any(event[0] in {"running", "attempt"} for event in lifecycle.events)


def test_retryable_failure_schedules_one_durable_attempt_per_lease() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    timeout_runner = _TimeoutRunner()
    first_worker = DatabricksWorker(
        worker_id="first",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC), attempt_ordinal=1)),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=timeout_runner,
        max_attempts=2,
    )

    assert asyncio.run(first_worker.run_once())

    attempts = [event[2] for event in lifecycle.events if event[0] == "attempt"]
    assert timeout_runner.calls == 1
    assert len(attempts) == 1
    assert attempts[0].ordinal == 1
    assert attempts[0].error_class is ErrorClass.CONNECTION_TIMEOUT
    assert attempts[0].retry_at - attempts[0].ended_at == timedelta(seconds=1)
    assert not any(event[0] == "complete" for event in lifecycle.events)

    second_worker = DatabricksWorker(
        worker_id="second",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC), attempt_ordinal=2)),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=_Runner(),
        max_attempts=2,
    )

    assert asyncio.run(second_worker.run_once())
    attempts = [event[2] for event in lifecycle.events if event[0] == "attempt"]
    completed = [event[2] for event in lifecycle.events if event[0] == "complete"]
    assert [attempt.ordinal for attempt in attempts] == [1, 2]
    assert completed[-1].outcome is ActionOutcome.PARTIAL


def test_rate_limit_retry_respects_longer_downstream_guidance() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC), attempt_ordinal=1)),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=_RateLimitRunner(),
        max_attempts=2,
    )

    assert asyncio.run(worker.run_once())

    attempt = next(event[2] for event in lifecycle.events if event[0] == "attempt")
    assert attempt.error_class is ErrorClass.DOWNSTREAM_RATE_LIMIT
    assert attempt.retry_at - attempt.ended_at == timedelta(seconds=120)
    assert not any(event[0] == "complete" for event in lifecycle.events)


def test_out_of_bounds_retry_after_disables_automatic_retry() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC), attempt_ordinal=1)),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=_RateLimitRunner(retry_after=None, retry_after_out_of_bounds=True),
        max_attempts=2,
    )

    assert asyncio.run(worker.run_once())

    attempt = next(event[2] for event in lifecycle.events if event[0] == "attempt")
    completion = next(event[2] for event in lifecycle.events if event[0] == "complete")
    assert attempt.retry_at is None
    assert completion.outcome is ActionOutcome.FAILED
    assert completion.error_class is ErrorClass.DOWNSTREAM_RATE_LIMIT


def test_retry_after_beyond_action_deadline_disables_automatic_retry() -> None:
    action = replace(
        _action("databricks.uc.catalogs.read"),
        deadline=datetime.now(UTC) + timedelta(seconds=30),
    )
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC), attempt_ordinal=1)),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=_RateLimitRunner(),
        max_attempts=2,
    )

    assert asyncio.run(worker.run_once())

    attempt = next(event[2] for event in lifecycle.events if event[0] == "attempt")
    completion = next(event[2] for event in lifecycle.events if event[0] == "complete")
    assert attempt.retry_at is None
    assert completion.outcome is ActionOutcome.FAILED
    assert completion.error_class is ErrorClass.DOWNSTREAM_RATE_LIMIT


def test_stale_lease_or_guard_failure_never_finalizes_action() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    for lifecycle, guard in ((_StaleLifecycle(), _Guard()), (_Lifecycle(), _FailGuard())):
        worker = DatabricksWorker(
            worker_id="first-worker",
            queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
            lifecycle=lifecycle,
            guard=guard,
            bindings=_Bindings(binding),
            ingestion=_Ingestion(),
            targets=_Targets(),
            runner=_Runner(),
        )
        asyncio.run(worker.run_once())
        assert not any(event[0] == "complete" for event in lifecycle.events)


@pytest.mark.parametrize("disposition", [GuardDisposition.CANCEL, GuardDisposition.SATISFY])
def test_guard_owned_terminal_transitions_are_not_completed_twice(
    disposition: GuardDisposition,
) -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_TerminalGuard(disposition),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=_Runner(),
    )
    asyncio.run(worker.run_once())
    assert not any(event[0] in {"running", "attempt", "complete"} for event in lifecycle.events)


def test_heartbeat_loss_cancels_running_cli_without_finalizing() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    runner = _BlockingRunner()
    lifecycle = _HeartbeatLostLifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=runner,
        heartbeat_seconds=0.001,
    )
    asyncio.run(worker.run_once())
    assert runner.cancelled
    assert not any(event[0] in {"attempt", "complete"} for event in lifecycle.events)


def test_heartbeat_persistence_failure_reaches_runtime_supervision() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    runner = _BlockingRunner()
    lifecycle = _HeartbeatFailedLifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=runner,
        heartbeat_seconds=0.001,
    )

    with pytest.raises(LifecyclePersistenceFailure, match="heartbeat"):
        asyncio.run(worker.run_once())

    assert runner.cancelled
    assert not any(event[0] in {"attempt", "complete"} for event in lifecycle.events)


def test_worker_persists_only_closed_diagnostics() -> None:
    action = _action("databricks.uc.catalogs.read")
    binding = ConnectionBinding(
        action.connection_binding_id,
        action.system_id,
        DATABRICKS_ADAPTER_KEY,
        DATABRICKS_ADAPTER_VERSION,
        True,
        {"profile": "local"},
    )
    lifecycle = _Lifecycle()
    worker = DatabricksWorker(
        worker_id="test",
        queue=_Queue(ActionLease(action, uuid4(), datetime.now(UTC))),
        lifecycle=lifecycle,
        guard=_Guard(),
        bindings=_Bindings(binding),
        ingestion=_Ingestion(),
        targets=_Targets(),
        runner=_FailRunner(),
        max_attempts=1,
    )
    asyncio.run(worker.run_once())
    persisted = str(lifecycle.events)
    assert "worker-secret" not in persisted
    assert "Users" not in persisted
    assert "/home/person" not in persisted
