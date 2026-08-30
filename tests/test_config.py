from pathlib import Path

import pytest

from async_api_view.config import (
    AppSettings,
    ConfigError,
    DatabricksSystemSettings,
    ProjectSettings,
    load_settings,
)


def write_config(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(content, encoding="utf-8")
    return path


def test_loads_loopback_databricks_configuration(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        """
[app]
database_path = "data/state.sqlite3"
host = "127.0.0.1"
port = 9000

[[databricks]]
id = "WorkSpace"
name = "workspace"
profile = "TEST_PROFILE"
workspace_root = "/Shared"
""",
    )

    settings = load_settings(path)

    assert settings.app.database_path == (tmp_path / "data/state.sqlite3").resolve()
    assert settings.app.host == "127.0.0.1"
    assert settings.databricks_systems[0].config_id == "workspace"
    assert settings.databricks_systems[0].profile == "TEST_PROFILE"
    assert settings.databricks_systems[0].workspace_root == "/Shared"
    assert settings.databricks_systems[0].authority_fingerprint == "0" * 64


@pytest.mark.parametrize("host", ["0.0.0.0", "127.0.0.2", "192.168.1.25", "::1", "example.com"])
def test_rejects_non_loopback_host(tmp_path: Path, host: str) -> None:
    path = write_config(tmp_path, f'[app]\nhost = "{host}"\n')

    with pytest.raises(ConfigError, match="loopback"):
        load_settings(path)


@pytest.mark.parametrize("field", ["worker_poll_seconds", "cli_timeout_seconds"])
@pytest.mark.parametrize("value", ["nan", "+nan", "-nan", "inf", "+inf", "-inf"])
def test_rejects_non_finite_timing_settings(tmp_path: Path, field: str, value: str) -> None:
    path = write_config(tmp_path, f"[app]\n{field} = {value}\n")

    with pytest.raises(ConfigError, match="at most"):
        load_settings(path)


@pytest.mark.parametrize("value", ["5e-324", "0.049999"])
def test_rejects_worker_poll_interval_below_practical_floor(tmp_path: Path, value: str) -> None:
    path = write_config(tmp_path, f"[app]\nworker_poll_seconds = {value}\n")

    with pytest.raises(ConfigError, match=r"at least 0\.05"):
        load_settings(path)


def test_programmatic_settings_enforce_poll_and_profile_boundaries(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"at least 0\.05"):
        AppSettings(database_path=tmp_path / "state.sqlite3", worker_poll_seconds=1e-12)
    with pytest.raises(ConfigError, match="must start with a letter or digit"):
        DatabricksSystemSettings("workspace", "-bad", "/")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"host": "0.0.0.0"}, "loopback"),
        ({"port": 0}, "app.port"),
        ({"cli_timeout_seconds": float("nan")}, "app.cli_timeout_seconds"),
        ({"cli_output_limit_bytes": 0}, "app.cli_output_limit_bytes"),
    ],
)
def test_programmatic_app_settings_share_toml_safety_contract(
    tmp_path: Path,
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ConfigError, match=message):
        AppSettings(database_path=tmp_path / "state.sqlite3", **overrides)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"name": ""}, "name"),
        ({"workspace_root": "relative"}, "absolute Workspace path"),
        ({"config_id": "bad;id"}, "letters"),
        ({"authority_fingerprint": "not-a-digest"}, "SHA-256"),
    ],
)
def test_programmatic_databricks_settings_share_toml_safety_contract(
    overrides: dict[str, object],
    message: str,
) -> None:
    values: dict[str, object] = {
        "name": "workspace",
        "profile": "TEST_PROFILE",
        "workspace_root": "/Shared",
        "config_id": "workspace",
    }
    values.update(overrides)

    with pytest.raises(ConfigError, match=message):
        DatabricksSystemSettings(**values)  # type: ignore[arg-type]


def test_programmatic_databricks_settings_normalize_identity_and_root() -> None:
    settings = DatabricksSystemSettings(
        name="workspace",
        profile="TEST_PROFILE",
        workspace_root="/Shared/",
        config_id="WorkSpace",
        authority_fingerprint="A" * 64,
    )

    assert settings.workspace_root == "/Shared"
    assert settings.config_id == "workspace"
    assert settings.authority_fingerprint == "a" * 64


def test_programmatic_project_settings_reject_duplicate_names_and_ids(tmp_path: Path) -> None:
    app = AppSettings(database_path=tmp_path / "state.sqlite3")
    first = DatabricksSystemSettings("workspace", "ONE", "/One", "primary")

    with pytest.raises(ConfigError, match="names must be unique"):
        ProjectSettings(
            app,
            (first, DatabricksSystemSettings("WORKSPACE", "TWO", "/Two", "secondary")),
        )
    with pytest.raises(ConfigError, match="IDs must be unique"):
        ProjectSettings(
            app,
            (first, DatabricksSystemSettings("secondary", "TWO", "/Two", "PRIMARY")),
        )
    with pytest.raises(ConfigError, match="authorities must be unique"):
        ProjectSettings(
            app,
            (
                first,
                DatabricksSystemSettings("secondary", "TWO", "/One", "secondary"),
            ),
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("profile", "TEST_PROFILE;whoami", "letters"),
        ("workspace_root", "../Shared", "absolute Workspace path"),
    ],
)
def test_rejects_command_shaped_databricks_settings(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    path = write_config(
        tmp_path,
        f"""
[[databricks]]
id = "workspace"
name = "workspace"
profile = "{value if field == "profile" else "TEST_PROFILE"}"
workspace_root = "{value if field == "workspace_root" else "/"}"
""",
    )

    with pytest.raises(ConfigError, match=message):
        load_settings(path)


@pytest.mark.parametrize("profile", ["-bad", ".bad", "_bad"])
def test_rejects_profile_that_bootstrap_cannot_use(tmp_path: Path, profile: str) -> None:
    path = write_config(
        tmp_path,
        f"""
[[databricks]]
name = "workspace"
profile = "{profile}"
workspace_root = "/"
""",
    )

    with pytest.raises(ConfigError, match="must start with a letter or digit"):
        load_settings(path)


def test_rejects_unknown_fields_that_could_hide_credentials(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        """
[[databricks]]
id = "workspace"
name = "workspace"
profile = "TEST_PROFILE"
workspace_root = "/"
token = "not-accepted"
""",
    )

    with pytest.raises(ConfigError, match="unknown"):
        load_settings(path)


def test_allows_legacy_databricks_configuration_without_id(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        """
[[databricks]]
name = "workspace"
profile = "TEST_PROFILE"
workspace_root = "/"
""",
    )

    settings = load_settings(path)

    assert settings.databricks_systems[0].config_id is None


def test_rejects_duplicate_databricks_system_ids(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        """
[[databricks]]
id = "workspace"
name = "one"
profile = "ONE"
workspace_root = "/One"

[[databricks]]
id = "WORKSPACE"
name = "two"
profile = "TWO"
workspace_root = "/Two"
""",
    )

    with pytest.raises(ConfigError, match="system IDs must be unique"):
        load_settings(path)
