from pathlib import Path

import pytest

from async_api_view.config import ConfigError, load_settings


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
id = "workspace"
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


@pytest.mark.parametrize("host", ["0.0.0.0", "127.0.0.2", "192.168.1.25", "::1", "example.com"])
def test_rejects_non_loopback_host(tmp_path: Path, host: str) -> None:
    path = write_config(tmp_path, f'[app]\nhost = "{host}"\n')

    with pytest.raises(ConfigError, match="loopback"):
        load_settings(path)


@pytest.mark.parametrize("field", ["worker_poll_seconds", "cli_timeout_seconds"])
@pytest.mark.parametrize("value", ["nan", "+nan", "-nan", "inf", "+inf", "-inf"])
def test_rejects_non_finite_timing_settings(tmp_path: Path, field: str, value: str) -> None:
    path = write_config(tmp_path, f"[app]\n{field} = {value}\n")

    with pytest.raises(ConfigError, match="greater than 0"):
        load_settings(path)


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
