from pathlib import Path

from async_api_view.cli import main


def test_init_creates_database_from_local_config(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        f"""
[app]
database_path = "{(tmp_path / "state.sqlite3").as_posix()}"

[[databricks]]
id = "test"
name = "test"
profile = "TEST_PROFILE"
workspace_root = "/"
""",
        encoding="utf-8",
    )

    result = main(["--config", str(config), "init"])

    assert result == 0
    assert (tmp_path / "state.sqlite3").is_file()


def test_init_rejects_missing_config(tmp_path: Path) -> None:
    result = main(["--config", str(tmp_path / "missing.toml"), "init"])

    assert result == 2
