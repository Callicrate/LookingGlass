"""Tests for the shared hardened subprocess primitives."""

from __future__ import annotations

import asyncio
import os
import sys

import pytest

from lookingglass.adapters._process import (
    ProcessOutputLimit,
    ProcessTree,
    absolute_path_entries,
    read_limited,
    terminate_process,
)


def test_read_limited_returns_empty_for_missing_stream() -> None:
    assert asyncio.run(read_limited(None, 16, message="unused")) == b""


def test_read_limited_accepts_output_within_cap() -> None:
    async def scenario() -> bytes:
        reader = asyncio.StreamReader()
        reader.feed_data(b"hello")
        reader.feed_eof()
        return await read_limited(reader, 16, message="too big")

    assert asyncio.run(scenario()) == b"hello"


def test_read_limited_raises_default_error_type_on_overflow() -> None:
    async def scenario() -> bytes:
        reader = asyncio.StreamReader()
        reader.feed_data(b"x" * 32)
        reader.feed_eof()
        return await read_limited(reader, 8, message="over the cap")

    with pytest.raises(ProcessOutputLimit, match="over the cap"):
        asyncio.run(scenario())


def test_read_limited_raises_caller_supplied_error_type() -> None:
    class AdapterLimit(RuntimeError):
        pass

    async def scenario() -> bytes:
        reader = asyncio.StreamReader()
        reader.feed_data(b"x" * 32)
        reader.feed_eof()
        return await read_limited(reader, 8, message="capped", error_type=AdapterLimit)

    with pytest.raises(AdapterLimit, match="capped"):
        asyncio.run(scenario())


def test_absolute_path_entries_drops_relative_and_empty_segments() -> None:
    if os.name == "nt":
        raw = os.pathsep.join(["C:\\tools", "relative", "", '"C:\\quoted"'])
        assert absolute_path_entries(raw) == ("C:\\tools", "C:\\quoted")
    else:
        raw = os.pathsep.join(["/usr/bin", "relative", "", '"/opt/bin"'])
        assert absolute_path_entries(raw) == ("/usr/bin", "/opt/bin")


def test_process_tree_owns_and_kills_a_real_child() -> None:
    async def scenario() -> int | None:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import time; time.sleep(30)",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **({} if os.name == "nt" else {"start_new_session": True}),
            **({"creationflags": 0x00000004} if os.name == "nt" else {}),
        )
        tree = ProcessTree(process)
        await terminate_process(process, tree)
        return process.returncode

    returncode = asyncio.run(scenario())
    assert returncode is not None
