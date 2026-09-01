"""Hardened async subprocess primitives shared by CLI-backed adapters.

This module owns the low-level, OS-specific process machinery used to run an
external command safely: bounded output reading, whole-tree ownership and
teardown (Windows Job Object / POSIX process group), and PATH sanitization. It
knows nothing about any particular adapter, capability, or canonical storage —
adapters wrap these primitives with their own error types and command mapping.
"""

from __future__ import annotations

import asyncio
import os
import signal
from contextlib import suppress
from pathlib import Path
from typing import Any


class ProcessOutputLimit(RuntimeError):
    """Raised when a subprocess writes more than its configured output cap."""


async def read_limited(
    stream: asyncio.StreamReader | None,
    cap: int,
    *,
    message: str,
    error_type: type[Exception] = ProcessOutputLimit,
) -> bytes:
    """Read a stream to completion, refusing to buffer more than ``cap`` bytes."""

    if stream is None:
        return b""
    chunks = bytearray()
    while block := await stream.read(min(65536, cap + 1)):
        remaining = cap - len(chunks)
        if len(block) > remaining:
            raise error_type(message)
        chunks.extend(block)
    return bytes(chunks)


class ProcessTree:
    """Own one child process tree until all output readers and descendants are done."""

    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process
        self._windows_job: int | None = None
        if os.name == "nt" and isinstance(process, asyncio.subprocess.Process):
            self._windows_job = self._assign_windows_job(process)
            try:
                self._resume_windows_process(process.pid)
            except BaseException:
                self._close_windows_job()
                raise

    @staticmethod
    def _assign_windows_job(process: asyncio.subprocess.Process) -> int:
        import ctypes
        from ctypes import wintypes

        class IoCounters(ctypes.Structure):
            _fields_ = (
                ("read_operations", ctypes.c_ulonglong),
                ("write_operations", ctypes.c_ulonglong),
                ("other_operations", ctypes.c_ulonglong),
                ("read_bytes", ctypes.c_ulonglong),
                ("write_bytes", ctypes.c_ulonglong),
                ("other_bytes", ctypes.c_ulonglong),
            )

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = (
                ("per_process_user_time", ctypes.c_longlong),
                ("per_job_user_time", ctypes.c_longlong),
                ("limit_flags", wintypes.DWORD),
                ("minimum_working_set", ctypes.c_size_t),
                ("maximum_working_set", ctypes.c_size_t),
                ("active_process_limit", wintypes.DWORD),
                ("affinity", ctypes.c_size_t),
                ("priority_class", wintypes.DWORD),
                ("scheduling_class", wintypes.DWORD),
            )

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = (
                ("basic_limit_information", BasicLimitInformation),
                ("io_info", IoCounters),
                ("process_memory_limit", ctypes.c_size_t),
                ("job_memory_limit", ctypes.c_size_t),
                ("peak_process_memory_used", ctypes.c_size_t),
                ("peak_job_memory_used", ctypes.c_size_t),
            )

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = (wintypes.LPVOID, wintypes.LPCWSTR)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
        )
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            code = ctypes.get_last_error()
            raise OSError(code, f"could not create process job: {ctypes.FormatError(code)}")
        try:
            limits = ExtendedLimitInformation()
            limits.basic_limit_information.limit_flags = 0x00002000
            if not kernel32.SetInformationJobObject(
                job,
                9,
                ctypes.byref(limits),
                ctypes.sizeof(limits),
            ):
                code = ctypes.get_last_error()
                raise OSError(
                    code,
                    f"could not configure process job: {ctypes.FormatError(code)}",
                )
            process_handle = kernel32.OpenProcess(0x00000101, False, process.pid)
            if not process_handle:
                code = ctypes.get_last_error()
                raise OSError(code, f"could not open process: {ctypes.FormatError(code)}")
            try:
                if not kernel32.AssignProcessToJobObject(job, process_handle):
                    code = ctypes.get_last_error()
                    raise OSError(
                        code,
                        f"could not own process tree: {ctypes.FormatError(code)}",
                    )
            finally:
                kernel32.CloseHandle(process_handle)
        except BaseException:
            kernel32.CloseHandle(job)
            raise
        return int(job)

    @staticmethod
    def _resume_windows_process(process_id: int) -> None:
        import ctypes
        from ctypes import wintypes

        class ThreadEntry32(ctypes.Structure):
            _fields_ = (
                ("size", wintypes.DWORD),
                ("usage_count", wintypes.DWORD),
                ("thread_id", wintypes.DWORD),
                ("owner_process_id", wintypes.DWORD),
                ("base_priority", wintypes.LONG),
                ("priority_delta", wintypes.LONG),
                ("flags", wintypes.DWORD),
            )

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
        kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        kernel32.Thread32First.argtypes = (wintypes.HANDLE, ctypes.POINTER(ThreadEntry32))
        kernel32.Thread32First.restype = wintypes.BOOL
        kernel32.Thread32Next.argtypes = (wintypes.HANDLE, ctypes.POINTER(ThreadEntry32))
        kernel32.Thread32Next.restype = wintypes.BOOL
        kernel32.OpenThread.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenThread.restype = wintypes.HANDLE
        kernel32.ResumeThread.argtypes = (wintypes.HANDLE,)
        kernel32.ResumeThread.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        snapshot = kernel32.CreateToolhelp32Snapshot(0x00000004, 0)
        if snapshot == ctypes.c_void_p(-1).value:
            code = ctypes.get_last_error()
            raise OSError(code, f"could not enumerate process threads: {ctypes.FormatError(code)}")
        resumed = 0
        try:
            entry = ThreadEntry32()
            entry.size = ctypes.sizeof(entry)
            available = kernel32.Thread32First(snapshot, ctypes.byref(entry))
            while available:
                if entry.owner_process_id == process_id:
                    thread = kernel32.OpenThread(0x0002, False, entry.thread_id)
                    if not thread:
                        code = ctypes.get_last_error()
                        raise OSError(
                            code,
                            f"could not open suspended process thread: {ctypes.FormatError(code)}",
                        )
                    try:
                        if kernel32.ResumeThread(thread) == 0xFFFFFFFF:
                            code = ctypes.get_last_error()
                            raise OSError(
                                code,
                                f"could not resume process: {ctypes.FormatError(code)}",
                            )
                        resumed += 1
                    finally:
                        kernel32.CloseHandle(thread)
                available = kernel32.Thread32Next(snapshot, ctypes.byref(entry))
        finally:
            kernel32.CloseHandle(snapshot)
        if resumed == 0:
            raise OSError("suspended process had no resumable thread")

    def kill(self) -> None:
        if self._windows_job is not None:
            self._terminate_windows_job()
            return
        if os.name != "nt" and isinstance(self.process, asyncio.subprocess.Process):
            with suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGKILL)
            return
        if self.process.returncode is None:
            self.process.kill()

    def _close_windows_job(self) -> None:
        handle = self._windows_job
        if handle is None:
            return
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        self._windows_job = None
        if not kernel32.CloseHandle(handle):
            code = ctypes.get_last_error()
            raise OSError(code, f"could not close process job: {ctypes.FormatError(code)}")

    def _terminate_windows_job(self) -> None:
        handle = self._windows_job
        if handle is None:
            return
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.TerminateJobObject.argtypes = (wintypes.HANDLE, wintypes.UINT)
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        try:
            if not kernel32.TerminateJobObject(handle, 1):
                code = ctypes.get_last_error()
                raise OSError(
                    code,
                    f"could not terminate process job: {ctypes.FormatError(code)}",
                )
            if kernel32.WaitForSingleObject(handle, 5000) != 0:
                raise OSError("process job did not terminate within five seconds")
        finally:
            self._close_windows_job()

    def close(self) -> None:
        self.kill()


async def terminate_process(
    process: asyncio.subprocess.Process,
    process_tree: ProcessTree,
    *tasks: asyncio.Task[Any],
) -> None:
    """Kill the whole process tree and drain its output-reader tasks."""

    process_tree.kill()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        if process.returncode is None:
            process.kill()
    for task in tasks:
        if not task.done():
            task.cancel()
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=5)
    except TimeoutError:
        for task in tasks:
            task.cancel()


def absolute_path_entries(value: str) -> tuple[str, ...]:
    """Return explicit absolute PATH entries without an implicit current directory."""

    entries: list[str] = []
    for raw_entry in value.split(os.pathsep):
        entry = (
            raw_entry[1:-1] if raw_entry.startswith('"') and raw_entry.endswith('"') else raw_entry
        )
        entry = os.path.expandvars(entry)
        if entry and Path(entry).is_absolute():
            entries.append(entry)
    return tuple(entries)
