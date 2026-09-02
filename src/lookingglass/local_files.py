"""Current-user filesystem boundaries for local LookingGlass state."""

from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path


def _effective_user_id() -> int:
    geteuid = getattr(os, "geteuid", None)
    if geteuid is None:
        raise OSError("POSIX user identity is unavailable")
    return int(geteuid())


def _set_descriptor_mode(descriptor: int, mode: int) -> None:
    fchmod = getattr(os, "fchmod", None)
    if fchmod is None:
        raise OSError("POSIX descriptor hardening is unavailable")
    fchmod(descriptor, mode)


def absolute_local_path(path: str | Path) -> Path:
    """Return an absolute path without following filesystem redirects."""

    return Path(os.path.abspath(os.fspath(path)))


def available_bytes(path: str | Path) -> int:
    """Return quota-aware bytes available to the current caller for an existing path."""

    return int(shutil.disk_usage(absolute_local_path(path)).free)


def _is_redirect(path: Path) -> bool:
    return path.is_symlink() or path.is_junction()


def _assert_no_redirects(path: Path) -> None:
    for candidate in (path, *path.parents):
        if os.path.lexists(candidate) and _is_redirect(candidate):
            raise OSError("LookingGlass private paths cannot contain a filesystem redirect")


def regular_file_identity(
    path: str | Path,
    *,
    expected_links: int | None = 1,
) -> tuple[int, int]:
    """Return one non-redirected, single-link regular-file identity."""

    requested = absolute_local_path(path)
    _assert_no_redirects(requested)
    details = requested.lstat()
    if not stat.S_ISREG(details.st_mode):
        raise OSError("LookingGlass state must be a regular file")
    if expected_links is not None and details.st_nlink != expected_links:
        raise OSError("LookingGlass state file hard links have an unexpected count")
    return details.st_dev, details.st_ino


class RegularFileGuard:
    """Hold one regular file identity and block Windows delete/rename replacement."""

    def __init__(self, path: str | Path) -> None:
        self.path = absolute_local_path(path)
        _assert_no_redirects(self.path)
        self._descriptor: int | None = None
        self._windows_handle: int | None = None
        if os.name == "nt":
            self._windows_handle = self._open_windows_handle()
        else:
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            self._descriptor = os.open(self.path, flags)
        try:
            self.identity = self._handle_identity()
            self.verify()
        except BaseException:
            self.close()
            raise

    def _open_windows_handle(self) -> int:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateFileW.argtypes = (
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        )
        kernel32.CreateFileW.restype = wintypes.HANDLE
        handle = kernel32.CreateFileW(
            str(self.path),
            0xC0000000,
            0x00000001 | 0x00000002,
            None,
            3,
            0x00000080,
            None,
        )
        if handle == ctypes.c_void_p(-1).value:
            code = ctypes.get_last_error()
            raise OSError(
                code, f"could not guard the LookingGlass state file: {ctypes.FormatError(code)}"
            )
        return int(handle)

    def _handle_identity(self) -> tuple[int, int]:
        if self._descriptor is not None:
            details = os.fstat(self._descriptor)
            if not stat.S_ISREG(details.st_mode):
                raise OSError("LookingGlass state must be a regular file")
            if details.st_nlink != 1:
                raise OSError("LookingGlass state files must not have multiple hard links")
            if details.st_uid != _effective_user_id():
                raise OSError("LookingGlass state file owner does not match the current user")
            return details.st_dev, details.st_ino
        return regular_file_identity(self.path)

    def verify(self, *, expected_links: int = 1) -> None:
        """Fail if the configured path no longer names the guarded file."""

        if regular_file_identity(self.path, expected_links=expected_links) != self.identity:
            raise OSError("LookingGlass state file identity changed while guarded")

    def harden(self) -> None:
        """Apply current-user protection to the guarded object, then reverify its path."""

        if self._descriptor is not None:
            _set_descriptor_mode(self._descriptor, 0o600)
            details = os.fstat(self._descriptor)
            if stat.S_IMODE(details.st_mode) != 0o600:
                raise OSError("LookingGlass state file permissions could not be restricted")
        else:
            _harden_windows_path_acl(self.path, inherit_to_children=False)
        self.verify()

    def sync(self) -> None:
        """Synchronize the guarded file's data and metadata before success."""

        if self._descriptor is not None:
            os.fsync(self._descriptor)
            return
        if self._windows_handle is None:  # pragma: no cover - closed guard misuse
            raise OSError("LookingGlass state file guard is closed")
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.FlushFileBuffers.argtypes = (wintypes.HANDLE,)
        kernel32.FlushFileBuffers.restype = wintypes.BOOL
        if not kernel32.FlushFileBuffers(self._windows_handle):
            code = ctypes.get_last_error()
            raise OSError(
                code, f"could not synchronize LookingGlass state: {ctypes.FormatError(code)}"
            )

    def close(self) -> None:
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        if self._windows_handle is not None:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
            kernel32.CloseHandle.restype = wintypes.BOOL
            kernel32.CloseHandle(self._windows_handle)
            self._windows_handle = None

    def __enter__(self) -> RegularFileGuard:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


class PrivateDirectoryGuard:
    """Hold a state directory and block Windows delete/rename replacement."""

    def __init__(self, path: str | Path) -> None:
        self.path = absolute_local_path(path)
        _assert_no_redirects(self.path)
        self._descriptor: int | None = None
        self._windows_handle: int | None = None
        if os.name == "nt":
            self._windows_handle = self._open_windows_handle()
        else:
            flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_DIRECTORY", 0)
            )
            self._descriptor = os.open(self.path, flags)
        try:
            self.identity = self._handle_identity()
            self.verify()
        except BaseException:
            self.close()
            raise

    def _open_windows_handle(self) -> int:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateFileW.argtypes = (
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        )
        kernel32.CreateFileW.restype = wintypes.HANDLE
        handle = kernel32.CreateFileW(
            str(self.path),
            0xC0000000,
            0x00000001 | 0x00000002,
            None,
            3,
            0x02000000,
            None,
        )
        if handle == ctypes.c_void_p(-1).value:
            code = ctypes.get_last_error()
            raise OSError(
                code,
                f"could not guard the LookingGlass state directory: {ctypes.FormatError(code)}",
            )
        return int(handle)

    def _handle_identity(self) -> tuple[int, int]:
        details = os.fstat(self._descriptor) if self._descriptor is not None else self.path.lstat()
        if not stat.S_ISDIR(details.st_mode):
            raise OSError("LookingGlass state directory must be a directory")
        if os.name != "nt" and details.st_uid != os.geteuid():
            raise OSError("LookingGlass state directory owner does not match the current user")
        return details.st_dev, details.st_ino

    def verify(self) -> None:
        _assert_no_redirects(self.path)
        details = self.path.lstat()
        if not stat.S_ISDIR(details.st_mode) or (details.st_dev, details.st_ino) != self.identity:
            raise OSError("LookingGlass state directory identity changed while guarded")

    def sync(self) -> None:
        """Synchronize publication metadata for the guarded directory."""

        if self._descriptor is not None:
            os.fsync(self._descriptor)
            return
        if self._windows_handle is None:  # pragma: no cover - closed guard misuse
            raise OSError("LookingGlass state directory guard is closed")
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.FlushFileBuffers.argtypes = (wintypes.HANDLE,)
        kernel32.FlushFileBuffers.restype = wintypes.BOOL
        if not kernel32.FlushFileBuffers(self._windows_handle):
            code = ctypes.get_last_error()
            raise OSError(
                code,
                f"could not synchronize LookingGlass directory metadata: "
                f"{ctypes.FormatError(code)}",
            )

    def close(self) -> None:
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        if self._windows_handle is not None:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
            kernel32.CloseHandle.restype = wintypes.BOOL
            kernel32.CloseHandle(self._windows_handle)
            self._windows_handle = None

    def __enter__(self) -> PrivateDirectoryGuard:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


class ExclusiveLockUnavailable(OSError):
    """Raised when a valid private lock is already held by another process."""


class ExclusiveFileLock:
    """Hold one private, process-scoped nonblocking filesystem lock."""

    def __init__(self, path: str | Path) -> None:
        self.path = absolute_local_path(path)
        self._descriptor: int | None = None
        self._guard: RegularFileGuard | None = None
        self._locked = False
        prepare_private_directory(self.path.parent)
        _assert_no_redirects(self.path)
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_BINARY", 0)
        descriptor = os.open(self.path, flags, 0o600)
        self._descriptor = descriptor
        try:
            harden_private_file(self.path)
            self._guard = RegularFileGuard(self.path)
            details = os.fstat(descriptor)
            if (details.st_dev, details.st_ino) != self._guard.identity:
                raise OSError("LookingGlass lock file identity changed while opening")
            if details.st_size == 0:
                os.write(descriptor, b"\0")
                os.fsync(descriptor)
            os.lseek(descriptor, 0, os.SEEK_SET)
            self._acquire()
            self._guard.verify()
        except BaseException:
            self.close()
            raise

    def _acquire(self) -> None:
        if self._descriptor is None:  # pragma: no cover - construction invariant
            raise OSError("LookingGlass lock file is closed")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._descriptor, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ExclusiveLockUnavailable("another LookingGlass process owns this lock") from exc
        self._locked = True

    def close(self) -> None:
        descriptor = self._descriptor
        if descriptor is not None:
            if self._locked:
                try:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    self._locked = False
            os.close(descriptor)
            self._descriptor = None
        if self._guard is not None:
            self._guard.close()
            self._guard = None

    def __enter__(self) -> ExclusiveFileLock:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


def _harden_windows_path_acl(path: Path, *, inherit_to_children: bool) -> None:
    """Replace a Windows DACL with one current-user full-control grant."""

    if os.name != "nt":
        return

    import ctypes
    from ctypes import wintypes

    class SidAndAttributes(ctypes.Structure):
        _fields_ = (("sid", wintypes.LPVOID), ("attributes", wintypes.DWORD))

    class TokenUser(ctypes.Structure):
        _fields_ = (("user", SidAndAttributes),)

    class AclSizeInformation(ctypes.Structure):
        _fields_ = (
            ("ace_count", wintypes.DWORD),
            ("bytes_in_use", wintypes.DWORD),
            ("bytes_free", wintypes.DWORD),
        )

    token = wintypes.HANDLE()
    required = wintypes.DWORD()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    advapi32.OpenProcessToken.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    )
    advapi32.OpenProcessToken.restype = wintypes.BOOL
    advapi32.GetTokenInformation.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    )
    advapi32.GetTokenInformation.restype = wintypes.BOOL
    advapi32.GetLengthSid.argtypes = (wintypes.LPVOID,)
    advapi32.GetLengthSid.restype = wintypes.DWORD
    advapi32.InitializeAcl.argtypes = (wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD)
    advapi32.InitializeAcl.restype = wintypes.BOOL
    advapi32.AddAccessAllowedAceEx.argtypes = (
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
    )
    advapi32.AddAccessAllowedAceEx.restype = wintypes.BOOL
    advapi32.SetNamedSecurityInfoW.argtypes = (
        wintypes.LPWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.LPVOID,
        wintypes.LPVOID,
        wintypes.LPVOID,
    )
    advapi32.SetNamedSecurityInfoW.restype = wintypes.DWORD
    advapi32.GetNamedSecurityInfoW.argtypes = (
        wintypes.LPWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.LPVOID),
        ctypes.POINTER(wintypes.LPVOID),
        ctypes.POINTER(wintypes.LPVOID),
        ctypes.POINTER(wintypes.LPVOID),
        ctypes.POINTER(wintypes.LPVOID),
    )
    advapi32.GetNamedSecurityInfoW.restype = wintypes.DWORD
    advapi32.GetSecurityDescriptorControl.argtypes = (
        wintypes.LPVOID,
        ctypes.POINTER(ctypes.c_ushort),
        ctypes.POINTER(wintypes.DWORD),
    )
    advapi32.GetSecurityDescriptorControl.restype = wintypes.BOOL
    advapi32.GetAclInformation.argtypes = (
        wintypes.LPVOID,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.c_int,
    )
    advapi32.GetAclInformation.restype = wintypes.BOOL
    advapi32.GetAce.argtypes = (
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.LPVOID),
    )
    advapi32.GetAce.restype = wintypes.BOOL
    advapi32.EqualSid.argtypes = (wintypes.LPVOID, wintypes.LPVOID)
    advapi32.EqualSid.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = (wintypes.HLOCAL,)
    kernel32.LocalFree.restype = wintypes.HLOCAL

    def last_error(message: str) -> OSError:
        code = ctypes.get_last_error()
        return OSError(code, f"{message}: {ctypes.FormatError(code)}")

    if not advapi32.OpenProcessToken(kernel32.GetCurrentProcess(), 0x0008, ctypes.byref(token)):
        raise last_error("could not open the current process token")
    try:
        advapi32.GetTokenInformation(token, 1, None, 0, ctypes.byref(required))
        if required.value == 0:
            raise last_error("could not size the current user token")
        token_buffer = ctypes.create_string_buffer(required.value)
        if not advapi32.GetTokenInformation(
            token, 1, token_buffer, required.value, ctypes.byref(required)
        ):
            raise last_error("could not read the current user token")
        user_sid = ctypes.cast(token_buffer, ctypes.POINTER(TokenUser)).contents.user.sid
        sid_length = advapi32.GetLengthSid(user_sid)
        if sid_length == 0:
            raise last_error("could not size the current user SID")

        acl_size = 16 + sid_length
        acl_buffer = ctypes.create_string_buffer(acl_size)
        acl = ctypes.cast(acl_buffer, wintypes.LPVOID)
        if not advapi32.InitializeAcl(acl, acl_size, 2):
            raise last_error("could not initialize the LookingGlass path ACL")
        inheritance_flags = 0x3 if inherit_to_children else 0
        if not advapi32.AddAccessAllowedAceEx(
            acl,
            2,
            inheritance_flags,
            0x001F01FF,
            user_sid,
        ):
            raise last_error("could not grant the current user access to the LookingGlass path")
        result = advapi32.SetNamedSecurityInfoW(
            str(path),
            1,
            0x80000005,
            user_sid,
            None,
            acl,
            None,
        )
        if result != 0:
            raise OSError(
                result,
                f"could not protect the LookingGlass path: {ctypes.FormatError(result)}",
            )

        owner_sid = wintypes.LPVOID()
        verified_acl = wintypes.LPVOID()
        security_descriptor = wintypes.LPVOID()
        result = advapi32.GetNamedSecurityInfoW(
            str(path),
            1,
            0x00000005,
            ctypes.byref(owner_sid),
            None,
            ctypes.byref(verified_acl),
            None,
            ctypes.byref(security_descriptor),
        )
        if result != 0:
            raise OSError(
                result,
                f"could not verify the LookingGlass path owner: {ctypes.FormatError(result)}",
            )
        try:
            if not advapi32.EqualSid(owner_sid, user_sid):
                raise OSError("LookingGlass path owner does not match the current user")
            control = ctypes.c_ushort()
            revision = wintypes.DWORD()
            if not advapi32.GetSecurityDescriptorControl(
                security_descriptor,
                ctypes.byref(control),
                ctypes.byref(revision),
            ) or not (control.value & 0x1000):
                raise OSError("LookingGlass path DACL is not protected")
            acl_information = AclSizeInformation()
            if not verified_acl or not advapi32.GetAclInformation(
                verified_acl,
                ctypes.byref(acl_information),
                ctypes.sizeof(acl_information),
                2,
            ):
                raise last_error("could not inspect the LookingGlass path DACL")
            if acl_information.ace_count != 1:
                raise OSError("LookingGlass path DACL must contain one access rule")
            ace = wintypes.LPVOID()
            if not advapi32.GetAce(verified_acl, 0, ctypes.byref(ace)):
                raise last_error("could not inspect the LookingGlass path access rule")
            if ace.value is None:
                raise OSError("LookingGlass path access rule pointer is unavailable")
            ace_address = int(ace.value)
            header = (ctypes.c_ubyte * 2).from_address(ace_address)
            mask = ctypes.c_uint32.from_address(ace_address + 4).value
            ace_sid = wintypes.LPVOID(ace_address + 8)
            if (
                header[0] != 0
                or header[1] != inheritance_flags
                or mask != 0x001F01FF
                or not advapi32.EqualSid(ace_sid, user_sid)
            ):
                raise OSError("LookingGlass path DACL does not match the current-user contract")
        finally:
            kernel32.LocalFree(security_descriptor)
    finally:
        kernel32.CloseHandle(token)


def _harden_posix_path(path: Path, mode: int) -> None:
    details = path.stat(follow_symlinks=False)
    if details.st_uid != _effective_user_id():
        raise OSError("LookingGlass path owner does not match the current user")
    os.chmod(path, mode, follow_symlinks=False)
    if stat.S_IMODE(path.stat(follow_symlinks=False).st_mode) != mode:
        raise OSError("LookingGlass path permissions could not be restricted")


def prepare_private_directory(path: str | Path) -> Path:
    """Create or harden one dedicated current-user directory without redirects."""

    requested = absolute_local_path(path)
    _assert_no_redirects(requested)
    missing: list[Path] = []
    candidate = requested
    while not os.path.lexists(candidate):
        missing.append(candidate)
        parent = candidate.parent
        if parent == candidate:  # pragma: no cover - absolute root always exists
            raise OSError("LookingGlass private directory has no existing ancestor")
        candidate = parent
    if not candidate.is_dir():
        raise OSError("LookingGlass private directory ancestor is not a directory")
    for directory in reversed(missing):
        directory.mkdir(mode=0o700)
        if os.name == "nt":
            _harden_windows_path_acl(directory, inherit_to_children=True)
        else:
            _harden_posix_path(directory, 0o700)
    if not requested.is_dir():
        raise OSError("LookingGlass private path is not a directory")
    if not missing:
        if os.name == "nt":
            _harden_windows_path_acl(requested, inherit_to_children=True)
        else:
            _harden_posix_path(requested, 0o700)
    _assert_no_redirects(requested)
    if requested.resolve(strict=True) != requested:
        raise OSError("LookingGlass private directory changed during preparation")
    return requested


def harden_private_file(path: str | Path) -> Path:
    """Restrict one regular, single-link file to the current user."""

    requested = absolute_local_path(path)
    with RegularFileGuard(requested) as guard:
        guard.harden()
    return requested
