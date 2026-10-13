"""Point-in-time writer probe for offline maintenance; unknown is never stopped.

The operator must keep the supervisor stopped for the entire maintenance operation.
An idle daemon can admit writes without holding any descriptors under its state root.
This module is also embedded by store-maintenance, so Python and shell callers share
the same enumeration, process-context and canonical-path rules.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
from dataclasses import dataclass
from typing import Callable

if __package__ in (None, "") and "lib" not in sys.modules:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from lib import state_root


@dataclass(frozen=True)
class Liveness:
    state: str
    reason: str

    def refusal(self, caller: str) -> str | None:
        if self.state == "no":
            return None
        return f"{caller}: liveness {self.state}; refusing offline mutation: {self.reason}. Stop the daemon first."


def enumerate_daemons(platform: str, run: Callable | None = None) -> list[int]:
    run = run or subprocess.run
    if platform == "win32":
        result = run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
             "$ErrorActionPreference='Stop'; $ids=@(Get-CimInstance Win32_Process | "
             "Where-Object {$_.Name -ieq 'ck-broca.exe'} | ForEach-Object {[int]$_.ProcessId}); "
             "ConvertTo-Json -InputObject $ids -Compress"],
            capture_output=True, text=True, check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"process enumeration failed (exit {result.returncode})")
        pids = json.loads(result.stdout)
        if not isinstance(pids, list) or any(type(pid) is not int or pid <= 0 for pid in pids):
            raise RuntimeError("invalid process enumeration")
        return pids
    result = run(["pgrep", "-x", "ck-broca"], capture_output=True, text=True, check=False)
    if result.returncode == 1 and not result.stdout.strip():
        return []
    if result.returncode != 0:
        raise RuntimeError(f"process enumeration failed (exit {result.returncode})")
    pids = result.stdout.split()
    if not pids or any(not pid.isdecimal() or int(pid) <= 0 for pid in pids):
        raise RuntimeError("invalid process enumeration")
    return [int(pid) for pid in pids]


def windows_context(pid: int) -> tuple[dict[str, str], pathlib.Path]:
    """Read the process's PEB, not the maintenance tool's environment or cwd.

    Inaccessible and cross-bitness processes fail closed. PEB layout/read failures
    are not evidence that the process cannot write the target root.
    """
    import ctypes
    from ctypes import wintypes

    windows_dll = getattr(ctypes, "WinDLL", None)
    if windows_dll is None:
        raise OSError("Windows process inspection is unavailable")
    kernel = windows_dll("kernel32", use_last_error=True)
    ntdll = windows_dll("ntdll")
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.ReadProcessMemory.argtypes = [wintypes.HANDLE, ctypes.c_void_p, ctypes.c_void_p,
                                       ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)]
    kernel.IsWow64Process.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.BOOL)]
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    ntdll.NtQueryInformationProcess.argtypes = [wintypes.HANDLE, wintypes.ULONG,
                                               ctypes.c_void_p, wintypes.ULONG, ctypes.c_void_p]
    ntdll.NtQueryInformationProcess.restype = wintypes.LONG
    handle = kernel.OpenProcess(0x0400 | 0x0010, False, pid)
    if not handle:
        raise OSError("cannot read process environment/working directory")
    try:
        own_wow, other_wow = wintypes.BOOL(), wintypes.BOOL()
        if (not kernel.IsWow64Process(kernel.GetCurrentProcess(), ctypes.byref(own_wow))
                or not kernel.IsWow64Process(handle, ctypes.byref(other_wow))
                or own_wow.value != other_wow.value):
            raise OSError("cannot inspect cross-bitness process context")
        width = ctypes.sizeof(ctypes.c_void_p)

        def read(address: int, size: int) -> bytes:
            buffer = ctypes.create_string_buffer(size)
            count = ctypes.c_size_t()
            if not kernel.ReadProcessMemory(handle, address, buffer, size, ctypes.byref(count)) or count.value != size:
                raise OSError("cannot read process environment/working directory")
            return buffer.raw

        def pointer(address: int) -> int:
            return int.from_bytes(read(address, width), "little")

        info = (ctypes.c_void_p * 6)()
        if ntdll.NtQueryInformationProcess(handle, 0, info, ctypes.sizeof(info), None) != 0:
            raise OSError("cannot read process PEB")
        params = pointer(info[1] + (0x20 if width == 8 else 0x10))
        cwd_descriptor = params + (0x38 if width == 8 else 0x24)
        length = int.from_bytes(read(cwd_descriptor, 2), "little")
        cwd = read(pointer(cwd_descriptor + width), length).decode("utf-16-le")
        if not cwd or not pathlib.Path(cwd).is_absolute():
            raise OSError("unreadable process working directory")
        environment = pointer(params + (0x80 if width == 8 else 0x48))
        # Read page-bounded pieces: the environment need not occupy a full final page.
        data = bytearray()
        while len(data) < 1024 * 1024:
            address = environment + len(data)
            chunk = read(address, min(4096 - address % 4096, 1024 * 1024 - len(data)))
            data.extend(chunk)
            terminator = next((i for i in range(0, len(data) - 3, 2)
                               if data[i:i + 4] == b"\0\0\0\0"), None)
            if terminator is not None:
                entries = bytes(data[:terminator]).decode("utf-16-le").split("\0")
                env = {}
                for entry in entries:
                    key, sep, value = entry.partition("=")
                    if sep and key:
                        env[key.upper()] = value
                return env, pathlib.Path(cwd)
        raise OSError("unterminated process environment")
    finally:
        kernel.CloseHandle(handle)


def process_context(pid: int, platform: str) -> tuple[dict[str, str], pathlib.Path]:
    if platform.startswith("linux"):
        entries = pathlib.Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
        env = {}
        for entry in entries:
            key, sep, value = entry.partition(b"=")
            if sep:
                env[os.fsdecode(key)] = os.fsdecode(value)
        return env, pathlib.Path(os.readlink(f"/proc/{pid}/cwd"))
    if platform == "win32":
        return windows_context(pid)
    if platform == "darwin":
        import ctypes

        libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        mib = (ctypes.c_int * 3)(1, 49, pid)  # CTL_KERN, KERN_PROCARGS2
        size = ctypes.c_size_t()
        if libc.sysctl(mib, 3, None, ctypes.byref(size), None, 0) != 0 or not 4 < size.value <= 4 * 1024 * 1024:
            raise OSError("cannot read process environment")
        buffer = ctypes.create_string_buffer(size.value)
        if libc.sysctl(mib, 3, buffer, ctypes.byref(size), None, 0) != 0:
            raise OSError("cannot read process environment")
        data = buffer.raw[:size.value]
        argc = int.from_bytes(data[:4], sys.byteorder)
        offset = data.index(b"\0", 4) + 1
        while offset < len(data) and data[offset] == 0:
            offset += 1
        for _ in range(argc):
            offset = data.index(b"\0", offset) + 1
        env = {}
        for entry in data[offset:].split(b"\0"):
            key, sep, value = entry.partition(b"=")
            if sep:
                env[os.fsdecode(key)] = os.fsdecode(value)
        cwd_result = subprocess.run(["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
                                    capture_output=True, text=True, check=False)
        names = [line[1:] for line in cwd_result.stdout.splitlines() if line.startswith("n")]
        if cwd_result.returncode != 0 or len(names) != 1:
            raise OSError("unreadable process environment/working directory")
        return env, pathlib.Path(names[0])
    raise OSError(f"unsupported process inspection platform {platform}")


def daemon_root(env: dict[str, str], platform: str) -> pathlib.Path:
    return state_root.default_state_root(env, platform=platform)


def probe(root: pathlib.Path, *, platform: str = sys.platform,
          enumerate_processes: Callable | None = None,
          read_context: Callable | None = None) -> Liveness:
    try:
        pids = (enumerate_processes or enumerate_daemons)(platform)
        target = root.resolve(strict=True)
        for pid in pids:
            env, cwd = (read_context or process_context)(pid, platform)
            if not cwd.is_absolute():
                raise OSError("unreadable process working directory")
            cwd = cwd.resolve(strict=True)
            if not cwd.is_dir():
                raise OSError("process working directory is not a directory")
            owned = daemon_root(env, platform)
            if not owned.is_absolute():
                owned = cwd / owned
            owned = owned.resolve(strict=True)
            if os.path.normcase(str(owned)) == os.path.normcase(str(target)):
                return Liveness("yes", f"ck-broca pid {pid} can write {target}")
        return Liveness("no", "enumeration succeeded; no daemon can write this root")
    except (OSError, RuntimeError, ValueError, UnicodeError) as error:
        return Liveness("unknown", str(error))


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: liveness.py STATE_ROOT", file=sys.stderr)
        return 2
    result = probe(pathlib.Path(sys.argv[1]))
    print(json.dumps({"state": result.state, "reason": result.reason}))
    return {"no": 0, "yes": 3, "unknown": 4}[result.state]


if __name__ == "__main__":
    sys.exit(main())
