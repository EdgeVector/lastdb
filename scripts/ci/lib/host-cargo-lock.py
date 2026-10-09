#!/usr/bin/env python3
"""Portable exclusive host-cargo lock (fcntl; works on macOS + Linux).

Commands:
  hold  --path LOCK --timeout SECS [--ready-file F] [--holder-file F]
        Acquire LOCK_EX (blocking up to --timeout; 0 = forever), write ready,
        then hold until SIGTERM/SIGINT or parent exit.
  run   --path LOCK --timeout SECS -- CMD [ARGS...]
        Acquire LOCK_EX, supervise CMD, then release after command exit.
  try   --path LOCK
        Non-blocking acquire; exit 0 if free (then release), 1 if busy.

Used by db-perf-guard to claim an exclusive cargo window, and by
scripts/ci/with-fold-host-cargo-lock.sh so agent/routine cargo can queue
behind the probe instead of thrashing the host.

Fairness: flock(2) is not a queue. A full suite that released the lock could
reacquire it before a targeted proof that had waited through the whole run
(papercut-fold-host-cargo-lock-starves-queued-proof-20260922). Waiters take a
ticket in <lock>.queue/ and only try the lock when theirs is the oldest live
ticket, so the lock hands off first-come first-served.

Sandbox: a routine sandbox may not write ~/.cache. flock(2) works on a
read-only descriptor, so an existing lock file is opened read-only and still
serializes with the rest of the host
(papercut-fold-cargo-lock-path-sandbox-20260922). Only when the file cannot be
opened at all does the helper fall back to $TMPDIR, and it says so.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import signal
import subprocess
import sys
import time
from typing import Optional, TextIO


def _fallback_lock_path(path: str) -> str:
    tmp = os.environ.get("TMPDIR") or "/tmp"
    return os.path.join(tmp, "fold-host-cargo-lock", os.path.basename(path) or "host-cargo.lock")


def _open_lock(path: str) -> TextIO:
    """Open the lock file; returns a descriptor usable with flock.

    The returned object carries `lock_path` (the path actually locked) and
    `writable` (False when opened read-only).
    """
    parent = os.path.dirname(path)
    try:
        if parent:
            os.makedirs(parent, exist_ok=True)
        # a+ so the file always exists; contents are advisory (pid notes only).
        fd = open(path, "a+", encoding="utf-8")
        fd.lock_path, fd.writable_lock = path, True  # type: ignore[attr-defined]
        return fd
    except PermissionError:
        pass
    if os.path.exists(path):
        try:
            fd = open(path, "r", encoding="utf-8")
            print(
                f"host_cargo_lock=read_only path={path} (sandbox; still host-wide)",
                file=sys.stderr,
                flush=True,
            )
            fd.lock_path, fd.writable_lock = path, False  # type: ignore[attr-defined]
            return fd
        except PermissionError:
            pass
    fallback = _fallback_lock_path(path)
    os.makedirs(os.path.dirname(fallback), exist_ok=True)
    print(
        f"::warning::host_cargo_lock cannot open {path}; using {fallback}. "
        "This lock does NOT serialize with cargo outside the sandbox. "
        "Set FOLD_HOST_CARGO_LOCK_PATH to choose.",
        file=sys.stderr,
        flush=True,
    )
    fd = open(fallback, "a+", encoding="utf-8")
    fd.lock_path, fd.writable_lock = fallback, True  # type: ignore[attr-defined]
    return fd


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class _Ticket:
    """A FIFO place in <lock>.queue/. Name: <ns>-<pid>. No-op when the queue
    directory cannot be written (read-only sandbox): plain flock then."""

    def __init__(self, lock_path: str) -> None:
        self.dir = lock_path + ".queue"
        self.path = ""
        try:
            os.makedirs(self.dir, exist_ok=True)
            name = f"{time.time_ns():020d}-{os.getpid()}"
            self.path = os.path.join(self.dir, name)
            with open(self.path, "w", encoding="utf-8"):
                pass
        except OSError:
            self.path = ""

    def is_head(self) -> bool:
        if not self.path:
            return True
        mine = os.path.basename(self.path)
        try:
            names = sorted(os.listdir(self.dir))
        except OSError:
            return True
        for name in names:
            if name >= mine:
                return True
            try:
                pid = int(name.rsplit("-", 1)[1])
            except (IndexError, ValueError):
                pid = 0
            if pid and _pid_alive(pid):
                return False
            # A dead waiter's ticket must not block the queue.
            try:
                os.unlink(os.path.join(self.dir, name))
            except OSError:
                pass
        return True

    def release(self) -> None:
        if self.path:
            try:
                os.unlink(self.path)
            except OSError:
                pass
            self.path = ""


def _acquire(fd: TextIO, timeout: float) -> None:
    """Acquire LOCK_EX in FIFO order. timeout=0 means wait forever."""
    lock_path = getattr(fd, "lock_path", "")
    ticket = _Ticket(lock_path) if lock_path else None
    deadline = time.time() + timeout if timeout > 0 else None
    try:
        while True:
            if ticket is None or ticket.is_head():
                try:
                    fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return
                except BlockingIOError:
                    pass
            if deadline is not None and time.time() >= deadline:
                raise TimeoutError(f"host_cargo_lock timeout after {timeout}s")
            time.sleep(0.25)
    finally:
        if ticket is not None:
            ticket.release()


def cmd_try(path: str) -> int:
    fd = _open_lock(path)
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fd.close()
        print(f"host_cargo_lock=busy path={path}", flush=True)
        return 1
    fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    fd.close()
    print(f"host_cargo_lock=free path={path}", flush=True)
    return 0


def cmd_hold(path: str, timeout: float, ready_file: str, holder_file: str) -> int:
    fd = _open_lock(path)
    try:
        _acquire(fd, timeout)
    except TimeoutError as exc:
        print(f"::error::{exc} path={path}", file=sys.stderr, flush=True)
        print(f"host_cargo_lock=timeout path={path} timeout_seconds={timeout}", flush=True)
        fd.close()
        return 4

    pid = os.getpid()
    if getattr(fd, "writable_lock", True):
        fd.seek(0)
        fd.truncate()
        fd.write(f"pid={pid}\n")
        fd.flush()

    if holder_file:
        parent = os.path.dirname(holder_file)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(holder_file, "w", encoding="utf-8") as hf:
            hf.write(f"{pid}\n")
    if ready_file:
        parent = os.path.dirname(ready_file)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(ready_file, "w", encoding="utf-8") as rf:
            rf.write(f"ready pid={pid}\n")

    print(f"host_cargo_lock=held pid={pid} path={path}", flush=True)

    stop = False

    def _stop(_signum: int, _frame: Optional[object]) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    parent = os.getppid()
    while not stop:
        if os.getppid() != parent:
            break
        time.sleep(0.5)

    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    finally:
        fd.close()
    print(f"host_cargo_lock=released pid={pid} path={path}", flush=True)
    return 0


def cmd_run(path: str, timeout: float, cmd: list[str]) -> int:
    if not cmd:
        print("host-cargo-lock run: missing command after --", file=sys.stderr)
        return 2
    fd = _open_lock(path)
    try:
        _acquire(fd, timeout)
    except TimeoutError as exc:
        print(f"::error::{exc} path={path}", file=sys.stderr, flush=True)
        print(f"host_cargo_lock=timeout path={path} timeout_seconds={timeout}", flush=True)
        fd.close()
        return 4

    pid = os.getpid()
    if getattr(fd, "writable_lock", True):
        fd.seek(0)
        fd.truncate()
        fd.write(f"pid={pid} cmd={' '.join(cmd)}\n")
        fd.flush()
    path = getattr(fd, "lock_path", path)
    print(f"host_cargo_lock=held pid={pid} path={path} cmd={cmd[0]}", flush=True)

    # Only this supervisor owns the descriptor. A compiler cache daemon may
    # outlive cargo; passing the descriptor through exec pins the lock forever.
    env = dict(os.environ, FOLD_HOST_CARGO_LOCK_PATH=path, FOLD_HOST_CARGO_LOCK_HELD="1")
    child = None
    pending_signals = []

    def forward(signum: int, _frame: Optional[object]) -> None:
        if child is None:
            pending_signals.append(signum)
            return
        try:
            os.killpg(child.pid, signum)
        except ProcessLookupError:
            pass

    previous = {sig: signal.signal(sig, forward) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        child = subprocess.Popen(cmd, env=env, close_fds=True, start_new_session=True)
        for signum in pending_signals:
            forward(signum, None)
        # Keep the lock during signal cleanup, until the command is reaped.
        result = child.wait()
        return result if result >= 0 else 128 - result
    except OSError as exc:
        print(f"host-cargo-lock run: {exc}", file=sys.stderr)
        return 127
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        fd.close()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="host-cargo-lock.py")
    sub = parser.add_subparsers(dest="command", required=True)

    p_try = sub.add_parser("try", help="non-blocking free/busy probe")
    p_try.add_argument("--path", required=True)

    p_hold = sub.add_parser("hold", help="hold lock until signal/parent exit")
    p_hold.add_argument("--path", required=True)
    p_hold.add_argument("--timeout", type=float, default=0.0)
    p_hold.add_argument("--ready-file", default="")
    p_hold.add_argument("--holder-file", default="")

    p_run = sub.add_parser("run", help="hold lock until command exit")
    p_run.add_argument("--path", required=True)
    p_run.add_argument("--timeout", type=float, default=0.0)
    p_run.add_argument("cmd", nargs=argparse.REMAINDER)

    args = parser.parse_args(argv)
    if args.command == "try":
        return cmd_try(args.path)
    if args.command == "hold":
        return cmd_hold(args.path, args.timeout, args.ready_file, args.holder_file)
    if args.command == "run":
        cmd = list(args.cmd)
        if cmd and cmd[0] == "--":
            cmd = cmd[1:]
        return cmd_run(args.path, args.timeout, cmd)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
