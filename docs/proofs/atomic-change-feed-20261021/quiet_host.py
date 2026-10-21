#!/usr/bin/env python3
"""Conservative macOS quiet-window evidence; hold the host Cargo lock externally."""
import argparse
from contextlib import contextmanager
import ctypes
import datetime
import json
import os
from pathlib import Path
import subprocess
import threading
import time

MIB = 1024 * 1024
COMPILERS = {'cargo', 'rustc', 'rustdoc', 'clang', 'clang++', 'cc', 'cc1', 'c++',
             'as', 'ld', 'ld64', 'lld', 'rust-lld', 'swiftc', 'swift-frontend',
             'ninja', 'make', 'cmake', 'llvm-ar', 'llvm-ranlib', 'dsymutil', 'libtool'}
DISK_JOBS = {'tar', 'bsdtar', 'gnutar', 'gzip', 'pigz', 'xz', 'zstd', 'zip', 'unzip',
             'ditto', 'cp', 'rsync', 'asr', 'hdiutil', 'diskimages-helper', 'apfs.util'}


class Usage(ctypes.Structure):
    """SDK sys/resource.h rusage_info_v2; libproc writes directly into this buffer."""
    _fields_ = [('uuid', ctypes.c_ubyte * 16)] + [(name, ctypes.c_uint64) for name in (
        'user', 'system', 'idle', 'interrupt', 'pageins', 'wired', 'resident', 'footprint',
        'birth', 'exit', 'child_user', 'child_system', 'child_idle', 'child_interrupt',
        'child_pageins', 'child_elapsed', 'reads', 'writes')]


class QuietWindowError(RuntimeError):
    pass


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def job_kind(name):
    if name in COMPILERS or name.startswith(('rustc-', 'clang-')):
        return 'compiler'
    if name.startswith(('lastdbd', 'folddb', 'laststore')):
        return 'database'
    return 'disk-job' if name in DISK_JOBS else None


class QuietHost:
    def __init__(self, owned_pids=(), interval=1., quiet_seconds=10.,
                 idle_mib_per_second=1., idle_transfers_per_second=100.,
                 foreign_mib_per_second=1., unaccounted_mib_per_second=2.):
        self.interval, self.quiet_seconds = interval, quiet_seconds
        if interval <= 0 or quiet_seconds <= 0:
            raise ValueError('The observer interval and quiet duration must be positive')
        self.limits = {'idle_mib_per_second': idle_mib_per_second,
            'idle_transfers_per_second': idle_transfers_per_second,
            'foreign_mib_per_second': foreign_mib_per_second,
            'unaccounted_mib_per_second': unaccounted_mib_per_second}
        self.library = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
        self.library.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
        self.library.proc_pid_rusage.restype = ctypes.c_int
        self.owned = set(owned_pids) | {os.getpid()}
        self.owned_births, self.samples, self.windows = {}, [], []
        self.condition, self.stop_event = threading.Condition(), threading.Event()
        self.io_lock, self.io_rows, self.io_errors = threading.Lock(), [], []
        self.io_process = self.worker = self.io_reader = None
        self.failure = None

    def snapshot(self):
        result = subprocess.run(['ps', '-axo', 'pid=,uid=,stat=,pcpu=,comm='],
            capture_output=True, text=True, timeout=10, check=True)
        processes, errors, malformed = {}, {}, 0
        for line in result.stdout.splitlines():
            pieces = line.split(None, 4)
            if len(pieces) != 5:
                malformed += 1
                continue
            pid, uid, state, cpu, executable = pieces
            pid, uid, cpu = int(pid), int(uid), float(cpu)
            value = Usage()
            found = self.library.proc_pid_rusage(pid, 2, ctypes.byref(value)) == 0
            code = None if found else ctypes.get_errno()
            if code:
                errors[str(code)] = errors.get(str(code), 0) + 1
            processes[pid] = {'pid': pid, 'uid': uid, 'state': state, 'cpu_percent': cpu,
                'executable': executable, 'name': Path(executable).name,
                'birth': int(value.birth) if found else None,
                'read_bytes': int(value.reads) if found else None,
                'write_bytes': int(value.writes) if found else None, 'errno': code}
        return {'at_ns': time.monotonic_ns(), 'utc': utc(), 'processes': processes,
                'errors': errors, 'malformed_lines': malformed}

    def set_owned_pids(self, pids):
        snapshot = self.snapshot()
        selected = set(pids) | {os.getpid()}
        if self.io_process:
            selected.add(self.io_process.pid)
        births = {}
        for pid in selected:
            row = snapshot['processes'].get(pid)
            if row is None or row['birth'] is None:
                raise QuietWindowError(f'Cannot identify owned process {pid}')
            births[pid] = row['birth']
        with self.condition:
            self.owned, self.owned_births = selected, births

    def _read_iostat(self):
        devices, first = [], True
        try:
            for raw in self.io_process.stdout:
                words = raw.split()
                if words and all(word.startswith('disk') for word in words):
                    devices = words
                    continue
                if not devices or len(words) != 3 * len(devices):
                    continue
                try:
                    numbers = [float(word) for word in words]
                except ValueError:
                    continue
                row = {'at_ns': time.monotonic_ns(), 'utc': utc(), 'boot_average': first,
                    'devices': {name: {'kib_per_transfer': numbers[3 * i],
                        'transfers_per_second': numbers[3 * i + 1],
                        'mib_per_second': numbers[3 * i + 2]} for i, name in enumerate(devices)}}
                first = False
                with self.io_lock:
                    self.io_rows.append(row)
        except Exception as error:
            self.io_errors.append(f'{type(error).__name__}: {error}')

    def _disk_observation(self, at_ns):
        with self.io_lock:
            rows = [row for row in self.io_rows if not row['boot_average']]
            latest = rows[-1] if rows else None
        if latest is None or at_ns - latest['at_ns'] > 2.5 * self.interval * 1e9:
            return None
        return latest

    def _process_deltas(self, before, after):
        elapsed = (after['at_ns'] - before['at_ns']) / 1e9
        rows, own_bytes, foreign_bytes, reasons = [], 0, 0, []
        with self.condition:
            owned = dict(self.owned_births)
        for pid, current in after['processes'].items():
            previous = before['processes'].get(pid)
            readable = current['birth'] is not None
            same = readable and previous is not None and previous['birth'] == current['birth']
            delta = (sum(current[key] - previous[key] for key in ('read_bytes', 'write_bytes'))
                     if same else sum(current[key] for key in ('read_bytes', 'write_bytes'))
                     if readable else 0)
            if delta < 0:
                reasons.append('counter-regression')
            delta = max(0, delta)
            ours = readable and owned.get(pid) == current['birth']
            kind = job_kind(current['name'])
            active = 'Z' not in current['state'] and (delta > 0 or 'R' in current['state']
                                                    or current['cpu_percent'] >= 1.)
            if ours:
                own_bytes += delta
            else:
                foreign_bytes += delta
                if kind == 'compiler' and 'Z' not in current['state']:
                    reasons.append(f'foreign-compiler:{pid}')
                elif kind and delta:
                    reasons.append(f'foreign-{kind}-io:{pid}')
                elif kind and not readable and active:
                    reasons.append(f'foreign-{kind}-unobservable-active:{pid}')
            if delta or kind or ours:
                rows.append({**current, 'owned': ours, 'kind': kind, 'delta_bytes': delta,
                             'new_or_reused_pid': not same})
        return elapsed, rows, own_bytes, foreign_bytes, reasons

    def _sample(self, before, after):
        elapsed, rows, own_bytes, foreign_bytes, reasons = self._process_deltas(before, after)
        disk = self._disk_observation(after['at_ns'])
        foreign_rate, own_rate = foreign_bytes / elapsed / MIB, own_bytes / elapsed / MIB
        if elapsed > max(self.interval * 1.75, self.interval + .25):
            reasons.append('observer-sample-gap')
        if after['malformed_lines']:
            reasons.append('malformed-process-inventory')
        if foreign_rate > self.limits['foreign_mib_per_second']:
            reasons.append('foreign-disk-traffic')
        if disk is None:
            reasons.append('missing-or-stale-iostat')
        else:
            # Device views can overlap. Preserve each and compare their maximum, never their sum.
            rate = max(value['mib_per_second'] for value in disk['devices'].values())
            if rate > own_rate + self.limits['unaccounted_mib_per_second']:
                reasons.append('unaccounted-device-traffic')
        idle_reasons = list(reasons)
        if disk:
            for value in disk['devices'].values():
                if value['mib_per_second'] > self.limits['idle_mib_per_second']:
                    idle_reasons.append('preflight-device-throughput')
                if value['transfers_per_second'] > self.limits['idle_transfers_per_second']:
                    idle_reasons.append('preflight-device-transfers')
        return {'start_ns': before['at_ns'], 'end_ns': after['at_ns'], 'utc': after['utc'],
            'seconds': elapsed, 'own_delta_bytes': own_bytes, 'foreign_delta_bytes': foreign_bytes,
            'foreign_mib_per_second': foreign_rate, 'process_count': len(after['processes']),
            'observable_count': sum(row['birth'] is not None for row in after['processes'].values()),
            'errno_counts': after['errors'], 'processes': rows, 'disk': disk,
            'reasons': sorted(set(reasons)), 'idle_reasons': sorted(set(idle_reasons))}

    def _observe(self, previous):
        try:
            while not self.stop_event.wait(self.interval):
                current = self.snapshot()
                sample = self._sample(previous, current)
                with self.condition:
                    self.samples.append(sample)
                    self.condition.notify_all()
                previous = current
        except Exception as error:
            with self.condition:
                self.failure = f'{type(error).__name__}: {error}'
                self.condition.notify_all()

    def start(self):
        if self.worker:
            raise QuietWindowError('The quiet observer already runs')
        previous = self.snapshot()
        self.io_process = subprocess.Popen(['/usr/sbin/iostat', '-d', '-w', str(self.interval)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        self.set_owned_pids(self.owned)
        self.io_reader = threading.Thread(target=self._read_iostat, daemon=True)
        self.worker = threading.Thread(target=self._observe, args=(previous,), daemon=True)
        self.io_reader.start()
        self.worker.start()
        return self

    def _check_observer(self):
        if self.failure or self.io_errors or self.io_process.poll() is not None:
            raise QuietWindowError(f'Quiet observer failed: {self.failure or self.io_errors or "iostat exits"}')

    def wait_quiet(self, timeout_seconds=300.):
        start, deadline = time.monotonic_ns(), time.monotonic() + timeout_seconds
        with self.condition:
            while True:
                self._check_observer()
                clean = []
                for row in reversed(self.samples):
                    if row['start_ns'] < start or row['idle_reasons']:
                        break
                    clean.append(row)
                duration = sum(row['seconds'] for row in clean)
                if duration >= self.quiet_seconds:
                    return {'accepted': True, 'seconds': duration, 'samples': len(clean),
                            'start_ns': clean[-1]['start_ns'], 'end_ns': clean[0]['end_ns']}
                left = deadline - time.monotonic()
                if left <= 0:
                    reasons = self.samples[-1]['idle_reasons'] if self.samples else ['no-samples']
                    raise QuietWindowError(f'No quiet preflight within {timeout_seconds}s: {reasons}')
                self.condition.wait(min(left, self.interval * 1.5))

    @contextmanager
    def window(self, label, timeout_seconds=300.):
        preflight = self.wait_quiet(timeout_seconds)
        certificate = {'label': label, 'accepted': False, 'preflight': preflight,
                       'start_ns': time.monotonic_ns(), 'started_utc': utc(),
                       'owned_process_births': dict(self.owned_births)}
        failed = None
        try:
            yield certificate
        except BaseException as error:
            failed = f'{type(error).__name__}: {error}'
            raise
        finally:
            certificate['end_ns'], certificate['finished_utc'] = time.monotonic_ns(), utc()
            deadline = time.monotonic() + max(5., 3 * self.interval)
            selected, reasons = [], []
            try:
                with self.condition:
                    while (not self.samples or self.samples[-1]['end_ns'] < certificate['end_ns']):
                        self._check_observer()
                        if time.monotonic() >= deadline:
                            raise QuietWindowError('The final boundary sample is absent')
                        self.condition.wait(self.interval)
                    selected = [i for i, row in enumerate(self.samples)
                                if row['end_ns'] >= certificate['start_ns']
                                and row['start_ns'] <= certificate['end_ns']]
                    reasons = sorted({reason for i in selected for reason in self.samples[i]['reasons']})
            except Exception as error:
                reasons = [f'observer-error:{type(error).__name__}: {error}']
            certificate.update(sample_indices=selected, reasons=reasons, operation_error=failed,
                               accepted=bool(selected) and not reasons and failed is None)
            self.windows.append(certificate)
            if failed is None and not certificate['accepted']:
                raise QuietWindowError(f'Timing window {label!r} refused: {reasons}')

    def evidence(self):
        return {'observer': 'libproc RUSAGE_INFO_V2 plus interval iostat, boot average excluded',
            'interval_seconds': self.interval, 'quiet_seconds': self.quiet_seconds,
            'limits': self.limits, 'owned_process_births': self.owned_births,
            'samples': self.samples, 'windows': self.windows, 'failure': self.failure,
            'iostat': self.io_rows, 'iostat_errors': self.io_errors,
            'limits_of_evidence': [
                'No privileged fs_usage; EPERM system processes remain unattributed.',
                'One-second process samples can miss a short-lived process.',
                'Process counters and device intervals do not share exact accounting boundaries.',
                'Device views can overlap; their rates are not summed.',
                'Quiet means this conservative acceptance policy, not absolute zero other I/O.',
                'No primary or other process is stopped or changed.']}

    def close(self):
        self.stop_event.set()
        if self.worker:
            self.worker.join(timeout=15)
        if self.io_process:
            if self.io_process.poll() is None:
                self.io_process.terminate()
            self.io_process.wait(timeout=5)
            if self.io_reader:
                self.io_reader.join(timeout=5)
            self.io_process.stdout.close()
            self.io_process.stderr.close()

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--timeout', type=float, default=300.)
    parser.add_argument('--quiet-seconds', type=float, default=10.)
    args = parser.parse_args()
    observer = QuietHost(quiet_seconds=args.quiet_seconds)
    result, error = None, None
    try:
        with observer:
            result = observer.wait_quiet(args.timeout)
    except Exception as failure:
        error = f'{type(failure).__name__}: {failure}'
    args.output.write_text(json.dumps({'accepted': result is not None, 'error': error,
        'result': result, 'evidence': observer.evidence()}, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'accepted': result is not None, 'error': error}))
    return 0 if result is not None else 2


if __name__ == '__main__':
    raise SystemExit(main())
