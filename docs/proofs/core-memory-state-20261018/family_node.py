"""Isolated diagnostic node with one fixed read-family log filter."""
import os
import subprocess
import time
from api import Node, measure

FAMILY_LOG = 'lastdb::native_read_family=debug'


class FamilyNode(Node):
    def start(self):
        if self.process is not None:
            raise RuntimeError('The owned diagnostic node is already active')
        environment = dict(os.environ, LASTDB_ISOLATED_COPY='1', LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
            HOME=str(self.user_home), LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
            LASTDB_RESIDENT_MODE='write', LASTDB_DISK_PACK='1', RUST_MIN_STACK='16777216')
        for name in ('LASTDB_DISK_CAP_BYTES', 'SENTRY_DSN', 'FOLD_SENTRY_DSN',
                     'LASTDB_NATIVE_READ_DIAGNOSTICS'):
            environment.pop(name, None)
        environment['RUST_LOG'] = FAMILY_LOG
        self.log = (self.directory / 'node.log').open('a')
        self.process = subprocess.Popen([str(self.binary), '--data-dir', str(self.home),
            '--socket-path', str(self.socket), '--full-socket-path', str(self.full_socket)],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.remember(operation='owned-node-start', pid=self.process.pid, home=str(self.home),
                      fixed_read_family_log=FAMILY_LOG)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError('The diagnostic node exits before health')
            if self.socket.exists():
                try:
                    result, _, _, _ = self.call('/health')
                    if result.get('status') == 'ok' or result.get('ok') is True:
                        return
                except (OSError, measure.profile.http.client.HTTPException):
                    pass
            time.sleep(.05)
        raise RuntimeError('The owned diagnostic node health deadline expired')

