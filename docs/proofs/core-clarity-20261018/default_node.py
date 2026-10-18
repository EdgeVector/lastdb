"""Start the ordinary generated-data node without an engine-selection flag."""
import http.client
import os
import subprocess
import time


def start(node):
    if node.process is not None:
        raise RuntimeError('The owned node is already active')
    environment = dict(os.environ, LASTDB_ISOLATED_COPY='1',
        LASTDB_SUPPRESS_RECOVERY_PHRASE='1', HOME=str(node.user_home),
        LASTDB_HOME=str(node.home), FOLDDB_HOME=str(node.home), RUST_MIN_STACK='16777216')
    for name in ('LASTDB_DISK_PACK', 'LASTDB_ENGINE', 'LASTDB_DISK_CAP_BYTES',
                 'SENTRY_DSN', 'FOLD_SENTRY_DSN', 'RUST_LOG',
                 'LASTDB_NATIVE_READ_DIAGNOSTICS', 'LASTDB_PROOF_REPLAY',
                 'LASTDB_RESIDENT_MODE', 'LASTDB_ATOM_CONTENT_BINARY'):
        environment.pop(name, None)
    selected = environment.get('LASTDB_SCHEMA_PRELOAD_SCHEMAS', '')
    environment['LASTDB_SCHEMA_PRELOAD_SCHEMAS'] = ','.join(filter(None,
        (selected, 'CoreClarityHashRange')))
    node.log = (node.directory / 'node.log').open('a')
    node.process = subprocess.Popen([str(node.binary), '--data-dir', str(node.home),
        '--socket-path', str(node.socket), '--full-socket-path', str(node.full_socket)],
        env=environment, stdin=subprocess.DEVNULL, stdout=node.log, stderr=node.log)
    node.remember(operation='owned-node-start', pid=node.process.pid, home=str(node.home),
        engine_selection_environment={'LASTDB_DISK_PACK': None, 'LASTDB_ENGINE': None,
                                      'LASTDB_RESIDENT_MODE': None},
        atom_content_binary_environment=None,
        cloud_activation=False)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if node.process.poll() is not None:
            raise RuntimeError('The default node exits before health; inspect node.log')
        if node.socket.exists():
            try:
                result, _, _, _ = node.call('/health')
                if result.get('status') == 'ok' or result.get('ok') is True:
                    node.status('default-native-boot-without-selection-environment')
                    marker = node.home / 'data' / 'laststore-pack-node-v1'
                    if not marker.is_file():
                        raise RuntimeError('The default boot lacks its native node format marker')
                    node.remember(operation='default-native-format', marker=str(marker),
                                  created_by_normal_product_boot=True)
                    return
            except (OSError, http.client.HTTPException):
                pass
        time.sleep(.05)
    raise RuntimeError('The owned default node health deadline expires')
