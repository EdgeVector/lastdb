"""Move normal generated mutation logs between two new guarded homes."""
import json
import os
import shutil
import stat
import subprocess

from api import Node, measure
import logical


class NamedNode(Node):
    def __init__(self, run, variant):
        measure.profile.Node.__init__(self, run, variant, run.binary, 'off')
        marker = self.home / '.synthetic-home'
        descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)


def provision_generated_owner(source, target):
    root = source.run.root
    info = root.stat()
    marker = json.loads((root / '.synthetic-seed-owner.json').read_text())
    if (source.process is not None or target.process is not None or source.run is not target.run
            or root.is_symlink() or root.resolve(strict=True) != root
            or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
            or marker['kind'] != 'synthetic-trial' or marker['owner_uid'] != os.getuid() or marker['device'] != info.st_dev
            or marker['inode'] != info.st_ino):
        raise RuntimeError('The generated-owner provision root is not stopped and owned')
    original, destination = source.home / 'identity.key', target.home / 'identity.key'
    for home in (source.home, target.home):
        if home.is_symlink() or home.resolve(strict=True) != home or not home.is_relative_to(root):
            raise RuntimeError('A generated replay home escapes its owned root')
    metadata = original.lstat()
    if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
            or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600
            or set(target.home.iterdir()) != {target.home / '.synthetic-home'}):
        raise RuntimeError('The new receiver or newly generated identity differs')
    files = subprocess.run(['lsof', '-Fpn', str(original)], capture_output=True, text=True)
    if files.returncode != 1 or files.stdout.strip() or files.stderr.strip():
        raise RuntimeError('The generated source identity is still open')
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with original.open('rb') as input_file, os.fdopen(descriptor, 'wb') as output_file:
        shutil.copyfileobj(input_file, output_file)
    before, after = measure.profile.file_hash(original), measure.profile.file_hash(destination)
    if before != after:
        raise RuntimeError('The generated owner identity differs after provision')
    source.remember(operation='core-owned-generated-owner-provision',
        source=str(original), receiver=str(destination), sha256=before,
        scope='Only the key generated in this fresh stopped source enters the empty receiver. No key bytes enter receipts or logs.')


def transfer(source, target, directory, phase):
    exported, _, _, identity = source.call('/api/db/export-mutation-log', {'dir': str(directory)})
    exported = exported.get('data', exported)['mutation_log_export']
    if exported.get('records_exported', 0) <= 0:
        raise RuntimeError('The generated source exports no mutation records')
    replayed, _, _, replay_identity = target.call('/api/db/replay-mutation-log', {'dir': str(directory)})
    replayed = replayed.get('data', replayed)['mutation_log_replay']
    if replayed.get('records_applied', 0) <= 0:
        raise RuntimeError('The receiver applies no generated mutation records')
    target.remember(operation='core-owned-local-replay', phase=phase,
        export_request_id=identity, replay_request_id=replay_identity,
        export_report=exported, replay_report=replayed,
        scope='The ordinary guarded owner route uses real encrypted mutation logs and the normal peer-apply path. It publishes disk-first core invalidation, not immediate typed value warming.')


def run(run):
    source, target = NamedNode(run, 'replay-source'), NamedNode(run, 'target-replay')
    previous = os.environ.get('LASTDB_PROOF_REPLAY')
    os.environ['LASTDB_PROOF_REPLAY'] = '1'
    try:
        source.start()
        source.stop()
        provision_generated_owner(source, target)
        source.start()
        target.start()
        specs = [logical.Spec('hash', 'CoreOwnedReplayHash'),
                 logical.Spec('hashrange', 'CoreOwnedReplayHashRange')]
        for spec in specs:
            logical.install(source, spec)
            logical.install(target, spec)
            logical.read(target, spec, {0: None, 1: None}, 'replay-warm-absence')
        logical.write(source, [logical.mutation(spec, number, 30, 'create', 'durable')
                               for spec in specs for number in (0, 1)], 'replay-source-create')
        transfer(source, target, run.root / 'replay-export-first', 'create-after-warm-absence')
        for spec in specs:
            logical.read(target, spec, {0: 30, 1: 30}, 'replay-first-read-after-create')
            logical.read(target, spec, {0: 30, 1: 30}, 'replay-warm-created-value', zero=True)
        jobs = [logical.mutation(spec, number, 31 if number == 0 else 0,
                'update' if number == 0 else 'delete', 'durable')
                for spec in specs for number in (0, 1)]
        logical.write(source, jobs, 'replay-source-update-and-delete')
        transfer(source, target, run.root / 'replay-export-second', 'update-delete-after-warm-values')
        for spec in specs:
            logical.read(target, spec, {0: 31, 1: None}, 'replay-after-update-delete')
        logical.drained(target, 'replay-receiver-flushed')
        target.stop()
        target.start()
        for spec in specs:
            logical.read(target, spec, {0: 31, 1: None}, 'replay-receiver-normal-reopen')
    finally:
        errors = []
        for node in (source, target):
            try:
                node.stop()
            except Exception as exc:
                errors.append(type(exc).__name__ + ': ' + str(exc))
        if previous is None:
            os.environ.pop('LASTDB_PROOF_REPLAY', None)
        else:
            os.environ['LASTDB_PROOF_REPLAY'] = previous
        if errors:
            raise RuntimeError('The replay nodes do not stop normally: ' + '; '.join(errors))
