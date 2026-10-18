"""Record the exact fresh container identity for supported later cleanup."""
import json
import os


def mark_created_root(root):
    """Call immediately after a new private root is created, before node start."""
    info = root.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o777 != 0o700:
        raise RuntimeError('Fresh owned root must have mode 0700')
    marker = root / '.synthetic-seed-owner.json'
    descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as output:
        json.dump({'version': 1, 'kind': 'synthetic-trial', 'owner_uid': os.getuid(),
                   'device': info.st_dev, 'inode': info.st_ino,
                   'creator_pid': os.getpid()}, output, sort_keys=True)
        output.write('\n')
