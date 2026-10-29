"""Remove API credentials even when an uncertain CI home must remain private."""

import os
import stat

from support import require


def clear(operation):
    operation.api_key = None
    for name in ("LASTDB_CLOUD_DEV_API_KEY", "LASTDB_CLOUD_DEV_RECOVERY_PHRASE"):
        os.environ.pop(name, None)
    if operation.root is None:
        return
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    root = os.open(operation.root, flags)
    try:
        info = os.fstat(root)
        require((info.st_dev, info.st_ino, info.st_uid) == operation.root_identity
                and info.st_mode & 0o077 == 0,
                "credential_cleanup_owned_root")
        for name in ("credentials", "restored"):
            try:
                directory = os.open(name, flags, dir_fd=root)
            except FileNotFoundError:
                continue
            try:
                owned = os.fstat(directory)
                require(owned.st_uid == os.getuid() and owned.st_mode & 0o077 == 0,
                        "credential_cleanup_private_home")
                for leaf in ("cloud_sync.json", "cloud_sync.json.paused"):
                    try:
                        saved = os.stat(leaf, dir_fd=directory, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    require(stat.S_ISREG(saved.st_mode) and saved.st_uid == os.getuid()
                            and saved.st_mode & 0o077 == 0 and saved.st_nlink == 1
                            and saved.st_size <= 16384, "credential_cleanup_private_file")
                    os.unlink(leaf, dir_fd=directory)
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        os.close(root)
    operation.summary["api_credentials_removed"] = True
