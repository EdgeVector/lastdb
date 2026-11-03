"""The home format of this release, for every script.

The source is `HOME_FORMAT`, `RECORD_NODE_BYTES` and `RECORD_PROFILE` in the
node store (`fold_db/crates/core/src/storage/disk_pack/run_admission.rs`). A
layout change raises them there and here, and nowhere else. The file names
below name their Rust definition.
"""

HOME_FORMAT = 11
# The clear node marker (`laststore-pack-node-v1`) of a home in this format.
NODE_MARKER = f"format={HOME_FORMAT}\nhome=disk-run\ncas=disk-cas\n".encode()
# The backup profile version of a home in this format.
BACKUP_PROFILE = 9
# The saved account credentials and manual backup consent in the home root
# (`CLOUD_CREDENTIALS_FILE` in `lastdb_node/src/cloud.rs`).
CREDENTIALS_FILE = "cloud-credentials.json"
# The backup source and run holds in `data/disk-run/` (`BACKUP_DIR` in
# `vendor/laststore/src/disk_run/home/mod.rs`).
BACKUP_DIR = "cloud-byte-v2"
