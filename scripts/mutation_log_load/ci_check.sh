#!/usr/bin/env bash
# Offline checks for the DEV mutation-log load. No AWS and no workflow dispatch.
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 -m pip install --quiet cryptography==41.0.7
cargo build --locked -p cloud_backup_protocol --bin node_read_token
python3 -m unittest discover -s scripts/mutation_log_load -p 'test_*.py'
