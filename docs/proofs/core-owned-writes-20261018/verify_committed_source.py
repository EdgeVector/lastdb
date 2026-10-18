#!/usr/bin/env python3
"""Verify every frozen product input against its exact recoverable Git source commit."""
import argparse
import json
from pathlib import Path
import re
import subprocess

import capture_before


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch('[0-9a-f]{40}', args.commit):
        parser.error('Use the exact full immutable Git commit')
    data = json.loads(args.manifest.read_text())
    if data.get('head_at_capture') != args.commit or data.get('state') != 'release_verified':
        raise RuntimeError('The completed source identity names another commit or lacks a verified release')
    capture_before.COMMIT = args.commit
    result = capture_before.verify_git_sources(data['source_files'])
    result.update(source_tree=subprocess.check_output(['git', 'rev-parse', args.commit + '^{tree}'],
        cwd=capture_before.ROOT, text=True).strip(),
        source_identity=str(args.manifest.resolve()),
        source_identity_sha256=capture_before.digest(args.manifest),
        binary_sha256=data['binary_sha256'], source_inventory_sha256=data['source_inventory_sha256'],
        scope='Exact committed product source bytes. Proof documents and later result-only commits are outside this product inventory.')
    capture_before.save(args.output, result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
