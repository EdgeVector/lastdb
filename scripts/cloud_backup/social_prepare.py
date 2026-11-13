"""Prepare an unsigned Social DEV registry declaration using the exact CI node."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from node import OwnedNode
from programs import Programs
from signed_files import running, summary
from social_app import ROOT
from support import OperationError, failure, require

sys.path.insert(0, str(ROOT / "apps/social"))
from app import SocialApp  # noqa: E402
from lastdb import LastDB  # noqa: E402


def prepare():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REF") == "refs/heads/main", "social_prepare_accepted_ci_only")
    programs = Programs(args.bin_dir.resolve(strict=True), args.version, dict(os.environ))
    with tempfile.TemporaryDirectory(prefix="ldb-social-", dir="/tmp") as temporary:
        root = Path(temporary).resolve()
        home = root / "node"
        initialized = programs.command(None, [
            "admin", "backup", "init", "--into", str(home), "--recovery-phrase-out",
            str(home / "recovery-phrase.txt"), "--json",
        ], timeout=20)
        node = OwnedNode(home, programs, account=initialized["account_id"])
        declaration = root / "declaration.json"
        with running(node):
            lastdb = LastDB(home, programs.directory / "lastdb")
            lastdb.require_sessions()
            app = SocialApp(lastdb)
            app.home()
            app.sharing.pending_edit("registry-preflight")
            origin = subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=ROOT, text=True).strip()
            expected_origin = "https://github.com/EdgeVector/lastdb.git"
            require(origin in (expected_origin, expected_origin.removesuffix(".git")), "social_prepare_origin")
            subprocess.run(["git", "remote", "set-url", "origin", expected_origin], cwd=ROOT, check=True)
            subprocess.run([
                str(programs.directory / "lastdb"), "--data-dir", str(home), "app", "publish", "--env", "dev",
                "--channel", "next", "--manifest", str(ROOT / "lastdb-app.json"), "--prepare", str(declaration),
            ], check=True, timeout=60)
        payload = json.loads(declaration.read_text(encoding="utf-8"))
        require(payload["sha"] == os.environ["GITHUB_SHA"] and payload["app_id"] == "social-graph"
                and payload["source"] == expected_origin and payload["channel"] == "next"
                and payload["lastdb_build"] == args.version,
                "social_prepare_exact_source")
    statement = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    print(f"SOCIAL_DECLARATION {statement}", flush=True)
    summary(f"Unsigned DEV registry declaration\n\n`{statement}`")


if __name__ == "__main__":
    try:
        prepare()
    except OperationError as error:
        print(json.dumps(failure(error)), flush=True)
        raise SystemExit(1) from None
