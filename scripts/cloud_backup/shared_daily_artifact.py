#!/usr/bin/env python3
"""Record source, download link, exact archive digest and expiration together."""

import os
import re
import sys

from shared_daily_io import github
from shared_origin import owner
from support import require


identity = owner(os.environ.get("GITHUB_SHA", ""))
artifact = github("actions/artifacts/" + sys.argv[1])
require(artifact["workflow_run"]["id"] == int(identity["run_id"])
        and artifact["workflow_run"]["head_sha"] == identity["source"]
        and artifact["name"].startswith("shared-daily-")
        and artifact["name"].endswith(f"-{identity['run_id']}-{identity['run_attempt']}")
        and artifact["expired"] is False
        and re.fullmatch(r"sha256:[0-9a-f]{64}", artifact["digest"]), "shared_daily_artifact_provenance")
url = f"https://github.com/EdgeVector/lastdb/actions/runs/{identity['run_id']}/artifacts/{artifact['id']}"
with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
    summary.write(f"\n[{artifact['name']}]({url})\n\nSource `{identity['source']}`; "
                  f"ZIP `{artifact['digest']}`; expires `{artifact['expires_at']}`.\n")
