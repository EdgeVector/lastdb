#!/usr/bin/env python3
"""Record source, download link, exact archive digest and expiration together."""

import os
import re
import sys
import time
from urllib.error import HTTPError, URLError

from shared_daily_io import github
from shared_origin import owner
from support import require


def uploaded_artifact(artifact_id):
    """Allow bounded metadata propagation after a successful artifact upload."""
    delays = (2, 4, 8, 16, 30)
    for attempt in range(len(delays) + 1):
        try:
            return github("actions/artifacts/" + artifact_id)
        except HTTPError as error:
            if error.code not in (404, 429, 500, 502, 503, 504) or attempt == len(delays):
                raise
            reason = f"HTTP {error.code}"
            error.close()
        except (URLError, TimeoutError):
            if attempt == len(delays):
                raise
            reason = "transport unavailable"
        # Retry only this just-uploaded artifact read, never admission or cleanup.
        print(f"Artifact metadata {reason}; retrying in {delays[attempt]}s", file=sys.stderr)
        time.sleep(delays[attempt])


identity = owner(os.environ.get("GITHUB_SHA", ""))
artifact = uploaded_artifact(sys.argv[1])
require(artifact["id"] == int(sys.argv[1])
        and artifact["workflow_run"]["id"] == int(identity["run_id"])
        and artifact["workflow_run"]["head_sha"] == identity["source"]
        and artifact["name"].startswith("shared-daily-")
        and artifact["name"].endswith(f"-{identity['run_id']}-{identity['run_attempt']}")
        and artifact["expired"] is False
        and re.fullmatch(r"sha256:[0-9a-f]{64}", artifact["digest"]), "shared_daily_artifact_provenance")
url = f"https://github.com/EdgeVector/lastdb/actions/runs/{identity['run_id']}/artifacts/{artifact['id']}"
with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
    summary.write(f"\n[{artifact['name']}]({url})\n\nSource `{identity['source']}`; "
                  f"ZIP `{artifact['digest']}`; expires `{artifact['expires_at']}`.\n")
