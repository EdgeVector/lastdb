"""Admit only the two accepted-main shared DEV workflows."""

import os
import re

from ownership import run_identity
from support import require


DAILY = ".github/workflows/cloud-shared-daily.yml"


def owner(source):
    daily = os.environ.get("GITHUB_WORKFLOW_REF") == f"EdgeVector/lastdb/{DAILY}@refs/heads/main"
    if not daily:
        require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch",
                "shared_manual_accepted_source")
        return run_identity(source)
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REPOSITORY") == "EdgeVector/lastdb"
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_EVENT_NAME") in ("workflow_dispatch", "schedule")
            and re.fullmatch(r"[0-9a-f]{40}", source) and os.environ.get("GITHUB_SHA") == source,
            "shared_daily_origin")
    values = {name: os.environ.get(variable, "") for name, variable in (
        ("run_id", "GITHUB_RUN_ID"), ("run_attempt", "GITHUB_RUN_ATTEMPT"))}
    require(all(re.fullmatch(r"[1-9][0-9]{0,19}", value) for value in values.values()),
            "shared_daily_run_identity")
    return {"repository": "EdgeVector/lastdb", "workflow": DAILY, "source": source, **values}
