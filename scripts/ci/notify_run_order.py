"""Select the newest decisive main run, not the last notification to finish."""

import os
import re

STATE = re.compile(r"<!-- lastdb-notify-order:(\d+):(\d+) -->")


def order(run):
    values = (run.get("run_number"), run.get("run_attempt"))
    if any(type(value) is not int or value < 1 for value in values):
        raise SystemExit("notify_run_order")
    return values


def reconcile_context(api, context, outcome, workflow_file):
    run_id = context["url"].rsplit("/", 1)[-1]
    current = api.call("GET", "/actions/runs/" + run_id)
    if (current.get("head_sha") != context["sha"]
            or current.get("head_branch") != "main"
            or current.get("path") != ".github/workflows/" + workflow_file):
        raise SystemExit("notify_run_mismatch")
    selected = current
    selected_order = order(current)
    # A rerun has the same run id; an old attempt must not use its stale results.
    if str(selected_order[1]) != os.environ.get("GITHUB_RUN_ATTEMPT"):
        return None
    workflow_id = current.get("workflow_id")
    if type(workflow_id) is not int or workflow_id < 1:
        raise SystemExit("notify_workflow_identity")
    for page in range(1, 11):
        result = api.call("GET", f"/actions/workflows/{workflow_id}/runs"
                          f"?branch=main&per_page=10&page={page}")
        runs = result.get("workflow_runs")
        if not isinstance(runs, list) or len(runs) > 10:
            raise SystemExit("notify_run_page")
        for run in runs:
            if (run.get("head_branch") == "main"
                    and run.get("event") in {"push", "schedule", "workflow_dispatch"}
                    and order(run) > order(current)
                    and run.get("status") != "completed"):
                # A newer notifier may have just succeeded without any issue to
                # hold a watermark. Never mutate behind its unfinished/API-lagged
                # workflow; that newer run owns the eventual reconciliation.
                return None
            if (run.get("head_branch") == "main"
                    and run.get("event") in {"push", "schedule", "workflow_dispatch"}
                    and run.get("status") == "completed"
                    and run.get("conclusion") in {"success", "failure"}
                    and order(run) > selected_order):
                selected, selected_order = run, order(run)
                outcome = run["conclusion"]
        # Run listing is newest-created first. Once the current run is reached,
        # all later pages are older; attempts are compared within the run number.
        if any(run.get("id") == current["id"] for run in runs) or len(runs) < 10:
            break
    else:
        raise SystemExit("notify_run_page_bound")
    return ({"url": selected["html_url"] + f"/attempts/{selected_order[1]}", "sha": selected["head_sha"],
             "event": selected["event"], "order": selected_order}, outcome)


def admit(api, issues, context):
    """Persist the high-water mark under the workflow's notification lock."""
    incoming = context["order"]
    for issue in issues:
        match = STATE.search(issue.get("body") or "")
        if match and tuple(map(int, match.groups())) > incoming:
            return False
    stamp = f"<!-- lastdb-notify-order:{incoming[0]}:{incoming[1]} -->"
    for issue in issues:
        body = issue.get("body") or ""
        updated = STATE.sub(stamp, body) if STATE.search(body) else body + "\n" + stamp
        if updated != body:
            api.call("PATCH", f"/issues/{issue['number']}", {"body": updated})
    return True
