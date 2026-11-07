"""Observe the real shared daemon policy and its completed sync passes."""

import time

from support import OperationError, require


ROUTE = "/api/sync/byte-backup-v2"


def policy(node, enabled, log_interval=1, snapshot_interval=86400):
    request = {"action": "disable_automatic"}
    if enabled:
        request = {"action": "enable_automatic", "log_interval_seconds": log_interval,
                   "snapshot_interval_seconds": snapshot_interval}
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            result = node.call(ROUTE, request)
            require(result["automatic_backup_enabled"] is enabled, "shared_automatic_policy")
            node.shared_sync = enabled
            return
        except OperationError as error:
            if str(error) != "socket_http_status":
                raise
            time.sleep(0.25)
    raise OperationError("shared_automatic_policy_deadline")


def progress(node):
    return node.call(ROUTE, {"action": "status"})["shared_sync"]["progress"]


def wait(operation, node, reason, *, uploaded=None, ranges=None, last=None, reclaimed=None):
    deadline = time.monotonic() + 70
    while time.monotonic() < deadline:
        state = progress(node)
        if (state["next_sync_at"] > 0 and not state["running"] and state["blocked_reason"] is None
                and (uploaded is None or state["uploaded_through"] == uploaded)
                and (ranges is None or state["applied_ranges"] == ranges)
                and (last is None or (state["last_peer_applied"], state["last_peer_through"]) == last)
                and (reclaimed is None or state["reclaimed_delete_notes"] == reclaimed)):
            return state
        time.sleep(0.2)
    operation.summary["daemon_sync_observation"] = {
        key: state[key] for key in ("uploaded_through", "applied_ranges", "last_peer_applied",
                                    "last_peer_through", "reclaimed_delete_notes", "blocked_reason")
    }
    raise OperationError(reason)


def upload(operation, node, through):
    policy(node, True)
    wait(operation, node, "shared_daemon_upload_deadline", uploaded=through)
    policy(node, False)
    return through


def receive(operation, node, *, ranges=None, last=None, reclaimed=None):
    policy(node, True)
    state = wait(operation, node, "shared_daemon_receive_deadline",
                 ranges=ranges, last=last, reclaimed=reclaimed)
    policy(node, False)
    return state
