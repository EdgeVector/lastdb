"""Interrupt a real receiver with one bounded original-event request pending."""

import copy
import http.client
import signal
import threading
import time

from logical_capture import observed_resources, read, require
from logical_peer_support import add_model, check_values, own_head, position, request


def interrupt(receiver, pages):
    pending, stop, lock = threading.Event(), threading.Event(), threading.Lock()
    state = {"acknowledged": [], "errors": []}

    def writer():
        for page in pages:
            if stop.is_set():
                return
            pending.set()
            try:
                receipt = request(receiver, {"action": "import", "events": page["events"]})
                with lock:
                    state["acknowledged"].append(receipt)
            except Exception as error:
                with lock:
                    state["errors"].append({"kind": type(error).__name__, "message": str(error)[:300],
                                            "transport": isinstance(error, (OSError, http.client.HTTPException))})
                return
            finally:
                pending.clear()

    thread = threading.Thread(target=writer, name="logical-peer-receiver", daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            with lock:
                ready, errors = len(state["acknowledged"]) >= 2, list(state["errors"])
            require(not errors, f"receiver failed before the deliberate kill: {errors}")
            if ready and pending.is_set() and thread.is_alive():
                break
            require(thread.is_alive(), "receiver completed without an active interruption window")
            time.sleep(0.0005)
        else:
            raise RuntimeError("no active peer-import interruption window")
        process = receiver.process
        require(process.poll() is None and pending.is_set(), "receiver import is not active")
        process.kill()
        process.wait(timeout=10)
        require(process.returncode == -signal.SIGKILL, "receiver did not stop through SIGKILL")
        receiver.process = None
        receiver.log.close()
        receiver.log = None
    finally:
        stop.set()
        thread.join(timeout=65)
        require(not thread.is_alive(), "the owned import client did not stop")
    require(all(error["transport"] for error in state["errors"]),
            "receiver returned a non-transport failure at the deliberate kill")
    return {"signal": "SIGKILL", "exit_code": process.returncode,
            "pending_client_import_observed": True, "pre_crash_flush": False, **state}


def recover(receiver, initial, local_head, pages, batches, base_model, result):
    receiver.environment.pop("LASTDB_LOGICAL_CAPTURE")
    receiver.start()
    recovered = position(receiver, initial["origin"])
    count = recovered["sequence"] - initial["sequence"]
    acknowledgements = result["interruption"]["acknowledged"]
    require(len(acknowledgements) <= count <= len(acknowledgements) + 1,
            "recovered peer position exceeds the acknowledged prefix and one pending page")
    require(own_head(receiver) == local_head, "peer import changed local capture after SIGKILL")
    events = [page["events"][0] for page in pages]
    expected_position = initial if count == 0 else {
        key: events[count - 1][key] for key in ("origin", "sequence", "sha256")}
    require(recovered == expected_position, "recovered position is not an exact signed event prefix")
    model = copy.deepcopy(base_model)
    add_model(model, batches[:count], events[:count])
    result["first_reopen_values"] = check_values(receiver, model)
    absent = [entry for batch in batches[count:] for entry in batch]
    if absent:
        read(receiver, [], absent)
    result["first_reopen_position"] = recovered
    result["resources_after_crash"] = observed_resources(receiver)
    receiver.stop()
    receiver.start()
    require(position(receiver, initial["origin"]) == recovered and own_head(receiver) == local_head,
            "second reopen changed durable peer or local positions")
    result["second_reopen_values"] = check_values(receiver, model)
    if absent:
        read(receiver, [], absent)
    result["second_reopen_position"] = position(receiver, initial["origin"])
    result["committed_pending_page"] = count > len(acknowledgements)
    return count
