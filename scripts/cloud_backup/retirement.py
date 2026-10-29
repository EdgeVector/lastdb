"""Resume one saved DEV cleanup; never replace an uncertain request identity."""

import time
import uuid

from ci_control import decimal
from retirement_status import MINIMUM, Status
from support import OperationError, require, save_json

TRANSIENT = frozenset(("ci_control_transport_unknown", "ci_control_service_provider_pending",
                       "ci_control_service_outcome_unknown", "ci_control_service_retry_later",
                       *("ci_control_http_" + str(code) for code in (408, 429, 500, 502, 503, 504))))


class Retirement:
    def __init__(self, control, baseline, observe=None):
        self.control, self.baseline = control, baseline
        self.observe = observe

    def observation(self, state=None, transient=None):
        if self.observe is not None:
            self.observe(state, transient)

    def pending(self):
        value = self.control.post("pending", {})
        require(value.get("scope") == self.control.scope and "active" in value,
                "ci_cleanup_pending_scope")
        active = value["active"]
        if active is not None:
            Status(self.control.scope, self.baseline).validate(active)
        return active

    def finish(self, active=None, request=None):
        self.observation()
        validator = Status(self.control.scope, self.baseline)
        require((active is None) != (request is None), "ci_cleanup_one_request_origin")
        if active is not None:
            state = validator.validate(active)
            request = state["input"]
        else:
            state = None
        if state is not None:
            self.observation(state=state)
        request_id = request["request_id"]
        for _step in range(10000):
            require(time.monotonic() < self.control.deadline, "ci_cleanup_deadline")
            if state is not None:
                if state["phase"] == "bootstrap_headroom":
                    return state
                if state["phase"] == "complete":
                    return self.confirm(validator, state)
                if state["phase"] == "drain" and state["resume_after_ms"] is not None:
                    delay = decimal(state["resume_after_ms"]) / 1000 - time.time()
                    if delay > 0:
                        # The service owns the final time check. Short waits
                        # avoid repeated delete attempts during its safety delay.
                        time.sleep(min(30, delay, max(0, self.control.deadline - time.monotonic())))
                        try:
                            value = self.control.post("status", {"request_id": request_id})
                        except OperationError as error:
                            if str(error) in TRANSIENT:
                                self.observation(transient=str(error))
                                continue
                            raise
                        state = validator.validate(value, request)
                        self.observation(state=state)
                        continue
            if state is None or state["plan_sha256"] is None:
                route = "plan"
                extra = {name: request[name] for name in ("request_id", "required_headroom_bytes")}
            else:
                route = "execute"
                extra = {"request_id": request_id, "plan_sha256": state["plan_sha256"]}
            try:
                value = self.control.post(route, extra)
            except OperationError as error:
                if str(error) not in TRANSIENT:
                    raise
                self.observation(transient=str(error))
                # A lost write reply never means failure or success. Read only
                # this request; retry only its exact saved inputs if absent.
                time.sleep(1)
                try:
                    value = self.control.post("status", {"request_id": request_id})
                except OperationError as read_error:
                    if str(read_error) in TRANSIENT or (
                        state is None and str(read_error) == "ci_control_service_missing_dependency"
                    ):
                        self.observation(transient=str(read_error))
                        continue
                    raise
            state = validator.validate(value, request)
            self.observation(state=state)
        raise OperationError("ci_cleanup_step_bound")

    def confirm(self, validator, state):
        extra = {"request_id": state["request_id"], "plan_sha256": state["plan_sha256"]}
        readback = validator.validate(self.control.post("status", {"request_id": extra["request_id"]}),
                                      state["input"])
        repeated = validator.validate(self.control.post("execute", extra), state["input"])
        require(readback["phase"] == repeated["phase"] == "complete"
                and all(readback[name] == repeated[name] == state[name] for name in (
                    "confirmed_allocations", "released_bytes", "protected_uncertain_bytes")),
                "ci_cleanup_repeat_changed_terminal_counts")
        require(all(readback["headroom"][name] == repeated["headroom"][name] for name in (
            "quota_bytes", "other_used_bytes", "reserved_bytes", "retained_bytes", "available_bytes")),
            "ci_cleanup_repeat_changed_quota")
        # Unknown legacy bytes can stay protected. The final usage check still
        # refuses a new upload unless its actual 96 MiB allowance is available.
        return repeated

    def create(self, output):
        request = {"version": 2, "scope": self.control.scope,
                   "request_id": str(uuid.uuid4()), "required_headroom_bytes": str(MINIMUM)}
        save_json(output / "cleanup-input.json", request)
        return self.finish(request=request)
