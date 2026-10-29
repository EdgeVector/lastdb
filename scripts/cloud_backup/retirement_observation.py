"""Keep only validated public cleanup progress and fixed retry codes on failure."""

from retirement import TRANSIENT
from support import require

FIELDS = ('request_id', 'phase', 'plan_sha256', 'eligible_allocations', 'confirmed_allocations',
          'protected_allocations', 'protected_uncertain_bytes', 'released_bytes', 'resume_after_ms')
QUOTA = ('quota_bytes', 'other_used_bytes', 'reserved_bytes', 'retained_bytes', 'available_bytes')
RETRY_CODES = TRANSIENT | {'ci_control_service_missing_dependency'}


class Observation:
    def __init__(self, summary):
        self.summary = summary
        self.reset()

    def reset(self):
        self.summary.pop('retirement', None)
        self.summary.pop('ci_cleanup_headroom', None)
        self.summary['retirement_diagnostics'] = {'state_observed': False, 'validated_states': 0,
            'transient_errors': 0, 'last_transient_code': None, 'transient_codes': {}}

    def __call__(self, state, transient):
        if state is None and transient is None:
            self.reset()
            return
        diagnostics = self.summary['retirement_diagnostics']
        if state is not None:
            # Retirement invokes this callback only after Status.validate().
            # Never retain the raw reply, saved input, kept recipe or provider grant.
            self.summary['retirement'] = {name: state.get(name) for name in FIELDS}
            self.summary['ci_cleanup_headroom'] = {name: int(state['headroom'][name]) for name in QUOTA}
            self.summary['ci_cleanup_headroom']['minimum_bytes'] = int(state['input']['required_headroom_bytes'])
            diagnostics['state_observed'] = True
            diagnostics['validated_states'] += 1
        if transient is not None:
            require(transient in RETRY_CODES, 'fixed_cleanup_retry_code')
            diagnostics['transient_errors'] += 1
            diagnostics['last_transient_code'] = transient
            counts = diagnostics['transient_codes']
            counts[transient] = counts.get(transient, 0) + 1
