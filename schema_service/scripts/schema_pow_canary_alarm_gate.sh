#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
config="$script_dir/../config/schema_pow_canary_alarm_gate.json"
stage="canary"
evidence_files=()

usage() {
  cat <<'USAGE'
Usage: schema_pow_canary_alarm_gate.sh [--config FILE] [--stage canary|promoted] --evidence FILE [--evidence FILE ...]

Evaluates redacted JSONL emitted by schema_pow_dev_proof_harness.sh and prints
one machine-readable promotion or rollback decision. Evidence payloads are
never copied to the output.
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)
      config="$2"
      shift 2
      ;;
    --stage)
      stage="$2"
      shift 2
      ;;
    --evidence)
      evidence_files+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "schema_pow_canary_alarm_gate: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$stage" in
  canary|promoted) ;;
  *)
    echo "schema_pow_canary_alarm_gate: --stage must be canary or promoted" >&2
    exit 2
    ;;
esac

if [ "${#evidence_files[@]}" -eq 0 ]; then
  echo "schema_pow_canary_alarm_gate: at least one --evidence file is required" >&2
  exit 2
fi

jq -e '
  .version == 1
  and .environment == "dev"
  and (.sample_window.minimum_completed_runs >= 1)
  and (.alarms | length >= 1)
  and ([.alarms[].name] | length == (unique | length))
  and ([.alarms[].threshold_count] | all(. >= 1))
  and (.decisions.canary.pass == "promote")
  and (.decisions.canary.fail == "hold")
  and (.decisions.promoted.fail == "rollback")
' "$config" >/dev/null || {
  echo "schema_pow_canary_alarm_gate: invalid gate config: $config" >&2
  exit 2
}

runs_file="$(mktemp "${TMPDIR:-/tmp}/schema-pow-canary-runs.XXXXXX")"
trap 'rm -f "$runs_file"' EXIT

for evidence in "${evidence_files[@]}"; do
  if [ ! -r "$evidence" ]; then
    echo "schema_pow_canary_alarm_gate: evidence is not readable: $evidence" >&2
    exit 2
  fi
  if ! jq -e -s --arg source "$(basename "$evidence")" \
    '{source: $source, events: .}' "$evidence" >>"$runs_file"; then
    echo "schema_pow_canary_alarm_gate: evidence is not valid JSONL: $evidence" >&2
    exit 2
  fi
done

jq -s --slurpfile config "$config" --arg stage "$stage" '
  def has_required_probe($events; $probe):
    if $probe == "valid_registration" then
      any($events[];
        .status == "PASS"
        and has("first_registration_ms")
        and has("repost_ms"))
    elif $probe == "missing_proof_rejected" then
      any($events[];
        .status == "PASS"
        and .negative_proof == "missing"
        and .rejection == "node_key_required")
    elif $probe == "invalid_proof_rejected" then
      any($events[];
        .status == "PASS"
        and .negative_proof == "invalid"
        and .rejection == "proof_of_work_invalid")
    else
      false
    end;

  ($config[0]) as $policy
  | ([ $policy.alarms[].failure_reasons[] ] | unique) as $known_reasons
  | map(
      . as $run
      | ([ $run.events[]
           | select(.status == "FAIL")
           | (.reason // .phase // "unclassified_failure")
           | . as $reason
           | if $known_reasons | index($reason) then $reason else "unclassified_failure" end
         ] | unique) as $reported_failures
      | ([ $policy.required_probes[]
           | select(has_required_probe($run.events; .) | not)
         ]) as $missing_probes
      | ([ $run.events[]
           | ..
           | objects
           | to_entries[]
           | select(.key as $key | $policy.evidence.forbidden_fields | index($key))
           | .key
         ] | unique) as $forbidden_fields
      | ([ $run.events[]
           | select(.private_key_persisted != false)
         ] | length) as $private_key_violations
      | ($reported_failures
          + (if ($missing_probes | length) > 0 then ["required_probe_missing"] else [] end)
          + (if (($forbidden_fields | length) > 0 or $private_key_violations > 0)
             then ["evidence_safety_violation"] else [] end)
        | unique) as $failure_reasons
      | {
          source: $run.source,
          completed: ($failure_reasons | length == 0),
          failure_reasons: $failure_reasons,
          missing_probes: $missing_probes,
          forbidden_field_names: $forbidden_fields
        }
    ) as $runs
  | ([ $policy.alarms[]
       | . as $alarm
       | ([ $runs[].failure_reasons[]
            | . as $reason
            | select($alarm.failure_reasons | index($reason))
          ] | length) as $observed_count
       | {
           name: $alarm.name,
           category: $alarm.category,
           threshold_count: $alarm.threshold_count,
           observed_count: $observed_count,
           active: ($observed_count >= $alarm.threshold_count)
         }
     ]) as $alarms
  | ([ $runs[] | select(.completed) ] | length) as $completed_runs
  | ([ $alarms[] | select(.active) ] | length) as $active_alarm_count
  | ($completed_runs >= $policy.sample_window.minimum_completed_runs
      and $active_alarm_count == 0) as $gate_passed
  | ($policy.decisions[$stage]) as $stage_decisions
  | {
      gate: $policy.gate,
      version: $policy.version,
      environment: $policy.environment,
      stage: $stage,
      sample_window: {
        hours: $policy.sample_window.hours,
        cadence_minutes: $policy.sample_window.cadence_minutes,
        minimum_completed_runs: $policy.sample_window.minimum_completed_runs,
        observed_runs: ($runs | length),
        completed_runs: $completed_runs
      },
      alarms: $alarms,
      failed_runs: [ $runs[] | select(.completed | not) ],
      promotion_ready: $gate_passed,
      rollback_signal: ($stage == "promoted" and ($gate_passed | not)),
      decision: (if $gate_passed then $stage_decisions.pass else $stage_decisions.fail end)
    }
' "$runs_file"
