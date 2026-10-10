#!/usr/bin/env ruby
# frozen_string_literal: true

# CI fan-out cost guard. Every matrix job pays checkout + toolchain + cache
# restore + compile (~5-7 min of billable runner time) before it runs a
# single test, so widening a matrix multiplies billable compute far faster
# than it cuts wall-clock. This lint caps the total shard fan-out of ci.yml
# so a wall-clock optimization cannot silently spiral into a compute
# explosion again.
#
# History (fbrain: fold-ci-cost-profile-2026-07): the 2026-06 wall-clock
# sharding fanned ci.yml out to 267 shards (128-way lib hash partitions),
# billing ~354k runner-minutes (~$2.2k) in June and ~173k minutes on July 1
# alone before it was collapsed by PR #1311 (full suite: 571 minutes).
#
# To raise a cap:
#   1. Measure billable job-minutes before/after on a real proof run
#      (sum of job durations via `gh api .../actions/runs/<id>/jobs`).
#   2. Bump the cap constant below IN THE SAME PR and put the measurement
#      in the PR body's `## Proof` block.
# A cap bump without a billable-minute measurement is not accepted.

require "json"
require "open3"
require "yaml"

# Per planned matrix (scripts/ci/plan-rust-ci-matrices.rb with every family
# enabled). Only the product Clippy matrix enters this guard.
PLANNED_CLIPPY_MAX = 10

# Per static `strategy.matrix.include` array in ci.yml. Current largest:
# app-isolation=12.
STATIC_PER_JOB_MAX = 16

# Whole-workflow ceiling: planned (all families) + every static matrix
# entry. Current value: 45 planned + 14 static = 59.
TOTAL_FANOUT_MAX = 90

workflow_path = ARGV.fetch(0, ".github/workflows/ci.yml")
errors = []

# Static matrices.
workflow = YAML.load_file(workflow_path)
static_total = 0
workflow.fetch("jobs").each do |name, job|
  include_entries = job.dig("strategy", "matrix", "include")
  next unless include_entries.is_a?(Array)

  static_total += include_entries.length
  if include_entries.length > STATIC_PER_JOB_MAX
    errors << "job #{name} has #{include_entries.length} static matrix entries " \
              "(cap #{STATIC_PER_JOB_MAX})"
  end
end

# Planned matrices (full-suite worst case).
stdout, stderr, status = Open3.capture3(
  { "FAM_NODE" => "true" },
  "ruby", "scripts/ci/plan-rust-ci-matrices.rb", "--all"
)
unless status.success?
  warn "lint-ci-fanout: planner failed:\n#{stderr}"
  exit 1
end

planned = {}
stdout.each_line do |line|
  key, _, value = line.partition("=")
  planned[key.strip] = JSON.parse(value).length if key.strip == "clippy_matrix" && value && !value.strip.empty?
end

{
  "clippy_matrix" => PLANNED_CLIPPY_MAX,
}.each do |key, cap|
  count = planned[key]
  if count.nil?
    errors << "planner emitted no #{key}"
  elsif count > cap
    errors << "planned #{key} has #{count} shards (cap #{cap})"
  end
end

planned_total = planned.values.sum
total = planned_total + static_total
if total > TOTAL_FANOUT_MAX
  errors << "total fan-out is #{total} shards (planned #{planned_total} + " \
            "static #{static_total}; cap #{TOTAL_FANOUT_MAX})"
end

if errors.any?
  warn "lint-ci-fanout: #{workflow_path} exceeds the CI fan-out cost caps:"
  errors.each { |e| warn "  - #{e}" }
  warn <<~MSG

    Every matrix job pays ~5-7 min of billable setup+compile before its first
    test; wide fan-out multiplies cost, not speed (June 2026: 267 shards ->
    ~354k runner-minutes, ~$2.2k — fbrain fold-ci-cost-profile-2026-07).
    Prefer fewer shards or the nextest-archive compile-once pattern
    (fkanban fold-ci-compile-once-collapse-shard-compute). To raise a cap,
    include a before/after billable-minute measurement in the PR's Proof
    block and bump the cap constant in scripts/lints/lint-ci-fanout.rb in
    the same PR.
  MSG
  exit 1
end

puts "lint-ci-fanout: ok - #{total} total shards " \
     "(planned #{planned_total} + static #{static_total}, cap #{TOTAL_FANOUT_MAX})"
