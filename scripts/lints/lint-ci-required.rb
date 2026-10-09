#!/usr/bin/env ruby
# frozen_string_literal: true

require "set"
require "yaml"

workflow_path = ARGV.fetch(0, ".github/workflows/ci.yml")
workflow = YAML.load_file(workflow_path)
jobs = workflow.fetch("jobs")

errors = []

ci_required = jobs["ci-required"]
errors << "missing jobs.ci-required" unless ci_required

if ci_required
  needs = ci_required.fetch("needs", [])
  needs = [needs] if needs.is_a?(String)
  needs = needs.to_a

  missing_jobs = needs.reject { |job| jobs.key?(job) }
  missing_jobs.each do |job|
    errors << "ci-required needs unknown job: #{job}"
  end

  excluded_from_umbrella = Set["ci-required", "merge-queue-failure-report"]
  missing_from_umbrella = jobs.keys.reject { |job| excluded_from_umbrella.include?(job) || needs.include?(job) }
  missing_from_umbrella.each do |job|
    errors << "job is not covered by ci-required: #{job}"
  end

  if ci_required["if"].to_s.strip != "always()"
    errors << "ci-required must use `if: always()` so it evaluates failed/skipped needs"
  end
end

# The merge queue policy for this repo is: substantive jobs run on pull_request
# before enqueue; merge_group only evaluates the umbrella and optional reporter.
merge_group_jobs = Set["ci-required", "merge-queue-failure-report"]
jobs.each do |name, job|
  next if merge_group_jobs.include?(name)

  condition = job["if"].to_s
  next if condition.include?("github.event_name != 'merge_group'")

  errors << "substantive job lacks merge_group skip condition: #{name}"
end

if errors.any?
  warn "lint-ci-required: #{workflow_path} has CI structure errors:"
  errors.each { |error| warn "  - #{error}" }
  exit 1
end

puts "lint-ci-required: ok - ci-required covers #{jobs.length - 2} substantive jobs and merge_group stays slim."
