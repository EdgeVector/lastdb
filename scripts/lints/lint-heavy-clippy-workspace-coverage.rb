#!/usr/bin/env ruby
# frozen_string_literal: true

# Assert that fold's heavy clippy lane actually LINTS THE WHOLE WORKSPACE.
#
# Why this exists (card `fold-heavy-clippy-lane-not-a-trustworthy-gate`, and the
# eight Brain papercuts it collects). The required Mini gate deliberately
# compiles only `-p lastdb_node --lib --bins`, so the heavy lane in
# `.github/workflows/ci-required.yml` has the required heavy job that lints
# the other workspace members. That makes its coverage claim load-bearing — and
# nothing checked the claim. Two ways it can quietly become a lie:
#
#   1. A new workspace member lands and no clippy invocation in the heavy lane
#      selects it. `--workspace` covers new members automatically, so this bites
#      through `--exclude`: excluding a package moves it out of the bulk resolve
#      and, unless the off-lane step names it back, out of every lint in the
#      repo. The two current excludes (the ONNX force-enablers) ARE named back;
#      `lint-workspace-fastembed-unification.sh` keeps that pair in sync, but it
#      only knows about fastembed. A third `--exclude` added for any other
#      reason makes the lane cheaper, greener, and blind, and that lint stays OK.
#
#   2. The bulk step drops `--all-targets` or `-D warnings`. Both look like
#      cosmetic flag edits and both silently stop the lane from failing on real
#      lints. `--lib --bins` not compiling test targets is exactly how
#      `papercut-fold-clippy-all-targets-red-await-holding-lock-host-rs` sat red
#      in test code that no required lane could see.
#
# The failure invariant the card names: a linter that is the sole coverage for
# most of a large Rust workspace AND cannot be trusted to have covered it is
# functionally no linter at all. A green run whose coverage shrank is worse than
# a red one, because nobody goes looking.
#
# The rule, in one line: the union of what the heavy lane's clippy invocations
# select must equal the workspace member set, every member must be reachable,
# and the invocation covering the bulk must still be able to fail.
#
# Usage:
#   ruby scripts/lints/lint-heavy-clippy-workspace-coverage.rb [workflow.yml...]
#
# Self-tests override the member set so they can run without cargo:
#   HEAVY_CLIPPY_LINT_MEMBERS="a,b,c" ruby ... fixture.yml

require "yaml"
require "json"
require "open3"

DEFAULT_WORKFLOWS = [".github/workflows/ci-required.yml"].freeze
HEAVY_JOB = "full_workspace_check"
REPO_ROOT = File.expand_path("../..", __dir__)

# Resolve the workspace member set. `cargo metadata --no-deps` does not build
# anything, so this stays cheap enough for the required gate. The env override
# exists for the self-test, which must not depend on the real manifest tree.
def workspace_members
  override = ENV["HEAVY_CLIPPY_LINT_MEMBERS"]
  unless override.nil? || override.strip.empty?
    return override.split(/[,\s]+/).reject(&:empty?).sort
  end

  stdout, stderr, status = Open3.capture3(
    "cargo", "metadata", "--no-deps", "--format-version", "1",
    chdir: REPO_ROOT
  )
  unless status.success?
    warn stderr
    abort "FAIL: cargo metadata --no-deps failed; cannot verify heavy-lane coverage"
  end

  meta = JSON.parse(stdout)
  ids = meta.fetch("workspace_members", []).to_set rescue nil
  ids ||= meta.fetch("workspace_members", [])
  meta.fetch("packages", [])
      .select { |p| ids.include?(p["id"]) }
      .map { |p| p["name"] }
      .sort
end

# Comments inside a `run:` block are not execution. A sibling lint was once
# satisfied by a commented-out command; strip them before matching.
def executable_lines(script)
  script.to_s.lines.map { |line| line.sub(/(?<![\w\-])#.*/, "") }
end

# Join YAML `run:` continuations so a multi-line `cargo clippy \` invocation is
# read as the single shell command it is. The heavy lane writes every clippy
# call that way, so a per-line matcher would see `--exclude` as its own command.
def shell_commands(script)
  joined = executable_lines(script).join.gsub(/\\\n/, " ")
  joined.split(/\n|&&|;/).map(&:strip).reject(&:empty?)
end

def clippy_invocations(job)
  job.fetch("steps", []).flat_map do |step|
    shell_commands(step["run"]).select do |cmd|
      cmd.match?(/\bcargo\b.*\bclippy\b/)
    end
  end
end

# What one `cargo clippy` invocation selects, given the full member set.
# `--workspace` (or `--all`) means everything minus its `--exclude`s; otherwise
# only the packages named with `-p` / `--package`.
def selected_packages(cmd, members)
  tokens = cmd.split(/\s+/)
  named = []
  excluded = []

  tokens.each_with_index do |tok, i|
    case tok
    when "-p", "--package" then named << tokens[i + 1]
    when "--exclude" then excluded << tokens[i + 1]
    else
      named << Regexp.last_match(1) if tok =~ /\A(?:-p|--package)=(.+)\z/
      excluded << Regexp.last_match(1) if tok =~ /\A--exclude=(.+)\z/
    end
  end

  if tokens.include?("--workspace") || tokens.include?("--all")
    members - excluded.compact
  else
    named.compact
  end
end

def bulk_invocation?(cmd)
  tokens = cmd.split(/\s+/)
  tokens.include?("--workspace") || tokens.include?("--all")
end

paths = ARGV.empty? ? DEFAULT_WORKFLOWS : ARGV
members = workspace_members
abort "FAIL: resolved an empty workspace member set" if members.empty?

errors = []
proofs = []

paths.each do |path|
  full = path.start_with?("/") ? path : File.join(REPO_ROOT, path)
  unless File.exist?(full)
    errors << "missing workflow #{path}"
    next
  end

  doc = YAML.safe_load(File.read(full), alias_dependencies: true) rescue YAML.load(File.read(full))
  jobs = doc.is_a?(Hash) ? doc.fetch("jobs", {}) : {}
  if jobs.empty?
    errors << "#{path}: no jobs"
    next
  end

  heavy_job = jobs[HEAVY_JOB]
  unless heavy_job.is_a?(Hash)
    errors << "#{path}: missing heavy clippy job `#{HEAVY_JOB}`"
    next
  end

  invocations = clippy_invocations(heavy_job)
  if invocations.empty?
    errors << "#{path}: no `cargo clippy` invocation at all — this workflow is " \
              "the only linter for most of the workspace"
    next
  end

  covered = invocations.flat_map { |cmd| selected_packages(cmd, members) }.uniq
  uncovered = members - covered
  unless uncovered.empty?
    errors << <<~MSG
      #{path}: #{uncovered.size} workspace member(s) are linted by NOTHING:

        #{uncovered.join("\n        ")}

      The required Mini gate only lints `-p lastdb_node --lib --bins`, so a
      package this lane does not select has no linter in the repo. If an
      `--exclude` was added to the bulk step, add a matching off-lane
      `cargo clippy -p <pkg>` step in the same workflow (that is how the two
      ONNX force-enablers stay covered). Do not drop coverage silently.
    MSG
  end

  # A bulk invocation that cannot fail, or that skips test targets, is coverage
  # on paper only.
  invocations.select { |cmd| bulk_invocation?(cmd) }.each do |cmd|
    unless cmd.include?("--all-targets")
      errors << "#{path}: bulk workspace clippy has no `--all-targets`, so test " \
                "code is never linted (the await_holding_lock rot in " \
                "lastdb_node/src/host.rs is what that costs): #{cmd}"
    end
    unless cmd.match?(/--\s+.*-D\s+warnings/)
      errors << "#{path}: bulk workspace clippy does not pass `-- -D warnings`, " \
                "so lints are advisory and the lane stays green while red: #{cmd}"
    end
  end

  # `continue-on-error` turns a red lane into a green one. The lane's whole
  # problem is already that people can ignore it; do not let the YAML do it.
  { HEAVY_JOB => heavy_job }.each do |name, job|
    if job["continue-on-error"] == true
      errors << "#{path}: job `#{name}` sets continue-on-error: true — a lint " \
                "lane that cannot fail is not a lint lane"
    end
    job.fetch("steps", []).each do |step|
      next unless step["continue-on-error"] == true
      next unless shell_commands(step["run"]).any? { |c| c.match?(/\bcargo\b.*\bclippy\b/) }

      errors << "#{path}: a clippy step sets continue-on-error: true"
    end
  end

  proofs << "#{path}: #{covered.uniq.size}/#{members.size} workspace members " \
            "linted across #{invocations.size} clippy invocation(s)"
end

if errors.empty?
  proofs.each { |p| puts "OK: #{p}" }
  puts
  puts "heavy clippy workspace-coverage lint passed"
  puts "  Members: #{members.size} (from cargo metadata --no-deps)"
  exit 0
end

warn "heavy clippy workspace-coverage lint FAILED"
warn ""
errors.each { |e| warn "FAIL: #{e}" }
exit 1
