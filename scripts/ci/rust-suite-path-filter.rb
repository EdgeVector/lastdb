#!/usr/bin/env ruby
# frozen_string_literal: true

require "optparse"
require "set"

SAFE_WORKFLOW_PATHS = Set.new(%w[
  .github/workflows/bench.yml
  .github/workflows/discord-notify.yml
  .github/workflows/release.yml
])

def docs_path?(path)
  path == "README.md" ||
    path == "SECURITY.md" ||
    path == ".github/pull_request_template.md" ||
    path.start_with?("docs/") ||
    path.end_with?(".md")
end

def safe_non_rust_path?(path)
  docs_path?(path) || SAFE_WORKFLOW_PATHS.include?(path)
end

options = { github_output: ENV["GITHUB_OUTPUT"] }
OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [--github-output PATH] [PATH ...]"
  opts.on("--github-output PATH", "Append rust_suite output for GitHub Actions") do |path|
    options[:github_output] = path
  end
end.parse!

paths = ARGV.empty? ? STDIN.read.lines(chomp: true) : ARGV
paths = paths.map(&:strip).reject(&:empty?).uniq

unsafe_paths = paths.reject { |path| safe_non_rust_path?(path) }
run_rust_suite = paths.empty? || unsafe_paths.any?
reason =
  if paths.empty?
    "no changed paths detected; failing open"
  elsif run_rust_suite
    "rust suite required by #{unsafe_paths.first}"
  else
    "only docs or selected safe workflow paths changed"
  end

if options[:github_output] && !options[:github_output].empty?
  File.open(options[:github_output], "a") do |f|
    f.puts "rust_suite=#{run_rust_suite}"
    f.puts "reason=#{reason}"
  end
end

puts "rust_suite=#{run_rust_suite}"
puts "reason=#{reason}"
