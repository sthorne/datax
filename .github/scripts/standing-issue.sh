#!/usr/bin/env bash
# standing-issue.sh <workflow> <red|green> [details-file]
#
# A scheduled workflow's result, written where it is read (issue #244).
# A red scheduled run on main is visible only to someone who opens the
# Actions tab, and nobody does that on a quiet repository: the weekly
# govulncheck run was red for seven weeks before anyone saw it. So a red
# run files one standing issue per workflow — or, when it is already
# open, adds the new run to it — carrying the run's link and the tail of
# its output; a green run closes the issue if one is open and otherwise
# does nothing. One issue per workflow, updated, not one per run: the
# alarm has to stay quieter than the thing it reports.
#
# Needs: gh (on the hosted runners), GH_TOKEN with issues: write,
# GITHUB_REPOSITORY, GITHUB_SERVER_URL, GITHUB_RUN_ID.
set -euo pipefail

workflow=$1
state=$2
details=${3:-}
label=scheduled-run
title="Scheduled run red: ${workflow}"
run_url="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
footer=$'\n---\n_Filed by the `'"${workflow}"$'` workflow on its schedule (.github/scripts/standing-issue.sh, issue #244)._'

# The open standing issue for this workflow, if any (by exact title under
# the label; the label is created on first use).
gh label create "$label" --description "a scheduled workflow is red on main" --color d93f0b --force >/dev/null 2>&1 || true
existing=$(gh issue list --repo "$GITHUB_REPOSITORY" --state open --label "$label" --json number,title \
  --jq ".[] | select(.title == \"${title}\") | .number" | head -n1)

tail_block() {
  # The last lines of the run's output, fenced, when a details file was given.
  if [ -n "$details" ] && [ -s "$details" ]; then
    printf '\n\nThe run'"'"'s last lines:\n\n```\n'
    tail -n 60 "$details"
    printf '\n```\n'
  fi
}

case "$state" in
  red)
    body="The \`${workflow}\` workflow's scheduled run on \`main\` is red: ${run_url}$(tail_block)${footer}"
    if [ -n "$existing" ]; then
      gh issue comment "$existing" --repo "$GITHUB_REPOSITORY" --body "Still red on today's run: ${run_url}$(tail_block)${footer}"
      echo "updated standing issue #${existing}"
    else
      gh issue create --repo "$GITHUB_REPOSITORY" --title "$title" --label "$label" --body "$body"
      echo "filed a standing issue"
    fi
    ;;
  green)
    if [ -n "$existing" ]; then
      gh issue comment "$existing" --repo "$GITHUB_REPOSITORY" --body "Green again: ${run_url}${footer}"
      gh issue close "$existing" --repo "$GITHUB_REPOSITORY" --reason completed
      echo "closed standing issue #${existing}"
    else
      echo "green, and no standing issue to close"
    fi
    ;;
  *)
    echo "usage: $0 <workflow> <red|green> [details-file]" >&2
    exit 2
    ;;
esac
