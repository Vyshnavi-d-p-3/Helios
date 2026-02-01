#!/usr/bin/env sh
set -eu

# commit-clean.sh creates a commit without editor-injected footer lines.
# It uses git commit-tree (bypasses local commit message mutation) and then
# moves the current branch tip to the new commit.
#
# Usage:
#   scripts/commit-clean.sh "subject"
#   scripts/commit-clean.sh "subject" "body line 1\nbody line 2"
#   scripts/commit-clean.sh -F /path/to/message.txt

if [ "${1:-}" = "" ]; then
  echo "usage: scripts/commit-clean.sh <subject> [body]"
  echo "   or: scripts/commit-clean.sh -F <message-file>"
  exit 1
fi

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "error: must run inside a git repository"
  exit 1
fi

# Ensure there is staged content and no unstaged changes in staged files.
if git diff --cached --quiet; then
  echo "error: no staged changes. stage files first (git add ...)"
  exit 1
fi

tree="$(git write-tree)"
parent="$(git rev-parse HEAD)"

msg_file="$(mktemp)"
cleanup() { rm -f "$msg_file"; }
trap cleanup EXIT INT TERM

if [ "$1" = "-F" ]; then
  if [ "${2:-}" = "" ]; then
    echo "error: -F requires a message file path"
    exit 1
  fi
  cp "$2" "$msg_file"
else
  subject="$1"
  body="${2:-}"
  printf "%s\n" "$subject" >"$msg_file"
  if [ "$body" != "" ]; then
    printf "\n%s\n" "$body" >>"$msg_file"
  fi
fi

new_commit="$(git commit-tree "$tree" -p "$parent" -F "$msg_file")"
branch_ref="$(git symbolic-ref -q HEAD)"
if [ "$branch_ref" = "" ]; then
  echo "error: detached HEAD is not supported by this helper"
  exit 1
fi

git update-ref "$branch_ref" "$new_commit"
echo "created commit: $new_commit"
