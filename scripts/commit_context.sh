#!/usr/bin/env bash
#
# Workflow: Generate Commit Message with Context (Read‑Only, No File Changes)
# – run this script gather context for your commit message.
#

set -euo pipefail

# 0) Sanity check: make sure we’re in a git repo
if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  echo "✖ Not a git repository!"; exit 1
fi

# 1) Turn off all pagers
export GIT_PAGER=cat

echo
echo "<<<COMMIT CONTEXT START>>>"
echo

# 2) Show staged files (or a notice if none)
echo "Staged files:"
staged=$(git --no-pager diff --cached --name-only)
if [ -z "$staged" ]; then
  echo "  (none — nothing staged!)"
else
  echo "$staged"
fi

# 3) Full zero‑context diffs
echo
echo "Diffs for staged files (zero-context hunks):"
if [ -n "$staged" ]; then
  git --no-pager diff --cached -U0
else
  echo "  (skipped)"
fi

# 4) Condensed view for small changesets
echo
echo "Condensed changes (files with ≤50 hunks):"
if [ -n "$staged" ]; then
  git --no-pager diff --cached --name-only \
  | while IFS= read -r file; do
      hunks=$(git --no-pager diff --cached -U0 "$file" | grep -c '^@@')
      if [ "$hunks" -le 50 ]; then
        echo
        echo "--- $file ($hunks hunks) ---"
        git --no-pager diff --cached -U0 "$file" \
          | grep -E '^\+|^-' \
          | grep -vE '^\+\+\+|^\-\-\-'
      fi
    done
else
  echo "  (skipped)"
fi

echo
echo "<<<COMMIT CONTEXT END>>>"
echo "<<<COMMIT TEMPLATE START>>>"
cat templates/commit-template.git.txt
echo "<<<COMMIT TEMPLATE END>>>"
echo
echo Review the output from the previous command.
echo Follow its instructions to manually compose your commit message using the context displayed.
