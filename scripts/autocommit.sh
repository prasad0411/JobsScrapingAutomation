#!/bin/bash
# Auto-commit with generated message
# Usage: ./scripts/autocommit.sh
cd "$(dirname "$0")/.."

git add -A

DIRS=$(git diff --cached --name-only | sed 's|/[^/]*$||' | sort -u | head -3 | tr '\n' ', ' | sed 's/,$//')
FILES=$(git diff --cached --name-only | wc -l | tr -d ' ')
ADDED=$(git diff --cached --name-only --diff-filter=A | wc -l | tr -d ' ')
MOD=$(git diff --cached --name-only --diff-filter=M | wc -l | tr -d ' ')

TYPE="chore"
[ "$ADDED" -gt 0 ] && TYPE="feat"
git diff --cached --name-only | grep -q "test_" && TYPE="test"
git diff --cached --name-only | grep -qi "fix\|patch\|bug" && TYPE="fix"

MSG="${TYPE}: update ${DIRS} (${FILES} files: ${ADDED} added, ${MOD} modified)"
echo "Committing: $MSG"
git commit -m "$MSG"
git push
