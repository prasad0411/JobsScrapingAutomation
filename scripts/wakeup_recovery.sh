#!/bin/bash
# Wakeup recovery — runs missed jobs when Mac wakes from sleep
BASE="/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker"
LOG="$BASE/.local/wakeup.log"
SCRIPTS="$BASE/scripts/cron_runner.sh"
LOCKFILE="$BASE/.local/wakeup.lock"

# Prevent parallel runs
if [ -f "$LOCKFILE" ]; then
    pid=$(cat "$LOCKFILE")
    if kill -0 "$pid" 2>/dev/null; then
        exit 0  # Already running
    fi
fi
echo $$ > "$LOCKFILE"
trap "rm -f $LOCKFILE" EXIT

source "$BASE/venv/bin/activate" 2>/dev/null

now=$(date +%s)
echo "=== Wakeup check $(date) ===" >> "$LOG"

# Auto-fix exit 78 for all agents
for label in aggregator autoblacklist bounceprocessor cleanup digest outreach send simplifyretry; do
    full_label="com.prasad.jobtracker.${label}.smart"
    exit_code=$(launchctl print gui/$(id -u)/${full_label} 2>/dev/null | grep "last exit code" | grep -o "[0-9]*" | head -1)
    if [[ "$exit_code" == "78" ]]; then
        echo "  [fix78] ${full_label} has exit 78 — reloading" >> "$LOG"
        launchctl remove "$full_label" 2>/dev/null
        sleep 1
        launchctl load "$HOME/Library/LaunchAgents/${full_label}.plist" 2>/dev/null
        echo "  [fix78] ${full_label} reloaded" >> "$LOG"
    fi
done

# Job definitions: "module|health_file|max_gap_seconds|schedule_description"
JOBS=(
    "aggregator|health_aggregator|7200|8AM/3PM/9PM"
    "scripts/send_scheduled|health_send_scheduled|86400|9AM-12:30PM"
    "scripts/process_bounces|health_process_bounces|3600|every 30min"
    "scripts/nightly_digest|health_nightly_digest|108000|12:22AM"
    "outreach|health_outreach|108000|midnight"
    "scripts/cleanup_not_applied|health_cleanup_not_applied|108000|7AM"
    "scripts/build_auto_blacklist|health_build_auto_blacklist|108000|12:30AM"
    "scripts/retry_simplify|health_retry_simplify|108000|6AM"
)

for JOB_DEF in "${JOBS[@]}"; do
    MODULE=$(echo "$JOB_DEF" | cut -d'|' -f1)
    HEALTH=$(echo "$JOB_DEF"  | cut -d'|' -f2)
    MAX_GAP=$(echo "$JOB_DEF" | cut -d'|' -f3)
    DESC=$(echo "$JOB_DEF"    | cut -d'|' -f4)

    HEALTH_FILE="$BASE/.local/${HEALTH}.json"
    if [[ ! -f "$HEALTH_FILE" ]]; then
        continue
    fi

    # Get last run timestamp
    last_run=$(python3 -c "
import json, datetime
d = json.load(open('$HEALTH_FILE'))
lr = d.get('last_run', '')
if lr:
    dt = datetime.datetime.strptime(lr, '%Y-%m-%dT%H:%M:%SZ')
    print(int(dt.timestamp()))
else:
    print(0)
" 2>/dev/null)

    if [[ -z "$last_run" || "$last_run" == "0" ]]; then
        continue
    fi

    gap=$((now - last_run))

    if [[ $gap -gt $MAX_GAP ]]; then
        echo "  [$MODULE] Missed — ${gap}s since last run (max ${MAX_GAP}s) — running now" >> "$LOG"
        bash "$SCRIPTS" "$MODULE" >> "$LOG" 2>&1
    else
        echo "  [$MODULE] OK — ${gap}s since last run" >> "$LOG"
    fi
done

echo "=== Wakeup done ===" >> "$LOG"
