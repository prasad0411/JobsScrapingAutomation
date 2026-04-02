#!/bin/bash

# ============================================================
# cron_runner.sh — self-healing job runner for Job Hunt Tracker
# ============================================================

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export HOME="/Users/prasadkanade"

# Wait for network (launchd-safe)
_WAIT=0
while ! curl -s --head https://www.google.com >/dev/null; do
    sleep 5
    _WAIT=$((_WAIT + 5))
    if [[ $_WAIT -ge 60 ]]; then
        echo "FATAL: No network after 60s — aborting"
        exit 1
    fi
done

BASE_DIR="/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker"
cd "$BASE_DIR" || { echo "FATAL: Cannot cd to $BASE_DIR"; exit 1; }

source venv/bin/activate || { echo "FATAL: Cannot activate venv"; exit 1; }

MODULE="$1"
if [[ -z "$MODULE" ]]; then
    echo "FATAL: No module specified"
    exit 1
fi

# ── Fix: sanitize module name for log path (strips slashes like scripts/send_scheduled → send_scheduled)
LOG_SAFE_NAME=$(basename "$MODULE")

LOG_DIR="$BASE_DIR/.local/cron_logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date '+%Y-%m-%d_%H-%M')
LOG_FILE="$LOG_DIR/${LOG_SAFE_NAME}_${TIMESTAMP}.log"
HEALTH_FILE="$BASE_DIR/.local/health_${LOG_SAFE_NAME}.json"

echo "=== [$MODULE] started at $(date) ===" >> "$LOG_FILE"
echo "=== Running as: $(whoami) | Python: $(python3 --version 2>&1) ===" >> "$LOG_FILE"

# ── Resume sync (with timeout so it can't block the main job)
# Only sync resume for jobs that actually send emails
if [[ "$MODULE" == "outreach" || "$MODULE" == "scripts/send_scheduled" ]]; then
    echo "--- resume_sync start ---" >> "$LOG_FILE"
    timeout 30 bash scripts/resume_sync.sh >> "$LOG_FILE" 2>&1
    SYNC_EXIT=$?
    if [[ $SYNC_EXIT -ne 0 ]]; then
        echo "WARNING: resume_sync.sh exited with code $SYNC_EXIT (continuing anyway)" >> "$LOG_FILE"
    fi
    echo "--- resume_sync done ---" >> "$LOG_FILE"
fi

# ── Run the actual job
START_TS=$(date +%s)

if [[ "$MODULE" == scripts/* ]]; then
    python3 "${MODULE}.py" >> "$LOG_FILE" 2>&1
else
    python3 -m "$MODULE" >> "$LOG_FILE" 2>&1
fi

EXIT_CODE=$?
END_TS=$(date +%s)
DURATION=$((END_TS - START_TS))

echo "=== [$MODULE] finished at $(date) | exit: $EXIT_CODE | duration: ${DURATION}s ===" >> "$LOG_FILE"

# ── Write health status (self-healing: lets watchdog know what happened)
cat > "$HEALTH_FILE" <<EOF
{
  "module": "$MODULE",
  "last_run": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "exit_code": $EXIT_CODE,
  "duration_seconds": $DURATION,
  "log": "$LOG_FILE"
}
EOF

# ── Self-alert: if job failed, append to a master failure log
if [[ $EXIT_CODE -ne 0 ]]; then
    FAIL_LOG="$BASE_DIR/.local/failures.log"
    echo "[$(date)] FAILED: $MODULE (exit $EXIT_CODE) — see $LOG_FILE" >> "$FAIL_LOG"
fi

# ── Keep only last 7 days of logs
find "$LOG_DIR" -name "*.log" -mtime +7 -delete

exit $EXIT_CODE
