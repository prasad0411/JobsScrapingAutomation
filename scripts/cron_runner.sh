#!/bin/bash
BASE_DIR="/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker"
cd "$BASE_DIR" || { echo "FATAL: Cannot cd to $BASE_DIR"; exit 1; }
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export HOME="/Users/prasadkanade"
source venv/bin/activate || { echo "FATAL: Cannot activate venv"; exit 1; }
MODULE="$1"
if [[ -z "$MODULE" ]]; then echo "FATAL: No module specified"; exit 1; fi
LOG_SAFE_NAME=$(basename "$MODULE")
LOG_DIR="$BASE_DIR/.local/cron_logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M')
LOG_FILE="$LOG_DIR/${LOG_SAFE_NAME}_${TIMESTAMP}.log"
HEALTH_FILE="$BASE_DIR/.local/health_${LOG_SAFE_NAME}.json"
echo "=== [$MODULE] started at $(date) ===" >> "$LOG_FILE"
echo "=== Python: $(python3 --version 2>&1) | User: $(whoami) ===" >> "$LOG_FILE"
if [[ "$MODULE" == "outreach" || "$MODULE" == "scripts/send_scheduled" ]]; then
    echo "--- resume_sync ---" >> "$LOG_FILE"
    timeout 30 bash scripts/resume_sync.sh >> "$LOG_FILE" 2>&1 || true
fi
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
cat > "$HEALTH_FILE" <<EOF
{
  "module": "$MODULE",
  "last_run": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "exit_code": $EXIT_CODE,
  "duration_seconds": $DURATION,
  "log": "$LOG_FILE"
}
EOF
if [[ $EXIT_CODE -ne 0 ]]; then
    echo "[$(date)] FAILED: $MODULE (exit $EXIT_CODE) — $LOG_FILE" >> "$BASE_DIR/.local/failures.log"
fi
find "$LOG_DIR" -name "*.log" -mtime +7 -delete
exit $EXIT_CODE
