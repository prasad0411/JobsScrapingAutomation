#!/bin/bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd "/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker"
source venv/bin/activate
MODULE="$1"
LOG_DIR=".local/cron_logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M')
LOG_FILE="$LOG_DIR/${MODULE}_${TIMESTAMP}.log"
echo "=== $MODULE started at $(date) ===" >> "$LOG_FILE"

# Sync latest resumes from Downloads before running
bash scripts/resume_sync.sh >> "$LOG_FILE" 2>&1

# Support both module paths (e.g. "aggregator") and script paths (e.g. "scripts/send_scheduled")
if [[ "$MODULE" == scripts/* ]]; then
    python3 "${MODULE}.py" >> "$LOG_FILE" 2>&1
else
    python3 -m "$MODULE" >> "$LOG_FILE" 2>&1
fi
EXIT_CODE=$?
echo "=== $MODULE finished at $(date) (exit: $EXIT_CODE) ===" >> "$LOG_FILE"
# Keep only last 7 days of logs
find "$LOG_DIR" -name "*.log" -mtime +7 -delete
