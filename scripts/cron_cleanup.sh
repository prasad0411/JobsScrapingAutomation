#!/bin/bash
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd "/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker"
source venv/bin/activate

LOG_DIR=".local/cron_logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M')
LOG_FILE="$LOG_DIR/cleanup_${TIMESTAMP}.log"

echo "=== Cleanup started at $(date) ===" >> "$LOG_FILE"
python3 scripts/cleanup_not_applied.py >> "$LOG_FILE" 2>&1
echo "=== Cleanup finished at $(date) ===" >> "$LOG_FILE"

find "$LOG_DIR" -name "*.log" -mtime +7 -delete
