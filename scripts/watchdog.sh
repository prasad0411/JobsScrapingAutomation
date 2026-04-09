#!/bin/bash
# ============================================================
# self_healing_watchdog.sh — fully autonomous self-healing watchdog
# Compatible with macOS bash 3.2 (no associative arrays)
# ============================================================

BASE_DIR="/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker"
LOG="$BASE_DIR/.local/watchdog.log"
ALERT_LOG="$BASE_DIR/.local/watchdog_alerts.log"
LOCAL="$BASE_DIR/.local"
SCRIPTS="$BASE_DIR/scripts"
mkdir -p "$LOCAL"

if [[ -f "$LOG" ]] && [[ $(stat -f%z "$LOG" 2>/dev/null || echo 0) -gt 512000 ]]; then
    mv "$LOG" "${LOG}.$(date +%Y%m%d)"
    find "$LOCAL" -name "watchdog.log.*" -mtime +7 -delete
fi

log()   { echo "  $1" >> "$LOG"; }
alert() {
    echo "[$(date)] $1" >> "$ALERT_LOG"
    osascript -e "display notification \"$1\" with title \"Job Tracker Watchdog\" sound name \"Basso\"" 2>/dev/null
}

echo "" >> "$LOG"
echo "=== Watchdog $(date) ===" >> "$LOG"

NOW=$(date +%s)
FAILURES=0
RESTARTS=0

# Jobs: "health_name|launchd_label|max_age_sec"
JOBS=(
    "aggregator|com.prasad.jobtracker.aggregator.smart|28800"
    "send_scheduled|com.prasad.jobtracker.send.smart|86400"
    "process_bounces|com.prasad.jobtracker.bounceprocessor.smart|3600"
    "nightly_digest|com.prasad.jobtracker.digest.smart|108000"
    "outreach|com.prasad.jobtracker.outreach.smart|108000"
    "cleanup_not_applied|com.prasad.jobtracker.cleanup.smart|108000"
    "build_auto_blacklist|com.prasad.jobtracker.autoblacklist.smart|108000"
    "retry_simplify|com.prasad.jobtracker.simplifyretry.smart|108000"
)

auto_fix_pycache() {
    find "$BASE_DIR/scripts/__pycache__" "$BASE_DIR/outreach/__pycache__" "$BASE_DIR/aggregator/__pycache__" \
         -name "*.pyc" -mtime +1 -delete 2>/dev/null
    log "  [autofix] Cleared stale .pyc files"
}

restart_agent() {
    local label="$1"
    local plist="$HOME/Library/LaunchAgents/${label}.plist"
    if [[ -f "$plist" ]]; then
        launchctl unload "$plist" 2>/dev/null
        sleep 2
        launchctl load "$plist"
        sleep 1
        # Verify it loaded correctly — exit 78 means config error, try once more
        local exit_code=$(launchctl print gui/$(id -u)/${label} 2>/dev/null | grep "last exit code" | grep -o "[0-9]*" | head -1)
        if [[ "$exit_code" == "78" ]]; then
            log "  [restart] exit 78 detected — force reloading $label"
            launchctl remove "$label" 2>/dev/null
            sleep 2
            launchctl load "$plist"
        fi
        log "  [restart] $label reloaded"
        RESTARTS=$((RESTARTS + 1))
    else
        log "  [restart] FAILED — plist not found: $plist"
    fi
}

for JOB_DEF in "${JOBS[@]}"; do
    MODULE=$(echo "$JOB_DEF" | cut -d'|' -f1)
    LABEL=$(echo "$JOB_DEF"  | cut -d'|' -f2)
    MAX_AGE=$(echo "$JOB_DEF"| cut -d'|' -f3)
    HEALTH_FILE="$LOCAL/health_${MODULE}.json"

    if [[ ! -f "$HEALTH_FILE" ]]; then
        log "[$MODULE] No health file yet"
        continue
    fi

    EXIT_CODE=$(python3 -c "import json; d=json.load(open('$HEALTH_FILE')); print(d.get('exit_code','?'))" 2>/dev/null)
    LAST_RUN=$(python3  -c "import json; d=json.load(open('$HEALTH_FILE')); print(d.get('last_run',''))" 2>/dev/null)
    DURATION=$(python3  -c "import json; d=json.load(open('$HEALTH_FILE')); print(d.get('duration_seconds','?'))" 2>/dev/null)
    FILE_MOD=$(stat -f %m "$HEALTH_FILE" 2>/dev/null || echo 0)
    FILE_AGE=$(( NOW - FILE_MOD ))

    log "[$MODULE] exit=$EXIT_CODE age=${FILE_AGE}s dur=${DURATION}s last=$LAST_RUN"

    if [[ "$EXIT_CODE" != "0" && "$EXIT_CODE" != "?" ]]; then
        FAILURES=$((FAILURES + 1))
        log "  ⚠ FAILED — auto-fixing and re-running job directly"
        alert "$MODULE failed (exit $EXIT_CODE) — re-running"
        auto_fix_pycache
        sleep 2
        bash "$SCRIPTS/cron_runner.sh" "$MODULE" >> "$LOG" 2>&1 &
    fi

    if [[ $FILE_AGE -gt $MAX_AGE ]]; then
        log "  ⚠ STALE — ${FILE_AGE}s since last run"
        if [[ "$MODULE" == "process_bounces" && $FILE_AGE -gt 7200 ]]; then
            log "  [autorun] Running process_bounces immediately"
            bash "$SCRIPTS/cron_runner.sh" scripts/process_bounces >> "$LOG" 2>&1 &
        fi
    fi
done

# Docker/Reacher check
log "[docker] Checking Reacher..."
if docker ps 2>/dev/null | grep -q "reacher"; then
    log "[docker] Reacher ✓ running"
else
    log "[docker] Reacher ✗ NOT running — attempting restart"
    alert "Reacher Docker down — auto-starting"
    if docker info >/dev/null 2>&1; then
        cd "$BASE_DIR" && docker compose up -d >> "$LOG" 2>&1
        sleep 5
        docker ps 2>/dev/null | grep -q "reacher" && log "[docker] Reacher restarted ✓" || \
            { log "[docker] Reacher restart failed"; alert "Reacher restart FAILED — check manually"; }
    else
        open -a Docker 2>/dev/null
        sleep 30
        docker info >/dev/null 2>&1 && cd "$BASE_DIR" && docker compose up -d >> "$LOG" 2>&1 && \
            log "[docker] Docker + Reacher started ✓" || \
            { log "[docker] Docker failed to start"; alert "Docker failed — open manually"; }
    fi
fi

# MS token check
log "[token] Checking MS token..."
TOKEN_FILE="$LOCAL/ms_token.json"
if [[ -f "$TOKEN_FILE" ]]; then
    TOKEN_AGE=$(( NOW - $(stat -f %m "$TOKEN_FILE") ))
    TOKEN_HOURS=$(( TOKEN_AGE / 3600 ))
    if [[ $TOKEN_AGE -gt 72000 ]]; then
        log "[token] ⚠ Token stale (${TOKEN_HOURS}h) — run test_ms_auth.py"
        alert "MS token stale (${TOKEN_HOURS}h) — run test_ms_auth.py"
    else
        log "[token] MS token age: ${TOKEN_HOURS}h ✓"
    fi
else
    log "[token] ⚠ No MS token file found"
    alert "MS token missing — run: python3 scripts/test_ms_auth.py"
fi

log ""
log "Summary: $FAILURES failure(s), $RESTARTS restart(s)"
log "=== Watchdog done ==="

# Trim log to 1000 lines
if [[ -f "$LOG" ]]; then
    LINES=$(wc -l < "$LOG")
    if [[ $LINES -gt 1000 ]]; then
        tail -800 "$LOG" > "${LOG}.tmp" && mv "${LOG}.tmp" "$LOG"
    fi
fi
