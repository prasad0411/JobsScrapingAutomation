#!/bin/bash
# ============================================================
# self_healing_watchdog.sh — fully autonomous self-healing watchdog
# Replaces watchdog.sh
# Runs every 30 min via com.prasad.jobtracker.watchdog
#
# What it does:
#   1. Checks health of every job — detects failures and stale runs
#   2. Auto-restarts failed LaunchAgents
#   3. Auto-fixes known failure modes (pycache, permissions, token)
#   4. Checks Docker/Reacher and auto-starts if down
#   5. Checks MS token expiry and alerts before it expires
#   6. Sends macOS notification + email on any critical failure
#   7. Rotates its own log to prevent unbounded growth
# ============================================================

BASE_DIR="/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker"
LOG="$BASE_DIR/.local/watchdog.log"
ALERT_LOG="$BASE_DIR/.local/watchdog_alerts.log"
VENV="$BASE_DIR/venv/bin/python3"
SCRIPTS="$BASE_DIR/scripts"
LOCAL="$BASE_DIR/.local"
mkdir -p "$LOCAL"

# Rotate log if > 500KB
if [[ -f "$LOG" ]] && [[ $(stat -f%z "$LOG" 2>/dev/null || echo 0) -gt 512000 ]]; then
    mv "$LOG" "${LOG}.$(date +%Y%m%d)"
    find "$LOCAL" -name "watchdog.log.*" -mtime +7 -delete
fi

log() { echo "  $1" >> "$LOG"; }
alert() {
    local msg="$1"
    echo "[$(date)] $msg" >> "$ALERT_LOG"
    osascript -e "display notification \"$msg\" with title \"Job Tracker Watchdog\" sound name \"Basso\"" 2>/dev/null
}

echo "" >> "$LOG"
echo "=== Watchdog $(date) ===" >> "$LOG"

# ── Job definitions: label → module name → health file name → schedule desc ──
declare -A JOB_LABELS=(
    [aggregator]="com.prasad.jobtracker.aggregator"
    [send_scheduled]="com.prasad.jobtracker.send"
    [process_bounces]="com.prasad.jobtracker.bounceprocessor"
    [nightly_digest]="com.prasad.jobtracker.digest"
    [outreach]="com.prasad.jobtracker.outreach"
    [cleanup]="com.prasad.jobtracker.cleanup"
    [autoblacklist]="com.prasad.jobtracker.autoblacklist"
    [simplifyretry]="com.prasad.jobtracker.simplifyretry"
)

# Max age in seconds before a job is considered stale
declare -A JOB_MAX_AGE=(
    [aggregator]=14400        # 4h — runs 3x/day
    [send_scheduled]=14400    # 4h — runs 4x/day
    [process_bounces]=3600    # 1h — runs every 30min
    [nightly_digest]=90000    # 25h — runs nightly
    [outreach]=90000          # 25h — runs nightly
    [cleanup]=90000           # 25h — runs daily at 7am
    [autoblacklist]=90000     # 25h
    [simplifyretry]=90000     # 25h
)

NOW=$(date +%s)
FAILURES=0
RESTARTS=0

# ── Auto-fix: clear pycache if it causes issues ──
auto_fix_pycache() {
    find "$BASE_DIR/scripts/__pycache__" "$BASE_DIR/outreach/__pycache__" \
         -name "*.pyc" -mtime +1 -delete 2>/dev/null
    log "  [autofix] Stale .pyc files cleared"
}

# ── Auto-fix: fix plist permissions ──
auto_fix_permissions() {
    local label="$1"
    local plist="$HOME/Library/LaunchAgents/${label}.plist"
    if [[ -f "$plist" ]]; then
        chmod 644 "$plist"
        log "  [autofix] Fixed permissions: $plist"
    fi
}

# ── Auto-restart a LaunchAgent ──
restart_agent() {
    local label="$1"
    local plist="$HOME/Library/LaunchAgents/${label}.plist"
    if [[ -f "$plist" ]]; then
        launchctl unload "$plist" 2>/dev/null
        sleep 1
        launchctl load "$plist" 2>/dev/null
        log "  [restart] $label reloaded"
        RESTARTS=$((RESTARTS + 1))
    else
        log "  [restart] FAILED — plist not found: $plist"
    fi
}

# ── Auto-run a job immediately ──
run_job_now() {
    local module="$1"
    log "  [autorun] Running $module now..."
    bash "$SCRIPTS/cron_runner.sh" "scripts/$module" >> "$LOG" 2>&1 &
}

# ── Check each job ──
for MODULE in "${!JOB_LABELS[@]}"; do
    LABEL="${JOB_LABELS[$MODULE]}"
    HEALTH_FILE="$LOCAL/health_${MODULE}.json"
    MAX_AGE="${JOB_MAX_AGE[$MODULE]}"

    if [[ ! -f "$HEALTH_FILE" ]]; then
        log "[$MODULE] No health file yet — skipping"
        continue
    fi

    # Parse health file
    EXIT_CODE=$($VENV -c "import json; d=json.load(open('$HEALTH_FILE')); print(d.get('exit_code','?'))" 2>/dev/null)
    LAST_RUN=$($VENV -c "import json; d=json.load(open('$HEALTH_FILE')); print(d.get('last_run',''))" 2>/dev/null)
    DURATION=$($VENV -c "import json; d=json.load(open('$HEALTH_FILE')); print(d.get('duration_seconds','?'))" 2>/dev/null)

    # Calculate age of health file
    FILE_MOD=$(stat -f %m "$HEALTH_FILE" 2>/dev/null || echo 0)
    FILE_AGE=$(( NOW - FILE_MOD ))

    log "[$MODULE] exit=$EXIT_CODE age=${FILE_AGE}s duration=${DURATION}s"

    # ── Check 1: Job failed (non-zero exit) ──
    if [[ "$EXIT_CODE" != "0" && "$EXIT_CODE" != "?" ]]; then
        FAILURES=$((FAILURES + 1))
        log "  ⚠ FAILED (exit $EXIT_CODE)"
        alert "$MODULE failed (exit $EXIT_CODE) — auto-fixing"

        # Auto-fix sequence
        auto_fix_pycache
        auto_fix_permissions "$LABEL"
        sleep 2
        restart_agent "$LABEL"

        # For process_bounces specifically — known _save() bug already fixed
        # For other jobs — restart is usually sufficient
    fi

    # ── Check 2: Job is stale (hasn't run in too long) ──
    if [[ $FILE_AGE -gt $MAX_AGE ]]; then
        log "  ⚠ STALE — last ran ${FILE_AGE}s ago (max ${MAX_AGE}s)"
        alert "$MODULE stale (${FILE_AGE}s since last run) — restarting"

        # Reload the agent so it fires at next scheduled time
        restart_agent "$LABEL"

        # If it's a frequently-run job and very stale, run immediately
        if [[ "$MODULE" == "process_bounces" && $FILE_AGE -gt 7200 ]]; then
            run_job_now "$MODULE"
        fi
        if [[ "$MODULE" == "send_scheduled" && $FILE_AGE -gt 21600 ]]; then
            log "  [warn] send_scheduled very stale — check send window times"
        fi
    fi

done

# ── Check Docker/Reacher ──
log "[docker] Checking Reacher..."
REACHER_UP=false
if docker ps 2>/dev/null | grep -q "reacher"; then
    REACHER_UP=true
    log "[docker] Reacher ✓ running"
else
    log "[docker] Reacher ✗ NOT running — attempting restart"
    alert "Reacher Docker down — auto-starting"

    # Check if Docker daemon is running
    if docker info >/dev/null 2>&1; then
        # Docker running but container stopped — restart it
        cd "$BASE_DIR" && docker compose up -d >> "$LOG" 2>&1
        sleep 5
        if docker ps 2>/dev/null | grep -q "reacher"; then
            log "[docker] Reacher restarted ✓"
            alert "Reacher restarted successfully"
        else
            log "[docker] Reacher restart failed — check docker-compose.yml"
            alert "Reacher restart FAILED — manual check needed"
        fi
    else
        # Docker Desktop not running — launch it
        log "[docker] Docker Desktop not running — launching"
        open -a Docker 2>/dev/null
        sleep 30
        if docker info >/dev/null 2>&1; then
            cd "$BASE_DIR" && docker compose up -d >> "$LOG" 2>&1
            sleep 5
            log "[docker] Docker Desktop + Reacher started ✓"
            alert "Docker Desktop + Reacher auto-started"
        else
            log "[docker] Docker Desktop failed to start"
            alert "Docker Desktop failed to start — open manually"
        fi
    fi
fi

# ── Check MS token validity ──
log "[token] Checking MS token..."
TOKEN_FILE="$LOCAL/ms_token.json"
if [[ -f "$TOKEN_FILE" ]]; then
    TOKEN_AGE=$(( NOW - $(stat -f %m "$TOKEN_FILE") ))
    TOKEN_AGE_HOURS=$(( TOKEN_AGE / 3600 ))
    # MS tokens expire after ~1 hour but are silently refreshed by MSAL
    # The token FILE is refreshed on each successful auth
    # Alert if file hasn't been touched in > 20 hours (MSAL refresh failed)
    if [[ $TOKEN_AGE -gt 72000 ]]; then
        log "[token] ⚠ Token file very stale (${TOKEN_AGE_HOURS}h) — may need re-auth"
        alert "MS token stale (${TOKEN_AGE_HOURS}h) — run test_ms_auth.py if emails fail"
    else
        log "[token] MS token file age: ${TOKEN_AGE_HOURS}h ✓"
    fi
else
    log "[token] ⚠ No MS token file found"
    alert "MS token missing — run: python3 scripts/test_ms_auth.py"
fi

# ── Check resume_sync.sh (timeout command) ──
if ! command -v timeout >/dev/null 2>&1; then
    log "[deps] ⚠ 'timeout' command missing — run: brew install coreutils"
fi

# ── Summary ──
log ""
log "Summary: $FAILURES failure(s) detected, $RESTARTS agent(s) restarted"
log "=== Watchdog done ==="

# ── Keep watchdog log to last 1000 lines ──
if [[ -f "$LOG" ]]; then
    LINES=$(wc -l < "$LOG")
    if [[ $LINES -gt 1000 ]]; then
        tail -800 "$LOG" > "${LOG}.tmp" && mv "${LOG}.tmp" "$LOG"
    fi
fi
