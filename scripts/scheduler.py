#!/usr/bin/env python3
"""
Permanent scheduler daemon — replaces all launchd plists.
Single KeepAlive process, no exit 78, handles sleep/wake gracefully.
"""
import time
import datetime
import subprocess
import logging
import os
import json
import signal
import sys

BASE = "/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker"
LOG_FILE = f"{BASE}/.local/scheduler.log"
CRON = f"{BASE}/scripts/cron_runner.sh"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scheduler] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# ── Schedule definitions ──────────────────────────────────────────
# Each job: (module, type, schedule_value)
# type="times" → list of (hour, minute)
# type="interval" → seconds between runs

JOBS = [
    {
        "name": "aggregator",
        "module": "aggregator",
        "type": "times",
        "times": [(8,0), (15,0), (21,0)],
    },
    {
        "name": "send_scheduled",
        "module": "scripts/send_scheduled",
        "type": "times",
        "times": [(9,0), (10,30), (11,30), (12,30)],
    },
    {
        "name": "nightly_digest",
        "module": "scripts/nightly_digest",
        "type": "times",
        "times": [(0,22)],
    },
    {
        "name": "outreach",
        "module": "outreach",
        "type": "times",
        "times": [(0,0)],
    },
    {
        "name": "cleanup_not_applied",
        "module": "scripts/cleanup_not_applied",
        "type": "times",
        "times": [(7,0)],
    },
    {
        "name": "build_auto_blacklist",
        "module": "scripts/build_auto_blacklist",
        "type": "times",
        "times": [(0,30)],
    },
    {
        "name": "retry_simplify",
        "module": "scripts/retry_simplify",
        "type": "times",
        "times": [(6,0)],
    },
    {
        "name": "process_bounces",
        "module": "scripts/process_bounces",
        "type": "interval",
        "interval": 1800,
    },
    {
        "name": "watchdog",
        "module": None,  # runs watchdog.sh directly
        "type": "interval",
        "interval": 1800,
    },
]

# Track last run times
STATE_FILE = f"{BASE}/.local/scheduler_state.json"

def load_state():
    try:
        return json.load(open(STATE_FILE))
    except:
        return {}

def save_state(state):
    json.dump(state, open(STATE_FILE, "w"), indent=2)

def has_network(timeout=5):
    """Check if network is available."""
    import socket
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(("8.8.8.8", 53))
        return True
    except Exception:
        return False

def wait_for_network(max_wait=300):
    """Wait for network, return True if available within max_wait seconds."""
    waited = 0
    while waited < max_wait:
        if has_network():
            return True
        log.warning(f"No network, waiting... ({waited}s/{max_wait}s)")
        _time.sleep(15)
        waited += 15
    return False

def run_job(job):
    name = job["name"]
    module = job["module"]
    log.info(f"▶ Running: {name}")
    # Wait for network before running (handles post-sleep network delay)
    if not wait_for_network(300):
        log.warning(f"⚠ No network after 5 min — skipping {name}")
        return
    try:
        if name == "watchdog":
            cmd = ["bash", f"{BASE}/scripts/watchdog.sh"]
        else:
            cmd = ["bash", CRON, module]
        result = subprocess.run(
            cmd,
            capture_output=True, text=True,
            timeout=600  # 10 min max per job
        )
        if result.returncode == 0:
            log.info(f"✓ {name} completed (exit 0)")
        else:
            log.warning(f"✗ {name} failed (exit {result.returncode})")
            if result.stderr:
                log.warning(f"  stderr: {result.stderr[:200]}")
    except subprocess.TimeoutExpired:
        log.error(f"✗ {name} timed out after 10 min")
    except Exception as e:
        log.error(f"✗ {name} error: {e}")

def should_run_timed(job, state, now):
    """Check if a time-based job should run now."""
    name = job["name"]
    last_run_str = state.get(name)
    
    for (h, m) in job["times"]:
        # Check if we're within 2 minutes of scheduled time
        scheduled = now.replace(hour=h, minute=m, second=0, microsecond=0)
        diff = abs((now - scheduled).total_seconds())
        if diff > 120:  # not within 2 min window
            continue
        
        # Check we haven't run in last 10 min (prevents double-run)
        if last_run_str:
            last_run = datetime.datetime.fromisoformat(last_run_str)
            if (now - last_run).total_seconds() < 600:
                return False
        
        return True
    return False

def should_run_interval(job, state, now):
    """Check if an interval-based job should run."""
    name = job["name"]
    last_run_str = state.get(name)
    
    if not last_run_str:
        return True
    
    last_run = datetime.datetime.fromisoformat(last_run_str)
    elapsed = (now - last_run).total_seconds()
    return elapsed >= job["interval"]

def check_missed_on_wake(state, now):
    """On startup/wake, catch up any missed jobs."""
    log.info("Checking for missed jobs since last run...")
    
    # Max gaps before considering a job missed
    MAX_GAPS = {
        "aggregator": 8 * 3600,       # 8h (runs 3x/day)
        "send_scheduled": 24 * 3600,   # 24h
        "process_bounces": 3600,        # 1h
        "nightly_digest": 30 * 3600,   # 30h
        "outreach": 30 * 3600,
        "cleanup_not_applied": 30 * 3600,
        "build_auto_blacklist": 30 * 3600,
        "retry_simplify": 30 * 3600,
        "watchdog": 3600,
    }
    
    for job in JOBS:
        name = job["name"]
        last_run_str = state.get(name)
        if not last_run_str:
            continue
        last_run = datetime.datetime.fromisoformat(last_run_str)
        elapsed = (now - last_run).total_seconds()
        max_gap = MAX_GAPS.get(name, 30*3600)
        
        if elapsed > max_gap:
            log.info(f"  Missed: {name} ({elapsed/3600:.1f}h ago) — running now")
            run_job(job)
            state[name] = now.isoformat()
            save_state(state)
            time.sleep(3)  # stagger

def main():
    log.info("=" * 50)
    log.info("Scheduler daemon started")
    log.info("=" * 50)
    
    # Graceful shutdown
    def handle_signal(sig, frame):
        log.info("Scheduler stopping...")
        sys.exit(0)
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    
    state = load_state()
    
    # On first start, check for missed jobs
    check_missed_on_wake(state, datetime.datetime.now())
    
    last_minute = -1
    
    while True:
        try:
            now = datetime.datetime.now()
            
            # Only check once per minute
            if now.minute == last_minute:
                time.sleep(10)
                continue
            last_minute = now.minute
            
            state = load_state()  # Reload in case external changes
            
            for job in JOBS:
                name = job["name"]
                should_run = False
                
                if job["type"] == "times":
                    should_run = should_run_timed(job, state, now)
                elif job["type"] == "interval":
                    should_run = should_run_interval(job, state, now)
                
                if should_run:
                    run_job(job)
                    state[name] = now.isoformat()
                    save_state(state)
            
            time.sleep(10)
            
        except Exception as e:
            log.error(f"Scheduler loop error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()
