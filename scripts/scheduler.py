#!/usr/bin/env python3
"""
Permanent scheduler daemon — single KeepAlive process managed by launchd.
Replaces all individual launchd plists.
"""
import time
import datetime
import subprocess
import threading
import logging
import os
import json
import signal
import sys
import socket

BASE = "/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker"
LOG_FILE = f"{BASE}/.local/scheduler.log"
CRON = f"{BASE}/scripts/cron_runner.sh"
STATE_FILE = f"{BASE}/.local/scheduler_state.json"
STATE_TMP = f"{BASE}/.local/scheduler_state.json.tmp"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scheduler] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

JOBS = [
    {"name":"aggregator","module":"aggregator","type":"times","times":[(8,0),(15,0),(21,0)],"timeout":900,"max_gap":8*3600},
    {"name":"send_scheduled","module":"scripts/send_scheduled","type":"times","times":[(9,0),(10,30),(11,30),(12,30)],"timeout":300,"max_gap":24*3600},
    {"name":"outreach","module":"outreach","type":"times","times":[(0,0)],"timeout":1800,"max_gap":30*3600},
    {"name":"nightly_digest","module":"scripts/nightly_digest","type":"times","times":[(0,22)],"timeout":120,"max_gap":30*3600},
    {"name":"build_auto_blacklist","module":"scripts/build_auto_blacklist","type":"times","times":[(0,30)],"timeout":120,"max_gap":30*3600},
    {"name":"cleanup_not_applied","module":"scripts/cleanup_not_applied","type":"times","times":[(7,30)],"timeout":300,"max_gap":30*3600},
    {"name":"retry_simplify","module":"scripts/retry_simplify","type":"times","times":[(6,0)],"timeout":300,"max_gap":30*3600},
    {"name":"process_bounces","module":"scripts/process_bounces","type":"interval","interval":1800,"timeout":120,"max_gap":3600},
    {"name":"watchdog","module":None,"type":"interval","interval":1800,"timeout":60,"max_gap":3600},
]

_running = {}
_running_lock = threading.Lock()

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state):
    try:
        with open(STATE_TMP, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(STATE_TMP, STATE_FILE)
    except Exception as e:
        log.error(f"Failed to save state: {e}")

def has_network(timeout=5):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect(("8.8.8.8", 53))
        s.close()
        return True
    except Exception:
        return False

def wait_for_network(max_wait=300):
    waited = 0
    while waited < max_wait:
        if has_network():
            if waited > 0:
                log.info(f"Network up after {waited}s")
            return True
        log.warning(f"No network, waiting... ({waited}s/{max_wait}s)")
        time.sleep(15)
        waited += 15
    log.warning("No network after 5min — proceeding anyway")
    return False

def run_job(job):
    name = job["name"]
    timeout = job.get("timeout", 600)
    with _running_lock:
        if _running.get(name):
            log.info(f"⏭ Skipping {name} — already running")
            return
        _running[name] = True
    try:
        wait_for_network(300)
        log.info(f"▶ Running: {name}")
        if name == "watchdog":
            cmd = ["bash", f"{BASE}/scripts/watchdog.sh"]
        else:
            cmd = ["bash", CRON, job["module"]]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=BASE)
        if result.returncode == 0:
            log.info(f"✓ {name} done (exit 0)")
        else:
            log.warning(f"✗ {name} failed (exit {result.returncode})")
            if result.stderr:
                log.warning(f"  stderr: {result.stderr[:300]}")
    except subprocess.TimeoutExpired:
        log.error(f"✗ {name} timed out after {timeout}s")
    except Exception as e:
        log.error(f"✗ {name} error: {e}")
    finally:
        with _running_lock:
            _running[name] = False

_job_failures: dict = {}  # tracks consecutive failures per job

def run_job_async(job, state):
    name = job["name"]
    def _run():
        run_job(job)
        # Check if job actually succeeded by reading health file
        health_f = f"{BASE}/.local/health_{name}.json"
        try:
            import json as _j
            h = _j.load(open(health_f))
            if h.get("exit_code", 1) != 0:
                _job_failures[name] = _job_failures.get(name, 0) + 1
                if _job_failures[name] == 1:
                    log.warning(f"↻ {name} failed — will retry in 30 min")
                    time.sleep(1800)
                    log.info(f"↻ Retrying {name} (attempt 2)")
                    run_job(job)
                else:
                    log.warning(f"✗ {name} failed twice — skipping until next window")
                    _job_failures[name] = 0
            else:
                _job_failures[name] = 0
        except Exception:
            pass
        state[name] = datetime.datetime.now().isoformat()
        save_state(state)
    t = threading.Thread(target=_run, name=f"job-{name}", daemon=True)
    t.start()

def should_run_timed(job, state, now):
    name = job["name"]
    last_run_str = state.get(name)
    for (h, m) in job["times"]:
        scheduled = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if abs((now - scheduled).total_seconds()) > 180:  # 3-min window
            continue
        if last_run_str:
            last_run = datetime.datetime.fromisoformat(last_run_str)
            if (now - last_run).total_seconds() < 600:
                return False
        return True
    return False

def should_run_interval(job, state, now):
    name = job["name"]
    last_run_str = state.get(name)
    if not last_run_str:
        return True
    last_run = datetime.datetime.fromisoformat(last_run_str)
    return (now - last_run).total_seconds() >= job["interval"]

def check_missed_on_wake(state, now):
    log.info("Checking for missed jobs since last run...")
    wait_for_network(300)
    for job in JOBS:
        name = job["name"]
        max_gap = job.get("max_gap", 30 * 3600)
        last_run_str = state.get(name)
        if not last_run_str:
            log.info(f"  {name}: no prior state — initializing")
            state[name] = (now - datetime.timedelta(seconds=max_gap - 60)).isoformat()
            save_state(state)
            continue
        last_run = datetime.datetime.fromisoformat(last_run_str)
        elapsed = (now - last_run).total_seconds()
        if elapsed > max_gap:
            log.info(f"  Missed: {name} ({elapsed/3600:.1f}h ago) — running now")
            run_job(job)
            state[name] = datetime.datetime.now().isoformat()
            save_state(state)
            time.sleep(5)
        else:
            log.info(f"  OK: {name} ({elapsed/3600:.1f}h ago)")

def rotate_log_if_needed():
    try:
        if not os.path.exists(LOG_FILE):
            return
        with open(LOG_FILE) as f:
            lines = f.readlines()
        if len(lines) > 1000:
            with open(LOG_FILE, "w") as f:
                f.writelines(lines[-700:])
    except Exception:
        pass

def main():
    log.info("=" * 50)
    log.info("Scheduler daemon started")
    log.info("=" * 50)

    def handle_signal(sig, frame):
        log.info("Scheduler stopping (signal received)")
        sys.exit(0)
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    state = load_state()
    check_missed_on_wake(state, datetime.datetime.now())

    last_minute = -1
    loop_count = 0

    while True:
        try:
            now = datetime.datetime.now()
            if now.minute == last_minute:
                time.sleep(10)
                loop_count += 1
                if loop_count % 360 == 0:
                    rotate_log_if_needed()
                continue
            last_minute = now.minute
            state = load_state()
            for job in JOBS:
                name = job["name"]
                should_run = False
                if job["type"] == "times":
                    should_run = should_run_timed(job, state, now)
                elif job["type"] == "interval":
                    should_run = should_run_interval(job, state, now)
                if should_run:
                    state[name] = now.isoformat()
                    save_state(state)
                    run_job_async(job, state)
            time.sleep(10)
        except Exception as e:
            log.error(f"Scheduler loop error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()
