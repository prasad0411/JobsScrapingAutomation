#!/usr/bin/env python3
"""
Pipeline Health Heartbeat — runs after every aggregator cycle.
Checks 6 health signals and logs/alerts on failures.
Designed to catch silent bugs within hours, not days.
"""
import os
import re
import json
import logging
import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = os.path.join(BASE, ".local", "skipped_jobs.log")
HEALTH_FILE = os.path.join(BASE, ".local", "health_state.json")
HEALTH_LOG = os.path.join(BASE, ".local", "health_alerts.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [health] %(message)s",
    handlers=[
        logging.FileHandler(HEALTH_LOG),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)


def load_state():
    try:
        with open(HEALTH_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    try:
        with open(HEALTH_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def get_recent_log_lines(hours=6):
    """Get log lines from the last N hours."""
    if not os.path.exists(LOG_FILE):
        return []
    cutoff = datetime.datetime.now() - datetime.timedelta(hours=hours)
    lines = []
    try:
        with open(LOG_FILE, "r") as f:
            for line in f:
                m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if m:
                    try:
                        ts = datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                        if ts >= cutoff:
                            lines.append(line.strip())
                    except ValueError:
                        continue
    except Exception:
        pass
    return lines


def get_last_summary(lines):
    """Extract the most recent SUMMARY line."""
    for line in reversed(lines):
        if "SUMMARY:" in line:
            m = re.search(r"(\d+) valid, (\d+) discarded", line)
            if m:
                return int(m.group(1)), int(m.group(2))
    return None, None


def check_health():
    """Run all health checks, return list of alerts."""
    alerts = []
    state = load_state()
    now = datetime.datetime.now()
    lines = get_recent_log_lines(hours=12)

    # ── CHECK 1: Discarded writes working? ──
    recent_rejections = sum(1 for l in lines if "REJECTED" in l and "SUMMARY" not in l)
    last_valid, last_discarded = get_last_summary(lines)

    if recent_rejections > 10 and (last_discarded is not None and last_discarded == 0):
        alerts.append(
            f"DISCARDED WRITES MAY BE BROKEN: {recent_rejections} rejections logged "
            f"but {last_discarded} discarded written"
        )

    # ── CHECK 2: Valid writes working? ──
    summaries = []
    for line in lines:
        if "SUMMARY:" in line:
            m = re.search(r"(\d+) valid", line)
            if m:
                summaries.append(int(m.group(1)))

    if len(summaries) >= 3 and all(s == 0 for s in summaries[-3:]):
        alerts.append(
            f"ZERO VALID JOBS for {len(summaries)} consecutive runs — "
            f"pipeline may be broken"
        )

    # ── CHECK 3: Sources alive? ──
    source_counts = {}
    for line in lines:
        if "ACCEPTED" in line:
            # Extract source from end of line
            for src in ["LinkedIn", "SimplifyJobs", "ZipRecruiter", "Jobright",
                        "speedyapply_swe", "speedyapply_ai", "simplify_newgrad",
                        "cvrve_newgrad", "SWE List", "direct_ats"]:
                if src in line:
                    source_counts[src] = source_counts.get(src, 0) + 1
                    break

    # Compare to historical averages
    prev_sources = state.get("last_source_counts", {})
    for src, prev_count in prev_sources.items():
        if prev_count >= 3 and source_counts.get(src, 0) == 0:
            alerts.append(
                f"SOURCE DOWN: {src} produced 0 jobs (was {prev_count} last check)"
            )

    state["last_source_counts"] = source_counts

    # ── CHECK 4: Scheduler running? ──
    last_run_ts = None
    for line in reversed(lines):
        if "SUMMARY:" in line:
            m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if m:
                last_run_ts = datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                break

    if last_run_ts:
        hours_ago = (now - last_run_ts).total_seconds() / 3600
        if hours_ago > 8:
            alerts.append(
                f"SCHEDULER STALE: Last run was {hours_ago:.1f} hours ago "
                f"(expected every 4-6 hours)"
            )
        state["last_run_ts"] = last_run_ts.isoformat()
    elif state.get("last_run_ts"):
        old_ts = datetime.datetime.fromisoformat(state["last_run_ts"])
        hours_ago = (now - old_ts).total_seconds() / 3600
        if hours_ago > 8:
            alerts.append(
                f"SCHEDULER DOWN: No runs detected in {hours_ago:.1f} hours"
            )

    # ── CHECK 5: Error spike? ──
    error_types = set()
    for line in lines:
        for err in ["NameError", "TypeError", "AttributeError", "KeyError",
                     "ImportError", "IndexError"]:
            if err in line and "test" not in line.lower():
                # Extract short context
                err_context = line[line.index(err):line.index(err)+60].strip()
                error_types.add(err_context)

    if len(error_types) >= 3:
        alerts.append(
            f"ERROR SPIKE: {len(error_types)} unique errors in recent logs:\n"
            + "\n".join(f"  - {e}" for e in list(error_types)[:5])
        )

    state["last_error_count"] = len(error_types)

    # ── CHECK 6: Sheet growth (compare to last known count) ──
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(
            os.path.join(BASE, ".local", "credentials.json"),
            scopes=["https://spreadsheets.google.com/feeds",
                     "https://www.googleapis.com/auth/drive"]
        )
        gc = gspread.authorize(creds)
        ws = gc.open("H1B visa").worksheet("Valid Entries")
        current_count = len(ws.get_all_values())

        prev_count = state.get("last_sheet_count", 0)
        if prev_count > 0 and current_count <= prev_count:
            days_stuck = state.get("sheet_stuck_days", 0) + 1
            state["sheet_stuck_days"] = days_stuck
            if days_stuck >= 2:
                alerts.append(
                    f"SHEET STUCK: Valid Entries at {current_count} rows "
                    f"for {days_stuck} days (no growth)"
                )
        else:
            state["sheet_stuck_days"] = 0
            growth = current_count - prev_count if prev_count > 0 else 0
            log.info(f"Sheet healthy: {current_count} rows (+{growth})")

        state["last_sheet_count"] = current_count
    except Exception as e:
        log.debug(f"Sheet check failed: {e}")

    # Save state
    state["last_check"] = now.isoformat()
    save_state(state)

    return alerts


def main():
    log.info("=" * 50)
    log.info("Pipeline Health Heartbeat")
    log.info("=" * 50)

    alerts = check_health()

    if alerts:
        log.warning(f"{len(alerts)} HEALTH ISSUES FOUND:")
        for i, alert in enumerate(alerts, 1):
            log.warning(f"  [{i}] {alert}")

        # Write alert summary to a prominent file the scheduler can check
        alert_file = os.path.join(BASE, ".local", "health_status.txt")
        with open(alert_file, "w") as f:
            f.write(f"UNHEALTHY — {len(alerts)} issues at {datetime.datetime.now()}\n\n")
            for alert in alerts:
                f.write(f"• {alert}\n\n")
    else:
        log.info("ALL CHECKS PASSED — pipeline healthy")
        alert_file = os.path.join(BASE, ".local", "health_status.txt")
        with open(alert_file, "w") as f:
            f.write(f"HEALTHY — all checks passed at {datetime.datetime.now()}\n")

    return len(alerts)


if __name__ == "__main__":
    exit(main())
