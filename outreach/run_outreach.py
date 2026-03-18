#!/usr/bin/env python3
"""
Outreach Pipeline

    python3 -m outreach              # Full: pull + extract + draft
    python3 -m outreach status       # Show status
    python3 -m outreach reset        # Reset API credits
"""

import sys, os, datetime, logging
from outreach.outreach_config import LOG_FILE
from outreach.outreach_data import Sheets, Credits
from outreach.outreach_finder import Finder
from outreach.outreach_mailer import Drafter, Mailer
from outreach.bounce_scanner import BounceScanner
from outreach.outreach_verifier import CircuitBreaker, AUTO_SEND_THRESHOLD

_log_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".local"
)
os.makedirs(_log_dir, exist_ok=True)

from logging.handlers import RotatingFileHandler

_fh = RotatingFileHandler(
    os.path.join(_log_dir, "outreach.log"),
    maxBytes=5 * 1024 * 1024,  # 5MB
    backupCount=3,
)
_fh.setLevel(logging.INFO)
_fh.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logging.getLogger().addHandler(_fh)
logging.getLogger().setLevel(logging.DEBUG)
_con = logging.StreamHandler()
_con.setLevel(logging.WARNING)
_con.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(_con)
# Suppress noisy third-party loggers
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests_oauthlib").setLevel(logging.WARNING)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("google_auth_oauthlib").setLevel(logging.WARNING)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("google_auth_oauthlib").setLevel(logging.WARNING)
log = logging.getLogger("outreach")


def phase_pull(sheets):
    return sheets.pull()


def phase_draft_existing(sheets, mailer):
    from outreach.outreach_data import _pad, C

    data = sheets.ws.get_all_values()
    sheets._p()
    stats = {"drafts": 0, "draft_failed": 0}
    for i, r in enumerate(data[1:], start=2):
        r = _pad(r)
        he = r[C["hm_email"]].strip()
        re_ = r[C["rec_email"]].strip()
        send_at = r[C["send_at"]].strip()
        co = r[C["company"]].strip()
        title = r[C["title"]].strip()
        jid = r[C["job_id"]].strip()
        # FIX 1b: Skip rows not marked for extraction
        extract_val = r[C["extract"]].strip().lower() if len(r) > C["extract"] else ""
        if extract_val != "yes":
            continue
        if (not he and not re_) or send_at:
            continue
        resume_type = sheets.get_resume_type(co, title)
        parts = []
        if he:
            hm_names = [
                n.strip() for n in r[C["hm_name"]].strip().split(",") if n.strip()
            ]
            hm_emails = [e.strip() for e in he.split(",") if e.strip()]
            for idx_h in range(max(len(hm_names), len(hm_emails))):
                name = (
                    hm_names[idx_h]
                    if idx_h < len(hm_names)
                    else hm_names[-1] if hm_names else co
                )
                email = hm_emails[idx_h] if idx_h < len(hm_emails) else None
                if not email:
                    continue
                draft = Drafter.draft(name, "hm", co, title, jid)
                result = mailer.send(
                    email, draft["subject"], draft["body"], resume_type
                )
                if result["success"]:
                    parts.append(f"HM draft created ({name.split()[0]})")
                    stats["drafts"] += 1
                elif "Duplicate" not in result.get("error", ""):
                    parts.append(f"HM draft failed ({name.split()[0]})")
                    stats["draft_failed"] += 1
        if re_:
            rec_names = [
                n.strip() for n in r[C["rec_name"]].strip().split(",") if n.strip()
            ]
            rec_emails = [e.strip() for e in re_.split(",") if e.strip()]
            for idx_r in range(max(len(rec_names), len(rec_emails))):
                name = (
                    rec_names[idx_r]
                    if idx_r < len(rec_names)
                    else rec_names[-1] if rec_names else co
                )
                email = rec_emails[idx_r] if idx_r < len(rec_emails) else None
                if not email:
                    continue
                draft = Drafter.draft(name, "rec", co, title, jid)
                result = mailer.send(
                    email, draft["subject"], draft["body"], resume_type
                )
                if result["success"]:
                    parts.append(f"Rec draft created ({name.split()[0]})")
                    stats["drafts"] += 1
                elif "Duplicate" not in result.get("error", ""):
                    parts.append(f"Rec draft failed ({name.split()[0]})")
                    stats["draft_failed"] += 1
        if parts:
            print(f"  {co}: {', '.join(parts)}")
        location = sheets.get_location(co, title)
        sa, sd = sheets.compute_send_at(location)
        sheets.write_send_at(
            i, sa
        )  # Don't write sent_date yet — written after actual send
    return stats


def phase_extract_and_draft(sheets, finder, mailer):
    rows = sheets.rows_for_extraction()
    if not rows:
        return {
            "extracted": 0,
            "extract_failed": 0,
            "processed": 0,
            "drafts": 0,
            "draft_failed": 0,
        }

    stats = {
        "extracted": 0,
        "extract_failed": 0,
        "processed": 0,
        "drafts": 0,
        "draft_failed": 0,
    }

    for row in rows:
        rn = row["row"]
        stats["processed"] += 1
        hm_res = rec_res = None
        parts = []

        jud = (
            sheets.get_job_url_domain(row["co"], row["title"])
            if hasattr(sheets, "get_job_url_domain")
            else ""
        )
        # FIX 2: Load bounce + failed pattern lists before extraction
        _all_bounced = set(BounceScanner.load_bounced().keys())
        _failed_pat_file = os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), ".local", "failed_patterns.json")
        _failed_pats = {}
        try:
            import json as _json
            if os.path.exists(_failed_pat_file):
                _failed_pats = _json.load(open(_failed_pat_file))
        except Exception:
            pass

        def _is_blocked(email):
            if not email or "@" not in email:
                return False
            el = email.lower().strip()
            # Check 1: known bounced email addresses
            if el in _all_bounced:
                log.info(f"Pre-send block: {email} is in bounce cache")
                return True
            domain = el.split("@")[1]
            local = el.split("@")[0]
            # Check 2: failed_patterns.json (local parts and pattern strings)
            for entry in _failed_pats.get(domain, []):
                if entry == local:
                    log.info(f"Pre-send block: {email} matches failed local in failed_patterns.json")
                    return True
                # entry might be a pattern string like {first}.{last} — skip those here
            # Check 3: domain_pattern_history.json failed patterns
            try:
                import json as _json2
                _dh_file = os.path.join(os.path.dirname(os.path.dirname(
                    os.path.abspath(__file__))), ".local", "domain_pattern_history.json")
                if os.path.exists(_dh_file):
                    _dh = _json2.load(open(_dh_file))
                    _entry = _dh.get(domain, {})
                    _failed_domain_pats = _entry.get("failed_patterns", [])
                    # Detect pattern of this email
                    _parts = local.split(".")
                    if len(_parts) == 2 and len(_parts[0]) > 1 and len(_parts[1]) > 1:
                        _pat = "{first}.{last}"
                    elif len(_parts) == 2 and len(_parts[0]) == 1:
                        _pat = "{f}.{last}"
                    elif "_" in local:
                        _pat = "{first}_{last}"
                    elif "-" in local:
                        _pat = "{first}-{last}"
                    else:
                        _pat = None
                    if _pat and _pat in _failed_domain_pats:
                        log.info(f"Pre-send block: {email} pattern '{_pat}' in domain_history failed list")
                        return True
            except Exception as _dhe:
                log.debug(f"domain_history check failed: {_dhe}")
            return False

        if row["need_h"]:
            # Support multiple comma-separated HM names
            hm_names = [n.strip() for n in row["hn"].split(",") if n.strip()]
            hm_emails = []
            hm_failed = False
            for hm_name in hm_names:
                hm_res = finder.find(hm_name, row["co"], row["hli"], job_url_domain=jud)
                if hm_res["email"]:
                    if _is_blocked(hm_res["email"]):
                        log.warning(f"Blocked pre-send: {hm_res['email']} for {row['co']}")
                        hm_failed = True
                        stats["extract_failed"] += 1
                    else:
                        hm_emails.append(hm_res["email"])
                        stats["extracted"] += 1
                else:
                    hm_failed = True
                    stats["extract_failed"] += 1
            if hm_emails:
                sheets.write_email(rn, "hm", ", ".join(hm_emails), hm_res["source"])
                # Write confidence (use highest confidence from HM results)
                hm_conf = hm_res.get("confidence", 0)
                if hm_conf > 0:
                    sheets.write_confidence(rn, hm_conf)
                parts.append("HM email extracted")
            elif hm_failed:
                sheets.write_error(rn, f"HM: {hm_res['error']}")
                parts.append("HM email failed")

        if row["need_r"]:
            # Support multiple comma-separated Rec names
            rec_names = [n.strip() for n in row["rn"].split(",") if n.strip()]
            rec_emails = []
            rec_failed = False
            for rec_name in rec_names:
                rec_res = finder.find(
                    rec_name, row["co"], row["rli"], job_url_domain=jud
                )
                if rec_res["email"]:
                    if _is_blocked(rec_res["email"]):
                        log.warning(f"Blocked pre-send: {rec_res['email']} for {row['co']}")
                        rec_failed = True
                        stats["extract_failed"] += 1
                    else:
                        rec_emails.append(rec_res["email"])
                        stats["extracted"] += 1
            if rec_emails:
                sheets.write_email(rn, "rec", ", ".join(rec_emails), rec_res["source"])
                # Write confidence (use lowest of HM and Rec — weakest link)
                rec_conf = rec_res.get("confidence", 0)
                existing_conf = hm_res.get("confidence", 0) if hm_res else 0
                final_conf = min(existing_conf, rec_conf) if existing_conf > 0 else rec_conf
                if final_conf > 0:
                    sheets.write_confidence(rn, final_conf)
                parts.append("Recruiter email extracted")
            elif rec_failed:
                sheets.append_error(rn, f"REC: {rec_res['error']}")
                parts.append("Recruiter email failed")
                stats["extract_failed"] += 1

        if parts:
            print(f"  {row['co']}: {', '.join(parts)}")

        hm_e = row.get("he") or (hm_res["email"] if hm_res else "")
        rec_e = row.get("re") or (rec_res["email"] if rec_res else "")
        # Block drafts for Low confidence emails
        from outreach.outreach_verifier import MANUAL_REVIEW_THRESHOLD
        hm_conf_val = hm_res.get("confidence", 0) if hm_res else 0
        rec_conf_val = rec_res.get("confidence", 0) if rec_res else 0
        if hm_e and hm_conf_val > 0 and hm_conf_val < MANUAL_REVIEW_THRESHOLD:
            log.info(f"Skipping HM draft for {row['co']}: Low confidence ({hm_conf_val})")
            hm_e = ""
        if rec_e and rec_conf_val > 0 and rec_conf_val < MANUAL_REVIEW_THRESHOLD:
            log.info(f"Skipping Rec draft for {row['co']}: Low confidence ({rec_conf_val})")
            rec_e = ""
        jid = row.get("jid", "")
        resume_type = sheets.get_resume_type(row["co"], row["title"])

        draft_parts = []
        if hm_e:
            hm_draft = Drafter.draft(
                row["hn"] or row["rn"], "hm", row["co"], row["title"], jid
            )
            result = mailer.send(
                hm_e, hm_draft["subject"], hm_draft["body"], resume_type
            )
            if result["success"]:
                draft_parts.append("HM draft created")
                stats["drafts"] += 1
            else:
                if "Duplicate" not in result["error"]:
                    draft_parts.append("HM draft failed")
                    stats["draft_failed"] += 1
                else:
                    draft_parts.append("HM draft exists")

        if rec_e:
            rec_draft = Drafter.draft(
                row["rn"] or row["hn"], "rec", row["co"], row["title"], jid
            )
            result = mailer.send(
                rec_e, rec_draft["subject"], rec_draft["body"], resume_type
            )
            if result["success"]:
                draft_parts.append("Recruiter draft created")
                stats["drafts"] += 1
            else:
                if "Duplicate" not in result["error"]:
                    draft_parts.append("Recruiter draft failed")
                    stats["draft_failed"] += 1
                else:
                    draft_parts.append("Recruiter draft exists")

        if draft_parts:
            print(f"  {row['co']}: {', '.join(draft_parts)}")

        if hm_e or rec_e:
            location = sheets.get_location(row["co"], row["title"])
            sa, sd = sheets.compute_send_at(location)
            sheets.write_send_at(
                rn, sa
            )  # Don't write sent_date — written after actual send

    return stats


def status(sheets, credits, mailer):
    print(f"  Extraction queue: {len(sheets.rows_for_extraction())}")
    c = mailer.capacity()
    print(f"  Capacity: {c['daily']} daily, {c['hourly']} hourly")


def main():
    now = datetime.datetime.now().strftime("%d %B, %Y")
    cmd = sys.argv[1].lower() if len(sys.argv) > 1 else "full"

    if cmd == "reset":
        Credits().reset_all()
        print("Credits reset.")
        return

    print(f"OUTREACH PIPELINE  {now}")
    print("-" * 40)

    cr = Credits()
    sh = Sheets()
    fi = Finder(cr)
    ma = Mailer(cr)

    if cmd == "status":
        status(sh, cr, ma)
        return

    # Circuit breaker check
    can_send, cb_reason = CircuitBreaker.can_send()
    if not can_send:
        print(f"  Circuit breaker TRIPPED: {cb_reason}")
        print("  No new emails will be drafted. Fix the issue first.")
        print(f"  Status: {CircuitBreaker.status()}")
    else:
        print(f"  Circuit breaker: {CircuitBreaker.status()}")

    print("Scanning for bounced emails...")
    try:
        svc = ma._service()
        bounced_emails = BounceScanner.scan(svc, days_back=14)
        if bounced_emails:
            ma.set_bounced(bounced_emails)
            sh.flag_bounced_rows(bounced_emails)
    except Exception as e:
        log.warning(f"Bounce scan failed (non-fatal): {e}")

    phase_pull(sh)
    sh.sync_with_valid()

    # Mark delivered emails (sent 12h+ ago, no bounce)
    try:
        all_bounced = set(BounceScanner.load_bounced().keys())
        sh.mark_delivered(all_bounced)
    except Exception as e:
        log.debug(f"Delivery marking skipped: {e}")
    s = phase_extract_and_draft(sh, fi, ma)

    sh.populate_linkedin_msgs()

    d = phase_draft_existing(sh, ma)
    s["drafts"] += d["drafts"]
    s["draft_failed"] += d["draft_failed"]
    print("-" * 40)
    print(
        f"Email IDs: {s['extracted']} extracted, {s['extract_failed']} failed ({s['processed']} processed)"
    )
    print(f"Drafts:    {s['drafts']} created, {s['draft_failed']} failed")


if __name__ == "__main__":
    main()
