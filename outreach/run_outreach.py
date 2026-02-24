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
_fh.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
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
        if (not he and not re_) or send_at:
            continue
        resume_type = sheets.get_resume_type(co, title)
        parts = []
        if he:
            hn = r[C["hm_name"]].strip() or co
            draft = Drafter.draft(hn, "hm", co, title, jid)
            result = mailer.send(he, draft["subject"], draft["body"], resume_type)
            if result["success"]:
                parts.append("HM draft created")
                stats["drafts"] += 1
            elif "Duplicate" not in result.get("error", ""):
                parts.append("HM draft failed")
                stats["draft_failed"] += 1
        if re_:
            rn = r[C["rec_name"]].strip() or co
            draft = Drafter.draft(rn, "rec", co, title, jid)
            result = mailer.send(re_, draft["subject"], draft["body"], resume_type)
            if result["success"]:
                parts.append("Recruiter draft created")
                stats["drafts"] += 1
            elif "Duplicate" not in result.get("error", ""):
                parts.append("Recruiter draft failed")
                stats["draft_failed"] += 1
        if parts:
            print(f"  {co}: {', '.join(parts)}")
        location = sheets.get_location(co, title)
        sa, sd = sheets.compute_send_at(location)
        sheets.write_send_at(i, sa, sd)
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

        jud = sheets.get_job_url_domain(row["co"], row["title"]) if hasattr(sheets, "get_job_url_domain") else ""
        if row["need_h"]:
            hm_res = finder.find(row["hn"], row["co"], row["hli"], job_url_domain=jud)
            if hm_res["email"]:
                sheets.write_email(rn, "hm", hm_res["email"], hm_res["source"])
                parts.append("HM email extracted")
                stats["extracted"] += 1
            else:
                sheets.write_error(rn, f"HM: {hm_res['error']}")
                parts.append("HM email failed")
                stats["extract_failed"] += 1

        if row["need_r"]:
            rec_res = finder.find(row["rn"], row["co"], row["rli"], job_url_domain=jud)
            if rec_res["email"]:
                sheets.write_email(rn, "rec", rec_res["email"], rec_res["source"])
                parts.append("Recruiter email extracted")
                stats["extracted"] += 1
            else:
                sheets.append_error(rn, f"REC: {rec_res['error']}")
                parts.append("Recruiter email failed")
                stats["extract_failed"] += 1

        if parts:
            print(f"  {row['co']}: {', '.join(parts)}")

        hm_e = row.get("he") or (hm_res["email"] if hm_res else "")
        rec_e = row.get("re") or (rec_res["email"] if rec_res else "")
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
            sheets.write_send_at(rn, sa, sd)

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
