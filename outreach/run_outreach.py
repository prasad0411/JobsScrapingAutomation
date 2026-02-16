#!/usr/bin/env python3
"""
Outreach Pipeline

    python3 -m outreach              # Full: pull + discover + send
    python3 -m outreach discover     # Pull + discover only
    python3 -m outreach send         # Send approved only
    python3 -m outreach status       # Show status
    python3 -m outreach reset        # Reset API credits
"""

import sys, os, datetime, logging
from outreach.outreach_config import LOG_FILE
from outreach.outreach_data import Sheets, Credits
from outreach.outreach_finder import Finder
from outreach.outreach_mailer import Drafter, Mailer

_log_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".local"
)
os.makedirs(_log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(_log_dir, "outreach.log"),
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_con = logging.StreamHandler()
_con.setLevel(logging.WARNING)
_con.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(_con)
log = logging.getLogger("outreach")


def phase_pull(sheets):
    return sheets.pull()


def phase_discover(sheets, finder):
    rows = sheets.rows_for_discovery()
    if not rows:
        return {"processed": 0, "valid": 0, "review": 0, "failed": 0}

    stats = {"processed": 0, "valid": 0, "review": 0, "failed": 0}

    for row in rows:
        rn = row["row"]
        stats["processed"] += 1
        hm_res = rec_res = None
        parts = []

        if row["need_h"]:
            hm_res = finder.find(row["hn"], row["co"], row["hli"])
            if hm_res["email"]:
                sheets.write_email(rn, "hm", hm_res["email"], hm_res["source"])
                parts.append("HM email extracted")
            else:
                sheets.write_error(rn, f"HM: {hm_res['error']}")
                parts.append("HM email failed")
            _count(stats, hm_res)

        if row["need_r"]:
            rec_res = finder.find(row["rn"], row["co"], row["rli"])
            if rec_res["email"]:
                sheets.write_email(rn, "rec", rec_res["email"], rec_res["source"])
                parts.append("Recruiter email extracted")
            else:
                sheets.append_error(rn, f"REC: {rec_res['error']}")
                parts.append("Recruiter email failed")
            _count(stats, rec_res)

        if parts:
            print(f"  {row['co']}: {', '.join(parts)}")

        hm_e = row.get("he") or (hm_res["email"] if hm_res else "")
        rec_e = row.get("re") or (rec_res["email"] if rec_res else "")
        if hm_e or rec_e:
            jid = row.get("jid", "")
            hm_d = Drafter.draft(row["hn"] or row["rn"], "hm", row["co"], row["title"], jid)
            rec_d = Drafter.draft(row["rn"] or row["hn"], "rec", row["co"], row["title"], jid)
            sheets.write_subject_body(rn, hm_d["subject"], hm_d["body"], rec_d["subject"], rec_d["body"])

    return stats


def phase_send(sheets, mailer):
    rows = sheets.rows_for_send()
    if not rows:
        return {"sent": 0, "failed": 0}

    stats = {"sent": 0, "failed": 0}

    for row in rows:
        rn = row["row"]
        parts = []

        both = row["h_ok"] and row["r_ok"]
        if both and row["he"].lower() == row["re"].lower():
            row["r_ok"] = False

        sent_any = False
        if row["h_ok"]:
            ok = _send_one(sheets, mailer, row, "hm", stats)
            sent_any = sent_any or ok
            parts.append("HM sent" if ok else "HM send failed")
            if mailer.capacity()["daily"] <= 0:
                print("  Daily send limit reached")
                break
            if row["r_ok"]:
                mailer.wait()
        if row["r_ok"]:
            ok = _send_one(sheets, mailer, row, "rec", stats)
            sent_any = sent_any or ok
            parts.append("Recruiter sent" if ok else "Recruiter send failed")
            if mailer.capacity()["daily"] <= 0:
                print("  Daily send limit reached")
                break

        if parts:
            print(f"  {row['co']}: {', '.join(parts)}")

        if sent_any:
            sheets.write_sent(rn)
        if row != rows[-1]:
            mailer.wait()

    return stats


def _send_one(sheets, mailer, row, ct, stats):
    name = row["hn"] if ct == "hm" else row["rn"]
    email = row["he"] if ct == "hm" else row["re"]
    if not name or not email:
        return False
    jid = row.get("jid", "")
    draft = Drafter.draft(name, ct, row["co"], row["title"], jid)
    subject = row.get("subj") or draft["subject"]
    resume_type = sheets.get_resume_type(row["co"], row["title"])
    result = mailer.send(email, subject, draft["body"], resume_type)
    if result["success"]:
        stats["sent"] += 1
        return True
    else:
        sheets.append_error(row["row"], f"{ct.upper()}: {result['error']}")
        stats["failed"] += 1
        return False


def _count(s, r):
    if r["status"] == "Valid":
        s["valid"] += 1
    elif r["status"] == "Manual Review":
        s["review"] += 1
    else:
        s["failed"] += 1


def status(sheets, credits, mailer):
    print(f"  Discovery queue: {len(sheets.rows_for_discovery())}")
    print(f"  Send queue:      {len(sheets.rows_for_send())}")
    c = mailer.capacity()
    print(f"  Capacity: {c['daily']} daily, {c['hourly']} hourly")


def main():
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

    d = {"processed": 0, "valid": 0, "review": 0, "failed": 0}
    s = {"sent": 0, "failed": 0}

    if cmd == "status":
        status(sh, cr, ma)
        return
    elif cmd == "discover":
        phase_pull(sh)
        d = phase_discover(sh, fi)
    elif cmd == "send":
        s = phase_send(sh, ma)
    elif cmd in ("full", "run"):
        phase_pull(sh)
        d = phase_discover(sh, fi)
        s = phase_send(sh, ma)
    else:
        print(f"Unknown: {cmd}")
        print("Commands: discover | send | full | status | reset")
        sys.exit(1)

    print("-" * 40)
    print(
        f"Discovery: {d['valid']} found, {d['failed']} failed ({d['processed']} processed)"
    )
    print(f"Sending:   {s['sent']} sent, {s['failed']} failed")


if __name__ == "__main__":
    main()
