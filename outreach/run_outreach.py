#!/usr/bin/env python3
"""
Outreach Pipeline — Run this file.

    python3 outreach_main.py              # Full: pull + discover + send
    python3 outreach_main.py discover     # Pull + discover only
    python3 outreach_main.py send         # Send approved only
    python3 outreach_main.py status       # Show status
    python3 outreach_main.py reset        # Reset API credits
"""

import sys, datetime, logging
from outreach.outreach_config import LOG_FILE
from outreach.outreach_data import Sheets, Credits
from outreach.outreach_finder import Finder
from outreach.outreach_mailer import Drafter, Mailer

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_con = logging.StreamHandler()
_con.setLevel(logging.INFO)
_con.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(_con)
log = logging.getLogger("outreach")


def banner():
    print("=" * 60)
    print("  OUTREACH PIPELINE")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


def phase_pull(sheets):
    print("\n--- PULL FROM VALID ENTRIES ---\n")
    n = sheets.pull()
    print(f"  Pulled {n} new rows" if n else "  No new rows to pull")
    return n


def phase_discover(sheets, finder):
    print("\n--- EMAIL DISCOVERY ---\n")
    rows = sheets.rows_for_discovery()
    if not rows:
        print("  No rows need discovery. (Fill HM/Recruiter Name columns)")
        return {"processed": 0, "valid": 0, "review": 0, "failed": 0}

    stats = {"processed": 0, "valid": 0, "review": 0, "failed": 0}
    print(f"  {len(rows)} rows to process\n")

    for row in rows:
        rn = row["row"]
        print(f"  [{rn}] {row['co']} — {row['title'][:45]}")
        stats["processed"] += 1
        hm_res = rec_res = None

        if row["need_h"]:
            print(f"       HM: {row['hn']}")
            hm_res = finder.find(row["hn"], row["co"], row["hli"])
            _show(hm_res, "HM")
            if hm_res["email"]:
                sheets.write_email(rn, "hm", hm_res["email"], hm_res["source"])
            else:
                sheets.write_error(rn, f"HM: {hm_res['error']}")
            _count(stats, hm_res)

        if row["need_r"]:
            print(f"       Rec: {row['rn']}")
            rec_res = finder.find(row["rn"], row["co"], row["rli"])
            _show(rec_res, "Rec")
            if rec_res["email"]:
                sheets.write_email(rn, "rec", rec_res["email"], rec_res["source"])
            else:
                sheets.append_error(rn, f"REC: {rec_res['error']}")
            _count(stats, rec_res)

        hm_e = row.get("he") or (hm_res["email"] if hm_res else "")
        rec_e = row.get("re") or (rec_res["email"] if rec_res else "")
        if hm_e or rec_e:
            name = row["hn"] or row["rn"]
            ct = "hm" if row["hn"] else "rec"
            draft = Drafter.draft(name, ct, row["co"], row["title"])
            sheets.write_subject_body(rn, draft["subject"], draft["body"])

    return stats


def phase_send(sheets, mailer):
    print("\n--- SEND EMAILS ---\n")
    rows = sheets.rows_for_send()
    if not rows:
        print("  No approved emails. (Set Send? = Yes)")
        return {"sent": 0, "failed": 0}

    stats = {"sent": 0, "failed": 0}
    cap = mailer.capacity()
    print(
        f"  {len(rows)} approved | Capacity: {cap['daily']} daily, {cap['hourly']} hourly\n"
    )

    for row in rows:
        rn = row["row"]
        print(f"  [{rn}] {row['co']} — {row['title'][:45]}")

        both = row["h_ok"] and row["r_ok"]
        if both and row["he"].lower() == row["re"].lower():
            print(f"       ⚠ Same email — HM template only")
            row["r_ok"] = False

        sent_any = False
        if row["h_ok"]:
            ok = _send_one(sheets, mailer, row, "hm", stats)
            sent_any = sent_any or ok
            if mailer.capacity()["daily"] <= 0:
                print("\n  ⚠ Daily limit.")
                break
            if row["r_ok"]:
                mailer.wait()
        if row["r_ok"]:
            ok = _send_one(sheets, mailer, row, "rec", stats)
            sent_any = sent_any or ok
            if mailer.capacity()["daily"] <= 0:
                print("\n  ⚠ Daily limit.")
                break

        if sent_any:
            sheets.write_sent(rn, datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
        if row != rows[-1]:
            mailer.wait()

    return stats


def _send_one(sheets, mailer, row, ct, stats):
    name = row["hn"] if ct == "hm" else row["rn"]
    email = row["he"] if ct == "hm" else row["re"]
    if not name or not email:
        return False
    draft = Drafter.draft(name, ct, row["co"], row["title"])
    subject = row["subj"] or draft["subject"]
    result = mailer.send(email, subject, draft["body"])
    if result["success"]:
        stats["sent"] += 1
        return True
    else:
        sheets.append_error(row["row"], f"{ct.upper()}: {result['error']}")
        stats["failed"] += 1
        print(f"       ✗ {result['error']}")
        return False


def _show(r, label):
    icon = {"Valid": "✓", "Manual Review": "⚠", "Failed": "✗"}.get(r["status"], "?")
    if r["email"]:
        print(f"       {icon} {label}: {r['email']} ({r['source']})")
    else:
        print(f"       {icon} {label}: {r['error']}")


def _count(s, r):
    if r["status"] == "Valid":
        s["valid"] += 1
    elif r["status"] == "Manual Review":
        s["review"] += 1
    else:
        s["failed"] += 1


def summary(d, s):
    print("\n" + "=" * 60)
    print(
        f"  Discovery: {d['processed']} rows → ✓{d['valid']} ⚠{d['review']} ✗{d['failed']}"
    )
    print(f"  Sending:   ✓{s['sent']} sent, ✗{s['failed']} failed")
    print("=" * 60)


def status(sheets, credits, mailer):
    print("\n--- STATUS ---")
    print(f"  Discovery queue: {len(sheets.rows_for_discovery())}")
    print(f"  Send queue:      {len(sheets.rows_for_send())}")
    print(f"\n{credits.report()}")
    c = mailer.capacity()
    print(f"\n  Capacity: {c['daily']} daily, {c['hourly']} hourly")


def main():
    banner()
    cmd = sys.argv[1].lower() if len(sys.argv) > 1 else "full"
    if cmd == "reset":
        Credits().reset_all()
        print("  Credits reset.")
        return
    cr = Credits()
    sh = Sheets()
    fi = Finder(cr)
    ma = Mailer(cr)
    if cmd == "status":
        status(sh, cr, ma)
    elif cmd == "discover":
        phase_pull(sh)
        d = phase_discover(sh, fi)
        summary(d, {"sent": 0, "failed": 0})
    elif cmd == "send":
        s = phase_send(sh, ma)
        summary({"processed": 0, "valid": 0, "review": 0, "failed": 0}, s)
    elif cmd in ("full", "run"):
        phase_pull(sh)
        d = phase_discover(sh, fi)
        s = phase_send(sh, ma)
        summary(d, s)
    else:
        print(
            f"\n  Unknown: {cmd}\n  Commands: discover | send | full | status | reset"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
