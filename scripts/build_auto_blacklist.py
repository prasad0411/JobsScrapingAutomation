#!/usr/bin/env python3
"""
scripts/build_auto_blacklist.py

Reads Discarded Entries from Google Sheets and auto-promotes companies
that have been rejected 3+ times for the same reason to COMPANY_BLACKLIST.

Run weekly:
    python3 scripts/build_auto_blacklist.py

Dry run (see what would be added without changing anything):
    python3 scripts/build_auto_blacklist.py --dry-run
"""

import sys, os, re, json
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DRY_RUN = "--dry-run" in sys.argv
MIN_REJECTIONS = 3  # promote after this many same-reason rejections

# Never auto-blacklist these — they are ATS platforms or garbage extraction artifacts
NEVER_BLACKLIST = {
    "myworkdayjobs", "corporate office", "usa", "us", "simplify",
    "unknown", "company", "careers", "jobs", "external", "portal",
    "job-boards", "worldwide", "ats", "marketing", "learning",
    "coherent",  # has both valid CS roles and hardware roles — don't blanket blacklist
}

# Reasons that qualify for auto-blacklist
AUTO_BLACKLIST_REASONS = [
    "security clearance",
    "clearance required",
    "us person",
    "citizenship required",
    "no.*sponsor",
    "hardware",
    "laser",
    "optics",
    "skillbridge",
    "military",
]

def reason_qualifies(reason):
    r = reason.lower()
    return any(re.search(pat, r) for pat in AUTO_BLACKLIST_REASONS)

def main():
    from aggregator.config import SHEETS_CREDS_FILE, SHEET_NAME, DISCARDED_WORKSHEET
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SHEETS_CREDS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).worksheet(DISCARDED_WORKSHEET)
    rows = sheet.get_all_values()[1:]  # skip header
    print(f"Loaded {len(rows)} discarded entries")

    # Count: company → reason → count
    counts = defaultdict(lambda: defaultdict(int))
    for row in rows:
        if len(row) < 3:
            continue
        company = row[2].strip()
        reason = row[1].strip() if len(row) > 1 else ""
        if company and reason and reason_qualifies(reason):
            short = reason.split("(")[0].strip()[:60]
            counts[company][short] += 1

    # Load existing blacklist
    config_path = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "aggregator", "config.py")
    with open(config_path, encoding="utf-8") as f:
        config = f.read()

    existing_match = re.search(
        r"COMPANY_BLACKLIST\s*=\s*\[([^\]]+)\]", config, re.DOTALL)
    existing = set()
    if existing_match:
        existing = set(re.findall(r'"([^"]+)"', existing_match.group(1)))
    print(f"Existing blacklist: {len(existing)} companies")

    # Find candidates
    candidates = []
    for company, reason_counts in counts.items():
        if company in existing:
            continue
        if company.lower().strip() in NEVER_BLACKLIST:
            continue
        for reason, count in reason_counts.items():
            if count >= MIN_REJECTIONS:
                candidates.append((company, reason, count))
                break

    if not candidates:
        print("No new auto-blacklist candidates found.")
        return

    print(f"\nCandidates ({len(candidates)}):")
    for company, reason, count in sorted(candidates, key=lambda x: -x[2]):
        print(f"  {company}: '{reason}' x{count}")

    if DRY_RUN:
        print("\n[DRY RUN] No changes made. Remove --dry-run to apply.")
        return

    # Append to COMPANY_BLACKLIST and COMPANY_BLACKLIST_REASONS in config.py
    new_companies = [c for c, _, _ in candidates]
    new_reasons = {c: r for c, r, _ in candidates}

    # Insert into COMPANY_BLACKLIST
    for company in new_companies:
        config = re.sub(
            r"(COMPANY_BLACKLIST\s*=\s*\[)",
            f'\1\n    "{company}",',
            config, count=1
        )
    # Insert into COMPANY_BLACKLIST_REASONS
    for company, reason in new_reasons.items():
        config = re.sub(
            r"(COMPANY_BLACKLIST_REASONS\s*=\s*\{)",
            f'\1\n    "{company}": "Auto-blacklisted: {reason}",',
            config, count=1
        )

    with open(config_path, "w", encoding="utf-8") as f:
        f.write(config)

    print(f"\nAdded {len(new_companies)} companies to COMPANY_BLACKLIST:")
    for c in new_companies:
        print(f"  + {c}")
    print(f"\nConfig updated: {config_path}")
    print("Run the aggregator on next schedule — new blacklist takes effect immediately.")
    try:
        from outreach.brain import Brain
        b = Brain.get()
        for company, reason, count in candidates:
            b.record_company_rejection(company, reason)
        if new_companies:
            msg = (
                f"Auto-blacklist: {len(new_companies)} companies added\n\n"
                + "\n".join(f"  + {c}: {r} (x{ct})" for c, r, ct in candidates)
            )
            b.send_email_alert(
                f"🚫 Auto-blacklist: {len(new_companies)} new companies blocked",
                msg
            )
        else:
            import logging as _log
            _log.getLogger(__name__).info("Auto-blacklist: clean run, no new companies")
    except Exception as _be:
        print(f"Brain sync failed (non-fatal): {_be}")

if __name__ == "__main__":
    main()
