#!/usr/bin/env python3
"""
Send Scheduled Emails
Reads Outreach Tracker for rows where:
  - Send At time has passed
  - Sent Date is empty (not yet sent)
  - HM or Rec email exists
Finds matching Gmail draft and sends it.

Features:
  - Deduplication: won't send to same recipient+subject within 7 days
  - Dead letter queue: after 3 failed attempts across runs, marks as "Failed"
  - Retry with backoff on transient errors
"""
import sys, os, re, datetime, time, logging, base64, json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from outreach.outreach_config import (
    C,
    SHEETS_CREDS,
    SPREADSHEET,
    OUTREACH_TAB,
    SENDER_EMAIL,
)
from outreach.outreach_data import _pad, _cl

import gspread
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# --- Persistent state files ---
_LOCAL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".local"
)
SENT_LOG_FILE = os.path.join(_LOCAL_DIR, "sent_log.json")
FAIL_COUNT_FILE = os.path.join(_LOCAL_DIR, "send_fail_counts.json")
DEAD_LETTER_MAX = 3  # Max failures before marking as dead


# ============================================================================
# Deduplication Guard
# ============================================================================
def load_sent_log():
    """Load {recipient||subject: timestamp} of recently sent emails."""
    try:
        if os.path.exists(SENT_LOG_FILE):
            return json.load(open(SENT_LOG_FILE))
    except Exception:
        pass
    return {}


def save_sent_log(sent_log):
    try:
        os.makedirs(_LOCAL_DIR, exist_ok=True)
        json.dump(sent_log, open(SENT_LOG_FILE, "w"), indent=2)
    except Exception as e:
        log.error(f"Failed to save sent log: {e}")


def is_duplicate(sent_log, to_email, subject, days=7):
    """Check if we already sent to this recipient+subject within N days."""
    key = f"{to_email.lower().strip()}||{subject.strip().lower()}"
    ts = sent_log.get(key)
    if not ts:
        return False
    try:
        sent_at = datetime.datetime.fromisoformat(ts)
        age = (datetime.datetime.now() - sent_at).total_seconds() / 86400
        return age < days
    except Exception:
        return False


def record_sent(sent_log, to_email, subject):
    """Record that we sent an email to this recipient+subject."""
    key = f"{to_email.lower().strip()}||{subject.strip().lower()}"
    sent_log[key] = datetime.datetime.now().isoformat()


def cleanup_sent_log(sent_log, days=14):
    """Remove entries older than N days to keep the file small."""
    cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
    cleaned = {}
    for key, ts in sent_log.items():
        try:
            if datetime.datetime.fromisoformat(ts) > cutoff:
                cleaned[key] = ts
        except Exception:
            pass
    return cleaned


# ============================================================================
# Dead Letter Queue
# ============================================================================
def load_fail_counts():
    """Load {row_key: fail_count} for tracking repeated send failures."""
    try:
        if os.path.exists(FAIL_COUNT_FILE):
            return json.load(open(FAIL_COUNT_FILE))
    except Exception:
        pass
    return {}


def save_fail_counts(fail_counts):
    try:
        os.makedirs(_LOCAL_DIR, exist_ok=True)
        json.dump(fail_counts, open(FAIL_COUNT_FILE, "w"), indent=2)
    except Exception as e:
        log.error(f"Failed to save fail counts: {e}")


def get_row_key(company, title, email):
    """Unique key for tracking failures per email per job."""
    return (
        f"{company.lower().strip()}||{title.lower().strip()}||{email.lower().strip()}"
    )


# ============================================================================
# Gmail & Sheets helpers
# ============================================================================
def get_gmail_service():
    """Authenticate with Gmail using OAuth pickle token."""
    import pickle
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    token_path = os.path.join(_LOCAL_DIR, "gmail_token.pickle")
    creds = None
    if os.path.exists(token_path):
        with open(token_path, "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_path, "wb") as f:
                pickle.dump(creds, f)
        else:
            log.error("Gmail token expired. Re-authenticate on your Mac.")
            sys.exit(1)
    return build("gmail", "v1", credentials=creds)


def get_sheets():
    """Get the Outreach Tracker worksheet."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SHEETS_CREDS, scope)
    gc = gspread.authorize(creds)
    ss = gc.open(SPREADSHEET)
    return ss.worksheet(OUTREACH_TAB)


def parse_send_at(send_at_str):
    """Parse 'Mar 02, 9:00 AM ET' into a datetime object in US/Eastern."""
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    if not send_at_str or send_at_str.strip() == "":
        return None

    clean = send_at_str.replace(" ET", "").strip()
    year = datetime.datetime.now().year
    try:
        dt = datetime.datetime.strptime(f"{clean} {year}", "%b %d, %I:%M %p %Y")
    except ValueError:
        log.warning(f"Cannot parse Send At: '{send_at_str}'")
        return None

    est = ZoneInfo("US/Eastern")
    return dt.replace(tzinfo=est)


def find_matching_draft(service, to_email, subject_fragment):
    """Find a Gmail draft matching the recipient and subject."""
    try:
        drafts = service.users().drafts().list(userId="me").execute()
        draft_list = drafts.get("drafts", [])

        for draft_meta in draft_list:
            draft = (
                service.users()
                .drafts()
                .get(userId="me", id=draft_meta["id"], format="metadata")
                .execute()
            )

            headers = draft.get("message", {}).get("payload", {}).get("headers", [])
            draft_to = ""
            draft_subject = ""
            for h in headers:
                if h["name"].lower() == "to":
                    draft_to = h["value"].lower()
                if h["name"].lower() == "subject":
                    draft_subject = h["value"]

            if (
                to_email.lower() in draft_to
                and subject_fragment.lower() in draft_subject.lower()
            ):
                return draft_meta["id"]

    except Exception as e:
        log.error(f"Error finding draft: {e}")
    return None


def send_draft(service, draft_id):
    """Send an existing Gmail draft."""
    try:
        result = (
            service.users().drafts().send(userId="me", body={"id": draft_id}).execute()
        )
        return True, result.get("id", "")
    except Exception as e:
        log.error(f"Failed to send draft: {e}")
        return False, str(e)


def sheets_retry(func, *args, retries=3, **kwargs):
    """Retry Sheets API calls with exponential backoff."""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                wait = 2 ** (attempt + 1)
                log.warning(f"Sheets rate limit, retrying in {wait}s...")
                time.sleep(wait)
            elif attempt < retries - 1:
                time.sleep(2)
            else:
                raise


# ============================================================================
# Main
# ============================================================================
def main():
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    est = ZoneInfo("US/Eastern")
    now = datetime.datetime.now(est)

    print(f"SEND SCHEDULED EMAILS  {now.strftime('%b %d, %Y %I:%M %p ET')}")
    print("-" * 50)

    # Load persistent state
    sent_log = load_sent_log()
    sent_log = cleanup_sent_log(sent_log)  # Prune old entries
    fail_counts = load_fail_counts()

    gmail = get_gmail_service()
    ws = get_sheets()
    time.sleep(1)

    data = sheets_retry(ws.get, "A1:Q600")
    time.sleep(1)

    if not data or len(data) < 2:
        print("No data found.")
        return

    sent_count = 0
    skipped_count = 0
    failed_count = 0
    dedup_count = 0
    dead_count = 0

    for i, row in enumerate(data[1:], start=2):
        row = _pad(row)

        send_at_str = row[C["send_at"]].strip()
        sent_date = row[C["sent_dt"]].strip()
        he = row[C["hm_email"]].strip()
        re_ = row[C["rec_email"]].strip()
        co = row[C["company"]].strip()
        title = row[C["title"]].strip()
        jid = row[C["job_id"]].strip()

        # Skip if no Send At, already sent, or no emails
        if not send_at_str or sent_date or (not he and not re_):
            continue

        # Parse Send At time
        send_time = parse_send_at(send_at_str)
        if not send_time:
            continue

        # Check if it's time to send
        if now < send_time:
            skipped_count += 1
            continue

        # Check staleness (skip if older than 7 days)
        age_hours = (now - send_time).total_seconds() / 3600
        if age_hours > 168:  # 7 days
            log.info(f"  {co}: Skipped (too old: {age_hours:.0f}h)")
            skipped_count += 1
            continue

        # Check confidence label — refuse to send Low confidence
        conf_label = row[C.get("confidence", 15)].strip() if len(row) > C.get("confidence", 15) else ""
        if conf_label == "Low":
            print(f"  {co}: Skipped (confidence: Low)")
            skipped_count += 1
            continue

        print(f"  {co} | {title[:40]}...")

        emails_sent = []
        row_had_failure = False

        # Process HM and Rec emails
        for label, email_field in [("HM", he), ("Rec", re_)]:
            if not email_field:
                continue

            for email in [e.strip() for e in email_field.split(",") if e.strip()]:
                subject_frag = title if not jid or jid == "N/A" else f"{title} | {jid}"
                subject_for_dedup = f"Prasad Kanade — Application for {subject_frag}"

                # --- Dead letter check ---
                row_key = get_row_key(co, title, email)
                current_fails = fail_counts.get(row_key, 0)
                if current_fails >= DEAD_LETTER_MAX:
                    print(
                        f"    ☠ {label} dead letter: {email} (failed {current_fails}x)"
                    )
                    dead_count += 1
                    continue

                # --- Deduplication check ---
                if is_duplicate(sent_log, email, subject_for_dedup):
                    print(f"    ⊘ {label} duplicate skipped: {email}")
                    dedup_count += 1
                    continue

                # --- Find and send draft ---
                draft_id = find_matching_draft(gmail, email, subject_frag)
                if draft_id:
                    success, msg_id = send_draft(gmail, draft_id)
                    if success:
                        print(f"    ✓ {label} sent: {email}")
                        emails_sent.append(email)
                        sent_count += 1
                        record_sent(sent_log, email, subject_for_dedup)
                        # Clear fail count on success
                        if row_key in fail_counts:
                            del fail_counts[row_key]
                    else:
                        print(f"    ✗ {label} failed: {email} ({msg_id})")
                        failed_count += 1
                        row_had_failure = True
                        # Increment fail count
                        fail_counts[row_key] = current_fails + 1
                        if fail_counts[row_key] >= DEAD_LETTER_MAX:
                            print(
                                f"    ☠ {label} moved to dead letter after {DEAD_LETTER_MAX} failures: {email}"
                            )
                else:
                    print(f"    ⊘ {label} draft not found: {email}")
                    skipped_count += 1

                time.sleep(3)

        # Update Sent Date if any emails were sent
        if emails_sent:
            sent_date_str = now.strftime("%b %d, %Y")
            try:
                sheets_retry(ws.update_acell, f"{_cl(C['sent_dt'])}{i}", sent_date_str)
                time.sleep(0.5)
            except Exception as e:
                log.error(f"Failed to update Sent Date for row {i}: {e}")

        # If row had failures, note it in the Notes column
        if row_had_failure:
            try:
                notes_col = _cl(C["notes"])
                existing_notes = sheets_retry(ws.acell, f"{notes_col}{i}")
                existing = (
                    existing_notes.value
                    if existing_notes and existing_notes.value
                    else ""
                )
                if "Send failed" not in existing:
                    fail_note = f"Send failed on {now.strftime('%b %d')}"
                    updated = (
                        f"{existing} | {fail_note}".strip(" |")
                        if existing
                        else fail_note
                    )
                    sheets_retry(ws.update_acell, f"{notes_col}{i}", updated)
                    time.sleep(0.5)
            except Exception as e:
                log.error(f"Failed to update notes for row {i}: {e}")

    # Save persistent state
    save_sent_log(sent_log)
    save_fail_counts(fail_counts)

    print("-" * 50)
    print(
        f"Sent: {sent_count} | Skipped: {skipped_count} | Failed: {failed_count} | Dedup: {dedup_count} | Dead: {dead_count}"
    )


if __name__ == "__main__":
    main()
