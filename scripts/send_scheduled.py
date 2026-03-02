#!/usr/bin/env python3
"""
Send Scheduled Emails
Reads Outreach Tracker for rows where:
  - Send At time has passed
  - Sent Date is empty (not yet sent)
  - HM or Rec email exists
Finds matching Gmail draft and sends it.
"""
import sys, os, re, datetime, time, logging, base64

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from outreach.outreach_config import C, SHEETS_CREDS, SS_NAME, O_TAB, SENDER_EMAIL
from outreach.outreach_data import _pad, _cl

import gspread
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def get_gmail_service():
    """Authenticate with Gmail using OAuth pickle token."""
    import pickle
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    token_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".local", "gmail_token.pickle"
    )
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
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(SHEETS_CREDS, scope)
    gc = gspread.authorize(creds)
    ss = gc.open(SS_NAME)
    return ss.worksheet(O_TAB)


def parse_send_at(send_at_str):
    """Parse 'Mar 02, 9:00 AM ET' into a datetime object in US/Eastern."""
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    if not send_at_str or send_at_str.strip() == "":
        return None

    # Format: "Mar 02, 9:00 AM ET" or "Mar 02, 10:00 AM ET"
    clean = send_at_str.replace(" ET", "").strip()
    # Try parsing with current year
    year = datetime.datetime.now().year
    try:
        dt = datetime.datetime.strptime(f"{clean} {year}", "%b %d, %I:%M %p %Y")
    except ValueError:
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
            draft = service.users().drafts().get(
                userId="me", id=draft_meta["id"], format="metadata",
                metadataHeaders=["To", "Subject"]
            ).execute()

            headers = draft.get("message", {}).get("payload", {}).get("headers", [])
            draft_to = ""
            draft_subject = ""
            for h in headers:
                if h["name"].lower() == "to":
                    draft_to = h["value"].lower()
                if h["name"].lower() == "subject":
                    draft_subject = h["value"]

            if to_email.lower() in draft_to and subject_fragment.lower() in draft_subject.lower():
                return draft_meta["id"]

    except Exception as e:
        log.error(f"Error finding draft: {e}")
    return None


def send_draft(service, draft_id):
    """Send an existing Gmail draft."""
    try:
        result = service.users().drafts().send(
            userId="me", body={"id": draft_id}
        ).execute()
        return True, result.get("id", "")
    except Exception as e:
        log.error(f"Failed to send draft: {e}")
        return False, str(e)


def main():
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    est = ZoneInfo("US/Eastern")
    now = datetime.datetime.now(est)

    print(f"SEND SCHEDULED EMAILS  {now.strftime('%b %d, %Y %I:%M %p ET')}")
    print("-" * 50)

    gmail = get_gmail_service()
    ws = get_sheets()
    time.sleep(1)

    # Read only the data rows (limit to avoid timeout)
    data = ws.get(f'A1:O500')
    time.sleep(1)

    if not data or len(data) < 2:
        print("No data found.")
        return

    sent_count = 0
    skipped_count = 0
    failed_count = 0

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

        # Check staleness (don't send if older than 7 days)
        age_hours = (now - send_time).total_seconds() / 3600
        if age_hours > 168:  # 7 days
            log.info(f"  {co}: Skipped (too old: {age_hours:.0f}h)")
            skipped_count += 1
            continue

        print(f"  {co} | {title[:40]}...")

        emails_sent = []

        # Send HM email
        if he:
            for email in [e.strip() for e in he.split(",") if e.strip()]:
                subject_frag = title if not jid or jid == "N/A" else f"{title} | {jid}"
                draft_id = find_matching_draft(gmail, email, subject_frag)
                if draft_id:
                    success, msg_id = send_draft(gmail, draft_id)
                    if success:
                        print(f"    ✓ HM sent: {email}")
                        emails_sent.append(email)
                        sent_count += 1
                    else:
                        print(f"    ✗ HM failed: {email} ({msg_id})")
                        failed_count += 1
                else:
                    print(f"    ⊘ HM draft not found: {email}")
                    skipped_count += 1
                time.sleep(1)

        # Send Rec email
        if re_:
            for email in [e.strip() for e in re_.split(",") if e.strip()]:
                subject_frag = title if not jid or jid == "N/A" else f"{title} | {jid}"
                draft_id = find_matching_draft(gmail, email, subject_frag)
                if draft_id:
                    success, msg_id = send_draft(gmail, draft_id)
                    if success:
                        print(f"    ✓ Rec sent: {email}")
                        emails_sent.append(email)
                        sent_count += 1
                    else:
                        print(f"    ✗ Rec failed: {email} ({msg_id})")
                        failed_count += 1
                else:
                    print(f"    ⊘ Rec draft not found: {email}")
                    skipped_count += 1
                time.sleep(1)

        # Update Sent Date if any emails were sent
        if emails_sent:
            sent_date_str = now.strftime("%b %d, %Y")
            try:
                ws.update_acell(f"{_cl(C['sent_dt'])}{i}", sent_date_str)
                time.sleep(0.5)
            except Exception as e:
                log.error(f"Failed to update Sent Date for row {i}: {e}")

    print("-" * 50)
    print(f"Sent: {sent_count} | Skipped: {skipped_count} | Failed: {failed_count}")


if __name__ == "__main__":
    main()
