#!/usr/bin/env python3
"""
Send scheduled outreach emails from Outlook 'Scheduled Outreach' folder.
Drafts are created by outreach_mailer.py with X-Send-At / X-Company headers.
Run every 15 min via launchd — zero manual intervention needed.
"""
import sys, os, datetime, time, logging, json, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from outreach.outreach_config import (
    MS_SENDER_EMAIL, MS_CLIENT_ID, MS_AUTHORITY, MS_SCOPES, MS_TOKEN_FILE,
    SHEETS_CREDS, SPREADSHEET, OUTREACH_TAB, C,
)
from outreach.outreach_data import _cl
from outreach.brain import Brain
import requests as _req, gspread
from oauth2client.service_account import ServiceAccountCredentials

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# ── smart bounce-retry ────────────────────────────────────────────────────────

def _email_to_pattern(email: str) -> str:
    """Reverse-engineer the pattern used to generate this email."""
    local = email.split("@")[0].lower()
    if "." in local:
        parts = local.split(".")
        if len(parts[0]) == 1:
            return "{f}.{last}"
        return "{first}.{last}"
    if "_" in local:
        return "{first}_{last}"
    if "-" in local:
        return "{first}-{last}"
    return "{first}"


def _next_best_email(original_email: str, name: str, brain) -> str | None:
    """
    Given a bounced email, ask Brain for the next best pattern for that domain.
    Returns a new candidate email or None if no alternatives exist.
    """
    if not original_email or "@" not in original_email:
        return None
    domain = original_email.split("@")[1].lower()
    bad_pattern = _email_to_pattern(original_email)

    # Record this pattern as failed in Brain
    brain.record_pattern_failure(domain, bad_pattern)

    # Parse name
    try:
        from outreach.outreach_data import NameParser
        parsed = NameParser.parse(name)
        if not parsed:
            return None
        first = parsed.get("first", "").lower()
        last = parsed.get("last", "").lower()
        f = first[0] if first else ""
        if not first or not last:
            return None
    except Exception:
        return None

    # All possible patterns
    all_patterns = [
        "{first}.{last}", "{f}.{last}", "{first}{last}",
        "{first}_{last}", "{first}-{last}", "{first}",
        "{last}.{first}", "{last}{first}",
    ]

    # Rank by Brain posterior, excluding already-failed patterns
    ranked = brain.rank_patterns_for(domain, all_patterns)

    # Generate email from top-ranked pattern
    for pattern in ranked:
        candidate = (
            pattern
            .replace("{first}", first)
            .replace("{last}", last)
            .replace("{f}", f)
        )
        if candidate != original_email.split("@")[0]:
            return f"{candidate}@{domain}"
    return None


def _load_fail_counts():
    """Load send fail counts — Brain is source of truth, file is fallback."""
    fc_file = os.path.join(_LOCAL, "send_fail_counts.json")
    try:
        file_fc = json.load(open(fc_file)) if os.path.exists(fc_file) else {}
    except Exception:
        file_fc = {}
    try:
        from outreach.brain import Brain
        brain_fc = Brain.get()._data.get("send_fail_counts", {})
        # Merge — take max of file and Brain
        for k, v in brain_fc.items():
            file_fc[k] = max(file_fc.get(k, 0), v)
    except Exception:
        pass
    return file_fc


def _save_fail_counts(fc):
    """Save fail counts to both file and Brain."""
    try:
        json.dump(fc, open(os.path.join(_LOCAL, "send_fail_counts.json"), "w"), indent=2)
    except Exception as e:
        log.error(f"fail count file save: {e}")
    try:
        from outreach.brain import Brain
        b = Brain.get()
        b._data["send_fail_counts"] = fc
        b.save()
    except Exception as e:
        log.error(f"fail count Brain save: {e}")



_LOCAL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".local")
SENT_LOG_FILE = os.path.join(_LOCAL, "sent_log.json")

SCHEDULED_FOLDER  = "Scheduled Outreach"
COLD_EMAILING_FOLDER = "Cold Emailing"

_TZ_MAP = {
    "new york": "America/New_York",   "boston": "America/New_York",
    "washington": "America/New_York", "atlanta": "America/New_York",
    "miami": "America/New_York",      "pittsburgh": "America/New_York",
    "chicago": "America/Chicago",     "dallas": "America/Chicago",
    "houston": "America/Chicago",     "austin": "America/Chicago",
    "minneapolis": "America/Chicago", "kansas city": "America/Chicago",
    "denver": "America/Denver",       "salt lake city": "America/Denver",
    "phoenix": "America/Phoenix",
    "san francisco": "America/Los_Angeles", "seattle": "America/Los_Angeles",
    "los angeles": "America/Los_Angeles",   "san jose": "America/Los_Angeles",
    "portland": "America/Los_Angeles",      "san diego": "America/Los_Angeles",
    "silicon valley": "America/Los_Angeles","bay area": "America/Los_Angeles",
}

# ── auth ──────────────────────────────────────────────────────────────────────

def _get_token():
    import msal
    cache = msal.SerializableTokenCache()
    if os.path.exists(MS_TOKEN_FILE):
        cache.deserialize(open(MS_TOKEN_FILE).read())
    app = msal.PublicClientApplication(MS_CLIENT_ID, authority=MS_AUTHORITY, token_cache=cache)
    accts = app.get_accounts()
    result = app.acquire_token_silent(MS_SCOPES, account=accts[0]) if accts else None
    if not result or "access_token" not in result:
        raise Exception("MS token expired — run: python3 scripts/test_ms_auth.py")
    if cache.has_state_changed:
        # File lock prevents race condition when multiple jobs refresh token simultaneously
        import fcntl as _fcntl
        with open(MS_TOKEN_FILE + ".lock", "w") as _lf:
            _fcntl.flock(_lf, _fcntl.LOCK_EX)
            open(MS_TOKEN_FILE, "w").write(cache.serialize())
            _fcntl.flock(_lf, _fcntl.LOCK_UN)
            _fcntl.flock(_lf, _fcntl.LOCK_UN)
    return result["access_token"]

# ── folder helpers ────────────────────────────────────────────────────────────

_FOLDER_CACHE = {}

def _get_folder_id(token, name):
    if name in _FOLDER_CACHE:
        return _FOLDER_CACHE[name]
    resp = _req.get(
        f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/mailFolders",
        headers={"Authorization": f"Bearer {token}"},
        params={"$top": 50}, timeout=10,
    )
    if resp.status_code == 200:
        for f in resp.json().get("value", []):
            _FOLDER_CACHE[f["displayName"]] = f["id"]
    return _FOLDER_CACHE.get(name)


def _ensure_folder(token, name):
    fid = _get_folder_id(token, name)
    if fid:
        return fid
    resp = _req.post(
        f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/mailFolders",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"displayName": name}, timeout=10,
    )
    if resp.status_code in (200, 201):
        fid = resp.json()["id"]
        _FOLDER_CACHE[name] = fid
        log.info(f"Created Outlook folder: {name}")
        return fid
    raise Exception(f"Could not create folder '{name}': {resp.text[:100]}")


def _get_drafts_in_folder(token, folder_id):
    msgs, url = [], (
        f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}"
        f"/mailFolders/{folder_id}/messages"
        f"?$top=50&$select=id,subject,toRecipients,internetMessageHeaders,createdDateTime"
    )
    while url:
        resp = _req.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if resp.status_code != 200:
            break
        data = resp.json()
        msgs.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return msgs


def _header(msg, name):
    for h in msg.get("internetMessageHeaders", []) or []:
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""

# ── timezone helpers ──────────────────────────────────────────────────────────

def _tz_for(company, location, brain):
    try:
        tz = brain._data.get("companies", {}).get(company.lower(), {}).get("timezone")
        if tz:
            return tz
    except Exception:
        pass
    loc = (location or "").lower()
    for city, tz in _TZ_MAP.items():
        if city in loc:
            return tz
    return "America/New_York"


def _should_send(send_at_iso):
    if not send_at_iso:
        return False
    try:
        # Try ISO format first: 2026-03-23T10:00:00
        try:
            send_at = datetime.datetime.fromisoformat(send_at_iso)
        except Exception:
            # Fallback: human format "Mar 23, 10:00 AM ET"
            clean = re.sub(r"\s*(ET|EST|EDT|PT|CT|MT)\s*$", "", send_at_iso.strip())
            year = datetime.datetime.now().year
            send_at = datetime.datetime.strptime(f"{clean} {year}", "%b %d, %I:%M %p %Y")
            # Treat as US Eastern
            from zoneinfo import ZoneInfo
            send_at = send_at.replace(tzinfo=ZoneInfo("America/New_York"))
        if send_at.tzinfo is None:
            send_at = send_at.replace(tzinfo=datetime.timezone.utc)
        now = datetime.datetime.now(datetime.timezone.utc)
        if (now - send_at).total_seconds() > 7 * 24 * 3600:
            return False  # stale — older than 7 days
        return now >= send_at
    except Exception:
        return False

# ── send + move ───────────────────────────────────────────────────────────────

def _send_draft(token, msg_id):
    resp = _req.post(
        f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/messages/{msg_id}/send",
        headers={"Authorization": f"Bearer {token}", "Content-Length": "0"},
        timeout=30,
    )
    if resp.status_code not in (200, 202):
        raise Exception(f"Send failed {resp.status_code}: {resp.text[:150]}")


def _move(token, msg_id, folder_id):
    resp = _req.post(
        f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/messages/{msg_id}/move",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"destinationId": folder_id}, timeout=10,
    )
    return resp.status_code in (200, 201)

# ── sheet update ──────────────────────────────────────────────────────────────

def _get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SHEETS_CREDS, scope)
    return gspread.authorize(creds).open(SPREADSHEET).worksheet(OUTREACH_TAB)


def _mark_sent(ws, company, title, date_str):
    try:
        data = ws.get_all_values()
        for i, row in enumerate(data[1:], start=2):
            if (len(row) > max(C["company"], C["title"]) and
                    row[C["company"]].strip().lower() == company.lower() and
                    row[C["title"]].strip().lower() == title.lower() and
                    not row[C["sent_dt"]].strip()):
                ws.update_acell(f"{_cl(C['sent_dt'])}{i}", date_str)
                time.sleep(0.5)
                return
    except Exception as e:
        log.debug(f"Sheet mark-sent failed: {e}")

# ── sent log ──────────────────────────────────────────────────────────────────

def _load_sl():
    try:
        if os.path.exists(SENT_LOG_FILE):
            return json.load(open(SENT_LOG_FILE))
    except Exception:
        pass
    return {}


def _save_sl(sl):
    try:
        json.dump(sl, open(SENT_LOG_FILE, "w"), indent=2)
    except Exception as e:
        log.error(f"Sent log save: {e}")


def _is_dup(sl, email, subj):
    k = f"{email.lower()}||{subj.lower()}"
    ts = sl.get(k)
    if not ts:
        return False
    try:
        return (datetime.datetime.now() - datetime.datetime.fromisoformat(ts)).days < 7
    except Exception:
        return False


def _rec_sent(sl, email, subj):
    sl[f"{email.lower()}||{subj.lower()}"] = datetime.datetime.now().isoformat()

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    now_et = datetime.datetime.now(ZoneInfo("America/New_York"))
    print(f"SEND SCHEDULED (Outlook Drafts)  {now_et.strftime('%b %d, %Y %I:%M %p ET')}")
    print("-" * 55)

    try:
        token = _get_token()
    except Exception as e:
        print(f"  ERROR: {e}"); return

    brain = Brain.get()
    sl    = _load_sl()

    # Load bounce lists — skip addresses confirmed bounced via NDR processor
    _BOUNCE_LOG = os.path.join(_LOCAL, "bounce_log.json")
    _BOUNCED_F  = os.path.join(_LOCAL, "bounced_emails.json")
    try:
        _bl = json.load(open(_BOUNCE_LOG)) if os.path.exists(_BOUNCE_LOG) else {}
        _be = json.load(open(_BOUNCED_F))  if os.path.exists(_BOUNCED_F)  else {}
    except Exception:
        _bl, _be = {}, {}
    all_bounced = set(list(_bl.keys()) + list(_be.keys()))
    if all_bounced:
        log.info(f"Bounce skip list: {len(all_bounced)} addresses loaded")

    ws    = None  # lazy — only loaded if we actually send

    scheduled_fid = _ensure_folder(token, SCHEDULED_FOLDER)
    cold_fid      = _ensure_folder(token, COLD_EMAILING_FOLDER)

    # Adaptive send limit
    try:
        cb = brain._data.get("circuit_breaker", {})
        s, b = cb.get("sent_today", 0), cb.get("bounced_today", 0)
        rate = b / s if s >= 5 else 0.0
        max_send = 30 if rate <= 0.02 else 25 if rate <= 0.05 else 20 if rate <= 0.10 else 15
    except Exception:
        max_send = 15
    log.info(f"Adaptive send limit: {max_send}/run")

    drafts = _get_drafts_in_folder(token, scheduled_fid)
    print(f"  Found {len(drafts)} draft(s) in '{SCHEDULED_FOLDER}'")

    sent_n = skipped = failed = dedup = 0

    for msg in drafts:
        if sent_n >= max_send:
            skipped += 1; continue

        msg_id   = msg["id"]
        subject  = msg.get("subject", "")
        recips   = [r["emailAddress"]["address"]
                    for r in msg.get("toRecipients", []) if r.get("emailAddress")]
        to_email = recips[0] if recips else ""

        if not to_email:
            skipped += 1; continue

        send_at_iso  = _header(msg, "X-Send-At")
        company      = _header(msg, "X-Company")
        title        = _header(msg, "X-Job-Title")
        location     = _header(msg, "X-Location")
        confidence   = _header(msg, "X-Confidence")

        # Skip very low confidence emails (pattern guess, unverified)
        try:
            conf_val = float(confidence) if confidence else 100.0
            if conf_val < 50:
                log.info(f"Low confidence skip: {to_email} conf={conf_val}")
                print(f"  ⊘ Skipped (low confidence {conf_val}): {to_email}")
                skipped += 1; continue
        except Exception:
            pass

        if _is_dup(sl, to_email, subject):
            log.debug(f"Dup: {to_email}"); dedup += 1; continue

        # Skip previously bounced addresses (learned from NDR processor)
        if to_email.lower() in all_bounced:
            log.info(f"Bounce skip: {to_email} (previously bounced)")
            print(f"  ⊘ Skipped (bounced): {to_email}")
            skipped += 1; continue

        # Pre-send MX check: verify domain can still receive email
        try:
            import dns.resolver as _dns
            _domain = to_email.split("@")[1]
            _dns.resolve(_domain, "MX", lifetime=5)
        except Exception as _mx_e:
            log.warning(f"MX check failed for {to_email}: {_mx_e} — skipping")
            print(f"  ⊘ Skipped (no MX): {to_email}")
            skipped += 1; continue

        if not _should_send(send_at_iso):
            log.debug(f"Not yet: {company} | send_at={send_at_iso}"); skipped += 1; continue

        print(f"  → {company} | {to_email} | {subject[:45]}")

        try:
            _send_draft(token, msg_id)
            print(f"    ✓ Sent")
            sent_n += 1
            _rec_sent(sl, to_email, subject)
            # Record pattern success in Brain — self-learning
            try:
                domain = to_email.split("@")[1]
                pattern = _email_to_pattern(to_email)
                brain.record_pattern_success(domain, pattern, to_email)
            except Exception:
                pass
            try: brain.cb_record_send()
            except Exception as _cb_e: log.debug(f'cb_record_send failed: {_cb_e}')

            # Move sent copy from Sent Items → Cold Emailing
            time.sleep(3)
            try:
                safe = subject.replace("'", "''")
                # Note: $orderby removed — causes 400 on some tenants
                si = _req.get(
                    f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}"
                    f"/mailFolders/sentitems/messages",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"$filter": f"subject eq '{safe}'", "$top": 10},
                    timeout=10,
                )
                moved = False
                if si.status_code == 200:
                    # Find most recent match for this recipient
                    candidates = []
                    for m in si.json().get("value", []):
                        rs = [r["emailAddress"]["address"].lower()
                              for r in m.get("toRecipients", [])]
                        if to_email.lower() in rs:
                            candidates.append(m)
                    if candidates:
                        # Sort by sentDateTime descending in Python
                        candidates.sort(
                            key=lambda x: x.get("sentDateTime", x.get("createdDateTime", "")),
                            reverse=True
                        )
                        if _move(token, candidates[0]["id"], cold_fid):
                            print(f"    ✓ Moved → '{COLD_EMAILING_FOLDER}'")
                            moved = True
                if not moved:
                    print(f"    ⚠ Could not move to '{COLD_EMAILING_FOLDER}' (will retry next run)")
            except Exception as me:
                log.debug(f"Move failed: {me}")

            # Update sheet
            if company and title:
                try:
                    if ws is None:
                        ws = _get_sheets(); time.sleep(1)
                    _mark_sent(ws, company, title, now_et.strftime("%b %d, %Y"))
                except Exception as se:
                    log.debug(f"Sheet update: {se}")

        except Exception as e:
            print(f"    ✗ {e}")
            failed += 1
            # Smart bounce-retry: find next best pattern and queue new draft
            try:
                next_email = _next_best_email(to_email, company, brain)
                if next_email:
                    print(f"    ↻ Retrying with next pattern: {next_email}")
                    # Get the full draft message to clone it
                    clone_resp = _req.get(
                        f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/messages/{msg_id}",
                        headers={"Authorization": f"Bearer {token}"},
                        params={"$select": "subject,body,attachments,internetMessageHeaders"},
                        timeout=10,
                    )
                    if clone_resp.status_code == 200:
                        orig = clone_resp.json()
                        # Build new draft with corrected email
                        new_payload = {
                            "subject": orig.get("subject", subject),
                            "body": orig.get("body", {"contentType": "HTML", "content": ""}),
                            "toRecipients": [{"emailAddress": {"address": next_email}}],
                            "internetMessageHeaders": orig.get("internetMessageHeaders", []),
                            "isDraft": True,
                        }
                        # Update X-Send-At to now + 1 hour
                        import datetime as _dt
                        new_send_at = (_dt.datetime.now(_dt.timezone.utc) +
                                       _dt.timedelta(hours=1)).isoformat()
                        new_payload["internetMessageHeaders"] = [
                            h for h in new_payload["internetMessageHeaders"]
                            if h.get("name") != "X-Send-At"
                        ] + [{"name": "X-Send-At", "value": new_send_at}]
                        cr = _req.post(
                            f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/messages",
                            headers={"Authorization": f"Bearer {token}",
                                     "Content-Type": "application/json"},
                            json=new_payload, timeout=30,
                        )
                        if cr.status_code in (200, 201):
                            new_id = cr.json()["id"]
                            _req.post(
                                f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/messages/{new_id}/move",
                                headers={"Authorization": f"Bearer {token}",
                                         "Content-Type": "application/json"},
                                json={"destinationId": scheduled_fid}, timeout=10,
                            )
                            print(f"    ✓ Retry draft queued: {next_email}")
                        else:
                            print(f"    ✗ Retry draft failed: {cr.status_code}")
            except Exception as re_err:
                log.debug(f"Bounce retry failed: {re_err}")

        # Adaptive delay
        try:
            cb  = brain._data.get("circuit_breaker", {})
            s   = cb.get("sent_today", 0)
            b   = cb.get("bounced_today", 0)
            r   = b / s if s >= 5 else 0.0
            delay = 60 if r > 0.05 else (30 if s > 10 else 45)
        except Exception:
            delay = 45
        time.sleep(delay)

    _save_sl(sl)
    print("-" * 55)
    print(f"Sent:{sent_n}  Skipped:{skipped}  Failed:{failed}  Dedup:{dedup}")


if __name__ == "__main__":
    main()
