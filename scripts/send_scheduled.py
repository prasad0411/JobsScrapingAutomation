#!/usr/bin/env python3
"""Send scheduled outreach emails via Microsoft Graph from kanade.pra@northeastern.edu"""
import sys, os, datetime, time, logging, json, base64, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from outreach.outreach_config import (
    C, SHEETS_CREDS, SPREADSHEET, OUTREACH_TAB,
    MS_SENDER_EMAIL, MS_SENDER_NAME, MS_CLIENT_ID, MS_AUTHORITY, MS_SCOPES, MS_TOKEN_FILE,
    HM_SUBJ, HM_BODY, REC_SUBJ, REC_BODY, RESUME_SDE, RESUME_ML, RESUME_DA,
)
from outreach.outreach_data import _pad, _cl, NameParser
from outreach.brain import Brain
import gspread, requests as _req
from oauth2client.service_account import ServiceAccountCredentials

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

_LOCAL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".local")
SENT_LOG_FILE   = os.path.join(_LOCAL, "sent_log.json")
FAIL_COUNT_FILE = os.path.join(_LOCAL, "send_fail_counts.json")
DEAD_MAX = 3


# ── helpers ──────────────────────────────────────────────────────────────────

def _load(f):
    try:
        if os.path.exists(f): return json.load(open(f))
    except Exception: pass
    return {}

def _save(f, d):
    try: json.dump(d, open(f, "w"), indent=2)
    except Exception as e: log.error(f"save failed {f}: {e}")

def _dup(sl, email, subj, days=7):
    k = f"{email.lower()}||{subj.lower()}"
    ts = sl.get(k)
    if not ts: return False
    try: return (datetime.datetime.now() - datetime.datetime.fromisoformat(ts)).days < days
    except Exception: return False

def _rec(sl, email, subj):
    sl[f"{email.lower()}||{subj.lower()}"] = datetime.datetime.now().isoformat()

def _clean_sl(sl, days=14):
    cut = datetime.datetime.now() - datetime.timedelta(days=days)
    return {k: v for k, v in sl.items() if datetime.datetime.fromisoformat(v) > cut}

def _row_key(co, title, email):
    return f"{co.lower()}||{title.lower()}||{email.lower()}"

def _parse_send_at(s):
    if not s or not s.strip(): return None
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    clean = s.replace(" ET", "").replace(" PT", "").replace(" CT", "").replace(" MT", "").strip()
    yr = datetime.datetime.now().year
    for fmt in ["%b %d, %I:%M %p %Y", "%b %d %I:%M %p %Y"]:
        try:
            dt = datetime.datetime.strptime(f"{clean} {yr}", fmt)
            return dt.replace(tzinfo=ZoneInfo("US/Eastern"))
        except ValueError:
            continue
    return None

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
        open(MS_TOKEN_FILE, "w").write(cache.serialize())
    return result["access_token"]

def _build_email(name, contact_type, co, title, jid):
    parsed = NameParser.parse(name)
    first = parsed["first"] if parsed else (name.split()[0] if name else co)
    st, bt = (HM_SUBJ, HM_BODY) if contact_type == "hm" else (REC_SUBJ, REC_BODY)
    jid_clean = jid if jid and jid not in ("N/A", "") else ""
    if not jid_clean:
        st = st.replace(" | {job_id}", "")
        bt = bt.replace(" | {job_id}", "")
    vals = {"first": first, "title": title, "job_id": jid_clean, "company": co}
    for k, v in vals.items():
        st = st.replace(f"{{{k}}}", v)
        bt = bt.replace(f"{{{k}}}", v)
    return st, bt.replace("\n\n\n", "\n\n")

def _to_html(body):
    style = "font-family:Arial,sans-serif;font-size:14px;line-height:1.6;color:#333;margin:0 0 14px 0;"
    parts = [f'<p style="{style}">{p.strip().replace(chr(10),"<br>")}</p>'
             for p in body.split("\n\n") if p.strip()]
    return '<div style="font-family:Arial,sans-serif;">' + "".join(parts) + "</div>"

def _resume_path(resume_type):
    return {"ML": RESUME_ML, "DA": RESUME_DA}.get(resume_type, RESUME_SDE)

def _send_ms(token, to_email, subject, body_html, resume_type):
    attach = []
    rp = _resume_path(resume_type)
    if os.path.exists(rp):
        attach = [{
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": os.path.basename(rp),
            "contentType": "application/pdf",
            "contentBytes": base64.b64encode(open(rp, "rb").read()).decode(),
        }]
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body_html},
            "toRecipients": [{"emailAddress": {"address": to_email}}],
            "from": {"emailAddress": {"name": MS_SENDER_NAME, "address": MS_SENDER_EMAIL}},
            "replyTo": [{"emailAddress": {"name": MS_SENDER_NAME, "address": MS_SENDER_EMAIL}}],
        },
        "saveToSentItems": "true",
    }
    if attach:
        payload["message"]["attachments"] = attach
    resp = _req.post(
        f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/sendMail",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload, timeout=30,
    )
    if resp.status_code not in (200, 202):
        raise Exception(f"Graph {resp.status_code}: {resp.text[:200]}")
    return True

def _get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SHEETS_CREDS, scope)
    return gspread.authorize(creds).open(SPREADSHEET).worksheet(OUTREACH_TAB)

def _sheets_retry(fn, *a, **kw):
    for i in range(3):
        try: return fn(*a, **kw)
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                time.sleep(2 ** (i + 1))
            elif i < 2: time.sleep(2)
            else: raise


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    est = ZoneInfo("US/Eastern")
    now = datetime.datetime.now(est)
    print(f"SEND SCHEDULED  {now.strftime('%b %d, %Y %I:%M %p ET')}")
    print("-" * 50)

    sl = _clean_sl(_load(SENT_LOG_FILE))
    fc = _load(FAIL_COUNT_FILE)

    try:
        token = _get_token()
    except Exception as e:
        print(f"  ERROR: {e}")
        return

    ws = _get_sheets()
    time.sleep(1)
    data = _sheets_retry(ws.get_all_values)
    time.sleep(1)
    if not data or len(data) < 2:
        print("No data."); return

    sent = skipped = failed = dedup = dead = 0
    # Brain-adaptive send limit based on 7-day rolling bounce rate
    def _adaptive_limit():
        try:
            b = Brain.get()
            cb = b._data.get("circuit_breaker", {})
            sent_today = cb.get("sent_today", 0)
            bounced_today = cb.get("bounced_today", 0)
            rate = bounced_today / sent_today if sent_today >= 5 else 0.0
            # Check 7-day history from Brain
            from outreach.outreach_data import Credits
            cr = Credits()
            if rate == 0.0 and sent_today < 5:
                # Use historical bounce rate from sent_log
                try:
                    sl_data = _load(SENT_LOG_FILE)
                    total_sent = len(sl_data)
                    # Conservative: stay at 20 until we have solid history
                    if total_sent < 50:
                        return 20
                except Exception:
                    pass
            if rate <= 0.02:   return 30  # <2% bounce → 30/run
            elif rate <= 0.05: return 25  # <5% bounce → 25/run
            elif rate <= 0.10: return 20  # <10% bounce → 20/run
            else:              return 15  # >10% bounce → back to 15
        except Exception:
            return 15  # safe fallback
    MAX_PER_RUN = _adaptive_limit()
    log.info(f"Adaptive send limit: {MAX_PER_RUN}/run")

    for i, row in enumerate(data[1:], start=2):
        row = _pad(row)
        send_at_str  = row[C["send_at"]].strip()
        sent_date    = row[C["sent_dt"]].strip()
        he           = row[C["hm_email"]].strip()
        re_          = row[C["rec_email"]].strip()
        co           = row[C["company"]].strip()
        title        = row[C["title"]].strip()
        jid          = row[C["job_id"]].strip()
        hn           = row[C["hm_name"]].strip()
        rn           = row[C["rec_name"]].strip()
        conf         = row[C["confidence"]].strip() if len(row) > C["confidence"] else ""
        resume_type  = "SDE"

        # Skip if no Send At, already sent, no emails, or low confidence
        if not send_at_str or sent_date or (not he and not re_):
            continue
        if conf == "Low":
            skipped += 1; continue
        if sent >= MAX_PER_RUN:
            skipped += 1; continue

        send_time = _parse_send_at(send_at_str)
        if not send_time or now < send_time:
            skipped += 1; continue

        # Skip if stale (>7 days)
        if (now - send_time).total_seconds() > 168 * 3600:
            skipped += 1; continue

        # Get resume type directly from Valid Entries sheet (cache-safe)
        try:
            import gspread as _gs
            from oauth2client.service_account import ServiceAccountCredentials as _SAC
            from outreach.outreach_config import SHEETS_CREDS, SPREADSHEET
            if not hasattr(_parse_send_at, "_resume_map"):
                _scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
                _creds = _SAC.from_json_keyfile_name(SHEETS_CREDS, _scope)
                _ws = _gs.authorize(_creds).open(SPREADSHEET).worksheet("Valid Entries")
                _parse_send_at._resume_map = {}
                for _r in _ws.get_all_values()[1:]:
                    if len(_r) > 9:
                        _k = (_r[2].strip().lower(), _r[3].strip().lower())
                        _v = _r[9].strip()
                        _parse_send_at._resume_map[_k] = _v if _v in ("SDE","ML","DA") else "SDE"
            resume_type = _parse_send_at._resume_map.get((co.lower(), title.lower()), "SDE")
        except Exception:
            pass

        print(f"  {co} | {title[:35]}...")
        emails_sent_this_row = []
        row_failed = False

        for label, email_field, name in [("HM", he, hn), ("Rec", re_, rn)]:
            if not email_field: continue
            for email in [e.strip() for e in email_field.split(",") if e.strip()]:
                subj, body = _build_email(name or co, "hm" if label == "HM" else "rec", co, title, jid)
                rk = _row_key(co, title, email)

                if fc.get(rk, 0) >= DEAD_MAX:
                    print(f"    x {label} dead: {email}"); dead += 1; continue
                if _dup(sl, email, subj):
                    print(f"    o {label} dup: {email}"); dedup += 1; continue

                try:
                    _send_ms(token, email, subj, _to_html(body), resume_type)
                    print(f"    + {label} sent: {email}")
                    emails_sent_this_row.append(email)
                    sent += 1
                    _rec(sl, email, subj)
                    fc.pop(rk, None)
                    try:
                        Brain.get().cb_record_send()
                    except Exception:
                        pass
                except Exception as e:
                    print(f"    - {label} failed: {email} ({e})")
                    failed += 1; row_failed = True
                    fc[rk] = fc.get(rk, 0) + 1

                # Brain-adaptive delay: faster if clean, slower if bounces elevated
                try:
                    _cb = Brain.get()._data.get("circuit_breaker", {})
                    _s = _cb.get("sent_today", 0)
                    _b = _cb.get("bounced_today", 0)
                    _r = _b / _s if _s >= 5 else 0.0
                    _delay = 60 if _r > 0.05 else (30 if _r == 0.0 and _s > 10 else 45)
                except Exception:
                    _delay = 45
                time.sleep(_delay)

        if emails_sent_this_row:
            try:
                _sheets_retry(ws.update_acell, f"{_cl(C['sent_dt'])}{i}", now.strftime("%b %d, %Y"))
                time.sleep(0.5)
            except Exception as e:
                log.error(f"Sent Date update failed row {i}: {e}")

        if row_failed:
            try:
                nc = _cl(C["notes"])
                existing = (_sheets_retry(ws.acell, f"{nc}{i}").value or "")
                time.sleep(0.5)
                if "Send failed" not in existing:
                    note = f"Send failed {now.strftime('%b %d')}"
                    _sheets_retry(ws.update_acell, f"{nc}{i}",
                                  f"{existing} | {note}".strip(" |") if existing else note)
                    time.sleep(0.5)
            except Exception as e:
                log.error(f"Notes update failed row {i}: {e}")

    _save(SENT_LOG_FILE, sl)
    _save(FAIL_COUNT_FILE, fc)
    print("-" * 50)
    print(f"Sent:{sent} Skipped:{skipped} Failed:{failed} Dup:{dedup} Dead:{dead}")


if __name__ == "__main__":
    main()
