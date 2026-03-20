#!/usr/bin/env python3
"""Nightly digest — sends run summary to prasadckanade@gmail.com"""
import sys, os, datetime, json, logging, re, sqlite3
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from outreach.outreach_config import (
    MS_SENDER_EMAIL, MS_SENDER_NAME, MS_CLIENT_ID, MS_AUTHORITY, MS_SCOPES, MS_TOKEN_FILE,
    SHEETS_CREDS, SPREADSHEET,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

_LOCAL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".local")
DIGEST_TO = "prasadckanade@gmail.com"


def _get_token():
    import msal
    cache = msal.SerializableTokenCache()
    if os.path.exists(MS_TOKEN_FILE):
        cache.deserialize(open(MS_TOKEN_FILE).read())
    app = msal.PublicClientApplication(MS_CLIENT_ID, authority=MS_AUTHORITY, token_cache=cache)
    accts = app.get_accounts()
    result = app.acquire_token_silent(MS_SCOPES, account=accts[0]) if accts else None
    if not result or "access_token" not in result:
        raise Exception("MS token expired")
    if cache.has_state_changed:
        open(MS_TOKEN_FILE, "w").write(cache.serialize())
    return result["access_token"]


def _latest_run_stats():
    """Get stats from SQLite run history."""
    db = os.path.join(_LOCAL, "run_history.db")
    if not os.path.exists(db):
        return {}
    try:
        con = sqlite3.connect(db)
        row = con.execute(
            "SELECT ts,valid,discarded,failed_http,elapsed_seconds FROM runs ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        con.close()
        if row:
            return {
                "ts": row[0][:16],
                "valid": row[1],
                "discarded": row[2],
                "failed_http": row[3],
                "elapsed_min": round(row[4] / 60, 1) if row[4] else 0,
            }
    except Exception as e:
        log.debug(f"DB read failed: {e}")
    return {}


def _bounced_today():
    """Count bounces in last 24h."""
    f = os.path.join(_LOCAL, "bounced_emails.json")
    if not os.path.exists(f):
        return 0
    try:
        d = json.load(open(f))
        cut = (datetime.datetime.now() - datetime.timedelta(hours=24)).isoformat()
        return sum(1 for v in d.values() if v.get("bounced_at", "") >= cut)
    except Exception:
        return 0


def _sent_today():
    """Count emails sent today from sent_log.json."""
    f = os.path.join(_LOCAL, "sent_log.json")
    if not os.path.exists(f):
        return 0
    try:
        d = json.load(open(f))
        today = datetime.date.today().isoformat()
        return sum(1 for v in d.values() if v[:10] == today)
    except Exception:
        return 0


def _recent_errors():
    """Get last 5 ERROR lines from outreach.log written today."""
    f = os.path.join(_LOCAL, "outreach.log")
    if not os.path.exists(f):
        return []
    today = datetime.date.today().strftime("%Y-%m-%d")
    errors = []
    try:
        with open(f, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if today in line and "| ERROR |" in line:
                    errors.append(line.strip()[:120])
        return errors[-5:]
    except Exception:
        return []


def _circuit_breaker_status():
    """Check circuit breaker state."""
    f = os.path.join(_LOCAL, "circuit_breaker.json")
    if not os.path.exists(f):
        return "OK"
    try:
        d = json.load(open(f))
        if d.get("tripped"):
            return f"TRIPPED: {d.get('trip_reason', '?')}"
        return f"OK (sent:{d.get('sent',0)}, bounced:{d.get('bounced',0)})"
    except Exception:
        return "Unknown"


def _outreach_queue_size():
    """Count rows with Extract=yes and no email yet."""
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        from outreach.outreach_config import C, OUTREACH_TAB
        from outreach.outreach_data import _pad
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SHEETS_CREDS, scope)
        ws = gspread.authorize(creds).open(SPREADSHEET).worksheet(OUTREACH_TAB)
        rows = ws.get_all_values()[1:]
        pending = sum(1 for r in rows
                      if len(r) > C["extract"]
                      and r[C["extract"]].strip().lower() == "yes"
                      and not r[C["hm_email"]].strip()
                      and not r[C["rec_email"]].strip())
        return pending
    except Exception:
        return -1


def _build_html(stats, sent, bounced, errors, cb, pending, burn_alerts=None):
    now = datetime.datetime.now().strftime("%b %d, %Y %I:%M %p")
    ok_color = "#2d8a4e"
    warn_color = "#b45309"
    err_color = "#b91c1c"

    def row(label, value, color="#333"):
        return f'<tr><td style="padding:6px 12px;color:#666;">{label}</td><td style="padding:6px 12px;font-weight:500;color:{color};">{value}</td></tr>'

    agg_html = ""
    if stats:
        agg_html = f"""
        <h3 style="color:#444;margin:20px 0 8px;">Aggregator (last run: {stats.get('ts','?')})</h3>
        <table style="border-collapse:collapse;width:100%;">
            {row("Valid jobs added", stats.get('valid',0), ok_color)}
            {row("Discarded", stats.get('discarded',0))}
            {row("HTTP failures", stats.get('failed_http',0), warn_color if stats.get('failed_http',0) > 5 else "#333")}
            {row("Run time", f"{stats.get('elapsed_min',0)} min")}
        </table>"""

    errors_html = ""
    if errors:
        err_lines = "".join(f"<li style='font-size:12px;color:{err_color};margin:4px 0;'>{e}</li>" for e in errors)
        errors_html = f"<h3 style='color:{err_color};margin:20px 0 8px;'>Errors today</h3><ul>{err_lines}</ul>"

    cb_color = err_color if "TRIPPED" in cb else ok_color

    return f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px;">
      <h2 style="color:#1a1a2e;border-bottom:2px solid #e2e8f0;padding-bottom:10px;">
        Job Hunt Pipeline — {now}
      </h2>

      <h3 style="color:#444;margin:20px 0 8px;">Outreach</h3>
      <table style="border-collapse:collapse;width:100%;">
        {row("Emails sent today", sent, ok_color if sent > 0 else "#333")}
        {row("Bounces (24h)", bounced, warn_color if bounced > 0 else ok_color)}
        {row("Pending extraction", pending if pending >= 0 else "?")}
        {row("Circuit breaker", cb, cb_color)}
      </table>

      {agg_html}
      {errors_html}

      {f'<h3 style="color:#b45309;margin:20px 0 8px;">⚠ API Credit Warnings</h3><ul>' + ''.join(f'<li style="color:#b45309;font-size:12px;">{a}</li>' for a in burn_alerts) + '</ul>' if burn_alerts else ''}
      <p style="color:#999;font-size:11px;margin-top:20px;">
        Sent from your Job Hunt Pipeline · kanade.pra@northeastern.edu
      </p>
    </div>
    """


def main():
    print(f"NIGHTLY DIGEST  {datetime.datetime.now().strftime('%b %d %I:%M %p')}")

    stats   = _latest_run_stats()
    sent    = _sent_today()
    bounced = _bounced_today()
    errors  = _recent_errors()
    cb      = _circuit_breaker_status()
    pending = _outreach_queue_size()
    burn_alerts = []
    try:
        from outreach.brain import Brain
        from outreach.outreach_data import Credits
        burn_alerts = Credits().burn_rate_alerts()
    except Exception:
        pass

    subject = f"Job Hunt — {datetime.date.today().strftime('%b %d')} | {stats.get('valid',0)} new jobs, {sent} emails sent"
    if errors:
        subject = "⚠ " + subject
    if "TRIPPED" in cb:
        subject = "🚨 Circuit breaker tripped! " + subject
    if burn_alerts:
        subject = "💳 " + subject

    html = _build_html(stats, sent, bounced, errors, cb, pending, burn_alerts)

    try:
        import requests as _req
        token = _get_token()
        payload = {
            "message": {
                "subject": subject,
                "body": {"contentType": "HTML", "content": html},
                "toRecipients": [{"emailAddress": {"address": DIGEST_TO}}],
                "from": {"emailAddress": {"name": MS_SENDER_NAME, "address": MS_SENDER_EMAIL}},
            },
            "saveToSentItems": "false",
        }
        resp = _req.post(
            f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/sendMail",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload, timeout=30,
        )
        if resp.status_code in (200, 202):
            print(f"  Digest sent to {DIGEST_TO}")
        else:
            print(f"  Digest failed: {resp.status_code} {resp.text[:100]}")
    except Exception as e:
        print(f"  Digest error: {e}")


if __name__ == "__main__":
    main()
