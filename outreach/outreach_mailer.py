#!/usr/bin/env python3
"""Outreach Pipeline — Gmail Draft Creator + Email Drafter."""

import os, pickle, time, random, datetime, logging, json, base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from outreach.outreach_config import (
    GMAIL_CREDS,
    GMAIL_TOKEN,
    GMAIL_SCOPES,
    SENDER_NAME,
    SENDER_EMAIL,
    MS_SENDER_EMAIL,
    MS_SENDER_NAME,
    MS_CLIENT_ID,
    MS_AUTHORITY,
    MS_SCOPES,
    MS_TOKEN_FILE,
    MAX_HOURLY,
    DELAY_MIN,
    DELAY_MAX,
    HM_SUBJ,
    HM_BODY,
    REC_SUBJ,
    REC_BODY,
    RESUME_SDE,
    RESUME_ML,
    RESUME_DA,
    DRAFT_HISTORY_FILE,
    warmup_limit,
)
from outreach.outreach_data import Credits, NameParser

log = logging.getLogger(__name__)


class Drafter:
    @staticmethod
    def draft(name, contact_type, company, title, job_id=""):
        parsed = NameParser.parse(name)
        first = parsed["first"] if parsed else name.split()[0]
        st, bt = (HM_SUBJ, HM_BODY) if contact_type == "hm" else (REC_SUBJ, REC_BODY)
        jid = job_id if job_id and job_id not in ("N/A", "") else ""
        subj, body = st, bt
        if not jid:
            subj = subj.replace(" | {job_id}", "")
            body = body.replace(" | {job_id}", "")
        vals = {
            "first": first,
            "title": title,
            "job_id": jid,
            "company": company,
            "sender": SENDER_NAME,
        }
        for k, v in vals.items():
            subj = subj.replace(f"{{{k}}}", v)
            body = body.replace(f"{{{k}}}", v)
        return {"subject": subj, "body": body.replace("\n\n\n", "\n\n")}


class Mailer:
    def __init__(self, credits: Credits):
        self.cr = credits
        self._svc = None
        self._hourly = 0
        self._hour_start = datetime.datetime.now()
        self._drafts_created = set()
        self._bounced_emails: set = set()
        self._load_draft_history()
        self._ms_access_token = None  # cached for this run
        self._ms_precheck()  # silently refresh MS token on init

    def _ms_precheck(self):
        """Silently pre-refresh MS token so it's ready when send() is called."""
        try:
            import msal
            from outreach.outreach_config import (
                MS_CLIENT_ID, MS_AUTHORITY, MS_SCOPES, MS_TOKEN_FILE
            )
            if not os.path.exists(MS_TOKEN_FILE):
                return  # first run — will authenticate on first send()
            cache = msal.SerializableTokenCache()
            cache.deserialize(open(MS_TOKEN_FILE).read())
            app = msal.PublicClientApplication(
                MS_CLIENT_ID, authority=MS_AUTHORITY, token_cache=cache
            )
            accounts = app.get_accounts()
            if not accounts:
                return
            result = app.acquire_token_silent(MS_SCOPES, account=accounts[0])
            if result and "access_token" in result:
                self._ms_access_token = result["access_token"]
                if cache.has_state_changed:
                    open(MS_TOKEN_FILE, "w").write(cache.serialize())
                log.info("MS token pre-checked: valid")
            else:
                log.info("MS token needs refresh — will authenticate on first send()")
        except Exception as e:
            log.debug(f"MS token precheck failed (non-fatal): {e}")

    def set_bounced(self, bounced: set):
        self._bounced_emails = {e.lower().strip() for e in bounced}
        if self._bounced_emails:
            log.info(f"Mailer: {len(self._bounced_emails)} bounced email(s) loaded")

    def _load_draft_history(self):
        try:
            if os.path.exists(DRAFT_HISTORY_FILE):
                self._drafts_created = set(json.load(open(DRAFT_HISTORY_FILE)))
        except:
            self._drafts_created = set()

    def _save_draft_history(self):
        try:
            json.dump(list(self._drafts_created), open(DRAFT_HISTORY_FILE, "w"))
        except:
            pass

    def _draft_key(self, to_email, subject):
        return f"{to_email.lower().strip()}||{subject.strip()}"

    def _ms_token(self):
        """Get Microsoft Graph access token via MSAL device flow or cached token."""
        if self._ms_access_token:
            return self._ms_access_token
        import msal, json as _j
        from outreach.outreach_config import MS_CLIENT_ID, MS_AUTHORITY, MS_SCOPES, MS_TOKEN_FILE, MS_SENDER_EMAIL
        cache = msal.SerializableTokenCache()
        if os.path.exists(MS_TOKEN_FILE):
            try:
                cache.deserialize(open(MS_TOKEN_FILE).read())
            except Exception:
                pass
        app = msal.PublicClientApplication(MS_CLIENT_ID, authority=MS_AUTHORITY, token_cache=cache)
        accounts = app.get_accounts()
        result = None
        if accounts:
            result = app.acquire_token_silent(MS_SCOPES, account=accounts[0])
        if not result or "access_token" not in result:
            print("\n" + "="*60)
            print("Microsoft sign-in required for Northeastern email.")
            print(f"Sign in with: {MS_SENDER_EMAIL}")
            print("="*60 + "\n")
            flow = app.initiate_device_flow(scopes=MS_SCOPES)
            if "user_code" not in flow:
                raise Exception(f"MS auth failed: {flow.get('error_description')}")
            print(flow["message"])
            result = app.acquire_token_by_device_flow(flow)
        if cache.has_state_changed:
            try:
                open(MS_TOKEN_FILE, "w").write(cache.serialize())
            except Exception as e:
                log.debug(f"MS token cache save failed: {e}")
        if "access_token" not in result:
            raise Exception(f"MS token error: {result.get('error_description', result)}")
        return result["access_token"]

    def _service(self):
        if self._svc:
            return self._svc
        from google.auth.transport.requests import Request
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build

        creds = None
        if os.path.exists(GMAIL_TOKEN):
            with open(GMAIL_TOKEN, "rb") as f:
                creds = pickle.load(f)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except:
                    creds = None
            if not creds:
                if not os.path.exists(GMAIL_CREDS):
                    raise FileNotFoundError(f"Missing: {GMAIL_CREDS}")
                flow = InstalledAppFlow.from_client_secrets_file(
                    GMAIL_CREDS, GMAIL_SCOPES
                )
                creds = flow.run_local_server(port=0)
            with open(GMAIL_TOKEN, "wb") as f:
                pickle.dump(creds, f)
        self._svc = build("gmail", "v1", credentials=creds)
        return self._svc

    def send(self, to_email, subject, body, resume_type="SDE"):
        result = {"success": False, "error": "", "timestamp": ""}
        if resume_type == "ML":
            resume_path = RESUME_ML
        elif resume_type == "DA":
            resume_path = RESUME_DA
        else:
            resume_path = RESUME_SDE

        key = self._draft_key(to_email, subject)
        if key in self._drafts_created:
            result["error"] = "Duplicate draft (already created)"
            log.info(f"Skipped duplicate draft: {to_email}")
            return result
        if to_email.lower().strip() in self._bounced_emails:
            result["error"] = f"Bounced: {to_email}"
            result["status"] = "Bounced"
            log.info(f"Skipped bounced email: {to_email}")
            return result

        wl, gl = warmup_limit(), self.cr.gmail_left()
        if min(wl, gl) <= 0:
            result["error"] = f"Daily limit (warm-up={wl}, left={gl})"
            return result

        now = datetime.datetime.now()
        if (now - self._hour_start).total_seconds() > 3600:
            self._hourly = 0
            self._hour_start = now
        if self._hourly >= MAX_HOURLY:
            result["error"] = f"Hourly limit ({MAX_HOURLY})"
            return result

        # Triple gate: suspicious email check at draft creation (handle comma-separated)
        from outreach.outreach_verifier import is_suspicious_email as verify_suspicious
        for _single_email in to_email.split(","):
            _single_email = _single_email.strip()
            if _single_email and verify_suspicious(_single_email):
                result["error"] = f"Suspicious domain blocked at draft creation: {_single_email}"
                log.warning(result["error"])
                return result

        if not to_email or "@" not in to_email:
            result["error"] = f"Invalid: {to_email}"
            return result

        try:
            svc = self._service()
            msg = MIMEMultipart()
            msg["From"] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
            msg["To"] = to_email
            msg["Subject"] = subject
            msg["Reply-To"] = SENDER_EMAIL
            html_body = Mailer._to_html(body)
            msg.attach(MIMEText(html_body, "html"))

            if os.path.exists(resume_path):
                with open(resume_path, "rb") as rf:
                    part = MIMEBase("application", "pdf")
                    part.set_payload(rf.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f"attachment; filename={os.path.basename(resume_path)}",
                    )
                    msg.attach(part)
            else:
                log.warning(f"Resume not found: {resume_path}")

            # Send via Microsoft Graph (Northeastern .edu address)
            import requests as _req, json as _j
            token = self._ms_token()

            # Build message payload for Graph API
            msg_payload = {
                "message": {
                    "subject": subject,
                    "body": {
                        "contentType": "HTML",
                        "content": html_body,
                    },
                    "toRecipients": [
                        {"emailAddress": {"address": to_email}}
                    ],
                    "from": {
                        "emailAddress": {
                            "name": MS_SENDER_NAME,
                            "address": MS_SENDER_EMAIL,
                        }
                    },
                    "replyTo": [
                        {"emailAddress": {
                            "name": MS_SENDER_NAME,
                            "address": MS_SENDER_EMAIL,
                        }}
                    ],
                },
                "saveToSentItems": "true",
            }

            # Attach resume
            if os.path.exists(resume_path):
                with open(resume_path, "rb") as rf:
                    import base64 as _b64
                    file_bytes = rf.read()
                    encoded = _b64.b64encode(file_bytes).decode()
                    msg_payload["message"]["attachments"] = [{
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": os.path.basename(resume_path),
                        "contentType": "application/pdf",
                        "contentBytes": encoded,
                    }]
            else:
                log.warning(f"Resume not found: {resume_path}")

            resp = _req.post(
                f"https://graph.microsoft.com/v1.0/users/{MS_SENDER_EMAIL}/sendMail",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                data=_j.dumps(msg_payload),
                timeout=30,
            )

            if resp.status_code not in (200, 202):
                raise Exception(f"Graph API error {resp.status_code}: {resp.text[:200]}")

            self._hourly += 1
            self.cr.use_gmail()
            self._drafts_created.add(key)
            self._save_draft_history()

            result["success"] = True
            result["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
            log.info(f"Sent via Northeastern -> {to_email}")
        except Exception as e:
            result["error"] = f"Send failed: {str(e)[:120]}"
            log.error(result["error"])
        return result

    @staticmethod
    def _to_html(body):
        """Convert plain text body to clean HTML with professional formatting."""
        paragraphs = body.split("\n\n")
        style = (
            "font-family: Arial, sans-serif; font-size: 14px; "
            "line-height: 1.6; color: #333333; margin: 0 0 14px 0;"
        )
        parts = []
        for p in paragraphs:
            p = p.strip()
            if not p:
                continue
            p_html = p.replace("\n", "<br>")
            parts.append(f'<p style="{style}">{p_html}</p>')
        return (
            '<div style="font-family: Arial, sans-serif;">'
            + "\n".join(parts)
            + "</div>"
        )

    def wait(self):
        time.sleep(random.randint(DELAY_MIN, DELAY_MAX))

    def capacity(self):
        return {
            "daily": min(warmup_limit(), self.cr.gmail_left()),
            "hourly": MAX_HOURLY - self._hourly,
        }
