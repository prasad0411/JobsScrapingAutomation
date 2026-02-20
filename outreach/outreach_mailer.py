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
    MAX_HOURLY,
    DELAY_MIN,
    DELAY_MAX,
    HM_SUBJ,
    HM_BODY,
    REC_SUBJ,
    REC_BODY,
    RESUME_SDE,
    RESUME_ML,
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
        resume_path = RESUME_ML if resume_type == "ML" else RESUME_SDE

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

            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            svc.users().drafts().create(
                userId="me", body={"message": {"raw": raw}}
            ).execute()

            self._hourly += 1
            self.cr.use_gmail()
            self._drafts_created.add(key)
            self._save_draft_history()

            result["success"] = True
            result["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
            log.info(f"Draft created -> {to_email}")
        except Exception as e:
            result["error"] = f"Draft failed: {str(e)[:120]}"
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
