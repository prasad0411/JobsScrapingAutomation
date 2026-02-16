#!/usr/bin/env python3
"""Outreach Pipeline — Gmail OAuth Sender + Email Drafter."""

import os, pickle, time, random, datetime, logging, base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
    warmup_limit,
)
from email.mime.base import MIMEBase
from email import encoders
from outreach.outreach_data import Credits, NameParser

log = logging.getLogger(__name__)


class Drafter:
    @staticmethod
    def draft(name, contact_type, company, title, job_id=""):
        parsed = NameParser.parse(name)
        first = parsed["first"] if parsed else name.split()[0]
        st, bt = (HM_SUBJ, HM_BODY) if contact_type == "hm" else (REC_SUBJ, REC_BODY)
        jid = job_id if job_id and job_id not in ("N/A", "") else ""
        vals = {"first": first, "title": title, "job_id": jid, "company": company, "sender": SENDER_NAME}
        subj, body = st, bt
        for k, v in vals.items():
            subj = subj.replace(f"{{{k}}}", v)
            body = body.replace(f"{{{k}}}", v)
        if not jid:
            subj = subj.replace(" | ", "")
            body = body.replace(" | ", " ")
        return {"subject": subj, "body": body.replace("\n\n\n", "\n\n")}


class Mailer:
    def __init__(self, credits: Credits):
        self.cr = credits
        self._svc = None
        self._hourly = 0
        self._hour_start = datetime.datetime.now()

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
            msg.attach(MIMEText(body, "plain"))
            if os.path.exists(resume_path):
                with open(resume_path, "rb") as rf:
                    part = MIMEBase("application", "pdf")
                    part.set_payload(rf.read())
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(resume_path)}")
                    msg.attach(part)
            else:
                log.warning(f"Resume not found: {resume_path}")
            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            svc.users().drafts().create(userId="me", body={"message": {"raw": raw}}).execute()
            self._hourly += 1
            self.cr.use_gmail()
            result["success"] = True
            result["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
            log.info(f"Draft created → {to_email}")
            print(f"    ✓ Sent → {to_email}")
        except Exception as e:
            result["error"] = f"Send failed: {str(e)[:120]}"
            log.error(result["error"])
        return result

    def wait(self):
        time.sleep(random.randint(DELAY_MIN, DELAY_MAX))

    def capacity(self):
        return {
            "daily": min(warmup_limit(), self.cr.gmail_left()),
            "hourly": MAX_HOURLY - self._hourly,
        }
