#!/usr/bin/env python3
"""Outreach Pipeline — Configuration."""

import os, datetime

_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_DIR)

# === Credentials (reuse existing) ===
SHEETS_CREDS = os.path.join(_ROOT, "credentials.json")
GMAIL_CREDS = os.path.join(_ROOT, "gmail_credentials.json")
GMAIL_TOKEN = os.path.join(_ROOT, "gmail_token.pickle")
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

# === Google Sheets ===
SPREADSHEET = "H1B visa"
VALID_TAB = "Valid Entries"
OUTREACH_TAB = "Outreach Tracker"

# Valid sheet columns (0-indexed)
V_COMPANY, V_TITLE, V_JOBID = 2, 3, 6

# === Outreach Tracker — 15 columns (A–O) ===
O_HEADERS = [
    "Sr. No.",  # A (0)
    "Company",  # B (1)
    "Job Title",  # C (2)
    "Job ID",  # D (3)
    "HM Name",  # E (4)
    "HM LinkedIn URL",  # F (5)
    "Recruiter Name",  # G (6)
    "Recruiter LinkedIn URL",  # H (7)
    "HM Email",  # I (8)
    "Recruiter Email",  # J (9)
    "Email Subject",  # K (10)
    "Email Body",  # L (11)
    "Sent Date",  # M (12)
    "Error Log",  # N (13)
    "Send?",  # O (14)
]

C = {
    "sr": 0,
    "company": 1,
    "title": 2,
    "job_id": 3,
    "hm_name": 4,
    "hm_li": 5,
    "rec_name": 6,
    "rec_li": 7,
    "hm_email": 8,
    "rec_email": 9,
    "subject": 10,
    "body": 11,
    "sent_dt": 12,
    "error": 13,
    "send": 14,
}

# === Sender ===
SENDER_NAME = "Prasad Kanade"
SENDER_EMAIL = "prasadkanade@gmail.com"  # ← YOUR GMAIL

# === Templates ===
_PS = "\n\nP.S. If you'd prefer I not follow up, just let me know — happy to respect that."

HM_SUBJ = "Regarding {title} at {company}"
HM_BODY = (
    "Hi {first},\n\n"
    "I came across the {title} role at {company} and wanted to reach out directly. "
    "I'm a CS Master's student at Northeastern University with hands-on experience in "
    "software engineering, and I believe my background aligns well with what your team "
    "is looking for.\n\n"
    "I've applied through the portal and would welcome the chance to connect "
    "if you think there could be a fit.\n\n"
    "Best regards,\n{sender}" + _PS
)

REC_SUBJ = "Regarding {title} at {company}"
REC_BODY = (
    "Hi {first},\n\n"
    "I recently applied for the {title} position at {company} and wanted to briefly "
    "introduce myself. I'm pursuing my Master's in Computer Science at Northeastern "
    "University, and I'm actively seeking summer 2026 SDE internship opportunities.\n\n"
    "I'd love the chance to discuss how my background could be a good fit for this role. "
    "Please let me know if there's a convenient time to connect.\n\n"
    "Best regards,\n{sender}" + _PS
)

# === 3-Phase Email Patterns ===
PAT_A = ["{first}.{last}", "{f}{last}", "{first}{last}"]
PAT_B = [
    "{first}_{last}",
    "{first}",
    "{f}.{last}",
    "{first}{l}",
    "{last}.{first}",
    "{last}{f}",
    "{first}.{l}",
    "{last}.{f}",
    "{first}-{last}",
    "{last}",
]
PAT_C = [
    "{last}_{first}",
    "{last}-{first}",
    "{last}{first}",
    "{f}_{last}",
    "{f}-{last}",
]

# === Domain Resolution ===
CLEARBIT_URL = "https://autocomplete.clearbit.com/v1/companies/suggest"
TLDS = [".com", ".io", ".co", ".us", ".org", ".ai", ".dev"]

# === APIs ===
APIS = {
    "apollo": {"limit": 120, "url": "https://api.apollo.io/api/v1/people/match"},
    "hunter": {"limit": 25, "url": "https://api.hunter.io/v2/email-finder"},
    "snov": {"limit": 50, "url": "https://api.snov.io/v1/get-emails-from-names"},
}

# === Reacher ===
REACHER_URL = "http://localhost:8080/v0/check_email"
REACHER_FROM = "test@example.org"
REACHER_TIMEOUT = 15
REACHER_WORKERS = 5

# === Rate Limits ===
MAX_DAILY = 450
MAX_HOURLY = 50
DELAY_MIN = 30
DELAY_MAX = 90
API_TIMEOUT = 10
API_RETRIES = 3
SHEET_PAUSE = 1.5
HUNTER_CONF = 70

WARMUP_ON = True
WARMUP_START = "2026-02-15"
WARMUP = [(7, 10), (14, 25), (21, 50), (999, MAX_DAILY)]

# === Local Files ===
CREDITS_FILE = os.path.join(_ROOT, "outreach_credits.json")
PATTERNS_FILE = os.path.join(_ROOT, "outreach_patterns.json")
LOG_FILE = os.path.join(_ROOT, "outreach.log")

# === Name Parsing ===
STRIP_PRE = {
    "dr.",
    "dr",
    "mr.",
    "mr",
    "mrs.",
    "mrs",
    "ms.",
    "ms",
    "prof.",
    "prof",
    "sir",
    "dame",
}
STRIP_SUF = {
    "jr.",
    "jr",
    "sr.",
    "sr",
    "ii",
    "iii",
    "iv",
    "phd",
    "ph.d.",
    "md",
    "m.d.",
    "esq",
    "esq.",
    "mba",
    "m.b.a.",
    "cpa",
    "c.p.a.",
}


# === Helpers ===
def _load_env():
    p = os.path.join(_ROOT, ".env")
    if os.path.exists(p):
        for ln in open(p):
            ln = ln.strip()
            if ln and not ln.startswith("#") and "=" in ln:
                k, v = ln.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def key(name):
    _load_env()
    return os.environ.get(name, "")


def warmup_limit():
    if not WARMUP_ON:
        return MAX_DAILY
    try:
        d = (
            datetime.datetime.now()
            - datetime.datetime.strptime(WARMUP_START, "%Y-%m-%d")
        ).days
        if d < 0:
            return 5
        for th, lim in WARMUP:
            if d < th:
                return lim
    except:
        pass
    return MAX_DAILY
