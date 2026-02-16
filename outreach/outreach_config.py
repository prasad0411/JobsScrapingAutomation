#!/usr/bin/env python3
"""Outreach Pipeline — Configuration."""

import os, datetime

_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_DIR)

SHEETS_CREDS = os.path.join(_ROOT, ".local", "credentials.json")
GMAIL_CREDS = os.path.join(_ROOT, ".local", "gmail_credentials.json")
GMAIL_TOKEN = os.path.join(_ROOT, ".local", "gmail_token.pickle")
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send", "https://www.googleapis.com/auth/gmail.compose"]

SPREADSHEET = "H1B visa"
VALID_TAB = "Valid Entries"
OUTREACH_TAB = "Outreach Tracker"

V_COMPANY, V_TITLE, V_JOBID, V_RESUME = 2, 3, 6, 9

O_HEADERS = [
    "Sr. No.",                # A (0)
    "Company",                # B (1)
    "Job Title",              # C (2)
    "Job ID",                 # D (3)
    "HM Name",                # E (4)
    "HM LinkedIn URL",        # F (5)
    "Recruiter Name",         # G (6)
    "Recruiter LinkedIn URL", # H (7)
    "HM Email",               # I (8)
    "Recruiter Email",        # J (9)
    "HM Subject",             # K (10)
    "HM Body",                # L (11)
    "Rec Subject",            # M (12)
    "Rec Body",               # N (13)
    "Send?",                  # O (14)
    "Sent Date",              # P (15)
    "Error Log",              # Q (16)
]

C = {
    "sr": 0, "company": 1, "title": 2, "job_id": 3,
    "hm_name": 4, "hm_li": 5, "rec_name": 6, "rec_li": 7,
    "hm_email": 8, "rec_email": 9,
    "hm_subject": 10, "hm_body": 11, "rec_subject": 12, "rec_body": 13,
    "send": 14, "sent_dt": 15, "error": 16,
}

SENDER_NAME = "Prasad Kanade"
SENDER_EMAIL = "prasadkanade@gmail.com"

HM_SUBJ = "Prasad Kanade \u2014 Application for {title} | {job_id}"
HM_BODY = (
    "Hi {first},\n\n"
    "I hope you're doing well. I recently applied for the {title} | {job_id} role at {company} "
    "and wanted to reach out personally because I am genuinely enthusiastic about this opportunity.\n\n"
    "I am pursuing my Master's in Computer Science at Northeastern University and bring 1.5 years "
    "of professional experience at Amdocs, where I built and optimized large-scale backend systems "
    "and delivered measurable performance improvements in production environments.\n\n"
    "I am driven by growth, accountability, and impact. Given the opportunity, I will approach this "
    "role with full ownership and a commitment to delivering beyond expectations \u2014 learning quickly, "
    "contributing immediately, and pushing myself to add real value to your team.\n\n"
    "I have attached my resume and would truly appreciate the chance to connect.\n\n"
    "Best regards,\nPrasad Kanade"
)
REC_SUBJ = "Prasad Kanade \u2014 Application for {title} | {job_id}"
REC_BODY = (
    "Hi {first},\n\n"
    "I hope you're doing well. I recently applied for the {title} | {job_id} role at {company} "
    "and wanted to reach out personally because I am genuinely enthusiastic about this opportunity.\n\n"
    "I'm pursuing my Master's in Computer Science at Northeastern University and bring 1.5 years "
    "of professional experience at Amdocs, where I worked on large-scale backend systems in "
    "production environments and contributed to measurable performance improvements.\n\n"
    "I am at a stage where I am eager to fully invest my skills and energy into the right "
    "opportunity - one where I can grow within a strong team, take on real responsibility, and "
    "make a meaningful impact from day one. I take my work seriously and approach every challenge "
    "with discipline and drive.\n\n"
    "I have attached my resume and would genuinely love the chance to discuss how I can contribute "
    "and what the next steps look like.\n\n"
    "Best regards,\nPrasad Kanade"
)

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

CLEARBIT_URL = "https://autocomplete.clearbit.com/v1/companies/suggest"
TLDS = [".com", ".io", ".co", ".us", ".org", ".ai", ".dev"]
APIS = {
    "apollo": {"limit": 120, "url": "https://api.apollo.io/api/v1/people/match"},
    "hunter": {"limit": 25, "url": "https://api.hunter.io/v2/email-finder"},
    "snov": {"limit": 50, "url": "https://api.snov.io/v1/get-emails-from-names"},
}
REACHER_URL = "http://localhost:8080/v0/check_email"
REACHER_FROM = "test@example.org"
REACHER_TIMEOUT = 15
REACHER_WORKERS = 5
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
CREDITS_FILE = os.path.join(_ROOT, ".local", "outreach_credits.json")
PATTERNS_FILE = os.path.join(_ROOT, ".local", "outreach_patterns.json")
LOG_FILE = os.path.join(_ROOT, ".local", "outreach.log")
RESUME_SDE = os.path.join(_ROOT, ".local", "Prasad Kanade SDE Resume.pdf")
RESUME_ML = os.path.join(_ROOT, ".local", "Prasad Kanade ML Resume.pdf")
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
