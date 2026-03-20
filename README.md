# Automated Job Hunt Pipeline

End-to-end system that aggregates 8,000+ weekly internship postings, validates eligibility, discovers hiring manager emails, and sends personalized outreach emails automatically — all from your Northeastern University email address.

**Built by [Prasad Kanade](https://www.linkedin.com/in/prasad-kanade-/) | MS Computer Science @ Northeastern University**

---

## What It Does

Two Python modules work together to automate the entire job search workflow with zero daily manual intervention:

**Module 1 — Job Aggregator** scrapes GitHub repositories (SimplifyJobs, vanshb03) and Gmail alerts (Jobright, SWE List, ZipRecruiter, Company Newsletters), validates each job against 25+ eligibility criteria, deduplicates across 1,500+ tracked entries, and maintains an organized Google Sheets tracker.

**Module 2 — Outreach Pipeline** takes validated jobs, mirrors them to an Outreach Tracker sheet, auto-sets Extract=yes based on smart signals (location, sponsorship, pattern history), discovers hiring manager and recruiter email addresses through an 8-layer verification system, sends personalized emails with resume attachment from `kanade.pra@northeastern.edu` via Microsoft Graph API, schedules timezone-aware delivery at 9:30 AM in the company's local timezone, tracks delivery status, auto-retries bounced emails with alternative patterns, and generates LinkedIn connection messages.

**Impact:** 6 hours/week → 15 minutes/week. Zero duplicate applications. 98%+ classification accuracy. 25+ outreach emails sent per run, fully automatically.

---

## Screenshots

### Aggregator Terminal Output

Processing jobs from GitHub repos and email sources with real-time validation feedback.
![Aggregator Output](docs/screenshots/aggregator_terminal.png)

### Aggregator Summary

Breakdown of valid, discarded, and duplicate jobs across all sources.
![Aggregator Summary](docs/screenshots/aggregator_summary.png)

### Valid Entries Sheet

Validated internship postings with status, company, title, date applied, job URL and other fields.
![Valid Entries](docs/screenshots/valid_entries.png)

### Valid Entries Detail

Job metadata including job type, location, remote status, entry timestamp and source.
![Valid Entries Detail](docs/screenshots/valid_entries_detail.png)

### Outreach Tracker Sheet

Email outreach tracking with HM/recruiter names, LinkedIn URLs, discovered emails, LinkedIn messages, scheduled send times, and delivery notes.
![Outreach Tracker](docs/screenshots/outreach_tracker.png)

### Outreach Tracker Detail

Recruiter LinkedIn URLs, emails, LinkedIn messages, Send At scheduling, and sent date tracking.
![Outreach Tracker Detail](docs/screenshots/outreach_tracker_detail.png)

### Gmail Drafts

Personalized email drafts created in Gmail prior to the MS Graph migration.
![Gmail Drafts](docs/screenshots/gmail_drafts.png)

### GitHub Actions — Scheduled Email Sending

Send Scheduled Emails workflow running 4× daily via GitHub Actions, covering all US timezones.
![GitHub Actions](docs/screenshots/github_actions.png)

---

## How It Works

### Job Aggregation Pipeline

```
GitHub Repos + Gmail Alerts
    → Parse 2,000+ weekly postings
    → Resolve redirects (SimplifyJobs, Jobright, ZipRecruiter)
    → Detect INACTIVE Simplify listings (auto-skip)
    → Extract Simplify metadata (location, remote, sponsorship, no_h1b flag)
    → Fetch career pages (Selenium + BeautifulSoup)
    → Extract metadata (company, location, job ID, type)
    → 25-stage validation (visa, degree, geography, role type, posting age...)
    → Multi-signal deduplication (URL + company|title + job ID)
    → Company name normalization (20+ alias mappings)
    → Google Sheets output
    → SQLite run history logged for trend analysis
```

### Email Discovery Pipeline

```
Company + Hiring Manager Name
    │
    ▼
Layer 1: Seed pattern cache (125+ companies, instant)
Layer 2: DomainHistory confirmed pattern (learned from past deliveries)
Layer 3: Microsoft 365 verification (definitive yes/no)
Layer 4: Website pattern mining (votes across all found emails)
Layer 5: Reacher SMTP verification (Docker-based)
Layer 6: Reacher pattern search
Layer 7: API cascade (Apollo → Hunter → Snov → Prospeo)
Layer 8: Statistical inference (80% of companies use first.last)
    │
    ▼
Pre-send bounce check → MS Graph send → Delivery Tracking → Bounce Auto-Retry
```

### Automated Email Delivery

```
Outreach run (midnight) discovers emails + writes Send At = next business day 9:30 AM local time
    → send_scheduled.py fires at 9:00 AM ET  → delivers to US/Eastern companies
    → send_scheduled.py fires at 10:30 AM ET → delivers to US/Central companies
    → send_scheduled.py fires at 11:30 AM ET → delivers to US/Mountain companies
    → send_scheduled.py fires at 12:30 PM ET → delivers to US/Pacific companies
    → All emails sent from kanade.pra@northeastern.edu via Microsoft Graph API
    → 45-second delay between emails, 15 emails per run cap (protects account reputation)
    → Nightly digest sent to personal Gmail at 12:22 AM with full run summary
```

### Outreach Lifecycle

```
Job validated → Synced to Outreach Tracker (exact Valid sheet order)
    → Extract=yes auto-set (sponsorship=Yes, tech hub location, PatternCache hit)
    → Email discovered for HM/Recruiter (only Extract=yes rows)
    → Resume type determined (SDE/ML/DA from Valid Entries)
    → Send At computed: next business day 9:30 AM in company's timezone
    → Email sent via Microsoft Graph from kanade.pra@northeastern.edu
    → [12 hours pass]
    → No bounce → Notes: "Delivered to HM and Rec"
    → Bounce detected → Notes: "HM email bounced on Mar 20, 2026"
        → DomainHistory queried for proven pattern at same domain
        → Auto-retry with domain-informed alternative pattern
        → Notes updated: "Retried: flast@co.com"
```

---

## Key Features

### Multi-Source Aggregation

- GitHub repositories (SimplifyJobs, vanshb03) with section category trust
- Gmail API integration (Jobright, SWE List, ZipRecruiter, Company Newsletters)
- Simplify URL resolution (5 methods with learned best-method cache per domain)
- Simplify metadata extraction including `no_h1b` flag — immediate rejection if set
- INACTIVE job detection on Simplify pages
- ZipRecruiter URL expiry pre-validation (rejects before fetching if `expires` param stale)
- ZipRecruiter page age validation (rejects postings older than 3 days)
- Selenium fallback for JavaScript-heavy career pages (Workday, Oracle, Ashby)
- HTTP response cache persisted to disk (6-hour per-entry TTL, max 500 entries)
- Selenium health check at startup — warns immediately if ChromeDriver is broken
- SQLite run history: every run logged with valid/discarded/failed counts and elapsed time

### 25-Stage Validation

- Company and platform blacklists (auto-growing via weekly build_auto_blacklist.py)
- Security clearance and US Person requirement detection
- Explicit F-1/foreign national visa rejection detection
- "Unable to sponsor" detection — catches all variants including "unable to sponsor or take over sponsorship"
- Citizenship required detection (including combined "legal right to work without sponsorship")
- Degree level filtering (BA/BS-only and PhD-only roles)
- Undergraduate-only role detection (junior/senior standing, four-year college enrollment, "obtaining a bachelor's degree")
- Hardware/optics/photonics/laser/materials science/AOSP/HAL/BSP role filtering
- SkillBridge and DoD military-only internship filtering
- Geographic restrictions (40+ international countries, German city/typo detection, Canadian province/CAN suffix)
- Ambiguous city disambiguation (Burlington MA vs Burlington ON)
- Rotational Program detection (full-time programs disguised as internships)
- Posting age validation (configurable threshold, 3-day default)
- Smart season detection (ignores copyright years, financial data; rejects Spring 2026 explicitly)
- Non-CS/Engineering role filtering with early title-level CS check before page fetch
- Expired job detection (20+ dead page patterns)
- ATS platform company name extraction
- Company name normalization with legal suffix stripping
- Location cleaning with COMPANY_HQ fallback (60 top companies)

### Intelligent Deduplication

- URL normalization and matching
- Company + title fuzzy matching
- Job ID cross-reference (including JR_XXXXX underscore pattern)
- Company name normalization (20+ alias mappings)

### Email Discovery and Verification

- Provider detection: MX record lookup identifies Google Workspace vs Microsoft 365 vs self-hosted
- Microsoft 365 verification: Definitive email existence check via GetCredentialType endpoint
- Website mining: Scrapes company pages, votes across found emails to learn domain pattern
- Pattern learning: Every successful delivery updates DomainHistory and PatternCache immediately
- DomainHistory memory cache: loaded once per process, zero disk I/O on repeated lookups
- Clearbit domain cache: persisted to disk with 30-day TTL, never re-calls for known companies
- Dual failure tracking: failed_patterns.json + domain_pattern_history.json
- Pre-send blocking: emails checked against bounce cache AND failed patterns before sending
- Statistical inference: most common pattern (first.last) as last resort
- Apollo, Hunter, Snov, Prospeo API cascade with credit tracking

### Microsoft Graph Email Sending

- All outreach emails sent from `kanade.pra@northeastern.edu` via Microsoft Graph API
- MSAL authentication with token cached to `.local/ms_token.json`
- Silent token refresh on every Mailer init — never needs manual re-authentication
- Resume type (SDE/ML/DA) correctly matched from Valid Entries per job
- 45-second delay between sends, 15 emails per run cap
- Correct resume attached per role type (SWE/ML/Data resume auto-selected)
- Emails saved to Sent Items in Northeastern Outlook automatically

### Smart Auto-Extract

- `auto_extract.py` runs after every outreach pull
- Sets Extract=yes for: sponsorship=Yes, location in 30+ major tech hubs, company in PatternCache
- Sets Extract=Skip for: sponsorship=No (never target these)
- Tech hubs covered: SF Bay Area, Seattle, NYC, Austin, Boston, Chicago, LA, San Jose, Mountain View, Palo Alto, Redmond, Bellevue, Denver, Atlanta, Reston, Remote, and more
- 347 rows auto-set on first run

### Timezone-Aware Scheduling

- `compute_send_at()` converts company location to US timezone
- All emails scheduled for 9:30 AM in company's local time
- send_scheduled.py fires 4× daily: 9:00, 10:30, 11:30, 12:30 ET
- Covers all US timezones: ET → CT → MT → PT
- Deduplication: never sends same email twice within 7 days
- Dead letter queue: after 3 failed attempts, marks row permanently

### Delivery Tracking and Bounce Recovery

- Bounce scanner reads Gmail inbox AND "Failed Emails" label for DSN notifications
- Bounced emails invalidate email_verify_cache.json entries automatically
- DomainHistory queried on bounce — uses proven pattern from same domain instead of guessing alphabetically
- Auto-retry generates domain-informed alternative patterns
- Delivery confirmed after 12 hours with no bounce
- Late bounces always override delivery status

### Nightly Digest

- Sent every night at 12:22 AM to `prasadckanade@gmail.com`
- Shows: jobs added, emails sent today, bounces in 24h, pending extraction queue
- Last aggregator run stats: valid/discarded/failed HTTP/elapsed time
- Last 5 error lines from outreach.log
- Circuit breaker status
- Subject prefixed with ⚠ if errors, 🚨 if circuit breaker tripped

### Self-Improving System

- DomainHistory consulted before any API call — instant resolution for known domains
- PatternCache updated on every successful delivery (125+ domains learned)
- Simplify resolver learns best resolution method per domain
- Auto-blacklist: `build_auto_blacklist.py` reads Discarded Entries, promotes companies with 3+ identical rejections
- ProcessedEmailTracker capped at 10,000 entries with oldest-first pruning
- SQLite run history enables trend analysis over time

### Automated Scheduling (macOS launchd)

- **Aggregator**: 8 AM, 3 PM, 9 PM daily
- **Outreach**: midnight daily
- **Send Scheduled**: 9:00 AM, 10:30 AM, 11:30 AM, 12:30 PM daily
- **Nightly Digest**: 12:22 AM daily
- **Cleanup**: every 2 days at 7 AM
- Catch-up on wake: missed runs execute immediately when Mac wakes
- All logs saved to `.local/cron_logs/` with 7-day retention
- Resume auto-sync: latest PDFs copied from Downloads to `.local/` before every run

---

## Performance

| Metric                  | Before      | After                       |
| ----------------------- | ----------- | --------------------------- |
| Weekly manual work      | 6 hours     | 15 minutes                  |
| Job processing time     | 40 min      | 10 min                      |
| Duplicate applications  | 2–5/run     | 0                           |
| Classification accuracy | ~85%        | 98%+                        |
| Email extraction rate   | 0% (manual) | ~95% automated              |
| Outreach emails/run     | 0 (manual)  | 25+ auto-sent               |
| LinkedIn messages/run   | 0 (manual)  | 25+ auto-generated          |
| Tracked entries         | 0           | 636+                        |
| Outreach emails sent    | 0           | 130+                        |
| Sender address          | Gmail       | kanade.pra@northeastern.edu |
| Manual Extract=yes      | 100% manual | 95% automated               |

---

## Tech Stack

**Core:** Python 3.12+, Google Sheets API, Gmail API, Microsoft Graph API

**Web Scraping:** Selenium WebDriver, BeautifulSoup4, Requests, lxml

**Email Discovery:** dnspython (MX records), Reacher (SMTP verification via Docker), Apollo API, Hunter API, Snov API, Prospeo API, Microsoft 365 GetCredentialType

**Email Sending:** Microsoft Graph API, MSAL (Microsoft Authentication Library)

**Infrastructure:** Docker (Reacher container), macOS launchd (scheduled automation), Google Cloud service accounts, OAuth 2.0, SQLite (run history)

**Intelligence:** Claude Haiku API (borderline CS role classification, GitHub sponsorship check)

**Codebase:** 13,000+ lines across 17 production modules

---

## Architecture

```
Job Hunt Tracker/
├── aggregator/                    # Module 1: Job aggregation
│   ├── config.py                  # Patterns, blacklists, normalizations (2,000+ lines)
│   ├── extractors.py              # Page fetching, Simplify resolution, GitHub scraper
│   ├── processors.py              # Validation, extraction, location processing (3,200+ lines)
│   ├── run_aggregator.py          # Pipeline orchestration
│   ├── sheets_manager.py          # Google Sheets integration
│   └── utils.py                   # HTTP retry, sanitization, date parsing
├── outreach/                      # Module 2: Email outreach
│   ├── outreach_config.py         # Column mapping, email templates, API keys
│   ├── outreach_data.py           # Sheets sync, PatternCache, NameParser, bounce handling
│   ├── outreach_finder.py         # 8-layer email discovery pipeline
│   ├── outreach_mailer.py         # MS Graph email sending with resume attachment
│   ├── outreach_provider.py       # MX lookup, Microsoft 365, website mining
│   ├── outreach_verifier.py       # Confidence scoring, CircuitBreaker, DomainHistory
│   ├── bounce_scanner.py          # Gmail bounce detection (RFC 3464 + Failed Emails label)
│   └── run_outreach.py            # Pipeline orchestration, delivery tracking
├── scripts/                       # Automation utilities
│   ├── send_scheduled.py          # MS Graph scheduled sender (4×/day)
│   ├── nightly_digest.py          # Nightly summary email to personal Gmail
│   ├── auto_extract.py            # Auto-sets Extract=yes based on smart signals
│   ├── build_auto_blacklist.py    # Weekly auto-blacklist from Discarded Entries
│   ├── cleanup_not_applied.py     # Moves Not Applied + expired jobs to Reviewed sheet
│   ├── test_ms_auth.py            # Microsoft Graph authentication helper
│   ├── cron_runner.sh             # launchd runner (syncs resumes, runs modules)
│   ├── cron_cleanup.sh            # launchd cleanup runner (2-day gate)
│   ├── install_new_plists.sh      # Installs all launchd agents
│   ├── com.prasad.jobtracker.aggregator.plist
│   ├── com.prasad.jobtracker.outreach.plist
│   ├── com.prasad.jobtracker.send.plist      # 9:00, 10:30, 11:30, 12:30 ET
│   ├── com.prasad.jobtracker.digest.plist    # 12:22 AM daily
│   └── com.prasad.jobtracker.cleanup.plist
├── .local/                        # Credentials, caches, logs (gitignored)
│   ├── credentials.json               # Google Sheets service account
│   ├── gmail_credentials.json         # Gmail OAuth client
│   ├── gmail_token.pickle             # Gmail OAuth token
│   ├── ms_token.json                  # Microsoft Graph OAuth token (MSAL)
│   ├── outreach_patterns.json         # Learned email patterns (125+ domains)
│   ├── domain_pattern_history.json    # Confirmed/failed patterns per domain
│   ├── bounced_emails.json            # Known bounced addresses
│   ├── failed_patterns.json           # Bounced email patterns (never retried)
│   ├── domain_cache.json              # Clearbit company→domain cache (30-day TTL)
│   ├── sent_log.json                  # Deduplication log (7-day window)
│   ├── send_fail_counts.json          # Dead letter queue state
│   ├── run_history.db                 # SQLite: one row per aggregator run
│   ├── simplify_method_cache.json     # Best Simplify resolution method per domain
│   ├── http_response_cache.json       # Persisted HTTP cache (6-hour per-entry TTL)
│   ├── retry_tracker.json             # Failed companies (3-day TTL)
│   └── outreach.log                   # Rotating log (5MB × 3)
├── test_pipeline.py               # 202-test production test suite (99% pass rate)
├── docker-compose.yml             # Reacher email verifier
├── requirements.txt
└── README.md
```

---

## Google Sheets Structure

### Valid Entries

Validated internship postings: Sr. No., Status, Company, Title, Date Applied, Job URL, Job ID, Job Type, Location, Resume, Remote?, Entry Date, Source, Sponsorship, Notes.

### Outreach Tracker

Email outreach tracking: Sr. No., Company, Job Title, Extract, Job ID, HM Name, HM LinkedIn URL, HM Email, HM LinkedIn Msg, Recruiter Name, Recruiter LinkedIn URL, Recruiter Email, Rec LinkedIn Msg, Send At, Sent Date, Notes, Confidence.

The **Extract** column is auto-managed:

- `yes` → auto-set for tech hub locations, sponsorship=Yes, PatternCache companies
- `Skip` → auto-set for sponsorship=No; ignored by all outreach automation

### Discarded Entries

Rejected postings with specific discard reason, full metadata preserved for review and weekly auto-blacklist building.

### Reviewed — Not Applied

Jobs moved from Valid Entries automatically (blank status, entry date 2+ days old) or manually. Reason column tracks why each was moved.

### Outreach Notes Column

| Status              | Example                                                  |
| ------------------- | -------------------------------------------------------- |
| Delivered to both   | Delivered to HM and Rec                                  |
| One bounced         | HM email bounced on Mar 20, 2026                         |
| Bounced and retried | HM email bounced on Mar 20, 2026 · Retried: flast@co.com |
| Send failed         | Send failed Mar 20                                       |

---

## Results

**Daily aggregation:** Processes 500+ postings, produces ~70 valid, ~50 discarded, ~200 duplicates caught in 10 minutes.

**Email discovery:** 25+ emails found per run across 15 companies. 8-layer system resolves previously-impossible companies.

**Cumulative:** 636+ tracked entries, 130+ outreach emails sent from Northeastern address, zero duplicate applications, fully automated end-to-end.

---

## Quick Start

### Prerequisites

- Python 3.12+
- Google Sheets API credentials (service account)
- Gmail API credentials (OAuth) — for bounce scanning only
- Microsoft account (Northeastern .edu) — for sending emails
- Docker (optional, for Reacher SMTP verification)
- ChromeDriver (for Selenium-based page fetching)

### Installation

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-local.txt
pip install msal

# Optional: Reacher for SMTP verification
docker compose up -d
```

### Running

```bash
# Aggregate new jobs
python3 -m aggregator

# Find emails and send outreach
python3 -m outreach

# Send scheduled emails (runs automatically via launchd)
python3 scripts/send_scheduled.py

# Send nightly digest
python3 scripts/nightly_digest.py

# Auto-set Extract=yes
python3 scripts/auto_extract.py

# Move reviewed/expired jobs
python3 scripts/cleanup_not_applied.py
```

### Automated Scheduling (macOS)

```bash
# Install all launchd agents
bash scripts/install_new_plists.sh

# Authenticate Microsoft Graph (one-time)
python3 scripts/test_ms_auth.py

# Check all agents are loaded
launchctl list | grep com.prasad

# View logs
ls -lt .local/cron_logs/
```

### Configuration

1. Place Google Sheets service account JSON in `.local/credentials.json`
2. Place Gmail OAuth client JSON in `.local/gmail_credentials.json`
3. Create `.env` with API keys:
   ```
   HUNTER_API_KEY=your_key
   APOLLO_API_KEY=your_key
   PROSPEO_API_KEY=your_key
   SNOV_API_KEY=your_key
   SNOV_USER_ID=your_id
   ANTHROPIC_API_KEY=your_key   # optional: Claude-powered CS role check
   SLACK_WEBHOOK_URL=your_url   # optional: Slack run alerts
   ```
4. Run Microsoft Graph auth once: `python3 scripts/test_ms_auth.py`
5. Place resumes in Downloads folder (auto-synced before each run):
   - `Prasad Kanade SWE Resume.pdf`
   - `Prasad Kanade ML Resume.pdf`
   - `Prasad Kanade Data Resume.pdf`

### Checking Run History

```bash
# Last 10 aggregator runs
python3 -c "
import sqlite3
con = sqlite3.connect('.local/run_history.db')
for r in con.execute('SELECT ts,valid,discarded,failed_http FROM runs ORDER BY ts DESC LIMIT 10'):
    print(r)
"
```

---

## Contact

**Prasad Chandrashekhar Kanade** · MS Computer Science | Northeastern University | May 2027

[Email](mailto:kanade.pra@northeastern.edu) · [LinkedIn](https://www.linkedin.com/in/prasad-kanade-/) · [GitHub](https://github.com/prasad0411)
