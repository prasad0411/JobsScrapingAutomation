# Automated Job Hunt Pipeline

**End-to-end system that aggregates 2,500+ weekly internship postings, validates eligibility, discovers hiring manager emails, and creates personalized outreach drafts — all automatically.**

Built by [Prasad Kanade](https://linkedin.com/in/prasad-kanade-) | MS Computer Science @ Northeastern University

---

## What It Does

Two Python modules work together to automate the entire job search workflow:

**Module 1 — Job Aggregator** scrapes GitHub repositories (SimplifyJobs, vanshb03) and Gmail alerts (Jobright, SWE List, ZipRecruiter, Company Newsletters), validates each job against 20+ eligibility criteria, deduplicates across 1,500+ tracked entries, and maintains an organized Google Sheets tracker.

**Module 2 — Outreach Pipeline** takes validated jobs, mirrors them to an Outreach Tracker sheet in exact order, discovers hiring manager and recruiter email addresses through an 8-layer verification system, creates personalized email drafts in Gmail, tracks delivery status, and auto-retries bounced emails with alternative patterns.

**Impact:** 6 hours/week to 45 minutes/week. Zero duplicate applications. 98%+ classification accuracy. 28 outreach emails generated per run.

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

Job metadata including jobtype, location, remote status, entry timestamp and other fields.

![Valid Entries Detail](docs/screenshots/valid_entries_detail.png)

---

## How It Works

### Job Aggregation Pipeline

```
GitHub Repos + Gmail Alerts
    -> Parse 2,000+ weekly postings
    -> Resolve redirects (SimplifyJobs, Jobright, ZipRecruiter)
    -> Detect INACTIVE Simplify listings (auto-skip)
    -> Fetch career pages (Selenium + BeautifulSoup)
    -> Extract metadata (company, location, job ID, type)
    -> 20-stage validation (visa, degree, geography, role type, posting age...)
    -> Multi-signal deduplication (URL + company|title + job ID)
    -> Company name normalization (20+ alias mappings)
    -> Google Sheets output
```

The aggregator processes jobs from multiple sources simultaneously. Each job passes through eligibility filters including F-1 visa requirements, security clearance detection, degree level filtering (BA/BS-only and PhD-only detection), geographic restrictions (40+ international countries detected), posting age validation, season verification, and smart deduplication that catches the same job posted across different platforms.

### Email Discovery Pipeline

```
Company + Hiring Manager Name
    |
    v
Layer 1: Seed pattern cache (35+ companies, instant)
Layer 2: Microsoft 365 verification (definitive yes/no)
Layer 3: Website pattern mining (scrapes company sites)
Layer 4: Reacher SMTP verification (Docker-based)
Layer 5: Reacher pattern search
Layer 6: API cascade (Hunter)
Layer 7: Microsoft 365 pattern discovery (tries 5 formats)
Layer 8: Statistical inference (80% of companies use first.last)
    |
    v
Gmail Draft Created -> Delivery Tracking -> Bounce Auto-Retry
```

The system learns over time. Every successful email discovery teaches it the pattern for that company's entire domain. Failed patterns are stored and never retried. After two weeks of operation, most companies resolve instantly from cache.

### Outreach Lifecycle

```
Job validated -> Synced to Outreach Tracker (exact Valid sheet order)
    -> Email discovered for HM/Recruiter
    -> Personalized Gmail draft created with resume
    -> Scheduled for 10 AM in company's timezone
    -> Email sent
    -> [12 hours pass]
    -> No bounce detected -> Notes: "Delivered to HM and Rec"
    -> Bounce detected -> Notes: "HM email bounced on Feb 23, 2026"
        -> Auto-retry with alternative pattern
        -> Notes updated: "Retried: flast@co.com"
```

---

## Key Features

### Multi-Source Aggregation

- GitHub repositories (SimplifyJobs, vanshb03) with section category trust
- Gmail API integration (Jobright alerts, SWE List, ZipRecruiter emails)
- Simplify URL resolution (5 methods including Next.js JSON extraction)
- INACTIVE job detection on Simplify pages
- ZipRecruiter page age validation (rejects stale postings)
- Selenium fallback for JavaScript-heavy career pages (Workday, Oracle, Ashby)

### 20-Stage Validation

- Company and platform blacklists
- Security clearance and US Person requirements
- Degree level filtering (BA/BS-only and PhD-only roles)
- Geographic restrictions (40+ international countries in title, location, URL)
- Ambiguous city disambiguation (Burlington MA vs Burlington ON, Cambridge MA vs Cambridge ON)
- F-1 visa eligibility (CPT/OPT exclusion detection)
- Permanent US work authorization detection
- Graduation year alignment (May 2027)
- Job posting age validation (configurable threshold)
- Smart season detection (ignores copyright years, financial data, past cohort references)
- Non-CS/Engineering role filtering with GitHub category override
- Canada, UK, and international location detection
- High school student role filtering
- URL-based international detection
- User preference exclusions

### Intelligent Deduplication

- URL normalization and matching
- Company + title fuzzy matching
- Job ID cross-reference
- Company name normalization (20+ alias mappings: WD/Sandisk to Western Digital, Boxinc to Box, etc.)

### Email Discovery and Verification

- Provider detection: MX record lookup identifies Google Workspace (~40%) vs Microsoft 365 (~35%) vs self-hosted
- Microsoft 365 verification: Definitive email existence check via GetCredentialType endpoint (free, no API key)
- Website mining: Scrapes company about/team/contact pages for @domain emails to learn the pattern
- Pattern learning: Every successful discovery teaches the pattern for that entire company domain
- Statistical inference: When all else fails, uses the most common pattern (first.last, 80% accuracy)
- Anti-bot measures: 2-3 second delays, rotating user agents, catch-all detection

### Delivery Tracking and Bounce Recovery

- Bounce scanner reads Gmail for delivery failure notifications (RFC 3464 DSN parsing)
- Failed emails are cleared and noted in plain language
- Auto-retry generates alternative email patterns, skipping the failed one
- Failed patterns stored permanently to prevent repeating mistakes
- Delivery confirmed after 12 hours with no bounce
- Late bounces overwrite delivery status (bounce is always the truth)

### Self-Improving System

- Pattern cache grows with every run and learns email formats automatically
- Failed Simplify URLs retried after 8 hours instead of being permanently skipped
- Retry tracker with 3-day TTL prevents infinite loops on truly undiscoverable companies
- MX cache and email verification cache prevent redundant lookups
- Outreach sheet stays perfectly synchronized with Valid Entries (exact order, verbatim names)

---

## Performance

| Metric                  | Before      | After          |
| ----------------------- | ----------- | -------------- |
| Weekly manual work      | 6 hours     | 45 minutes     |
| Job processing time     | 40 min      | 10 min         |
| Duplicate applications  | 2-5/run     | 0              |
| Classification accuracy | ~85%        | 98%+           |
| Email extraction rate   | 0% (manual) | ~95% automated |
| Outreach emails/run     | 0 (manual)  | 28             |

---

## Tech Stack

**Core:** Python 3.10+, Google Sheets API, Gmail API

**Web Scraping:** Selenium WebDriver, BeautifulSoup4, Requests, lxml

**Email Discovery:** dnspython (MX records), Reacher (SMTP verification via Docker), Hunter API, Microsoft 365 GetCredentialType

**Infrastructure:** Docker (Reacher container), Google Cloud service accounts, OAuth 2.0

**Codebase:** 10,000+ lines across 14 production modules

---

## Architecture

```
Job Hunt Tracker/
|-- aggregator/               # Module 1: Job aggregation
|   |-- config.py             # Patterns, blacklists, normalizations (1,400+ lines)
|   |-- extractors.py         # Page fetching, Simplify resolution, GitHub scraper
|   |-- processors.py         # Validation, extraction, location processing
|   |-- run_aggregator.py     # Pipeline orchestration
|   |-- sheets_manager.py     # Google Sheets integration
|   |-- utils.py              # HTTP retry, sanitization, date parsing
|-- outreach/                 # Module 2: Email outreach
|   |-- outreach_config.py    # Column mapping, email templates, API keys
|   |-- outreach_data.py      # Sheets sync, PatternCache, NameParser, bounce handling
|   |-- outreach_finder.py    # 8-layer email discovery pipeline
|   |-- outreach_mailer.py    # Gmail draft creation with resume attachment
|   |-- outreach_provider.py  # MX lookup, Microsoft 365, website mining
|   |-- bounce_scanner.py     # Gmail bounce detection (RFC 3464)
|   |-- run_outreach.py       # Pipeline orchestration, delivery tracking
|-- scripts/                  # Maintenance utilities
|-- .local/                   # Credentials, caches, logs (gitignored)
|   |-- credentials.json      # Google Sheets service account
|   |-- gmail_credentials.json
|   |-- domain_overrides.json # Manual company-to-domain fixes
|   |-- failed_patterns.json  # Bounced email patterns (never retried)
|   |-- bounced_emails.json   # Known bounced addresses
|   |-- outreach_patterns.json # Learned email patterns per domain
|   |-- retry_tracker.json    # Failed companies (3-day TTL)
|   |-- outreach.log          # Rotating log (5MB x 3)
|-- docs/screenshots/         # README images
|-- docker-compose.yml        # Reacher email verifier
|-- requirements.txt
|-- README.md
```

---

## Quick Start

### Prerequisites

- Python 3.10+
- Google Sheets API credentials (service account)
- Gmail API credentials (OAuth)
- Docker (optional, for Reacher SMTP verification)
- ChromeDriver (for Selenium-based page fetching)

### Installation

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Optional: Reacher for SMTP verification
docker compose up -d
```

### Running

```bash
# Aggregate new jobs
python3 -m aggregator

# Find emails and create outreach drafts
python3 -m outreach

# Check outreach status
python3 -m outreach status

# Reset API credits
python3 -m outreach reset
```

### Configuration

1. Place Google Sheets service account JSON in `.local/credentials.json`
2. Place Gmail OAuth client JSON in `.local/gmail_credentials.json`
3. Create `.env` with API keys: APOLLO_API_KEY, HUNTER_API_KEY, SNOV_API_KEY, SNOV_USER_ID
4. Edit `outreach/outreach_config.py` for sender name and email templates
5. Add domain overrides in `.local/domain_overrides.json` for companies with wrong Clearbit results

---

## Google Sheets Structure

### Valid Entries

Validated internship postings with status, company, title, date applied, job URL, job ID, job type, location, remote status, resume type, sponsorship, entry date, and source.

### Outreach Tracker

Email outreach tracking with company, title, job ID, HM/recruiter names, LinkedIn URLs (clickable), discovered emails, scheduled send time, sent date, and delivery notes. Automatically synchronized with Valid Entries in the same row order.

### Discarded Entries

Rejected postings with the specific discard reason (Non-USA location, Non-tech role, Blacklisted company, Undergraduate only, PhD only, Security clearance required, Wrong season, etc.), preserving full metadata for review.

---

## Outreach Notes Column

The Notes column in the Outreach Tracker automatically tracks email delivery status:

| Status              | Notes                                                   |
| ------------------- | ------------------------------------------------------- | ---------------- |
| Delivered to one    | Delivered to HM                                         |
| Delivered to both   | Delivered to HM and Rec                                 |
| One bounced         | HM email bounced on Feb 23, 2026                        |
| Both bounced        | HM and Rec emails bounced on Feb 23, 2026               |
| Bounced and retried | HM email bounced on Feb 23, 2026. Retried: flast@co.com |
| Partial             | HM email bounced on Feb 23, 2026                        | Delivered to Rec |

---

## Results

**Daily aggregation:** Processes 500+ postings, produces around 70 valid, 50 discarded, and 200 duplicates caught in 10 minutes.

**Email discovery:** 28 emails extracted per run across 15 companies. The 8-layer system resolves previously-impossible companies (T-Mobile, Skyryse, Cleveland Clinic).

**Cumulative:** 400+ tracked entries, 100+ outreach emails sent, zero duplicate applications.

---

## Use Cases

- Graduate students managing hundreds of internship applications
- International students with F-1 visa restrictions (CPT/OPT filtering built in)
- Job seekers automating cold outreach to hiring managers and recruiters
- Anyone aggregating job postings from multiple sources into a single tracker

---

## Contact

**Prasad Chandrashekhar Kanade**
MS Computer Science | Northeastern University | May 2027

Email: kanade.pra@northeastern.edu |
[LinkedIn](https://linkedin.com/in/prasad-kanade-) |
[GitHub](https://github.com/prasad0411)
