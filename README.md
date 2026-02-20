# Automated Job Hunt Pipeline

**End-to-end system that aggregates 2,500+ weekly internship postings, validates eligibility, discovers hiring manager emails, and creates personalized outreach drafts — all automatically.**

Built by [Prasad Kanade](https://linkedin.com/in/prasad-kanade-) | MS Computer Science @ Northeastern University

---

## What It Does

Two Python modules work together to automate the entire job search workflow:

**Module 1 — Job Aggregator** scrapes GitHub repositories (SimplifyJobs, vanshb03) and Gmail alerts (Jobright, SWE List, ZipRecruiter, Company Newsletters), validates each job against 11 eligibility criteria, deduplicates across 1,500+ tracked entries, and maintains an organized Google Sheets tracker.

**Module 2 — Outreach Pipeline** takes validated jobs, discovers hiring manager and recruiter email addresses through an 8-layer verification system, and creates personalized email drafts in Gmail — ready to review and send.

**Impact:** 6 hours/week → 45 minutes/week. Zero duplicate applications. 98%+ classification accuracy. 28 outreach emails generated per run.

---

## How It Works

### Job Aggregation Pipeline

```
GitHub Repos + Gmail Alerts
    → Parse 2,000+ weekly postings
    → Resolve redirects (SimplifyJobs, Jobright, LinkedIn)
    → Fetch career pages (Selenium + BeautifulSoup)
    → Extract metadata (company, location, job ID, type)
    → 11-stage validation (visa, degree, geography, role type...)
    → Multi-signal deduplication (URL + company|title + job ID)
    → Google Sheets output
```

The aggregator processes jobs from multiple sources simultaneously. Each job passes through eligibility filters including F-1 visa requirements, security clearance detection, degree level filtering, geographic restrictions (40+ international countries detected), and smart deduplication that catches the same job posted across different platforms.

### Email Discovery Pipeline

```
Company + Hiring Manager Name
    ↓
Layer 1: Seed pattern cache (35+ companies, instant)
Layer 2: Microsoft 365 verification (definitive yes/no)
Layer 3: Website pattern mining (scrapes company sites)
Layer 4: Reacher SMTP verification (Docker-based)
Layer 5: Reacher pattern search
Layer 6: API cascade (Hunter)
Layer 7: Microsoft 365 pattern discovery (tries 5 formats)
Layer 8: Statistical inference (80% of companies use first.last)
    ↓
Gmail Draft Created
```

The system learns over time. Every successful email discovery teaches it the pattern for that company's entire domain. After two weeks of operation, most companies resolve instantly from cache.

---

## Key Features

### Multi-Source Aggregation
- GitHub repositories (SimplifyJobs, vanshb03) — 2,500+ combined listings
- Gmail API integration (Jobright alerts, SWE List emails)
- Automatic redirect resolution through 4 methods
- Selenium fallback for JavaScript-heavy career pages

### 11-Stage Validation
- Company and platform blacklists
- Security clearance and US Person requirements
- Degree level filtering (Undergraduate/PhD only roles)
- Geographic restrictions (40+ international countries in title/location)
- F-1 visa eligibility (CPT/OPT detection)
- Graduation year alignment
- Job posting age validation
- Non-CS role filtering
- Canada/UK/international location detection
- User preference exclusions

### Intelligent Email Discovery
- **Provider detection:** MX record lookup identifies Google Workspace (~40%) vs Microsoft 365 (~35%) vs self-hosted
- **Microsoft 365 verification:** Definitive email existence check via GetCredentialType endpoint (free, no API key)
- **Website mining:** Scrapes company about/team/contact pages for @domain emails to learn the pattern
- **Pattern learning:** Every successful discovery teaches the pattern for that entire company domain
- **Statistical inference:** When all else fails, uses the most common pattern (first.last, 80% accuracy)
- **Anti-bot measures:** 2-3s delays, rotating user agents, catch-all detection

### Self-Improving System
- Pattern cache grows with every run — learns email formats automatically
- Retry tracker with 3-day TTL prevents infinite loops on truly undiscoverable companies
- MX cache prevents redundant DNS lookups
- Email verification cache prevents re-checking known addresses

---

## Performance

| Metric | Before | After |
|--------|--------|-------|
| Weekly manual work | 6 hours | 45 minutes |
| Job processing time | 40 min | 10 min |
| Duplicate applications | 2-5/run | 0 |
| Classification accuracy | ~85% | 98%+ |
| Email extraction rate | 0% (manual) | ~95% automated |
| Outreach emails/run | 0 (manual) | 28 |

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
├── aggregator/               # Module 1: Job aggregation
│   ├── config.py             # Patterns, blacklists, selectors (1,400+ lines)
│   ├── extractors.py         # Page fetching, redirect resolution
│   ├── processors.py         # Validation, extraction, location processing
│   ├── run_aggregator.py     # Pipeline orchestration
│   ├── sheets_manager.py     # Google Sheets integration
│   └── utils.py              # HTTP retry, sanitization, date parsing
├── outreach/                 # Module 2: Email outreach
│   ├── outreach_config.py    # Headers, column mapping, API keys
│   ├── outreach_data.py      # Sheets I/O, PatternCache, NameParser
│   ├── outreach_finder.py    # 8-layer email discovery pipeline
│   ├── outreach_mailer.py    # Gmail draft creation
│   ├── outreach_provider.py  # MX lookup, Microsoft 365, website mining
│   └── run_outreach.py       # Logging, entry point
├── scripts/                  # Maintenance utilities
│   ├── cleanup_not_applied.py
│   └── backup_secrets.py
├── .local/                   # Credentials, caches, logs (gitignored)
│   ├── credentials.json      # Google Sheets service account
│   ├── gmail_credentials.json
│   ├── domain_overrides.json # Manual company→domain fixes
│   ├── mx_cache.json         # MX record cache
│   ├── email_verify_cache.json
│   ├── outreach_patterns.json # Learned email patterns
│   ├── retry_tracker.json    # Failed companies (3-day TTL)
│   └── outreach.log          # Rotating log (5MB × 3)
└── README.md
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
pip install gspread google-auth google-auth-oauthlib beautifulsoup4 selenium requests lxml dnspython

# Optional: Reacher for SMTP verification
docker run -d --name reacher -p 8080:8080 --platform linux/amd64 \
  -e RCH__FROM_EMAIL=test@example.org reacherhq/backend:latest
```

### Running
```bash
# Aggregate new jobs
python3 -m aggregator

# Find emails and create outreach drafts
docker start reacher 2>/dev/null; sleep 2; python3 -m outreach
```

### Configuration
1. Place Google Sheets service account JSON in `.local/credentials.json`
2. Place Gmail OAuth client JSON in `.local/gmail_credentials.json`
3. Edit `outreach/outreach_config.py` for sender name, email, API keys
4. Add domain overrides in `.local/domain_overrides.json` for companies with wrong Clearbit results

---

## Google Sheets Structure

### Valid Entries (14 columns)
Validated internship postings with company, title, job ID, URLs, location, remote status, source, sponsorship, and notes.

### Outreach Tracker (13 columns)
Email outreach tracking with HM/recruiter names, LinkedIn URLs (clickable), discovered emails, send timestamps, and notes.

### Discarded Entries (13 columns)
Rejected postings with discard reason (Non-USA, Non-tech, Blacklisted, etc.), preserving full metadata for review.

---

## Results

**Daily aggregation:** Processes 500+ postings → ~70 valid, ~50 discarded, ~200 duplicates caught — in 10 minutes.
 
**Email discovery:** 28 emails extracted per run across 15 companies. 8-layer system resolves previously-impossible companies (T-Mobile, Skyryse, Cleveland Clinic).

**Cumulative:** 370+ tracked entries, 100+ outreach emails sent, zero duplicate applications.

---

## Use Cases

- Graduate students managing hundreds of internship applications
- International students with F-1 visa restrictions (CPT/OPT filtering)
- Job seekers automating cold outreach to hiring managers
- Anyone aggregating job postings from multiple sources

---

## Contact

**Prasad Chandrashekhar Kanade**
MS Computer Science | Northeastern University | May 2027

📧 kanade.pra@northeastern.edu
💼 [LinkedIn](https://linkedin.com/in/prasad-kanade-)
🐙 [GitHub](https://github.com/prasad0411)