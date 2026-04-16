# Automated Job Hunt Pipeline

End-to-end system that aggregates 8,000+ weekly internship postings, validates eligibility, discovers hiring manager emails, and sends personalized outreach — fully automated from a Northeastern University email address.

**Built by [Prasad Kanade](https://prasad0411.github.io/Prasad-Portfolio) · MS Computer Science @ Northeastern University**

---

## What It Does

**Module 1 — Job Aggregator** scrapes GitHub repositories and Gmail alerts, validates each posting against 25+ eligibility criteria, deduplicates across 1,500+ tracked entries, and writes qualified jobs to Google Sheets with resume classification (SDE / ML / DA).

**Module 2 — Outreach Pipeline** discovers hiring manager and recruiter emails via an 8-layer verification system, sends personalized emails with the right resume from `kanade.pra@northeastern.edu` via Microsoft Graph API, schedules timezone-aware delivery at 9:30 AM local time, and auto-retries on bounce.

**Module 3 — Scheduler Daemon** runs all jobs on a permanent KeepAlive launchd process — survives Mac sleep, shutdown, and restarts with automatic missed-job catchup on wake.

**Impact:** 6 hours/week → 15 minutes/week · Zero duplicate applications · 98%+ classification accuracy

---

## Screenshots

| Aggregator Output | Valid Entries Sheet |
|---|---|
| ![Aggregator](docs/screenshots/aggregator.png) | ![Valid Entries](docs/screenshots/valid_entries.png) |

| Outreach Tracker | Nightly Digest |
|---|---|
| ![Outreach](docs/screenshots/outreach.png) | ![Digest](docs/screenshots/digest.png) |

---

## Performance

| Metric | Value |
|---|---|
| Weekly manual work | 15 min (was 6 hours) |
| Valid jobs processed (all-time) | 605+ |
| Currently tracked entries | 890+ |
| Outreach emails sent | 130+ |
| Email extraction rate | ~95% automated |
| Domains with learned patterns | 219 |
| Companies tracked in Brain | 134 |
| Classification accuracy | 98%+ |
| Duplicate applications | 0 |
| Codebase size | 16,000+ lines · 18 production modules |

---

## How It Works

### Aggregation Pipeline

```
GitHub Repos + Gmail Alerts
  → Resolve redirects (Simplify, Jobright, ZipRecruiter)
  → Fetch career pages (Selenium + BeautifulSoup)
  → 25-stage validation (visa, degree, geography, role, salary, age...)
  → Deduplicate (URL + company|title + job ID)
  → Company name normalization + auto-learning (brain.json)
  → Resume classification (SDE / ML / DA)
  → Google Sheets output + SQLite run history
```

### Email Discovery Pipeline

```
Company + Hiring Manager Name
  → Layer 1: Seed pattern cache (125+ companies, instant)
  → Layer 2: DomainHistory (proven patterns from past deliveries)
  → Layer 3: Brain contact cache (verified contacts — skips all API calls)
  → Layer 4: Microsoft 365 verification (definitive yes/no)
  → Layer 5: Website pattern mining
  → Layer 6-7: Reacher SMTP verification
  → Layer 8: API cascade (Apollo → Hunter → Snov → Prospeo)
  → Layer 9: Statistical inference (first.last)
  → Pre-send bounce check → MS Graph send → Bounce auto-retry
```

### Timezone-Aware Scheduling

```
Outreach midnight → discovers emails → schedules Send At = next business day 9:30 AM local
  9:00 AM ET  → Eastern companies
  10:30 AM ET → Central companies
  11:30 AM ET → Mountain companies
  12:30 PM ET → Pacific companies
```

---

## Key Features

### Aggregation
- **Sources:** SimplifyJobs + vanshb03 GitHub repos, Jobright, SWE List, ZipRecruiter, company newsletters
- **Simplify metadata extraction:** location, remote status, no_h1b flag (immediate reject)
- **Selenium fallback** for JS-heavy pages (Workday, Oracle, Ashby)
- **HTTP response cache** (6-hour TTL, 500 entries max)

### 25-Stage Validation
- Security clearance, ITAR, US Person, citizenship requirements
- Visa rejection ("unable to sponsor" in all variants)
- Degree filtering: undergrad-only, PhD-only, associate's degree
- Role filtering: hardware, optics, RF, robotics, military, non-CS
- Salary filter: rejects if listed and under $25/hr
- Geographic filter: 40+ countries, Canadian provinces
- Posting age: 3-day default threshold
- Season detection (ignores copyright years, rejects wrong semester)
- Auto-learning company names: URL domain → company saved to brain.json

### Outreach
- **8-layer email discovery** with confidence scoring
- **Brain contact cache:** if a verified contact exists for a company, skips all API calls entirely
- **DomainHistory:** instant resolution for known domains, zero API calls
- **PatternCache:** learns from every successful delivery — 219 domains learned
- **Auto-Extract:** sets Extract=yes for tech hub locations, sponsorship=Yes, PatternCache hits
- **Bounce recovery:** uses DomainHistory to generate domain-informed retry patterns
- **Dead letter queue:** after 3 failed attempts, marks row permanently

### Scheduler (Permanent Daemon)
- Single Python scheduler daemon with `KeepAlive=true` — managed by launchd, auto-restarts on crash, reboot, or login
- Missed-job catchup on every Mac wake or restart
- Per-job timeout limits (aggregator: 15 min, outreach: 30 min)
- Auto-retry: failed jobs retry once after 30 minutes before waiting for next window
- Thread isolation: one failed job cannot affect others
- Atomic state writes: no JSON corruption on crash

| Job | Schedule |
|---|---|
| Aggregator | 8 AM, 3 PM, 9 PM |
| Send Scheduled | 9 AM, 10:30, 11:30, 12:30 |
| Outreach | Midnight |
| Nightly Digest | 12:22 AM |
| Cleanup | 7:30 AM |
| Auto-Blacklist | 12:30 AM |
| Retry Simplify | 6 AM |
| Process Bounces | Every 30 min |
| Watchdog | Every 30 min |

### Self-Healing & Intelligence
- **Scheduler daemon:** single KeepAlive process, launchd auto-restarts on crash
- **Watchdog:** monitors all job health files, auto-reruns stale or failed jobs, verifies MS token validity, auto-restarts Docker/Reacher
- **MS token:** silent refresh on every init, email alert if refresh token expires
- **ChromeDriver:** webdriver_manager auto-updates on macOS updates
- **Sheets quota:** exponential backoff retry (5s → 10s → 20s → 40s → 80s)
- **Brain pruning:** simplify retry queue, job ID registry, draft history auto-pruned on every save
- **Log rotation:** automatic caps on all log files, 7-day cron log retention, daily .pyc cleanup
- **Auto-blacklist:** weekly build from Discarded Entries (3+ same-reason rejections), with backup + syntax check before config modification
- **Nightly digest:** email summary with aggregator stats, outreach sent/bounced counts, circuit breaker status, scheduler health table, API credit warnings

---

## Tech Stack

| Layer | Technologies |
|---|---|
| Core | Python 3.14, Google Sheets API, Gmail API, Microsoft Graph API |
| Scraping | Selenium, BeautifulSoup4, Requests, webdriver-manager |
| Email | dnspython, Reacher (Docker SMTP), Apollo, Hunter, Snov, Prospeo, MSAL |
| Infrastructure | Single Python daemon (KeepAlive), SQLite, Docker |
| Intelligence | brain.json learning system, PatternCache, DomainHistory, company_contacts |
| Codebase | 16,000+ lines across 18 production modules |

---

## Architecture

```
Job Hunt Tracker/
├── aggregator/
│   ├── config.py          # Patterns, blacklists, normalizations (3,200+ lines)
│   ├── extractors.py      # Page fetching, Simplify resolution, GitHub scraper
│   ├── processors.py      # Validation, extraction, location (3,600+ lines)
│   ├── run_aggregator.py  # Pipeline orchestration
│   ├── sheets_manager.py  # Google Sheets (with quota retry)
│   └── utils.py           # HTTP retry, sanitization
├── outreach/
│   ├── brain.py              # Shared intelligence layer — learns patterns, contacts, domains
│   ├── outreach_config.py    # Column mapping, templates, API keys
│   ├── outreach_data.py      # Sheets sync, PatternCache, bounce handling
│   ├── outreach_finder.py    # 8-layer email discovery
│   ├── outreach_mailer.py    # MS Graph sending + token self-refresh
│   ├── outreach_provider.py  # MX lookup, M365 verification, web mining
│   ├── outreach_verifier.py  # Confidence scoring, CircuitBreaker
│   ├── bounce_scanner.py     # Gmail bounce detection
│   └── run_outreach.py       # Pipeline orchestration
├── scripts/
│   ├── scheduler.py           # KeepAlive daemon — owns all scheduled jobs
│   ├── run_scheduler.sh       # Wrapper for launchd bootstrap
│   ├── cron_runner.sh         # Job runner (resume sync, module exec, health files)
│   ├── send_scheduled.py      # Timezone-aware email sender (4×/day)
│   ├── nightly_digest.py      # Nightly summary email with scheduler health
│   ├── auto_extract.py        # Auto-sets Extract=yes based on smart signals
│   ├── build_auto_blacklist.py# Learns from rejections, updates config safely
│   ├── cleanup_not_applied.py # Moves stale jobs + log rotation + file hygiene
│   ├── process_bounces.py     # NDR processor → Brain pattern failure learning
│   ├── retry_simplify.py      # Retries failed Simplify URL resolutions
│   ├── watchdog.sh            # Health monitor + auto-rerun + token check
│   └── resume_sync.sh         # Syncs latest resumes from Downloads
└── .local/                    # Credentials, caches, logs (gitignored)
    ├── brain.json             # Learned: 219 domains, 134 companies, patterns, contacts
    ├── scheduler_state.json   # Last run times for all 9 jobs
    ├── run_history.db         # SQLite: per-run stats
    └── cron_logs/             # 7-day rotating job logs
```

---

## Quick Start

### Prerequisites
- Python 3.12+, Docker, ChromeDriver
- Google Sheets service account, Gmail OAuth, Northeastern Microsoft account

### Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
docker compose up -d  # Reacher SMTP verifier

# One-time MS Graph auth
python3 scripts/test_ms_auth.py

# Place resumes in Downloads (auto-synced before each run):
# "Prasad Kanade SWE Resume.pdf"
# "Prasad Kanade ML Resume.pdf"
# "Prasad Kanade Data Resume.pdf"
```

### Environment

```bash
# .env
HUNTER_API_KEY=
APOLLO_API_KEY=
PROSPEO_API_KEY=
SNOV_API_KEY=
SNOV_USER_ID=
ANTHROPIC_API_KEY=   # optional
SLACK_WEBHOOK_URL=   # optional
```

### Start Scheduler

```bash
# Install and start the KeepAlive daemon
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.prasad.jobtracker.scheduler.plist

# Verify it's running
launchctl print gui/$(id -u)/com.prasad.jobtracker.scheduler | grep -E "state|pid"

# Monitor
tail -f .local/scheduler.log
```

### Manual Runs

```bash
python3 -m aggregator                        # Aggregate new jobs
python3 -m outreach                          # Find emails + create drafts
python3 scripts/send_scheduled.py            # Send due emails now
python3 scripts/cleanup_not_applied.py       # Clean sheet + rotate logs
python3 scripts/watchdog.sh                  # Run health check
```

### Check Run History

```bash
python3 -c "
import sqlite3
for r in sqlite3.connect('.local/run_history.db').execute(
    'SELECT ts,valid,discarded,failed_http FROM runs ORDER BY ts DESC LIMIT 10'):
    print(r)
"
```

---

## Contact

**Prasad Chandrashekhar Kanade** · MS CS @ Northeastern University · May 2027

[Email](mailto:kanade.pra@northeastern.edu) · [LinkedIn](https://linkedin.com/in/prasad-kanade) · [GitHub](https://github.com/prasad0411) · [Portfolio](https://prasad0411.github.io/Prasad-Portfolio)
