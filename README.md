# Automated Job Hunt Pipeline

End-to-end system that aggregates 8,000+ weekly internship postings, validates eligibility through a 25-stage pipeline, discovers hiring manager emails, and sends personalized outreach — fully automated with crash recovery, anomaly detection, and real-time analytics.

**Built by [Prasad Kanade](https://prasad0411.github.io/Prasad-Portfolio) · MS Computer Science @ Northeastern University**

[![CI](https://github.com/prasad0411/JobsScrapingAutomation/actions/workflows/ci.yml/badge.svg)](https://github.com/prasad0411/JobsScrapingAutomation/actions)
[![Tests](https://img.shields.io/badge/tests-237%20passed-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.14-blue)]()
[![Dashboard](https://img.shields.io/badge/dashboard-live-orange)](https://job-scraping-analytics.streamlit.app/)

---

## Impact

| Metric | Value |
|---|---|
| Weekly manual work | **15 min** (was 6 hours) |
| Total jobs processed | **6,120+** across 12 dimensions |
| Valid jobs discovered | **870+** |
| Outreach emails sent | **262** |
| Companies reached | **132** (270 LinkedIn messages) |
| Domains with learned patterns | **219** |
| Classification accuracy | **98%+** |
| Duplicate applications | **0** |
| Codebase | **27,500+ lines · 237 automated tests** |

---

## Architecture

```
SCHEDULER DAEMON (KeepAlive launchd · survives sleep/reboot)
├── Aggregator (3x/day) ──→ 6 Sources ──→ Validation Pipeline ──→ WAL ──→ Sheets + Analytics DB
├── Outreach (midnight) ──→ Brain Cache ──→ 8-Layer Email Discovery ──→ MS Graph Send
├── Send Scheduled (4x/day) ──→ TZ-Aware Delivery ──→ Applied Trigger
├── Watchdog (30 min) ──→ Docker + Token + Health Check
├── Digest (12:22 AM) ──→ Anomaly Alerts + Data Quality Report
└── Cleanup (7:30 AM) ──→ Log Rotation + Sheet Formatting

DATA LAYER
├── Google Sheets (Valid · Discarded · Outreach · Reviewed)
├── SQLite Analytics DB (star schema · 6,120 jobs · 12 dimensions)
├── brain.json (219 domains · 255 companies · Bayesian pattern learning)
└── Write-Ahead Log (.local/wal/ · crash-safe sheet mutations)
```

---

## Engineering Highlights

### Resilience Patterns
- **Write-Ahead Log (WAL):** Every sheet mutation is journaled before execution. Uncommitted transactions auto-replay on next startup — zero data loss on crash, network drop, or Ctrl+C.
- **Circuit Breaker:** Generalized CLOSED→OPEN→HALF_OPEN pattern across Google Sheets API, Selenium, and email discovery APIs. Graceful degradation with automatic recovery.
- **Retry with Exponential Backoff + Jitter:** All external API calls use decorrelated jitter to prevent thundering herd. Configurable max retries with dead letter routing.

### Observability
- **Structured Logging with Correlation IDs:** Every aggregator run gets a `run_id`, every job gets a `job_trace_id`. End-to-end tracing from source ingestion through validation to outreach delivery.
- **Statistical Anomaly Detection:** Rolling Z-scores and SPC bounds (UCL/LCL at ±2σ) monitor source quality. Alerts surface in the nightly digest — monitor-only, never throttles volume.
- **Data Quality Scoring:** 8-dimension completeness scoring (company, title, location, URL, job ID, salary, sponsorship, remote) with A-F grading and source-level breakdowns.

### Data Engineering
- **Star Schema Analytics Store:** SQLite warehouse with `jobs` fact table and 4 dimension tables. WAL mode, indexed for common query patterns, real-time ingestion on every aggregator run.
- **ETL Pipeline:** Backfills from Google Sheets, then incremental real-time recording. 6,120 jobs backfilled across 12 dimensions.
- **Feature Store:** 8 engineered features per job posting (source reliability, company history, title similarity, location density) for future ML scoring.

### ML / NLP
- **TF-IDF Cosine Similarity Dedup:** Pure Python TF-IDF engine catches near-duplicates that exact match misses. Zero false positives at 0.90 threshold.
- **Bayesian Email Pattern Learning:** Brain tracks pattern success/failure rates per domain with exponential moving average. Ranks candidate patterns by posterior probability.

### Software Architecture
- **Pluggable Validation Pipeline:** 6 composable stages loaded from YAML config. Add/remove rules without code changes.
- **Data Contracts:** Typed dataclasses with runtime validation and legacy field coercion across 18+ modules.
- **Configuration Hot-Reload:** File watcher detects config changes and reloads without daemon restart.

---

## Pipeline Details

### Aggregation

```
GitHub Repos + Gmail Alerts + Jobright + SWE List + ZipRecruiter
  → Resolve redirects (Simplify, Jobright, ZipRecruiter)
  → Fetch career pages (Selenium + BeautifulSoup)
  → 6-stage validation pipeline (title → location → page → age → salary → sponsorship)
  → Deduplicate (URL + company|title + job ID + TF-IDF fuzzy match)
  → Company normalization + auto-learning (brain.json)
  → Resume classification (SDE / ML / DA)
  → WAL-protected Google Sheets write
  → Real-time analytics recording (SQLite)
```

### Email Discovery (8 Layers)

```
Company + Hiring Manager Name
  → Layer 1: Brain contact cache (verified contacts — instant)
  → Layer 2: DomainHistory (proven patterns from past deliveries)
  → Layer 3: PatternCache (125+ companies, learned success rates)
  → Layer 4: Microsoft 365 verification (definitive yes/no)
  → Layer 5: Website pattern mining
  → Layer 6-7: Reacher SMTP verification (Docker)
  → Layer 8: API cascade (Apollo → Hunter → Snov → Prospeo)
  → Pre-send bounce check → MS Graph send → Bounce auto-retry
```

### Scheduler

| Job | Schedule | Protection |
|---|---|---|
| Aggregator | 8 AM, 3 PM, 9 PM | WAL + Circuit Breaker |
| Outreach | Midnight | Brain Cache + Dead Letter |
| Send Scheduled | 9, 10:30, 11:30, 12:30 | TZ-aware + Applied Trigger |
| Nightly Digest | 12:22 AM | Anomaly Alerts + DQ Report |
| Cleanup | 7:30 AM | Log Rotation + Sheet Format |
| Process Bounces | Every 30 min | Pattern Failure Learning |
| Watchdog | Every 30 min | Docker + Token + Health |

---

## Source Quality (Live)

```
Source               Valid%   Volume
Discord              100.0%        1
LinkedIn             100.0%       18
Manual                83.3%        6
GitHub                57.9%       19
Jobright              27.9%       86
SimplifyJobs          21.9%      686
SWE List               9.9%      476
```

---

## Test Coverage

```
237 tests · 15 test files · <1s execution

URL mappings (40) · Salary extraction (10) · Undergrad detection (11)
Location parsing (13) · Company normalization (12) · Title validation (11)
Dedup (5) · Brain patterns (10) · Date parsing (9)
Validation pipeline (16) · Analytics store (20) · Anomaly detection (9)
WAL transactions (16) · Engineering patterns (33) · Data quality + TF-IDF (22)
```

---

## Tech Stack

| Layer | Technologies |
|---|---|
| Core | Python 3.14, Google Sheets API, Gmail API, Microsoft Graph API |
| Scraping | Selenium, BeautifulSoup4, Requests, webdriver-manager |
| Email | dnspython, Reacher (Docker), Apollo, Hunter, Snov, Prospeo, MSAL |
| Analytics | SQLite (WAL mode), star schema, TF-IDF, Z-score anomaly detection |
| Infrastructure | KeepAlive daemon (launchd), Write-Ahead Log, Circuit Breakers |
| Testing | pytest (237 tests), GitHub Actions CI/CD |
| Dashboard | [Streamlit (live)](https://job-scraping-analytics.streamlit.app/) |
| Intelligence | brain.json (Bayesian learning, contact cache, domain history) |

---

## Repository Structure

```
Job Hunt Tracker/                         27,500+ lines
├── aggregator/
│   ├── config.py                         # 3,200+ patterns, blacklists, normalizations
│   ├── processors.py                     # 3,600+ lines: validation, extraction, location
│   ├── run_aggregator.py                 # Orchestration + WAL + analytics recording
│   ├── validation/                       # Pluggable validation pipeline (YAML config)
│   ├── wal.py                            # Write-Ahead Log
│   ├── circuit_breaker.py                # Generalized circuit breaker
│   ├── retry.py                          # Exponential backoff + jitter
│   ├── correlation.py                    # Correlation IDs + structured logging
│   ├── hot_reload.py                     # Config file watcher
│   └── contracts.py                      # Typed data contracts
├── analytics/
│   ├── schema.py                         # Star schema DDL
│   ├── store.py                          # Analytics read/write + feature vectors
│   ├── anomaly.py                        # Rolling Z-scores + SPC bounds
│   ├── data_quality.py                   # 8-dimension quality scoring
│   └── similarity.py                     # TF-IDF cosine similarity engine
├── outreach/
│   ├── brain.py                          # Intelligence layer (patterns, contacts)
│   ├── outreach_finder.py                # 8-layer email discovery
│   └── run_outreach.py                   # Orchestration + Brain auto-fill
├── scripts/
│   ├── scheduler.py                      # KeepAlive daemon
│   ├── nightly_digest.py                 # Anomaly alerts + DQ report
│   ├── applied_trigger.py                # Applied → instant Extract=yes
│   └── autocommit.sh                     # Auto-generated commit messages
├── dashboard/app.py                      # Streamlit analytics dashboard
├── tests/                                # 237 tests across 15 files
├── .github/workflows/ci.yml              # GitHub Actions CI
└── .local/                               # brain.json, analytics.db, WAL (gitignored)
```

---

## Quick Start

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
docker compose up -d  # Reacher SMTP verifier
python3 scripts/test_ms_auth.py  # One-time MS Graph auth

# Start daemon
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.prasad.jobtracker.scheduler.plist

# Manual runs
python3 -m aggregator          # Aggregate new jobs
python3 -m outreach            # Find emails + create drafts
python3 -m analytics.etl       # Backfill analytics DB
python3 -m pytest tests/ -v    # Run all tests
```

---

## Contact

**Prasad Chandrashekhar Kanade** · MS CS @ Northeastern University · May 2027

[Email](mailto:kanade.pra@northeastern.edu) · [LinkedIn](https://linkedin.com/in/prasad-kanade) · [GitHub](https://github.com/prasad0411) · [Portfolio](https://prasad0411.github.io/Prasad-Portfolio)