# Job Hunt Tracker - Automated Internship Aggregation System

[![Python 3.14+](https://img.shields.io/badge/python-3.14+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Enterprise-grade automation system that processes **1,300+ daily job postings** across multiple platforms, delivering **25-35 validated internship opportunities** with **96%+ accuracy** to Google Sheets.

---

## Impact

| Metric           | Value           |
| ---------------- | --------------- |
| Daily Processing | 1,300+ postings |
| Accuracy         | 96%+            |
| Time Saved       | ~15 hrs/week    |
| Output Quality   | 97% precision   |

---

## Architecture

```
GitHub (SimplifyJobs) + Gmail (SWE List, Jobright) + Direct URLs
                          ↓
        Multi-Source Extraction (6+ ATS platforms)
                          ↓
              Validation Pipeline (7 filters)
                          ↓
           Deduplication (355+ URLs tracked)
                          ↓
                  Google Sheets
```

---

## Key Features

### **Multi-Source Aggregation**

- GitHub repositories (SimplifyJobs, vanshb03)
- Gmail integration via OAuth2
- Direct URL processing (Workday, Greenhouse, Lever, iCIMS, Taleo)

### **Intelligent URL Resolution**

| Platform      | Success Rate | Methods                                                     |
| ------------- | ------------ | ----------------------------------------------------------- |
| Simplify.jobs | 85-95%       | 5-method cascade (HTTP → Parse → Click → Selenium → iframe) |
| Jobright      | 95-100%      | Click-based + email parsing hybrid                          |
| Direct URLs   | 98%          | Standard fetch with retry                                   |

### **Data Extraction**

- **Company**: 6-method voting (URL, domain, meta tags, JSON-LD, page parsing)
- **Location**: 8+ sources (URL patterns, HTML selectors, Workday codes)
- **Job ID**: Regex patterns for 7 ATS platforms

### **Validation Pipeline**

**Geographic**

- Rejects Canada (10 provinces, 25+ cities), international (40+ countries)
- Handles ambiguous cases (London ON vs London UK)

**Academic Eligibility**

- Graduation year: Accepts 2027, rejects 2026/2028+
- Degree level: Detects Bachelor's-only positions
- Parses date ranges ("12/2025 - 6/2028" → identifies 2027)

**Security & Role**

- 10 clearance patterns (DOD Secret, TS/SCI, citizenship requirements)
- 50+ technical keywords for role classification
- Season filtering (Summer 2026 only)

### **Quality Assurance**

- 7-point scoring system
- Triple deduplication (URL, Company+Title, Job ID)
- Data cleaning (Unicode stripping, legal suffix removal)

---

## Tech Stack

**Core**: Python 3.14, Selenium WebDriver, BeautifulSoup4  
**APIs**: Google Sheets v4, Gmail API (OAuth2)  
**HTTP**: Requests with session management

---

## Performance

| Stage            | Volume             | Time          | Success  |
| ---------------- | ------------------ | ------------- | -------- |
| GitHub scraping  | 1,368 jobs         | 3-5 min       | 100%     |
| Email processing | 140-250 URLs       | 40-50 min     | 95-100%  |
| **Total**        | **1,300+ → 25-35** | **45-60 min** | **96%+** |

**Filter Distribution**: Geographic (15-20%) • Degree (10-15%) • Non-technical (20-25%) • Season (15-20%) • Duplicates (30-35%)

---

## Setup

```bash
# Install
git clone https://github.com/prasad0411/JobsScrapingAutomation.git
cd JobsScrapingAutomation
pip install -r requirements.txt

# Configure (config.py)
GMAIL_CREDS_FILE = "credentials.json"
MAX_JOB_AGE_DAYS = 7
GOOGLE_SHEET_ID = "your-sheet-id"

# Run
python3 job_aggregator.py
```

**Prerequisites**: Python 3.14+, Chrome/ChromeDriver, Google Cloud credentials

---

## Output

**Google Sheets Columns**: Company | Job ID | Title | Type | Location | Remote | Date | URL | Source | H1B

**Additional Sheets**: Rejected Jobs (audit trail) | Duplicate Tracking | Analytics

---

## Reliability

**Crash Safety**

- No email tracking (reprocesses with duplicate prevention)
- Restart-safe at any point
- Selenium cleanup every 50 pages

**Data Integrity**

- Multi-source validation
- Confidence scoring with review flags
- Comprehensive logging (timestamped, categorized)

---

## Project Structure

```
├── job_aggregator.py       # Main orchestrator
├── extractors.py           # URL and data extraction
├── processors.py           # Validation pipeline
├── sheets_manager.py       # Google Sheets integration
├── config.py               # Configuration
└── requirements.txt
```

---

## Author

**Prasad Kanade**  
Software Engineer | Ex-Amdocs | MS CS @ Northeastern  
[LinkedIn](https://linkedin.com/in/prasad-kanade-) • [GitHub](https://github.com/prasad0411)

---

**Version** 2.0 • **Updated** January 2026 • **Status** Production
