# JobsScrapingAutomation

Automated job scraper that aggregates internship positions from multiple sources into Google Sheets. This tool monitors SimplifyJobs GitHub repository and email sources (ZipRecruiter, Jobright, Adzuna, SWE List) to find relevant software engineering internship opportunities.

## Features

- 🔍 **Multi-Source Scraping**: Fetches jobs from SimplifyJobs GitHub and multiple email platforms
- 🤖 **Intelligent Processing**: Uses 4-tier extraction mechanism (Standard HTTP, Rotating User-Agents, Selenium, Email Parsing)
- 📊 **Google Sheets Integration**: Automatically organizes jobs into Valid, Discarded, and Reviewed sheets
- 🔐 **Gmail Integration**: Reads job emails with "Job Hunt" label from last 24 hours
- 🌐 **Location Filtering**: Filters for US-based positions, flags international locations
- 🎯 **Smart Deduplication**: Prevents duplicate entries across all sheets using URL, job ID, and company+title matching
- 💼 **CS Role Filtering**: Focuses on software engineering, developer, and tech internships
- 📝 **Sponsorship Detection**: Identifies H1B sponsorship information when available
- ⚡ **Platform-Specific Parsers**: Custom extractors for Jobright, ZipRecruiter, Adzuna, and more

## Prerequisites

- Python 3.8+
- Google Cloud Project with Sheets API and Gmail API enabled
- Chrome browser (for Selenium)

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/prasad0411/JobsScrapingAutomation.git
   cd JobsScrapingAutomation
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Google Sheets API credentials**
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing one
   - Enable Google Sheets API and Google Drive API
   - Create Service Account credentials
   - Download JSON credentials and save as `credentials.json` in project root
   - Share your Google Sheet with the service account email (found in credentials.json)

4. **Set up Gmail API credentials**
   - In the same Google Cloud project, enable Gmail API
   - Create OAuth 2.0 credentials (Desktop app)
   - Download and save as `gmail_credentials.json`
   - First run will open browser for authentication, creates `gmail_token.pickle`

5. **Configure Google Sheet**
   - Create a Google Sheet named "H1B visa"
   - Create three worksheets:
     - "Valid Entries" (13 columns)
     - "Discarded Entries" (13 columns)
     - "Reviewed - Not Applied" (12 columns)
   - Headers will be auto-created on first run

6. **Label emails in Gmail**
   - Create a label "Job Hunt" in Gmail
   - Apply this label to job notification emails from:
     - ZipRecruiter
     - Jobright
     - Adzuna
     - SWE List
     - Other job sources

## Usage

Run the scraper:
```bash
python job_scraper_unified.py
```

The script will:
1. Load existing jobs from all sheets to prevent duplicates
2. Scrape SimplifyJobs GitHub for new postings (< 2 days old)
3. Fetch emails with "Job Hunt" label from last 24 hours
4. Process each job through comprehensive validation
5. Add valid jobs to "Valid Entries" sheet
6. Add filtered jobs to "Discarded Entries" with reasons
7. Generate processing summary with method statistics

## Configuration

### Sheet Names
Edit these constants in the script to match your setup:
```python
SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Valid Entries"
DISCARDED_WORKSHEET = "Discarded Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"
```

### Credential Files
```python
SHEETS_CREDS_FILE = 'credentials.json'
GMAIL_CREDS_FILE = 'gmail_credentials.json'
GMAIL_TOKEN_FILE = 'gmail_token.pickle'
```

### Location Settings
The script filters for US locations and flags:
- Canadian positions (marks with "Location: Canada")
- UK positions
- Other international locations
- Positions requiring security clearance or US citizenship

## Output Structure

### Valid Entries Sheet (13 columns)
| Sr. No. | Status | Company | Title | Date Applied | Job URL | Job ID | Job Type | Location | Remote? | Entry Date | Source | Sponsorship |

### Discarded Entries Sheet (13 columns)
| Sr. No. | Discard Reason | Company | Title | Date Applied | Job URL | Job ID | Job Type | Location | Remote? | Entry Date | Source | Sponsorship |

Common discard reasons:
- Location: Canada/UK/International
- Non-CS role
- PhD required
- Security clearance required
- Low quality data
- Position closed

### Reviewed - Not Applied Sheet (12 columns)
| Sr. No. | Reason | Company | Title | Job URL | Job ID | Job Type | Location | Remote? | Moved Date | Source | Sponsorship |

## Processing Summary

After each run, the script displays:
- ✓ Valid jobs added
- ✗ Jobs discarded (with reasons)
- ⊘ Duplicates skipped
- ⚠ Failures (HTTP/extraction)
- Method breakdown (Standard/Rotating UA/Selenium/Email parsing)

## Key Features Explained

### 4-Tier Extraction Mechanism
1. **Standard Request**: Basic HTTP request with user-agent
2. **Rotating User-Agents**: Tries 3 different user-agents
3. **Selenium**: Headless Chrome for JavaScript-heavy sites (ZipRecruiter)
4. **Email Parsing**: Extracts job data directly from email HTML

### Platform-Specific Parsers
- **Jobright**: Extracts company, title, location, and H1B sponsorship badges
- **ZipRecruiter**: Parses plain text format with company•location structure
- **Adzuna**: Handles ad-based job links
- **Generic**: Fallback parser for unknown sources

### Deduplication Strategy
Jobs are considered duplicates if ANY of these match:
- Exact URL (cleaned, parameters removed)
- Same company + title (normalized)
- Same Job ID

Special handling for variants:
- Different job IDs = keep both (different positions)
- Different companies = keep both
- Different titles = keep both

### Quality Scoring (out of 7 points)
- Company present (not "Unknown"): +2
- Location present: +2
- Job ID available: +1
- Title length 15-120 chars: +1
- Sponsorship info known: +1
- Minimum score of 3 required for valid entry

## Troubleshooting

### Gmail Authentication Issues
- Delete `gmail_token.pickle` and re-authenticate
- Ensure Gmail API is enabled in Google Cloud Console
- Check OAuth consent screen is configured

### Selenium Issues
- Install ChromeDriver: `pip install webdriver-manager`
- Ensure Chrome browser is installed
- Check Chrome version compatibility

### Sheet Access Errors
- Verify service account email is added as editor to your sheet
- Check sheet name matches `SHEET_NAME` constant
- Ensure all required worksheets exist

### No Jobs Found
- Check SimplifyJobs GitHub has recent postings
- Verify "Job Hunt" label exists in Gmail with recent emails
- Review `job_scraper.log` for detailed processing info

## Logs

The script creates `job_scraper.log` with detailed information:
- Each job URL processed
- Extraction method used
- Validation decisions
- Skip/discard reasons
- API errors

## Limitations

- GitHub scraping limited to jobs posted within 1 day
- Email processing limited to last 24 hours
- Rate limits on HTTP requests (1.5-2.5s delay between requests)
- Selenium slower than standard requests
- Browser storage APIs (localStorage/sessionStorage) not used

## Security Notes

⚠️ **Never commit these files:**
- `credentials.json` (Google Sheets API)
- `gmail_credentials.json` (Gmail API)
- `gmail_token.pickle` (Gmail auth token)
- `*.log` files

These are already in `.gitignore`.
