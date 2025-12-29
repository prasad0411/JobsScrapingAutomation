# JobsScrapingAutomation

Automated job scraper that aggregates internship positions from multiple sources into Google Sheets. This tool monitors the SimplifyJobs GitHub repository and email sources (ZipRecruiter, Jobright, Adzuna, SWE List) to find relevant software engineering internship opportunities.

## Features

* Multi-source scraping from SimplifyJobs GitHub and multiple email platforms
* Intelligent processing with a four-tier extraction mechanism (Standard HTTP, Rotating User-Agents, Selenium, Email Parsing)
* Google Sheets integration with automatic organization into Valid, Discarded, and Reviewed sheets
* Gmail integration that reads job emails with the "Job Hunt" label from the last 24 hours
* Location filtering for US-based positions with international roles flagged
* Smart deduplication across all sheets using URL, job ID, and company plus title matching
* CS role filtering focused on software engineering, developer, and technical internships
* Sponsorship detection for H1B information when available
* Platform-specific parsers for Jobright, ZipRecruiter, Adzuna, and others

## Prerequisites

* Python 3.8 or higher
* Google Cloud Project with Sheets API and Gmail API enabled
* Chrome browser (required for Selenium)

## Installation

1. Clone the repository

   ```bash
   git clone https://github.com/prasad0411/JobsScrapingAutomation.git
   cd JobsScrapingAutomation
   ```

2. Install dependencies

   ```bash
   pip install -r requirements.txt
   ```

3. Set up Google Sheets API credentials

   * Go to the Google Cloud Console
   * Create a new project or select an existing one
   * Enable Google Sheets API and Google Drive API
   * Create Service Account credentials
   * Download the JSON credentials and save them as `credentials.json` in the project root
   * Share your Google Sheet with the service account email found in `credentials.json`

4. Set up Gmail API credentials

   * In the same Google Cloud project, enable Gmail API
   * Create OAuth 2.0 credentials (Desktop app)
   * Download and save as `gmail_credentials.json`
   * On first run, a browser window will open for authentication and create `gmail_token.pickle`

5. Configure Google Sheet

   * Create a Google Sheet named "H1B visa"
   * Create three worksheets:

     * "Valid Entries" (13 columns)
     * "Discarded Entries" (13 columns)
     * "Reviewed - Not Applied" (12 columns)
   * Headers will be auto-created on the first run

6. Label emails in Gmail

   * Create a label named "Job Hunt" in Gmail
   * Apply this label to job notification emails from:

     * ZipRecruiter
     * Jobright
     * Adzuna
     * SWE List
     * Other job sources

## Usage

Run the scraper:

```bash
python job_scraper_unified.py
```

The script performs the following steps:

1. Loads existing jobs from all sheets to prevent duplicates
2. Scrapes the SimplifyJobs GitHub repository for new postings less than two days old
3. Fetches emails with the "Job Hunt" label from the last 24 hours
4. Processes each job through comprehensive validation
5. Adds valid jobs to the "Valid Entries" sheet
6. Adds filtered jobs to the "Discarded Entries" sheet with reasons
7. Generates a processing summary with method statistics

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

* Canadian positions
* UK positions
* Other international locations
* Positions requiring security clearance or US citizenship

## Output Structure

### Valid Entries Sheet (13 columns)

| Sr. No. | Status | Company | Title | Date Applied | Job URL | Job ID | Job Type | Location | Remote? | Entry Date | Source | Sponsorship |

### Discarded Entries Sheet (13 columns)

| Sr. No. | Discard Reason | Company | Title | Date Applied | Job URL | Job ID | Job Type | Location | Remote? | Entry Date | Source | Sponsorship |

Common discard reasons include:

* Location outside the United States
* Non-CS role
* PhD requirement
* Security clearance requirement
* Low quality data
* Position closed

### Reviewed - Not Applied Sheet (12 columns)

| Sr. No. | Reason | Company | Title | Job URL | Job ID | Job Type | Location | Remote? | Moved Date | Source | Sponsorship |

## Processing Summary

After each run, the script displays:

* Number of valid jobs added
* Number of jobs discarded with reasons
* Number of duplicates skipped
* Number of failures due to HTTP or extraction issues
* Extraction method breakdown (Standard, Rotating User-Agent, Selenium, Email parsing)

## Key Features Explained

### Four-Tier Extraction Mechanism

1. Standard Request: Basic HTTP request with a user-agent
2. Rotating User-Agents: Attempts multiple user-agents
3. Selenium: Headless Chrome for JavaScript-heavy sites such as ZipRecruiter
4. Email Parsing: Extracts job data directly from email HTML

### Platform-Specific Parsers

* Jobright: Extracts company, title, location, and H1B sponsorship badges
* ZipRecruiter: Parses plain text format with company and location structure
* Adzuna: Handles ad-based job links
* Generic: Fallback parser for unknown sources

### Deduplication Strategy

Jobs are considered duplicates if any of the following match:

* Exact cleaned URL
* Same company and title after normalization
* Same job ID

Special handling rules:

* Different job IDs are treated as different positions
* Different companies are treated as different entries
* Different titles are treated as different entries

### Quality Scoring

Quality is scored out of seven points:

* Company present and not marked as unknown: 2 points
* Location present: 2 points
* Job ID available: 1 point
* Title length between 15 and 120 characters: 1 point
* Sponsorship information known: 1 point

A minimum score of three is required for a valid entry.

## Troubleshooting

### Gmail Authentication Issues

* Delete `gmail_token.pickle` and re-authenticate
* Ensure the Gmail API is enabled in the Google Cloud Console
* Verify the OAuth consent screen is configured

### Selenium Issues

* Install ChromeDriver using `pip install webdriver-manager`
* Ensure Chrome browser is installed
* Verify Chrome and ChromeDriver versions are compatible

### Sheet Access Errors

* Confirm the service account email has editor access to the Google Sheet
* Verify the sheet name matches the `SHEET_NAME` constant
* Ensure all required worksheets exist

### No Jobs Found

* Check that SimplifyJobs GitHub has recent postings
* Verify the "Job Hunt" Gmail label exists and has recent emails
* Review `job_scraper.log` for detailed processing information

## Logs

The script generates `job_scraper.log` with detailed information including:

* Job URLs processed
* Extraction methods used
* Validation decisions
* Skip and discard reasons
* API and scraping errors

## Limitations

* GitHub scraping is limited to jobs posted within one day
* Email processing is limited to the last 24 hours
* HTTP requests are rate-limited with delays between requests
* Selenium is slower than standard HTTP requests
* Browser storage APIs such as localStorage and sessionStorage are not used

## Security Notes

Do not commit the following files:

* `credentials.json`
* `gmail_credentials.json`
* `gmail_token.pickle`
* Log files

These files are already included in `.gitignore`.
