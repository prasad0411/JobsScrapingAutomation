#!/usr/bin/env python3
# cSpell:disable
"""
Configuration module for job aggregation pipeline.
Contains all constants, credentials paths, and lookup dictionaries.
"""

# Sheet Configuration
SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Valid Entries"
DISCARDED_WORKSHEET = "Discarded Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"

# Credentials
SHEETS_CREDS_FILE = "credentials.json"
GMAIL_CREDS_FILE = "gmail_credentials.json"
GMAIL_TOKEN_FILE = "gmail_token.pickle"
JOBRIGHT_COOKIES_FILE = "jobright_cookies.json"
HANDSHAKE_COOKIES_FILE = "handshake_cookies.json"  # NEW

# External Sources
SIMPLIFY_URL = "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/master/README.md"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# Handshake Configuration (NEW)
HANDSHAKE_CONFIG = {
    # Your pre-configured search URL (paste your filter URL here)
    'search_url': 'https://app.joinhandshake.com/job-search/10586679?employmentTypes=1&employmentTypes=2&jobType=3&remoteWork=onsite&remoteWork=remote&remoteWork=hybrid',
    
    # Safety limits
    'max_jobs_per_session': 50,        # Conservative limit
    'max_sessions_per_day': 1,          # Once daily only
    'scrape_only_weekdays': True,       # No weekends
    'scrape_hours': (8, 20),            # 8 AM - 8 PM only
    
    # Human-like behavior timing
    'delay_between_jobs': (5, 15),      # Random 5-15 seconds
    'scroll_delay': (1, 3),             # Random 1-3 seconds
    'read_time_per_job': (45, 90),      # Random 45-90 seconds on each job page
    'blank_click_probability': 0.3,     # 30% chance to click blank area
    'scroll_probability': 0.7,          # 70% chance to scroll
    
    # Extraction settings
    'extract_full_description': True,   # Get complete job description
    'extract_work_authorization': True, # Get H1B sponsorship info
    'extract_deadline': True,           # Get application deadline
    'posted_within_hours': 24,          # Only jobs from last 24 hours
    
    # Error handling
    'stop_on_captcha': True,           # Immediately stop if CAPTCHA
    'retry_on_failure': False,         # Don't retry (too risky)
    'fail_silently': True,             # Continue with other sources
}

# User Agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# US States mapping
US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}

# Canadian Provinces
CANADA_PROVINCES = {"ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE", "YT", "NT", "NU"}

# City to State mapping
CITY_TO_STATE = {
    "new york": "NY", "brooklyn": "NY", "manhattan": "NY", "queens": "NY", "bronx": "NY",
    "los angeles": "CA", "san francisco": "CA", "san diego": "CA", "san jose": "CA",
    "palo alto": "CA", "mountain view": "CA", "sunnyvale": "CA", "santa clara": "CA",
    "cupertino": "CA", "menlo park": "CA", "redwood city": "CA", "irvine": "CA",
    "santa monica": "CA", "pasadena": "CA", "berkeley": "CA", "oakland": "CA",
    "sacramento": "CA", "fresno": "CA", "long beach": "CA", "anaheim": "CA",
    "cerritos": "CA", "san mateo": "CA", "fremont": "CA", "san carlos": "CA",
    "seattle": "WA", "bellevue": "WA", "redmond": "WA", "tacoma": "WA", "spokane": "WA",
    "boston": "MA", "cambridge": "MA", "somerville": "MA", "worcester": "MA",
    "chicago": "IL", "naperville": "IL", "aurora": "IL", "rockford": "IL",
    "houston": "TX", "dallas": "TX", "austin": "TX", "san antonio": "TX",
    "phoenix": "AZ", "tucson": "AZ", "mesa": "AZ", "chandler": "AZ",
    "philadelphia": "PA", "pittsburgh": "PA", "denver": "CO", "atlanta": "GA",
    "miami": "FL", "detroit": "MI", "minneapolis": "MN", "portland": "OR",
    "las vegas": "NV", "baltimore": "MD", "milwaukee": "WI", "nashville": "TN",
}

# Canadian Cities
CANADA_CITIES = {
    "toronto": "ON", "markham": "ON", "ottawa": "ON", "mississauga": "ON",
    "montreal": "QC", "quebec city": "QC", "quebec": "QC",
    "vancouver": "BC", "victoria": "BC", "burnaby": "BC",
    "calgary": "AB", "edmonton": "AB",
    "winnipeg": "MB", "regina": "SK", "halifax": "NS",
}

# Job Board Domains
JOB_BOARD_DOMAINS = [
    "greenhouse", "lever.co", "workday", "ashbyhq", "smartrecruiters",
    "icims.com", "myworkdayjobs", "jobs.lever.co", "boards.greenhouse.io",
    "simplify.jobs", "linkedin.com/jobs", "indeed.com", "glassdoor.com",
    "angellist.com", "wellfound.com", "monster.com", "dice.com", "builtin.com",
    "ycombinator.com/jobs", "stackoverflow.com/jobs", "jobs.github.com",
    "careers.", "apply.workable.com", "breezy.hr", "recruiting.",
    "talentify", "workable", "jobvite", "ultipro", "paylocity",
    "paycomonline", "bamboohr", "fountain.com",
    "ziprecruiter", "ziprecruiter.com", "adzuna", "adzuna.com",
    "jobright", "jobright.ai", "fursah", "fursah.com",
    "joinhandshake.com", "handshake",  # NEW
]

# Company Name Mappings
SPECIAL_COMPANY_NAMES = {
    "stanfordhealthcare": "Stanford Health Care",
    "bmo": "BMO",
    "jpmorgan": "JPMorgan",
    "figma": "Figma",
    "ibm": "IBM",
    "simplify": "Simplify Jobs",
}

# Thresholds
MIN_QUALITY_SCORE = 3
MAX_JOB_AGE_DAYS = 3

# Sheet Colors
STATUS_COLORS = {
    "Not Applied": {"red": 0.6, "green": 0.76, "blue": 1.0},
    "Applied": {"red": 0.58, "green": 0.93, "blue": 0.31},
    "Rejected": {"red": 0.97, "green": 0.42, "blue": 0.42},
    "OA Round 1": {"red": 1.0, "green": 0.95, "blue": 0.4},
    "OA Round 2": {"red": 1.0, "green": 0.95, "blue": 0.4},
    "Interview 1": {"red": 0.82, "green": 0.93, "blue": 0.94},
    "Offer accepted": {"red": 0.16, "green": 0.65, "blue": 0.27},
    "Assessment": {"red": 0.89, "green": 0.89, "blue": 0.89},
}
