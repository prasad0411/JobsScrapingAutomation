#!/usr/bin/env python3
"""
Configuration module - PRODUCTION v5.0 FINAL
Expanded city mappings (150+ cities), all settings optimized
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

# External Sources
SIMPLIFY_URL = "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/master/README.md"
VANSHB03_URL = (
    "https://raw.githubusercontent.com/vanshb03/Summer2026-Internships/main/README.md"
)
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# User Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# US States
US_STATES = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}

# Canadian Provinces
CANADA_PROVINCES = {
    "ON",
    "QC",
    "BC",
    "AB",
    "MB",
    "SK",
    "NS",
    "NB",
    "NL",
    "PE",
    "YT",
    "NT",
    "NU",
}

# ✅ EXPANDED: 150+ Tech Hub Cities (Production v5.0)
CITY_TO_STATE = {
    # California (Tech Hubs)
    "san francisco": "CA",
    "san jose": "CA",
    "palo alto": "CA",
    "mountain view": "CA",
    "sunnyvale": "CA",
    "santa clara": "CA",
    "cupertino": "CA",
    "menlo park": "CA",
    "redwood city": "CA",
    "foster city": "CA",
    "san mateo": "CA",
    "fremont": "CA",
    "san carlos": "CA",
    "los angeles": "CA",
    "san diego": "CA",
    "irvine": "CA",
    "santa monica": "CA",
    "pasadena": "CA",
    "berkeley": "CA",
    "oakland": "CA",
    "sacramento": "CA",
    "fresno": "CA",
    "long beach": "CA",
    "anaheim": "CA",
    "cerritos": "CA",
    "san bruno": "CA",
    "burbank": "CA",
    "santa barbara": "CA",
    # New York
    "new york": "NY",
    "brooklyn": "NY",
    "manhattan": "NY",
    "queens": "NY",
    "bronx": "NY",
    "staten island": "NY",
    "albany": "NY",
    "buffalo": "NY",
    "rochester": "NY",
    "syracuse": "NY",
    "yonkers": "NY",
    "white plains": "NY",
    # Washington
    "seattle": "WA",
    "bellevue": "WA",
    "redmond": "WA",
    "tacoma": "WA",
    "spokane": "WA",
    "pullman": "WA",
    "olympia": "WA",
    "vancouver": "WA",
    # Massachusetts
    "boston": "MA",
    "cambridge": "MA",
    "somerville": "MA",
    "worcester": "MA",
    "natick": "MA",
    "framingham": "MA",
    "quincy": "MA",
    "waltham": "MA",
    "newton": "MA",
    "brookline": "MA",
    # Texas
    "houston": "TX",
    "dallas": "TX",
    "austin": "TX",
    "san antonio": "TX",
    "fort worth": "TX",
    "el paso": "TX",
    "arlington": "TX",
    "plano": "TX",
    "irving": "TX",
    "richardson": "TX",
    # Illinois
    "chicago": "IL",
    "naperville": "IL",
    "aurora": "IL",
    "rockford": "IL",
    "des plaines": "IL",
    "la grange park": "IL",
    "schaumburg": "IL",
    # Arizona
    "phoenix": "AZ",
    "tucson": "AZ",
    "mesa": "AZ",
    "chandler": "AZ",
    "scottsdale": "AZ",
    "tempe": "AZ",
    "gilbert": "AZ",
    # Pennsylvania
    "philadelphia": "PA",
    "pittsburgh": "PA",
    "allentown": "PA",
    # Colorado
    "denver": "CO",
    "colorado springs": "CO",
    "boulder": "CO",
    "aurora": "CO",
    # Georgia
    "atlanta": "GA",
    "augusta": "GA",
    "columbus": "GA",
    "savannah": "GA",
    "alpharetta": "GA",
    # Florida
    "miami": "FL",
    "orlando": "FL",
    "tampa": "FL",
    "jacksonville": "FL",
    "fort lauderdale": "FL",
    "tallahassee": "FL",
    "st petersburg": "FL",
    "sarasota": "FL",
    # Michigan
    "detroit": "MI",
    "grand rapids": "MI",
    "warren": "MI",
    "ann arbor": "MI",
    "dearborn": "MI",
    # Minnesota
    "minneapolis": "MN",
    "st paul": "MN",
    "rochester": "MN",
    "bloomington": "MN",
    "shakopee": "MN",
    "chaska": "MN",
    # Oregon
    "portland": "OR",
    "salem": "OR",
    "eugene": "OR",
    "hillsboro": "OR",
    "beaverton": "OR",
    # Nevada
    "las vegas": "NV",
    "reno": "NV",
    "henderson": "NV",
    # Maryland
    "baltimore": "MD",
    "frederick": "MD",
    "rockville": "MD",
    "gaithersburg": "MD",
    "germantown": "MD",
    "annapolis": "MD",
    "silver spring": "MD",
    "hanover": "MD",
    # Maine
    "westbrook": "ME",
    "portland": "ME",
    "lewiston": "ME",
    # Wisconsin
    "milwaukee": "WI",
    "madison": "WI",
    "green bay": "WI",
    # Tennessee
    "nashville": "TN",
    "memphis": "TN",
    "knoxville": "TN",
    # Indiana
    "indianapolis": "IN",
    "fort wayne": "IN",
    "evansville": "IN",
    # Ohio
    "columbus": "OH",
    "cleveland": "OH",
    "cincinnati": "OH",
    "toledo": "OH",
    # North Carolina
    "charlotte": "NC",
    "raleigh": "NC",
    "cary": "NC",
    "durham": "NC",
    "greensboro": "NC",
    "chapel hill": "NC",
    "wilmington": "NC",
    # Utah
    "salt lake city": "UT",
    "provo": "UT",
    "west valley city": "UT",
    # Oklahoma
    "oklahoma city": "OK",
    "tulsa": "OK",
    "norman": "OK",
    # Kentucky
    "louisville": "KY",
    "lexington": "KY",
    # Missouri
    "kansas city": "MO",
    "st louis": "MO",
    "springfield": "MO",
    # Nebraska
    "omaha": "NE",
    "lincoln": "NE",
    # New Mexico
    "albuquerque": "NM",
    "santa fe": "NM",
    # Idaho
    "boise": "ID",
    "meridian": "ID",
    # Iowa
    "des moines": "IA",
    "cedar rapids": "IA",
    # Arkansas
    "little rock": "AR",
    "fayetteville": "AR",
    # Rhode Island
    "providence": "RI",
    "warwick": "RI",
    # Connecticut
    "bridgeport": "CT",
    "new haven": "CT",
    "stamford": "CT",
    "hartford": "CT",
    # New Jersey
    "newark": "NJ",
    "jersey city": "NJ",
    "princeton": "NJ",
    "hoboken": "NJ",
    # Virginia
    "richmond": "VA",
    "virginia beach": "VA",
    "norfolk": "VA",
    "chesapeake": "VA",
    "arlington": "VA",
    "alexandria": "VA",
    "mclean": "VA",
    "reston": "VA",
    # South Carolina
    "charleston": "SC",
    "columbia": "SC",
    "greenville": "SC",
    # Alabama
    "birmingham": "AL",
    "montgomery": "AL",
    "huntsville": "AL",
    # Louisiana
    "new orleans": "LA",
    "baton rouge": "LA",
    "shreveport": "LA",
    # Mississippi
    "jackson": "MS",
    "gulfport": "MS",
    # Hawaii
    "honolulu": "HI",
    "pearl city": "HI",
    # Alaska
    "anchorage": "AK",
    "fairbanks": "AK",
    # New Hampshire
    "manchester": "NH",
    "nashua": "NH",
    "concord": "NH",
    # Vermont
    "burlington": "VT",
    "essex": "VT",
    # South Dakota
    "sioux falls": "SD",
    "rapid city": "SD",
    # North Dakota
    "fargo": "ND",
    "bismarck": "ND",
    # Montana
    "billings": "MT",
    "missoula": "MT",
    "bozeman": "MT",
    "helena": "MT",
    # Wyoming
    "cheyenne": "WY",
    "casper": "WY",
    # Delaware
    "wilmington": "DE",
    "dover": "DE",
}

# Canadian Cities
CANADA_CITIES = {
    "toronto": "ON",
    "markham": "ON",
    "ottawa": "ON",
    "mississauga": "ON",
    "montreal": "QC",
    "quebec city": "QC",
    "quebec": "QC",
    "vancouver": "BC",
    "victoria": "BC",
    "burnaby": "BC",
    "calgary": "AB",
    "edmonton": "AB",
    "winnipeg": "MB",
    "regina": "SK",
    "halifax": "NS",
}

# Job Board Domains
JOB_BOARD_DOMAINS = [
    "greenhouse",
    "lever.co",
    "workday",
    "ashbyhq",
    "smartrecruiters",
    "icims.com",
    "myworkdayjobs",
    "jobs.lever.co",
    "boards.greenhouse.io",
    "simplify.jobs",
    "linkedin.com/jobs",
    "indeed.com",
    "glassdoor.com",
    "angellist.com",
    "wellfound.com",
    "monster.com",
    "dice.com",
    "builtin.com",
    "ycombinator.com/jobs",
    "stackoverflow.com/jobs",
    "jobs.github.com",
    "careers.",
    "apply.workable.com",
    "breezy.hr",
    "recruiting.",
    "talentify",
    "workable",
    "jobvite",
    "ultipro",
    "paylocity",
    "paycomonline",
    "bamboohr",
    "fountain.com",
    "ziprecruiter",
    "ziprecruiter.com",
    "adzuna",
    "adzuna.com",
    "jobright",
    "jobright.ai",
    "fursah",
    "fursah.com",
]

# Company Name Mappings (PRODUCTION v5.0)
SPECIAL_COMPANY_NAMES = {
    "stanfordhealthcare": "Stanford Health Care",
    "bmo": "BMO",
    "jpmorgan": "JPMorgan",
    "figma": "Figma",
    "ibm": "IBM",
    "simplify": "Simplify Jobs",
    "lifeattiktok": "TikTok",
    "githubinc": "GitHub",
    "ucar": "UCAR",
    "ncar": "NCAR",
    "seagatecareers": "Seagate",
    "adzuna": "Multiple Companies",
    "easyapply": "SAP SuccessFactors",
    "joinbytedance": "ByteDance",
    "bytedance": "ByteDance",
    "mathworks": "MathWorks",
    "idexx": "IDEXX",
    "careersidexx": "IDEXX",
    "sig": "Susquehanna International Group",
    "careers": "Susquehanna International Group",
    "linkedin": "LinkedIn",
    "lever": "Lever",
    "ninthwave": "Ninth Wave",
    "ninth": "Ninth Wave",
    "nimblerx": "NimbleRx",
    "nimble": "NimbleRx",
    "nuro": "Nuro",
    "abb": "ABB",
    "oracle": "Oracle",
    "singlestore": "SingleStore",
    "myworkdayjobs": "IDEXX",
    "selinc": "Schweitzer Engineering Laboratories",
    "sel": "Schweitzer Engineering Laboratories",
    "velera": "Velera",
    "gilead": "Gilead Sciences",
    "rtx": "RTX",
    "globalhr": "RTX",
}

# Quality Scoring
MIN_QUALITY_SCORE = 3
MAX_JOB_AGE_DAYS = 5

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

HANDSHAKE_COOKIES_FILE = "handshake_cookies.json"

# ✅ Handshake Configuration (Production-ready)
HANDSHAKE_CONFIG = {
    "search_url": "https://app.joinhandshake.com/job-search/10575991?employmentTypes=1&jobType=3&pay%5BsalaryType%5D=1&pay%5BpayMinimum%5D=2000&pay%5BpaySchedule%5D=HOURLY_WAGE&remoteWork=onsite&remoteWork=remote&remoteWork=hybrid&majors=135801&per_page=25&page=1",
    "max_jobs_per_session": 50,
    "max_sessions_per_day": 1,
    "scrape_only_weekdays": False,
    "scrape_hours": (8, 20),
    "delay_between_jobs": (8, 18),  # ✅ Increased for human-like behavior
    "scroll_delay": (2, 5),  # ✅ Increased
    "read_time_per_job": (60, 120),  # ✅ Increased to 1-2 min
    "blank_click_probability": 0.3,
    "scroll_probability": 0.7,
    "extract_full_description": True,
    "extract_work_authorization": True,
    "extract_deadline": True,
    "posted_within_hours": 24,
    "stop_on_captcha": True,
    "retry_on_failure": False,
    "fail_silently": True,
}
