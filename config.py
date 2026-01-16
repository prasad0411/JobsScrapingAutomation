#!/usr/bin/env python3

import warnings

warnings.filterwarnings("ignore")  # Suppress all warnings

# ============================================================================
# CRITICAL: Library Availability Detection
# ============================================================================

# LXML Parser Detection
try:
    import lxml.etree

    LXML_AVAILABLE = True
    DEFAULT_PARSER = "lxml"
except ImportError:
    LXML_AVAILABLE = False
    DEFAULT_PARSER = "html.parser"

# HTML5LIB Parser Detection
try:
    import html5lib

    HTML5LIB_AVAILABLE = True
except ImportError:
    HTML5LIB_AVAILABLE = False

# Parser Priority Chain
PARSER_CHAIN = []
if LXML_AVAILABLE:
    PARSER_CHAIN.append("lxml")
if HTML5LIB_AVAILABLE:
    PARSER_CHAIN.append("html5lib")
PARSER_CHAIN.append("html.parser")

# ============================================================================
# Other Library Detection
# ============================================================================

try:
    from uszipcode import SearchEngine

    US_ZIPCODE_AVAILABLE = True
    _search_engine = SearchEngine()
except:
    US_ZIPCODE_AVAILABLE = False
    _search_engine = None

try:
    import pycountry

    PYCOUNTRY_AVAILABLE = True
except:
    PYCOUNTRY_AVAILABLE = False

try:
    import us as us_library

    US_LIBRARY_AVAILABLE = True
except:
    US_LIBRARY_AVAILABLE = False

try:
    import tldextract

    TLDEXTRACT_AVAILABLE = True
except:
    TLDEXTRACT_AVAILABLE = False

try:
    from rapidfuzz import fuzz, process

    RAPIDFUZZ_AVAILABLE = True
except:
    RAPIDFUZZ_AVAILABLE = False

try:
    from dateutil import parser as dateutil_parser

    DATEUTIL_AVAILABLE = True
except:
    DATEUTIL_AVAILABLE = False

try:
    import validators

    VALIDATORS_AVAILABLE = True
except:
    VALIDATORS_AVAILABLE = False

try:
    import pgeocode

    PGEOCODE_AVAILABLE = True
    _pgeocode_nomi = pgeocode.Nominatim("us")
except:
    PGEOCODE_AVAILABLE = False
    _pgeocode_nomi = None

try:
    from unidecode import unidecode as unidecode_func

    UNIDECODE_AVAILABLE = True
except:
    UNIDECODE_AVAILABLE = False

# ============================================================================
# Google Sheets Configuration
# ============================================================================

SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Valid Entries"
DISCARDED_WORKSHEET = "Discarded Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"

SHEETS_CREDS_FILE = "credentials.json"
GMAIL_CREDS_FILE = "gmail_credentials.json"
GMAIL_TOKEN_FILE = "gmail_token.pickle"
JOBRIGHT_COOKIES_FILE = "jobright_cookies.json"

# ============================================================================
# Data Source URLs
# ============================================================================

SIMPLIFY_URL = "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/master/README.md"
VANSHB03_URL = (
    "https://raw.githubusercontent.com/vanshb03/Summer2026-Internships/main/README.md"
)

# ============================================================================
# Filtering & Quality Configuration
# ============================================================================

MAX_JOB_AGE_DAYS = 3
MIN_QUALITY_SCORE = 4
MIN_CONFIDENCE_JOB_ID = 0.70
MIN_CONFIDENCE_LOCATION = 0.70
MIN_CONFIDENCE_COMPANY = 0.70
REQUIRE_MULTIPLE_CONFIRMATIONS = True

# Retry Configuration
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2
BACKOFF_MULTIPLIER = 2

# ============================================================================
# US States & Cities - EXPANDED
# ============================================================================

US_STATES_FALLBACK = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}

CITY_TO_STATE_FALLBACK = {
    # California
    "san francisco": "CA",
    "san jose": "CA",
    "palo alto": "CA",
    "mountain view": "CA",
    "sunnyvale": "CA",
    "santa clara": "CA",
    "cupertino": "CA",
    "santa monica": "CA",
    "south san francisco": "CA",
    "foster city": "CA",
    "fremont": "CA",
    "milpitas": "CA",
    "los angeles": "CA",
    "san diego": "CA",
    "sacramento": "CA",
    "oakland": "CA",
    "irvine": "CA",
    "anaheim": "CA",
    # New York
    "new york": "NY",
    "brooklyn": "NY",
    "buffalo": "NY",
    # Washington
    "seattle": "WA",
    "bellevue": "WA",
    "redmond": "WA",
    # Massachusetts
    "boston": "MA",
    "cambridge": "MA",
    "worcester": "MA",
    # Illinois
    "chicago": "IL",
    # Georgia
    "atlanta": "GA",
    # Pennsylvania
    "philadelphia": "PA",
    "pittsburgh": "PA",
    # Colorado
    "denver": "CO",
    "golden": "CO",
    "boulder": "CO",
    # Arizona
    "phoenix": "AZ",
    "tempe": "AZ",
    "scottsdale": "AZ",
    # Florida
    "orlando": "FL",
    "miami": "FL",
    "tampa": "FL",
    # Texas
    "dallas": "TX",
    "austin": "TX",
    "plano": "TX",
    "houston": "TX",
    # North Carolina
    "charlotte": "NC",
    "raleigh": "NC",
    # Maryland
    "rockville": "MD",
    "baltimore": "MD",
    # Minnesota
    "bloomington": "MN",
    "minneapolis": "MN",
    # Utah
    "draper": "UT",
    "salt lake city": "UT",
}

# EXPANDED: Full state name to code mapping
FULL_STATE_NAMES = {
    # US States
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
}

# NEW: City abbreviation to full name
CITY_ABBREVIATIONS = {
    "sf": "San Francisco, CA",
    "nyc": "New York, NY",
    "la": "Los Angeles, CA",
    "dc": "Washington, DC",
    "philly": "Philadelphia, PA",
    "chi": "Chicago, IL",
}

# NEW: Location suffixes to strip
LOCATION_SUFFIXES = [
    " Office",
    " office",
    " Headquarters",
    " headquarters",
    " HQ",
    " hq",
    " Campus",
    " campus",
    " Bay Area",
    " bay area",
    " Metro Area",
    " metro area",
    " Area",
    " area",
]

# ============================================================================
# Canadian Detection - EXPANDED
# ============================================================================

CANADA_PROVINCES = {"ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE"}

CANADA_PROVINCE_NAMES = {
    "ontario": "ON",
    "quebec": "QC",
    "british columbia": "BC",
    "alberta": "AB",
    "manitoba": "MB",
    "saskatchewan": "SK",
    "nova scotia": "NS",
    "new brunswick": "NB",
    "newfoundland": "NL",
    "prince edward island": "PE",
}

# EXPANDED: Major + Medium Canadian cities
MAJOR_CANADIAN_CITIES = {
    # Major cities
    "toronto": "ON",
    "ottawa": "ON",
    "montreal": "QC",
    "vancouver": "BC",
    "calgary": "AB",
    "edmonton": "AB",
    "winnipeg": "MB",
    "quebec city": "QC",
    # Medium cities (100k+)
    "mississauga": "ON",
    "brampton": "ON",
    "hamilton": "ON",
    "kitchener": "ON",
    "london": "ON",
    "markham": "ON",
    "vaughan": "ON",
    "gatineau": "QC",
    "laval": "QC",
    "waterloo": "ON",
    "guelph": "ON",
    "kanata": "ON",  # Ottawa suburb
    "richmond": "BC",
    "burnaby": "BC",
    "surrey": "BC",
}

# NEW: Ambiguous cities that exist in both US and Canada
AMBIGUOUS_CITIES = {
    "vancouver": {"US": "WA", "Canada": "BC"},
    "ontario": {"US": "CA", "Canada": "ON"},
    "cambridge": {"US": "MA", "Canada": "ON", "UK": "England"},
    "london": {"US": "OH", "Canada": "ON", "UK": "England"},
    "waterloo": {"US": "IA", "Canada": "ON"},
    "windsor": {"US": "CT", "Canada": "ON"},
    "richmond": {"US": "VA", "Canada": "BC"},
}

US_CONTEXT_KEYWORDS = ["usa", "united states", "u.s.", "bay area", "silicon valley"]
CANADA_CONTEXT_KEYWORDS = ["canada", "canadian", "gta", "greater toronto"]

# ============================================================================
# Platform Detection & Configuration - ENHANCED
# ============================================================================

PLATFORM_DETECTION_PATTERNS = {
    "workday": r"\.wd\d+\.myworkdayjobs\.com",
    "greenhouse": r"(boards\.|job-boards\.)?greenhouse\.io",
    "lever": r"jobs\.lever\.co",
    "ashby": r"(?:jobs\.)?ashbyhq\.com",
    "linkedin": r"linkedin\.com/jobs",
    "icims": r"\.icims\.com",
    "smartrecruiters": r"(jobs\.)?smartrecruiters\.com",
    "oracle": r"(\.fa\.|oraclecloud\.com)",
    "eightfold": r"\.eightfold\.ai",
    "ea": r"jobs\.ea\.com",
}

PLATFORM_CONFIGS = {
    "workday": {
        "requires_selenium": True,
        "wait_time": 12,  # Increased from 8
        "location_selectors": [
            ('dd[data-automation-id="locations"]', 0.95),
            ('dd[data-automation-id="location"]', 0.93),
            ('[data-automation-id*="location"]', 0.85),
            ('span[data-automation-id="jobLocation"]', 0.92),
            ('div[data-automation-id="jobProperties"] dd', 0.75),
            (".jobProperty .jobPropertyValue", 0.70),
            ("div.css-1ij27gp", 0.80),
        ],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": 'h1[data-automation-id="jobTitle"]',
        "job_id_pattern": r"_([A-Z]R?-?\d{5,})(?:-\d+)?(?:\?|$)",
    },
    "greenhouse": {
        "requires_selenium": False,
        "wait_time": 3,
        "location_selectors": [
            (".location", 0.92),
            (".job-location", 0.88),
            ("div.location", 0.90),
            ("[data-qa='job-location']", 0.88),
            (".app-title + div", 0.75),
            ("h1 + div", 0.70),
            (".posting-headline + div", 0.72),
            ("[class*='location' i]", 0.80),
        ],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": ".app-title",
        "job_id_pattern": r"/jobs?/(\d{7,})",
    },
    "oracle": {
        "requires_selenium": True,
        "wait_time": 15,
        "location_selectors": [
            ('[data-automation="jobLocation"]', 0.95),
            (".jobProperty", 0.80),
        ],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": 'h1[data-automation="jobTitle"]',
        "job_id_pattern": r"/job/(\d{6,})",
    },
    "lever": {
        "requires_selenium": False,
        "wait_time": 3,
        "location_selectors": [
            (".location", 0.92),
            (".posting-categories .location", 0.88),
        ],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": ".posting-headline h2",
        "job_id_pattern": r"(?:jobs\.)?lever\.co/[^/]+/([a-f0-9-]{36})",
    },
    "smartrecruiters": {
        "requires_selenium": False,
        "wait_time": 3,
        "location_selectors": [
            ('[itemprop="jobLocation"]', 0.92),
            (".job-location", 0.85),
        ],
        "company_selector": 'img[alt*="logo"]',
        "title_selector": 'h1[itemprop="title"]',
        "job_id_pattern": r"/(\d{15})",
    },
    "ashby": {
        "requires_selenium": True,  # NEW: Mark as JS-heavy
        "wait_time": 5,
        "location_selectors": [
            ('[class*="JobLocation"]', 0.92),
            ('div[class*="location" i]', 0.88),
        ],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": "h1",
        "job_id_pattern": r"(?:jobs\.)?ashbyhq\.com/[^/]+/([a-f0-9-]{36})",
    },
    "ea": {
        "requires_selenium": False,
        "wait_time": 3,
        "location_selectors": [],  # Use regex on page text
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": "h2",
        "job_id_pattern": r"/(\d{6,})",
    },
}

WORKDAY_HQ_CODES = {
    "USNYNYC": ("New York", "NY"),
    "USCASFO": ("San Francisco", "CA"),
    "USWAEAT": ("Seattle", "WA"),
    "USTXAUS": ("Austin", "TX"),
    "USMABOA": ("Boston", "MA"),
    "USCALA": ("Los Angeles", "CA"),
    "USCASJO": ("San Jose", "CA"),
}

# ============================================================================
# Company Name Mappings
# ============================================================================

URL_TO_COMPANY_MAPPING = {
    r"quickenloans\.wd\d+\.myworkdayjobs\.com": "Rocket Companies",
    r"geico\.wd\d+\.myworkdayjobs\.com": "GEICO",
    r"cox\.wd\d+\.myworkdayjobs\.com": "Cox Automotive",
    r"roche\.wd\d+\.myworkdayjobs\.com": "Roche",
    r"cranecompany\.wd\d+\.myworkdayjobs\.com": "Crane Co.",
    r"motorola.*\.wd\d+\.myworkdayjobs\.com": "Motorola Solutions",
    r"nvidia\.wd\d+\.myworkdayjobs\.com": "NVIDIA",
    r"tmobile\.wd\d+\.myworkdayjobs\.com": "T-Mobile",
    r"att\.wd\d+\.myworkdayjobs\.com": "AT&T Services",
    r"disney\.wd\d+\.myworkdayjobs\.com": "The Walt Disney Company",
    r"pru\.wd\d+\.myworkdayjobs\.com": "Prudential Financial",
    r"coke\.wd\d+\.myworkdayjobs\.com": "The Coca-Cola Company",
    r"lilly\.wd\d+\.myworkdayjobs\.com": "Eli Lilly and Company",
    r"donaldson\.wd\d+\.myworkdayjobs\.com": "Donaldson Company",
    r"intapp\.wd\d+\.myworkdayjobs\.com": "Intapp",
    r"assetmark\.wd\d+\.myworkdayjobs\.com": "AssetMark",
    r"axos\.wd\d+\.myworkdayjobs\.com": "Axos Bank",
    r"nrel\.wd\d+\.myworkdayjobs\.com": "National Renewable Energy Laboratory",
    r"hhmi\.wd\d+\.myworkdayjobs\.com": "Howard Hughes Medical Institute",
    r"nasdaq\.wd\d+\.myworkdayjobs\.com": "Nasdaq",
    r"comcast\.wd\d+\.myworkdayjobs\.com": "Comcast",
    r"sbdinc\.wd\d+\.myworkdayjobs\.com": "Stanley Black & Decker",
    r"abb\.wd\d+\.myworkdayjobs\.com": "ABB",
    r"selinc\.wd\d+\.myworkdayjobs\.com": "Schweitzer Engineering Laboratories",
    r"asml\.wd\d+\.myworkdayjobs\.com": "ASML",
    r"ciena\.wd\d+\.myworkdayjobs\.com": "Ciena",
    r"globalfoundries\.wd\d+\.myworkdayjobs\.com": "GlobalFoundries",
    r"spgi\.wd\d+\.myworkdayjobs\.com": "S&P Global",
    r"cadence\.wd\d+\.myworkdayjobs\.com": "Cadence Design Systems",
    r"finra\.wd\d+\.myworkdayjobs\.com": "Finra",
    r"rb\.wd\d+\.myworkdayjobs\.com": "The Federal Reserve System",
    r"group1001wd\.wd\d+\.myworkdayjobs\.com": "Group 1001",
    r"uline\.wd\d+\.myworkdayjobs\.com": "Uline",
    r"wnc\.wd\d+\.myworkdayjobs\.com": "WNC",
    r"warnerbros\.wd\d+\.myworkdayjobs\.com": "Warner Bros.",
    r"kbr\.wd\d+\.myworkdayjobs\.com": "Wyle Services",
    r"philips\.wd\d+\.myworkdayjobs\.com": "Myworkdayjobs",
    r"job-boards\.greenhouse\.io/asteraearlycareer": "Astera Labs",
    r"job-boards\.greenhouse\.io/samsungresearchamericainternship": "Samsung Research America",
    r"job-boards\.greenhouse\.io/obsidiansecurity": "Obsidian Security",
    r"job-boards\.greenhouse\.io/commvault": "Commvault",
    r"job-boards\.greenhouse\.io/audaxgroup": "Audax Group",
    r"job-boards\.greenhouse\.io/verkada": "Verkada",
    r"job-boards\.greenhouse\.io/auctane": "Auctane",
    r"generatebiomedicines\.com": "Generate Biomedicines",
    r"jobs\.smartrecruiters\.com/Visa": "Visa",
    r"jobs\.smartrecruiters\.com/Intuitive": "Intuitive Surgical",
    r"jobs\.smartrecruiters\.com/Experian": "Experian",
    r"jobs\.smartrecruiters\.com/BoschGroup": "Robert Bosch Venture Capital",
    r"jobs\.lever\.co/zoox": "Zoox",
    r"jobs\.lever\.co/veeva": "Veeva Systems",
    r"jobs\.ashbyhq\.com/uipath": "UiPath",
    r"jobs\.ashbyhq\.com/Ridealso": "ALSO",
    r"jobs\.ashbyhq\.com/atomicsemi": "Atomic Semi",
    r"jobs\.ea\.com": "Electronic Arts",
    r"eeho\.fa\.us2\.oraclecloud\.com": "Oracle",
    r"fa-evmr.*\.oraclecloud\.com": "Nokia",
    r"jobs-legrand\.icims\.com": "Legrand",
    r"jobs\.paccar\.com": "Paccar",
    r"apply\.careers\.microsoft\.com": "Microsoft",
    r"3ds\.com/careers": "Dassault Systèmes",
}

JUNK_SUBDOMAIN_PATTERNS = [
    r".*\d{4,}.*",
    r".*earlycareer.*",
    r".*internship.*",
    r".*careers?$",
    r".*jobs?$",
    r".*wd\d+$",
]

COMPANY_SLUG_MAPPING = {
    "sig": "Susquehanna International Group",
    "spgi": "S&P Global",
    "hhmi": "Howard Hughes Medical Institute",
    "nrel": "National Renewable Energy Laboratory",
    "sbdinc": "Stanley Black & Decker",
    "selinc": "Schweitzer Engineering Laboratories",
    "asml": "ASML",
    "abb": "ABB",
    "geico": "GEICO",
    "asteraearlycareer2026": "Astera Labs",
    "asteraearlycareer": "Astera Labs",
    "samsungresearchamericainternship": "Samsung Research America",
    "obsidiansecurity": "Obsidian Security",
    "group1001wd": "Group 1001",
    "audaxgroup": "Audax Group",
    "ridealso": "ALSO",
    "openai": "OpenAI",
    "tiktok": "TikTok",
    "linkedin": "LinkedIn",
    "paypal": "PayPal",
    "verkada": "Verkada",
    "atomicsemi": "Atomic Semi",
}

COMPANY_PLACEHOLDERS = [
    "Unknown",
    "N/A",
    "Company",
    "Employer",
    "Careers",
    "Jobs",
    "External",
    "Portal",
    "Applicant",
    "Apply",
]

COMPANY_NAME_PREFIXES = [
    "lifeat",
    "joinat",
    "join",
    "careersat",
    "careers",
    "workfor",
    "workat",
    "work",
    "hiringat",
    "hiring",
]

COMPANY_NAME_STOPWORDS = [
    "Careers at ",
    "Careers | ",
    "Work at ",
    "Join ",
    " Careers",
    " Jobs",
    " - Careers",
    " Career Site",
]

# ============================================================================
# Extraction Patterns - FIXED
# ============================================================================

JOB_ID_PATTERNS = [
    (r"/jobs?/(\d{10})", 0.96),
    (r"gh_jid=(\d{7,})", 0.96),
    (r"/jobs?/(\d{6,})", 0.94),  # CHANGED: 6+ instead of 7+
    (r"_([A-Z]R?-?\d{5,})(?:-\d+)?(?:\?|$)", 0.93),
    (r"/([A-Z]{2,3}\d{5,})(?:-\d+)?(?:\?|$)", 0.91),
    (r"(?:jobs\.)?lever\.co/[^/]+/([a-f0-9-]{36})", 0.96),  # FIXED: Added jobs.
    (r"(?:jobs\.)?ashbyhq\.com/[^/]+/([a-f0-9-]{36})", 0.96),  # FIXED: Added jobs.
    (r"smartrecruiters\.com/[^/]+/(\d{15})", 0.96),
    (r"REQ[_-]?(\d{6,})", 0.92),
    (r"job[/_]([A-Z0-9_-]{6,15})(?:\?|$|/)", 0.86),
    (r"[?&]reqId=([A-Z0-9_-]{4,15})(?:&|$)", 0.88),
    (r"/(\d{6,})(?:/|\?|$)", 0.76),  # CHANGED: 6+ instead of 7+
]

LOCATION_SELECTORS = [
    ('[data-qa="location"]', 0.92),
    ('[data-automation-id="locations"]', 0.95),
    ('[data-automation="jobLocation"]', 0.95),
    ('[itemprop="jobLocation"]', 0.92),
    (".location", 0.86),
    (".job-location", 0.86),
    (".posting-categories .location", 0.88),
]

LOCATION_METADATA_PATTERNS = [
    r"time\s+type.*$",
    r"Full\s+time.*$",
    r"Part\s+time.*$",
    r"posted\s+on.*$",
    r"Employment\s+Type.*$",
    r"Details.*$",
    r"Program.*$",
]

HTML_ARTIFACT_PATTERNS = [
    r"^s(?=[A-Z])",
    r"^p(?=[A-Z])",
]

INVALID_LOCATION_KEYWORDS = [
    "time",
    "type",
    "full",
    "part",
    "posted",
    "employment",
]

DEPARTMENT_KEYWORDS = [
    "quantum",
    "performance",
    "analytics",
    "maintenance",
    "wearables",
    "search",
    "external",
    "product",
    "oracle analytics",
]

# ============================================================================
# Role Categories
# ============================================================================

ROLE_CATEGORIES = {
    "Pure Software": {
        "keywords": ["backend", "frontend", "full stack", "web developer"],
        "exclude": ["embedded", "firmware", "hardware"],
        "action": "ACCEPT",
        "alert": "✅ SOFTWARE",
    },
    "Data & AI": {
        "keywords": ["data scien", "machine learning", "ai engineer", "analytics"],
        "exclude": [],
        "action": "ACCEPT",
        "alert": "✅ DATA/AI",
    },
}

# ============================================================================
# Email & Network Configuration
# ============================================================================

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

JOB_BOARD_DOMAINS = [
    "greenhouse",
    "lever.co",
    "workday",
    "ashbyhq",
    "smartrecruiters",
    "icims.com",
    "myworkdayjobs",
    "simplify.jobs",
    "linkedin.com/jobs",
]

STATUS_COLORS = {
    "Not Applied": {"red": 1.0, "green": 1.0, "blue": 1.0},
    "Applied": {"red": 1.0, "green": 0.9, "blue": 0.6},
    "Interview Scheduled": {"red": 1.0, "green": 0.8, "blue": 0.4},
    "Offer Received": {"red": 0.6, "green": 0.9, "blue": 0.6},
}

# ============================================================================
# Helper Functions
# ============================================================================


def get_state_for_city(city_name):
    if US_ZIPCODE_AVAILABLE and _search_engine:
        try:
            results = _search_engine.by_city(city_name.strip().title())
            if results:
                states = [r.state for r in results if r.state]
                if states:
                    from collections import Counter

                    return Counter(states).most_common(1)[0][0]
        except:
            pass
    return CITY_TO_STATE_FALLBACK.get(city_name.lower())


def validate_us_state_code(state_code):
    if US_LIBRARY_AVAILABLE:
        try:
            return us_library.states.lookup(state_code) is not None
        except:
            pass
    return state_code.upper() in US_STATES_FALLBACK


def get_canadian_province(text):
    if PYCOUNTRY_AVAILABLE:
        try:
            for subdivision in pycountry.subdivisions.get(country_code="CA"):
                code = subdivision.code.split("-")[1]
                if code in text.upper() or subdivision.name.lower() in text.lower():
                    return code
        except:
            pass
    for province_name, code in CANADA_PROVINCE_NAMES.items():
        if province_name in text.lower():
            return code
    return None


def extract_domain_and_subdomain(url):
    if TLDEXTRACT_AVAILABLE:
        try:
            extracted = tldextract.extract(url)
            return extracted.subdomain, f"{extracted.domain}.{extracted.suffix}"
        except:
            pass
    import re

    match = re.search(r"https?://(?:www\.)?([^/]+)", url)
    if match:
        parts = match.group(1).split(".")
        return (parts[0], ".".join(parts[-2:])) if len(parts) >= 2 else (None, None)
    return None, None


def fuzzy_match_company(candidate, known_companies, threshold=85):
    if not RAPIDFUZZ_AVAILABLE:
        return None
    try:
        result = process.extractOne(candidate, known_companies, scorer=fuzz.ratio)
        return result[0] if result and result[1] >= threshold else None
    except:
        return None


def parse_date_flexible(date_string):
    if not DATEUTIL_AVAILABLE:
        return None
    try:
        return dateutil_parser.parse(date_string, fuzzy=True)
    except:
        return None


def is_valid_url(url):
    if VALIDATORS_AVAILABLE:
        try:
            return validators.url(url) == True
        except:
            pass
    import re

    return bool(re.match(r"https?://.+\..+", url))


def normalize_unicode(text):
    if UNIDECODE_AVAILABLE:
        try:
            return unidecode_func(text)
        except:
            pass
    return text.replace("é", "e").replace("è", "e").replace("à", "a").replace("ô", "o")


def get_city_state_from_zipcode(zipcode):
    if PGEOCODE_AVAILABLE and _pgeocode_nomi:
        try:
            result = _pgeocode_nomi.query_postal_code(zipcode)
            if result is not None and not result.isna().all():
                return result.get("place_name"), result.get("state_code")
        except:
            pass
    return None, None
