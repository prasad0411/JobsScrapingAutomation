#!/usr/bin/env python3

# ============================================================================
# Suppress All Warnings for Clean Output
# ============================================================================
import warnings
import os

warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

# ============================================================================
# Library Availability Detection (Silent)
# ============================================================================

try:
    import lxml.etree

    LXML_AVAILABLE = True
    DEFAULT_PARSER = "lxml"
except ImportError:
    LXML_AVAILABLE = False
    DEFAULT_PARSER = "html.parser"

try:
    import html5lib

    HTML5LIB_AVAILABLE = True
except ImportError:
    HTML5LIB_AVAILABLE = False

PARSER_CHAIN = []
if LXML_AVAILABLE:
    PARSER_CHAIN.append("lxml")
if HTML5LIB_AVAILABLE:
    PARSER_CHAIN.append("html5lib")
PARSER_CHAIN.append("html.parser")

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

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2
BACKOFF_MULTIPLIER = 2

# ============================================================================
# US States - Complete Mapping
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

FULL_STATE_NAMES = {
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

CITY_TO_STATE_FALLBACK = {
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
    "redwood city": "CA",
    "menlo park": "CA",
    "berkeley": "CA",
    "new york": "NY",
    "brooklyn": "NY",
    "buffalo": "NY",
    "seattle": "WA",
    "bellevue": "WA",
    "redmond": "WA",
    "boston": "MA",
    "cambridge": "MA",
    "worcester": "MA",
    "chicago": "IL",
    "atlanta": "GA",
    "philadelphia": "PA",
    "pittsburgh": "PA",
    "denver": "CO",
    "golden": "CO",
    "boulder": "CO",
    "phoenix": "AZ",
    "tempe": "AZ",
    "scottsdale": "AZ",
    "orlando": "FL",
    "miami": "FL",
    "tampa": "FL",
    "dallas": "TX",
    "austin": "TX",
    "plano": "TX",
    "houston": "TX",
    "charlotte": "NC",
    "raleigh": "NC",
    "rockville": "MD",
    "baltimore": "MD",
    "bloomington": "MN",
    "draper": "UT",
    "salt lake city": "UT",
    "sioux falls": "SD",
    "towson": "MD",
    "pleasant prairie": "WI",
    "milwaukee": "WI",
}

CITY_ABBREVIATIONS = {
    "sf": "San Francisco, CA",
    "nyc": "New York, NY",
    "la": "Los Angeles, CA",
    "dc": "Washington, DC",
    "philly": "Philadelphia, PA",
}

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
]

# ============================================================================
# Canadian Detection - Comprehensive
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
    "newfoundland and labrador": "NL",
    "newfoundland": "NL",
    "prince edward island": "PE",
}

MAJOR_CANADIAN_CITIES = {
    "toronto": "ON",
    "ottawa": "ON",
    "mississauga": "ON",
    "brampton": "ON",
    "hamilton": "ON",
    "london": "ON",
    "markham": "ON",
    "vaughan": "ON",
    "kitchener": "ON",
    "windsor": "ON",
    "guelph": "ON",
    "kanata": "ON",
    "waterloo": "ON",
    "burlington": "ON",
    "oakville": "ON",
    "montreal": "QC",
    "quebec city": "QC",
    "laval": "QC",
    "gatineau": "QC",
    "vancouver": "BC",
    "surrey": "BC",
    "burnaby": "BC",
    "richmond": "BC",
    "victoria": "BC",
    "calgary": "AB",
    "edmonton": "AB",
    "winnipeg": "MB",
    "saskatoon": "SK",
    "regina": "SK",
    "halifax": "NS",
}

AMBIGUOUS_CITIES = {
    "vancouver": {"US": "WA", "Canada": "BC"},
    "ontario": {"US": "CA", "Canada": "ON"},
    "cambridge": {"US": "MA", "Canada": "ON"},
    "london": {"US": "OH", "Canada": "ON"},
    "waterloo": {"US": "IA", "Canada": "ON"},
    "windsor": {"US": "CT", "Canada": "ON"},
    "richmond": {"US": "VA", "Canada": "BC"},
}

US_CONTEXT_KEYWORDS = ["usa", "united states", "u.s.", "bay area", "silicon valley"]
CANADA_CONTEXT_KEYWORDS = ["canada", "canadian", "gta", "greater toronto"]

# ============================================================================
# Platform Detection & Configuration
# ============================================================================

PLATFORM_DETECTION_PATTERNS = {
    "workday": r"\.wd\d+\.myworkdayjobs\.com",
    "greenhouse": r"(boards\.|job-boards\.)?greenhouse\.io",
    "lever": r"(?:jobs\.)?lever\.co",
    "ashby": r"(?:jobs\.)?ashbyhq\.com",
    "linkedin": r"linkedin\.com/jobs",
    "icims": r"\.icims\.com",
    "smartrecruiters": r"(jobs\.)?smartrecruiters\.com",
    "oracle": r"(\.fa\.|oraclecloud\.com)",
    "eightfold": r"\.eightfold\.ai",
    "ea": r"jobs\.ea\.com",
    "glassdoor": r"glassdoor\.com",
    "workatastartup": r"workatastartup\.com",
}

PLATFORM_CONFIGS = {
    "workday": {
        "requires_selenium": True,
        "wait_time": 15,
        "location_selectors": [
            ('dd[data-automation-id="locations"]', 0.95),
            ('dd[data-automation-id="location"]', 0.93),
            ('span[data-automation-id="jobLocation"]', 0.92),
            ('[data-automation-id*="location"]', 0.85),
            ('div[data-automation-id="jobProperties"] dd', 0.78),
            ('[aria-label*="location"]', 0.80),
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
            ("div.location", 0.90),
            (".job-location", 0.88),
            ("[data-qa='job-location']", 0.88),
            (".app-title + div", 0.75),
            ("h1 + div", 0.72),
        ],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": ".app-title",
        "job_id_pattern": r"/jobs?/(\d{7,})",
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
        "job_id_pattern": r"/(\d{12,})",
    },
    "ashby": {
        "requires_selenium": True,
        "wait_time": 6,
        "location_selectors": [
            ('[class*="JobLocation"]', 0.92),
            ('div[class*="location"]', 0.88),
        ],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": "h1",
        "job_id_pattern": r"(?:jobs\.)?ashbyhq\.com/[^/]+/([a-f0-9-]{36})",
    },
    "ea": {
        "requires_selenium": False,
        "wait_time": 3,
        "location_selectors": [],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": "h2",
        "job_id_pattern": r"/(\d{6,})",
    },
    "glassdoor": {
        "requires_selenium": False,
        "wait_time": 3,
        "location_selectors": [
            ('[data-test="location"]', 0.92),
            (".location", 0.85),
        ],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": "h1",
        "job_id_pattern": None,
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
# Company Mappings
# ============================================================================

URL_TO_COMPANY_MAPPING = {
    r"quickenloans\.wd\d+\.myworkdayjobs\.com": "Rocket Companies",
    r"geico\.wd\d+\.myworkdayjobs\.com": "GEICO",
    r"cox\.wd\d+\.myworkdayjobs\.com": "Cox Automotive",
    r"roche\.wd\d+\.myworkdayjobs\.com": "Roche",
    r"nvidia\.wd\d+\.myworkdayjobs\.com": "NVIDIA",
    r"tmobile\.wd\d+\.myworkdayjobs\.com": "T-Mobile",
    r"att\.wd\d+\.myworkdayjobs\.com": "AT&T Services",
    r"disney\.wd\d+\.myworkdayjobs\.com": "The Walt Disney Company",
    r"pru\.wd\d+\.myworkdayjobs\.com": "Prudential Financial",
    r"coke\.wd\d+\.myworkdayjobs\.com": "The Coca-Cola Company",
    r"lilly\.wd\d+\.myworkdayjobs\.com": "Eli Lilly and Company",
    r"sbdinc\.wd\d+\.myworkdayjobs\.com": "Stanley Black & Decker",
    r"abb\.wd\d+\.myworkdayjobs\.com": "ABB",
    r"asml\.wd\d+\.myworkdayjobs\.com": "ASML",
    r"uline\.wd\d+\.myworkdayjobs\.com": "Uline",
    r"warnerbros\.wd\d+\.myworkdayjobs\.com": "Warner Bros.",
    r"kbr\.wd\d+\.myworkdayjobs\.com": "KBR",
    r"philips\.wd\d+\.myworkdayjobs\.com": "Philips",
    r"jci\.wd\d+\.myworkdayjobs\.com": "Johnson Controls",
    r"job-boards\.greenhouse\.io/verkada": "Verkada",
    r"job-boards\.greenhouse\.io/samsungresearchamericainternship": "Samsung Research America",
    r"job-boards\.greenhouse\.io/obsidiansecurity": "Obsidian Security",
    r"job-boards\.greenhouse\.io/auctane": "Auctane",
    r"(?:jobs\.)?lever\.co/zoox": "Zoox",
    r"(?:jobs\.)?ashbyhq\.com/atomicsemi": "Atomic Semi",
    r"jobs\.ea\.com": "Electronic Arts",
    r"jobs\.smartrecruiters\.com/Visa": "Visa",
    r"careers\.adobe\.com": "Adobe",
    r"jobs\.siemens\.com": "Siemens",
    r"ats\.rippling\.com/.*/redaspen": "Red Aspen",
    r"careers\.merzaesthetics\.com": "Merz North America",
    r"jolera\.com": "Jolera",
    r"workatastartup\.com": "Y Combinator Startup",
}

COMPANY_SLUG_MAPPING = {
    "sbdinc": "Stanley Black & Decker",
    "verkada": "Verkada",
    "atomicsemi": "Atomic Semi",
    "geico": "GEICO",
}

COMPANY_PLACEHOLDERS = ["Unknown", "N/A", "Company", "Employer", "Careers", "Jobs"]
COMPANY_NAME_PREFIXES = ["lifeat", "joinat", "careersat", "workat"]
COMPANY_NAME_STOPWORDS = ["Careers at ", "Work at ", " Careers", " Jobs"]

JUNK_SUBDOMAIN_PATTERNS = [r".*\d{4,}.*", r".*careers?$", r".*jobs?$"]

# ============================================================================
# Extraction Patterns
# ============================================================================

JOB_ID_PATTERNS = [
    (r"/jobs?/(\d{10})", 0.96),
    (r"(?:jobs\.)?lever\.co/[^/]+/([a-f0-9-]{36})", 0.96),
    (r"(?:jobs\.)?ashbyhq\.com/[^/]+/([a-f0-9-]{36})", 0.96),
    (r"/jobs?/(\d{6,})", 0.94),
    (r"_([A-Z]R?-?\d{5,})(?:-\d+)?(?:\?|$)", 0.93),
    (r"/(\d{6,})(?:/|\?|$)", 0.76),
    (r"SALES(\d{6})", 0.85),
]

LOCATION_SELECTORS = [
    ('[data-automation-id="locations"]', 0.95),
    (".location", 0.86),
    (".job-location", 0.86),
]

LOCATION_METADATA_PATTERNS = [
    r"time\s+type.*$",
    r"Full\s+time.*$",
    r"Employment\s+Type.*$",
]
HTML_ARTIFACT_PATTERNS = [r"^s(?=[A-Z])", r"^p(?=[A-Z])"]
INVALID_LOCATION_KEYWORDS = ["time", "type", "full", "posted", "employment"]
DEPARTMENT_KEYWORDS = ["quantum", "analytics", "external", "product"]

# ============================================================================
# Role Categories
# ============================================================================

ROLE_CATEGORIES = {
    "Pure Software": {
        "keywords": ["backend", "frontend", "full stack"],
        "exclude": ["embedded", "firmware"],
        "action": "ACCEPT",
        "alert": "✅ SOFTWARE",
    },
    "Data & AI": {
        "keywords": ["data scien", "machine learning", "ai engineer"],
        "exclude": [],
        "action": "ACCEPT",
        "alert": "✅ DATA/AI",
    },
}

# ============================================================================
# Network Configuration
# ============================================================================

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
USER_AGENTS = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"]
JOB_BOARD_DOMAINS = ["greenhouse", "lever.co", "workday", "ashbyhq", "simplify.jobs"]
STATUS_COLORS = {"Not Applied": {"red": 1.0, "green": 1.0, "blue": 1.0}}

# ============================================================================
# Helper Functions
# ============================================================================


def get_state_for_city(city_name):
    if US_ZIPCODE_AVAILABLE and _search_engine:
        try:
            results = _search_engine.by_city(city_name.strip().title())
            if results:
                from collections import Counter

                states = [r.state for r in results if r.state]
                if states:
                    return Counter(states).most_common(1)[0][0]
        except:
            pass
    return CITY_TO_STATE_FALLBACK.get(city_name.lower())


def validate_us_state_code(state_code):
    if not state_code or len(state_code) != 2:
        return False
    if US_LIBRARY_AVAILABLE:
        try:
            return us_library.states.lookup(state_code) is not None
        except:
            pass
    return state_code.upper() in US_STATES_FALLBACK


def get_canadian_province(text):
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
    import re

    return bool(re.match(r"https?://.+\..+", url))


def normalize_unicode(text):
    if UNIDECODE_AVAILABLE:
        try:
            return unidecode_func(text)
        except:
            pass
    replacements = {"é": "e", "è": "e", "à": "a", "ô": "o", "ü": "u"}
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)
    return text


def get_city_state_from_zipcode(zipcode):
    if PGEOCODE_AVAILABLE and _pgeocode_nomi:
        try:
            result = _pgeocode_nomi.query_postal_code(zipcode)
            if result is not None and not result.isna().all():
                return result.get("place_name"), result.get("state_code")
        except:
            pass
    return None, None
