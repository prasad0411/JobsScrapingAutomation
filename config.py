#!/usr/bin/env python3

import warnings
import os

warnings.filterwarnings("ignore")
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

import logging

logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("selenium").setLevel(logging.ERROR)

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

SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Valid Entries"
DISCARDED_WORKSHEET = "Discarded Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"

SHEETS_CREDS_FILE = "credentials.json"
GMAIL_CREDS_FILE = "gmail_credentials.json"
GMAIL_TOKEN_FILE = "gmail_token.pickle"
JOBRIGHT_COOKIES_FILE = "jobright_cookies.json"
PROCESSED_EMAILS_FILE = "processed_emails.json"
FAILED_SIMPLIFY_CACHE = "failed_simplify_urls.json"

SIMPLIFY_URL = "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/master/README.md"
VANSHB03_URL = (
    "https://raw.githubusercontent.com/vanshb03/Summer2026-Internships/main/README.md"
)

MAX_JOB_AGE_DAYS = 3
MAX_REASONABLE_AGE_DAYS = 365
PAGE_AGE_THRESHOLD_DAYS = 3
MIN_QUALITY_SCORE = 4
MIN_CONFIDENCE_JOB_ID = 0.70
MIN_CONFIDENCE_LOCATION = 0.70
MIN_CONFIDENCE_COMPANY = 0.70
REQUIRE_MULTIPLE_CONFIRMATIONS = True
EMAIL_TRACKING_RETENTION_DAYS = 7

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2
BACKOFF_MULTIPLIER = 2

BLACKLIST_DOMAINS = ["workatastartup.com"]

PLATFORM_BLACKLIST = []

PLATFORM_BLACKLIST_REASONS = {}

COMPANY_BLACKLIST = [
    "RTX",
    "Raytheon",
    "Raytheon Technologies",
    "Northrop Grumman",
    "Lockheed Martin",
    "Leidos",
    "Leidos Defense Systems",
]

COMPANY_BLACKLIST_REASONS = {
    "RTX": "Company always requires security clearance",
    "Raytheon": "Company always requires security clearance",
    "Raytheon Technologies": "Company always requires security clearance",
    "Northrop Grumman": "Company always requires security clearance",
    "Lockheed Martin": "Company always requires security clearance",
    "Leidos": "Company always requires security clearance",
    "Leidos Defense Systems": "Company always requires security clearance",
}

VERBOSE_OUTPUT = False
SHOW_LOADING_STATS = False
SHOW_GITHUB_COUNTS = False

USER_LOCATION = "Boston"

REPROCESS_EMAILS_DAYS = 4
EMAIL_DATE_FILTER_ENABLED = True

PAGE_TEXT_QUICK_SCAN = 2000
PAGE_TEXT_STANDARD_SCAN = 5000
PAGE_TEXT_FULL_SCAN = 15000

JOB_ID_PREFERENCES = {
    "hash_fallback_enabled": False,
    "fallback_value": "N/A",
    "require_digit": True,
    "minimum_confidence": 0.70,
}

DATA_SANITIZATION_PREFERENCES = {
    "remove_emojis": True,
    "normalize_unicode": True,
    "decode_html_entities": True,
    "strip_field_prefixes": True,
    "trim_whitespace": True,
    "standardize_location_format": True,
    "validate_garbage_locations": True,
    "normalize_sponsorship_values": True,
}

FIELD_PREFIXES_TO_REMOVE = [
    "Title:",
    "Company:",
    "Location:",
    "Position:",
    "Role:",
    "Job:",
    "locations",
    "location ",
]

GARBAGE_LOCATION_PATTERNS = [
    "experience, and",
    "experience and",
    "salary",
    "compensation",
    "nearest major market",
    "multiple locations",
]

INTERNSHIP_INDICATORS = [
    "intern",
    "co-op",
    "coop",
    "apprentice",
    "apprenticeship",
    "emerging talent",
    "fellowship",
    "trainee",
    "student program",
    "early career program",
    "rotational program",
]

VALID_INTERNSHIP_TYPES = [
    "Internship",
    "Co-op",
    "Fellowship",
    "Apprenticeship",
    "Trainee",
]

GRADUATE_PROGRAM_PATTERNS = [
    r"graduate.*202[6-9]",
    r"graduate.*program.*(?:intern|summer)",
    r"graduate.*(?:intern|co-op)",
    r"(?:masters|ms).*202[6-9]",
    r"(?:ms|master).*(?:intern|graduate)",
]

DURATION_INTERNSHIP_PATTERNS = [
    r"\b(?:10|12|8)\s*[-–]?\s*week",
    r"\b(?:3|6|12)\s*[-–]?\s*month",
    r"june\s*(?:through|to|-|–)\s*august",
    r"may\s*(?:through|to|-|–)\s*august",
    r"summer\s*202[6-9]",
    r"(?:start|begin).*(?:june|may|august)\s*202[6-9]",
    r"(?:temporary|fixed[\s-]term)\s*position",
    r"internship\s+(?:duration|program|period)",
]

ENROLLMENT_PATTERNS = [
    r"must\s+be\s+(?:currently\s+)?enrolled",
    r"currently\s+pursuing.*degree",
    r"(?:pursuing|enrolled\s+in).*(?:bachelor|master|degree)",
    r"graduating.*202[6-9]",
    r"expected\s+graduation.*202[6-9]",
    r"graduation\s+date.*202[6-9]",
]

CONFLICTING_SIGNAL_PATTERNS = [
    r"full[\s-]time\s+(?:position|role|opportunity|employee)",
    r"permanent\s+(?:position|role)",
    r"(?:new|recent)\s+grad(?:uate)?s?\s+(?:welcome|encouraged)",
]

ASSOCIATE_BACHELOR_ONLY_PATTERNS = [
    r"(?:associate|associates|aa|as)\s+(?:or|and)\s+bachelor",
    r"(?:associate|aa)\s+degree.*only",
    r"no\s+(?:prior\s+)?experience.*bachelor.*program",
    r"bachelor.*program\s+(?:required|only)",
    r"entering.*(?:junior|senior)\s+year",
    r"(?:junior|senior)\s+year\s+(?:preferred|required|students?)",
    r"(?:sophomore|junior)\s+(?:or|and)\s+(?:junior|senior)",
    r"at\s+least\s+(?:a\s+)?(?:sophomore|junior)",
    r"minimum.*sophomore",
    r"completed.*sophomore\s+year",
    r"(?:rising|entering)\s+(?:junior|senior)",
    r"graduate.*202[67].*between.*(?:junior|senior)",
    r"summer\s+between.*(?:junior|senior)\s+year",
    r"currently\s+enrolled.*pursuing.*bachelor'?s?\s+degree",
    r"enrolled.*bachelor'?s?\s+(?:degree\s+)?program",
    r"actively\s+enrolled.*bachelor'?s?\s+program",
    r"student\s+pursuing.*bachelor'?s?",
    r"pursuit\s+of.*bachelor'?s?\s+degree",
    r"bachelor'?s?\s+degree\s+program.*enrollment",
    r"pursuing.*(?:an?\s+)?undergraduate'?s?\s+degree",
    r"enrolled.*undergraduate'?s?\s+(?:degree|program)",
    r"undergraduate'?s?\s+degree.*(?:required|program)",
    r"(?:in\s+)?(?:an?\s+)?undergraduate'?s?\s+degree",
    r"associate'?s?\s+or\s+bachelor'?s?\s+degree",
    r"pursuing.*associate'?s?\s+or\s+bachelor'?s?",
    r"senior\s+level\s+student",
    r"junior\s+level\s+student",
    r"(?:junior|senior)-level\s+student",
    r"junior\s+(?:or|and|to)\s+senior\s+level",
    r"senior.*graduating.*(?:summer|spring|may|june|202[67])",
    r"(?:junior|senior).*graduating.*(?:this\s+)?(?:summer|spring)",
]

CPT_OPT_EXCLUSION_PATTERNS = [
    r"will\s+not\s+(?:provide|offer|support|sign).{0,80}(?:cpt|opt|curricular\s+practical|optional\s+practical)",
    r"(?:does\s+not|doesn't|cannot)\s+(?:support|provide|sponsor).{0,80}(?:cpt|opt)",
    r"(?:cpt|opt|curricular\s+practical|optional\s+practical).{0,80}(?:not|n't|cannot).{0,50}(?:support|provide|available|offered)",
    r"no.{0,30}(?:assistance|support|documentation).{0,50}(?:for|with|regarding).{0,30}(?:cpt|opt)",
    r"will\s+not.*sign.*documentation.{0,50}(?:cpt|opt)",
    r"(?:cpt|opt).{0,50}not\s+(?:available|supported|provided|offered)",
    r"not\s+eligible.{0,30}(?:for|under).{0,30}(?:cpt|opt)",
    r"visa.*not\s+available.{0,200}f-1.{0,100}(?:cpt|opt|ead)",
    r"not\s+available.{0,150}(?:includes|including).{0,100}f-1.{0,50}(?:cpt|opt|ead)",
    r"sponsorship.*not\s+available.{0,200}(?:includes|including).{0,100}(?:cpt|opt|f-1)",
]

GEOGRAPHIC_ENROLLMENT_PATTERNS = [
    r"enrolled\s+at.*(?:college|university).*in\s+(?:the\s+)?([A-Za-z\s/]+)\s+area",
    r"must\s+be\s+enrolled.*in\s+([A-Za-z\s/]+).*to\s+be\s+considered",
    r"attend.*(?:college|university).*(?:within|in)\s+([A-Za-z\s/]+)",
    r"(?:college|university).*in\s+the\s+([A-Za-z\s/]+).*(?:area|region)",
]

HIGH_SCHOOL_ONLY_PATTERNS = [
    r"high\s+school\s+(?:student|senior|graduate)",
    r"graduating\s+(?:from\s+)?high\s+school",
    r"on\s+track\s+to\s+graduating\s+high\s+school",
    r"current(?:ly)?\s+(?:in\s+)?high\s+school",
    r"high\s+school\s+diploma",
    r"must\s+be.*high\s+school",
    r"high\s+school.*plans\s+to\s+attain",
]

PERMANENT_US_AUTHORIZATION_PATTERNS = [
    r"permanent.*(?:us|united\s+states|u\.s\.).*(?:work|employment)\s+authorization",
    r"requisite.*permanent.*(?:work|employment).*authorization",
    r"must\s+(?:have|possess).*permanent.*(?:right|authorization)\s+to\s+work",
    r"permanently\s+authorized\s+to\s+work",
    r"permanent.*(?:right|ability)\s+to\s+work.*(?:in\s+the\s+)?(?:us|united\s+states)",
]

US_PERSON_DOD_PATTERNS = [
    r"\bus\s+person\b",
    r"u\.s\.\s+person",
    r"united\s+states\s+person",
    r"\bdod\b",
    r"department\s+of\s+defense",
    r"dod\s+contract",
    r"defense\s+contract",
]

ENHANCED_PHD_PATTERNS = [
    r"current\s+phd\s+student",
    r"currently.*phd\s+student",
    r"active\s+phd\s+candidate",
    r"must\s+be\s+pursuing.*phd.*\(enrolled",
    r"criteria:?.*pursuing.*phd",
    r"one\s+of.*following.*phd",
    r"enrolled.*phd\s+student",
    r"phd-level\s+student",
    r"ongoing\s+ph\.?d\.?",
    r"current\s+ph\.?d\.?",
    r"active\s+ph\.?d\.?",
    r"ph\.?d\.?\s+(?:student|candidate|intern)",
    r"pursuing.*ph\.?d\.?\s+(?:degree|program)",
]

DEGREE_LIST_PATTERNS = [
    r"(?:pursuing|currently\s+in|degree\s+in|enrolled\s+in).{0,80}(?:ba|bs|ms|ma|phd|ph\.d\.).{0,50}(?:ba|bs|ms|ma|phd|ph\.d\.|or|and|,)",
    r"(?:bachelor|master|doctoral|phd|ph\.d\.).{0,50}(?:or|and|,).{0,50}(?:bachelor|master|phd|ph\.d\.)",
    r"(?:ba|bs|ms|ma|phd|ph\.d\.)[\s,/]+(?:ba|bs|ms|ma|phd|ph\.d\.)",
]

PHD_MS_FLEXIBILITY_KEYWORDS = [
    "master",
    " ms ",
    "ms/phd",
    "or master",
    "master's",
    "graduate students",
    "advanced degree students",
    "masters",
    "m.s.",
    "ms degree",
    "ms students",
]

INTERNATIONAL_URL_INDICATORS = [
    ".co.uk",
    ".uk",
    "/uk/",
    "/gb/",
    ".ca",
    "/canada/",
    "/canadian/",
    ".com.au",
    ".au",
    "/australia/",
    ".de",
    "/germany/",
    "/deutschland/",
    ".fr",
    "/france/",
    ".in",
    "/india/",
    ".sg",
    "/singapore/",
]

INTERNATIONAL_TEXT_INDICATORS = [
    (r"\bunited\s+kingdom\b", "UK"),
    (r",\s*uk\b", "UK"),
    (r",\s*gb\b", "UK"),
    (r"\blondon,\s*uk", "UK"),
    (r"\bengland\b", "UK"),
    (r",\s*england\b", "UK"),
    (r"\bscotland\b", "UK"),
    (r"\bwales\b", "UK"),
    (r"\bnorthern\s+ireland\b", "UK"),
    (r"\bharrogate\b", "UK"),
    (r"\bmanchester\b", "UK"),
    (r"\bedinburgh\b", "UK"),
    (r"\bglasgow\b", "UK"),
    (r"\bleeds\b", "UK"),
    (r"\bbristol\b", "UK"),
    (r"\bcambridge,\s*uk", "UK"),
    (r"canada", "Canada"),
    (r"ontario,\s*can", "Canada"),
    (r"toronto,\s*on\b", "Canada"),
    (r"montreal,\s*qc", "Canada"),
    (r"vancouver,\s*bc", "Canada"),
]

UK_CITIES = [
    "london",
    "manchester",
    "edinburgh",
    "glasgow",
    "birmingham",
    "leeds",
    "bristol",
    "harrogate",
    "cambridge",
    "oxford",
    "reading",
    "milton keynes",
    "southampton",
    "nottingham",
]

CITY_STATE_DISAMBIGUATION = {
    "wyoming": {"MN": "Wyoming, Minnesota"},
    "ontario": {"CA": "Ontario, California"},
    "paris": {"TX": "Paris, Texas"},
    "portland": {"ME": "Portland, Maine", "OR": "Portland, Oregon"},
    "kansas city": {"KS": "Kansas City, Kansas", "MO": "Kansas City, Missouri"},
}

PORTAL_NAME_INDICATORS = [
    "Agency Contractor",
    "Preferential Rehire",
    "Job Site",
    "External Career",
    "External Job",
    "Career Portal",
]

WORKDAY_ABBREVIATIONS = {
    "bcbsmn": "Blue Cross and Blue Shield of Minnesota",
    "hp": "HP Inc.",
    "cat": "Caterpillar",
    "bmo": "Bank of Montreal",
    "cibc": "Canadian Imperial Bank of Commerce",
}

GUARANTEED_TECHNICAL_PHRASES = [
    "computer science",
    "software engineer",
    "software developer",
    "software intern",
    "data scientist",
    "data engineer",
    "machine learning engineer",
    "ml engineer",
    "ai engineer",
]

ENHANCED_REMOTE_PATTERNS = [
    "work from home",
    "wfh",
    "telecommute",
    "distributed",
    "remote-first",
    "remote friendly",
    "location: remote",
    "anywhere in",
    "work anywhere",
    "fully remote",
    "100% remote",
    "remote work",
    "remote position",
]

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
    "san mateo": "CA",
    "santa fe": "NM",
    "new york": "NY",
    "brooklyn": "NY",
    "buffalo": "NY",
    "seattle": "WA",
    "bellevue": "WA",
    "redmond": "WA",
    "bothell": "WA",
    "boston": "MA",
    "cambridge": "MA",
    "worcester": "MA",
    "westford": "MA",
    "braintree": "MA",
    "waltham": "MA",
    "chicago": "IL",
    "atlanta": "GA",
    "philadelphia": "PA",
    "pittsburgh": "PA",
    "denver": "CO",
    "golden": "CO",
    "boulder": "CO",
    "louisville": "CO",
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
    "san antonio": "TX",
    "fort worth": "TX",
    "charlotte": "NC",
    "raleigh": "NC",
    "durham": "NC",
    "rockville": "MD",
    "baltimore": "MD",
    "towson": "MD",
    "bloomington": "MN",
    "minneapolis": "MN",
    "draper": "UT",
    "salt lake city": "UT",
    "sioux falls": "SD",
    "pleasant prairie": "WI",
    "milwaukee": "WI",
    "cedar rapids": "IA",
    "newark": "NJ",
    "berkeley heights": "NJ",
    "middletown": "NJ",
    "middletown": "CT",
    "washington": "DC",
    "englewood cliffs": "NJ",
}

CITY_ABBREVIATIONS = {
    "sf": "San Francisco, CA",
    "nyc": "New York, NY",
    "la": "Los Angeles, CA",
    "dc": "Washington, DC",
    "philly": "Philadelphia, PA",
    "chi": "Chicago, IL",
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
    " Metropolitan Area",
    " Area",
    " area",
]

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
    "montreal": "QC",
    "vancouver": "BC",
    "calgary": "AB",
    "edmonton": "AB",
    "winnipeg": "MB",
    "quebec city": "QC",
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
    "kanata": "ON",
    "burlington": "ON",
    "oakville": "ON",
    "richmond hill": "ON",
    "pickering": "ON",
    "ajax": "ON",
    "whitby": "ON",
    "kingston": "ON",
    "windsor": "ON",
    "oshawa": "ON",
    "barrie": "ON",
    "longueuil": "QC",
    "sherbrooke": "QC",
    "surrey": "BC",
    "burnaby": "BC",
    "richmond": "BC",
    "abbotsford": "BC",
    "coquitlam": "BC",
    "victoria": "BC",
    "kelowna": "BC",
    "red deer": "AB",
    "lethbridge": "AB",
    "saskatoon": "SK",
    "regina": "SK",
    "halifax": "NS",
    "moncton": "NB",
    "st johns": "NL",
    "st. johns": "NL",
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

CANADIAN_COMPANIES = {
    "bmo",
    "bank of montreal",
    "shopify",
    "wealthsimple",
    "cae",
    "blackberry",
    "kinaxis",
    "opentext",
    "hootsuite",
}

US_CONTEXT_KEYWORDS = ["usa", "united states", "u.s.", "bay area", "silicon valley"]
CANADA_CONTEXT_KEYWORDS = ["canada", "canadian", "gta", "greater toronto area"]

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
    "glassdoor": r"glassdoor\.com",
    "boomi": r"boomi\.com",
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
            ('div[data-automation-id="jobProperties"] dd', 0.75),
            (".jobProperty .jobPropertyValue", 0.70),
            ("div.css-1ij27gp", 0.80),
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
            (".job-location", 0.88),
            ("div.location", 0.90),
            ("[data-qa='job-location']", 0.88),
            (".app-title + div", 0.75),
            ("h1 + div", 0.70),
            (".posting-headline + div", 0.72),
            ("[class*='location']", 0.80),
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
    "icims": {
        "requires_selenium": False,
        "wait_time": 3,
        "location_selectors": [],
        "company_selector": 'meta[property="og:site_name"]',
        "title_selector": "h1",
        "job_id_pattern": r"/jobs/(\d+)/job",
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
    r"kbr\.wd\d+\.myworkdayjobs\.com": "KBR",
    r"philips\.wd\d+\.myworkdayjobs\.com": "Philips",
    r"jci\.wd\d+\.myworkdayjobs\.com": "Johnson Controls",
    r"bmo\.wd\d+\.myworkdayjobs\.com": "Bank of Montreal",
    r"biibhr\.wd\d+\.myworkdayjobs\.com": "Biogen",
    r"icf\.wd\d+\.myworkdayjobs\.com": "ICF",
    r"highmarkhealth\.wd\d+\.myworkdayjobs\.com": "Highmark Health",
    r"labcorp\.wd\d+\.myworkdayjobs\.com": "LabCorp",
    r"websteronline\.wd\d+\.myworkdayjobs\.com": "Webster Bank",
    r"blueorigin\.wd\d+\.myworkdayjobs\.com": "Blue Origin",
    r"bloomenergy\.wd\d+\.myworkdayjobs\.com": "Bloom Energy",
    r"premierinc\.wd\d+\.myworkdayjobs\.com": "Premier Inc",
    r"leidos\.wd\d+\.myworkdayjobs\.com": "Leidos",
    r"usaa\.wd\d+\.myworkdayjobs\.com": "USAA",
    r"pwc\.wd\d+\.myworkdayjobs\.com": "PwC",
    r"sec\.wd\d+\.myworkdayjobs\.com": "Samsung Electronics America",
    r"jj\.wd\d+\.myworkdayjobs\.com": "Johnson & Johnson",
    r"alcon\.wd\d+\.myworkdayjobs\.com": "Alcon",
    r"kla\.wd\d+\.myworkdayjobs\.com": "KLA Corporation",
    r"job-boards\.greenhouse\.io/asteraearlycareer": "Astera Labs",
    r"job-boards\.greenhouse\.io/samsungresearchamericainternship": "Samsung Research America",
    r"job-boards\.greenhouse\.io/obsidiansecurity": "Obsidian Security",
    r"job-boards\.greenhouse\.io/commvault": "Commvault",
    r"job-boards\.greenhouse\.io/audaxgroup": "Audax Group",
    r"job-boards\.greenhouse\.io/verkada": "Verkada",
    r"job-boards\.greenhouse\.io/auctane": "Auctane",
    r"job-boards\.greenhouse\.io/faire": "Faire",
    r"job-boards\.greenhouse\.io/internshiplist2000": "Greenhouse",
    r"job-boards\.greenhouse\.io/waterloocoop": "Waterloo",
    r"job-boards\.greenhouse\.io/clear": "CLEAR",
    r"job-boards\.greenhouse\.io/mongodb": "MongoDB",
    r"job-boards\.greenhouse\.io/sift": "Sift",
    r"job-boards\.greenhouse\.io/ramp": "Ramp",
    r"job-boards\.greenhouse\.io/zoox": "Zoox",
    r"job-boards\.greenhouse\.io/twosigma": "Two Sigma",
    r"generatebiomedicines\.com": "Generate Biomedicines",
    r"jobs\.smartrecruiters\.com/Visa": "Visa",
    r"jobs\.smartrecruiters\.com/Intuitive": "Intuitive Surgical",
    r"jobs\.smartrecruiters\.com/Experian": "Experian",
    r"jobs\.smartrecruiters\.com/BoschGroup": "Robert Bosch Venture Capital",
    r"jobs\.smartrecruiters\.com/WesternDigital": "Western Digital",
    r"jobs\.lever\.co/zoox": "Zoox",
    r"jobs\.lever\.co/veeva": "Veeva Systems",
    r"jobs\.lever\.co/wealthsimple": "Wealthsimple",
    r"jobs\.lever\.co/seatgeek": "SeatGeek",
    r"jobs\.ashbyhq\.com/uipath": "UiPath",
    r"jobs\.ashbyhq\.com/Ridealso": "ALSO",
    r"jobs\.ashbyhq\.com/atomicsemi": "Atomic Semi",
    r"jobs\.ashbyhq\.com/cohere": "Cohere",
    r"jobs\.ea\.com": "Electronic Arts",
    r"eeho\.fa\.us2\.oraclecloud\.com": "Oracle",
    r"fa-evmr.*\.oraclecloud\.com": "Nokia",
    r"jobs-legrand\.icims\.com": "Legrand",
    r"careers-gdms\.icims\.com": "General Dynamics Mission Systems",
    r"jobs\.paccar\.com": "Paccar",
    r"apply\.careers\.microsoft\.com": "Microsoft",
    r"3ds\.com/careers": "Dassault Systèmes",
    r"careers\.adobe\.com": "Adobe",
    r"jobs\.siemens\.com": "Siemens",
    r"ats\.rippling\.com/.*/redaspen": "Red Aspen",
    r"careers\.merzaesthetics\.com": "Merz North America",
    r"jolera\.com": "Jolera",
    r"boomi\.com": "Boomi",
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
    "boomi": "Boomi",
    "biibhr": "Biogen",
    "icf": "ICF",
    "highmarkhealth": "Highmark Health",
    "labcorp": "LabCorp",
    "websteronline": "Webster Bank",
    "blueorigin": "Blue Origin",
    "bloomenergy": "Bloom Energy",
    "premierinc": "Premier Inc",
    "usaa": "USAA",
    "pwc": "PwC",
    "sec": "Samsung Electronics America",
    "jj": "Johnson & Johnson",
    "alcon": "Alcon",
    "kla": "KLA Corporation",
    "clear": "CLEAR",
    "mongodb": "MongoDB",
    "sift": "Sift",
    "ramp": "Ramp",
    "seatgeek": "SeatGeek",
    "twosigma": "Two Sigma",
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

JOB_ID_PATTERNS = [
    (r"/jobs?/(\d{10})", 0.96),
    (r"gh_jid=(\d{7,})", 0.96),
    (r"/jobs?/(\d{6,})", 0.94),
    (r"_([A-Z]R?-?\d{5,})(?:-\d+)?(?:\?|$)", 0.93),
    (r"/([A-Z]{2,3}\d{5,})(?:-\d+)?(?:\?|$)", 0.91),
    (r"(?:jobs\.)?lever\.co/[^/]+/([a-f0-9-]{36})", 0.96),
    (r"(?:jobs\.)?ashbyhq\.com/[^/]+/([a-f0-9-]{36})", 0.96),
    (r"smartrecruiters\.com/[^/]+/(\d{15})", 0.96),
    (r"/jobs/(\d+)/job", 0.94),
    (r"REQ[_-]?(\d{6,})", 0.92),
    (r"job[/_]([A-Z0-9_-]{6,15})(?:\?|$|/)", 0.86),
    (r"[?&]reqId=([A-Z0-9_-]{4,15})(?:&|$)", 0.88),
    (r"/(\d{6,})(?:/|\?|$)", 0.76),
    (r"SALES(\d{6})", 0.85),
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
    "Product Management": {
        "keywords": ["product management", "product manager"],
        "exclude": [],
        "action": "ACCEPT",
        "alert": "🔍 PRODUCT MANAGEMENT",
    },
}

TECHNICAL_ROLE_KEYWORDS = {
    "software",
    "engineer",
    "engineering",
    "developer",
    "development",
    "developing",
    "programmer",
    "programming",
    "coding",
    "code",
    "coder",
    "data",
    "ml",
    "ai",
    "machine learning",
    "artificial intelligence",
    "full stack",
    "backend",
    "frontend",
    "web",
    "mobile",
    "cloud",
    "devops",
    "sre",
    "platform",
    "security",
    "qa",
    "test",
    "testing",
    "automation",
    "technology",
    "technical",
    "it ",
    "information technology",
    "systems",
    "digital",
    "quantitative",
    "analytics",
    "solutions",
    "infrastructure",
    "cybersecurity",
    "r&d",
    "llm",
    "nlp",
    "natural language",
    "natural language processing",
    "computer vision",
    "computer science",
    "computer",
    "cs",
    "deep learning",
    "neural network",
    "generative ai",
    "multimodal",
    "transformer",
    "reinforcement learning",
    "embedded",
    "firmware",
    "fpga",
    "gpu",
    "cuda",
    "robotics",
    "autonomous",
    "perception",
    "controls",
    "database",
    "sql",
    "nosql",
    "etl",
    "pipeline",
    "distributed systems",
    "microservices",
    "api development",
    "kubernetes",
    "docker",
    "containerization",
    "pytorch",
    "tensorflow",
    "computational",
    "bioinformatics",
    "algorithm",
    "hpc",
    "5g",
    "ran",
    "baseband",
    "wireless",
    "product management",
    "product manager",
    "application",
}

TECHNICAL_PATTERNS = [
    r"\bprogramm(er|ing)\b",
    r"\bdevelop(er|ment|ing)\b",
    r"\bengineer(ing)?\b",
    r"\bnatural\s+language\b",
    r"\bsoftware\s+\w+",
    r"\bapplication\s+\w*develop",
    r"\bproduct\s+manag(er|ement)\b",
]

NON_TECHNICAL_PURE = {
    "marketing analyst",
    "sales",
    "recruiter",
    "hr specialist",
    "finance analyst",
    "accountant",
    "legal",
}

SPONSORSHIP_REJECT_PATTERNS = [
    r"(?:no|not|without).{0,100}(?:current|future).{0,50}sponsor(?:ship)?",
    r"(?:no|not).{0,50}sponsor(?:ship)?\s+(?:available|offered|provided)",
    r"sponsor(?:ship)?\s+(?:not available|unavailable|not offered)",
    r"must (?:be|have).{0,50}(?:authorized|authorization).{0,50}(?:without|no).{0,50}sponsor",
    r"(?:clearance.*required)",
]

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
    "jobright.ai",
]

STATUS_COLORS = {
    "Not Applied": {"red": 1.0, "green": 1.0, "blue": 1.0},
    "Applied": {"red": 1.0, "green": 0.9, "blue": 0.6},
    "Interview Scheduled": {"red": 1.0, "green": 0.8, "blue": 0.4},
    "Offer Received": {"red": 0.6, "green": 0.9, "blue": 0.6},
}


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
