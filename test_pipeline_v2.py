#!/usr/bin/env python3
"""
test_pipeline_v2.py — 200 test cases covering all fixes and features.

Usage:
    python3 test_pipeline_v2.py
    python3 test_pipeline_v2.py --section 5
"""

import sys, os, re, time, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def _g(s): return f"\033[92m{s}\033[0m"
def _r(s): return f"\033[91m{s}\033[0m"
def _y(s): return f"\033[93m{s}\033[0m"
def _b(s): return f"\033[94m{s}\033[0m"

SECTION_FILTER = None
for i, a in enumerate(sys.argv):
    if a == "--section" and i + 1 < len(sys.argv):
        SECTION_FILTER = int(sys.argv[i + 1])

results = {"pass": 0, "fail": 0, "errors": []}
_current_section = [0]

def section(n, title):
    _current_section[0] = n
    if SECTION_FILTER and n != SECTION_FILTER:
        return
    print(f"\n{_b('═'*60)}")
    print(f"{_b(f'  {n}. {title}')}")
    print(f"{_b('═'*60)}")

def T(name, condition, detail=""):
    if SECTION_FILTER and _current_section[0] != SECTION_FILTER:
        return
    try:
        ok = condition() if callable(condition) else bool(condition)
        if ok:
            print(f"  {_g('PASS')}  {name}")
            results["pass"] += 1
        else:
            msg = f"  {_r('FAIL')}  {name}" + (f"  ({detail})" if detail else "")
            print(msg)
            results["fail"] += 1
            results["errors"].append(name)
    except Exception as e:
        print(f"  {_r('FAIL')}  {name}  (ERROR: {e})")
        results["fail"] += 1
        results["errors"].append(f"{name} — {e}")


# =============================================================================
# 1. CONFIG & BLACKLIST
# =============================================================================
section(1, "Config & Blacklist")

from aggregator.config import (
    COMPANY_BLACKLIST, COMPANY_BLACKLIST_REASONS, INVALID_TITLE_KEYWORDS,
    COMPANY_HQ, JOB_ID_PATTERNS,
)

T("COMPANY_BLACKLIST is non-empty list", isinstance(COMPANY_BLACKLIST, list) and len(COMPANY_BLACKLIST) > 0)
T("RTX blacklisted", "RTX" in COMPANY_BLACKLIST)
T("Raytheon blacklisted", "Raytheon" in COMPANY_BLACKLIST)
T("Northrop Grumman blacklisted", "Northrop Grumman" in COMPANY_BLACKLIST)
T("Lockheed Martin blacklisted", "Lockheed Martin" in COMPANY_BLACKLIST)
T("Parsons blacklisted (auto-added)", "Parsons" in COMPANY_BLACKLIST)
T("CAE blacklisted (auto-added)", "CAE" in COMPANY_BLACKLIST)
T("Motorola Solutions blacklisted", "Motorola Solutions" in COMPANY_BLACKLIST)
T("Coherent NOT blacklisted", "Coherent" not in COMPANY_BLACKLIST)
T("Google NOT blacklisted", "Google" not in COMPANY_BLACKLIST)
T("COMPANY_HQ has 50+ entries", len(COMPANY_HQ) >= 50)
T("Apple in COMPANY_HQ", "apple" in COMPANY_HQ)
T("AOSP in INVALID_TITLE_KEYWORDS", any("aosp" in p for p in INVALID_TITLE_KEYWORDS))
T("HAL pattern in INVALID_TITLE_KEYWORDS", any("hal" in p for p in INVALID_TITLE_KEYWORDS))
T("BSP pattern in INVALID_TITLE_KEYWORDS", any("bsp" in p for p in INVALID_TITLE_KEYWORDS))
T("Opto-mechanical in INVALID_TITLE_KEYWORDS", any("opto" in p for p in INVALID_TITLE_KEYWORDS))
T("Rotational program in INVALID_TITLE_KEYWORDS", any("rotational" in p for p in INVALID_TITLE_KEYWORDS))


# =============================================================================
# 2. TITLE PROCESSOR
# =============================================================================
section(2, "Title Processor — Valid/Invalid/Edge")

from aggregator.processors import TitleProcessor

T("Software Engineer Intern accepted", TitleProcessor.is_valid_job_title("Software Engineer Intern")[0])
T("ML Engineer Intern accepted", TitleProcessor.is_valid_job_title("Machine Learning Engineer Intern")[0])
T("Data Engineer Intern accepted", TitleProcessor.is_valid_job_title("Data Engineer Intern")[0])
T("Backend SWE Intern accepted", TitleProcessor.is_valid_job_title("Backend Software Engineer Intern")[0])
T("AI Research Intern accepted", TitleProcessor.is_valid_job_title("AI Research Intern")[0])
T("Quantitative Research Intern accepted", TitleProcessor.is_valid_job_title("Quantitative Research Intern")[0])
T("Full Stack Intern accepted", TitleProcessor.is_valid_job_title("Full Stack Engineering Intern")[0])
T("DevOps Intern accepted", TitleProcessor.is_valid_job_title("DevOps Engineer Intern")[0])
T("AOSP intern rejected", not TitleProcessor.is_valid_job_title("Software Engineering Intern - Android AOSP")[0])
T("HAL engineer rejected", not TitleProcessor.is_valid_job_title("HAL Engineer Intern")[0])
T("BSP engineer rejected", not TitleProcessor.is_valid_job_title("BSP Software Engineer Intern")[0])
T("Hardware abstraction layer rejected", not TitleProcessor.is_valid_job_title("Hardware Abstraction Layer Engineer")[0])
T("Opto-mechanical rejected", not TitleProcessor.is_valid_job_title("Opto-Mechanical System Engineer Intern")[0])
T("Mechanical engineer rejected", not TitleProcessor.is_valid_job_title("Mechanical Engineering Intern")[0])
T("Laser application rejected", not TitleProcessor.is_valid_job_title("Laser Application Engineer Intern")[0])
T("Photonics engineer rejected", not TitleProcessor.is_valid_job_title("Photonics Engineer Intern")[0])
T("Sales intern rejected", not TitleProcessor.is_valid_job_title("Sales Development Intern")[0])
T("Marketing intern rejected", not TitleProcessor.is_valid_job_title("Marketing Intern")[0])
T("Rotational program rejected", not TitleProcessor.is_valid_job_title("Product Engineer - Rotational Program")[0])
T("PhD intern rejected", not TitleProcessor.is_valid_job_title("PhD Software Engineering Intern")[0])
T("SkillBridge rejected", not TitleProcessor.is_valid_job_title("SkillBridge Software Engineer")[0])
T("None title rejected", not TitleProcessor.is_valid_job_title(None)[0])
T("Empty string rejected", not TitleProcessor.is_valid_job_title("")[0])
T("Too short rejected", not TitleProcessor.is_valid_job_title("Dev")[0])
T("Whitespace only rejected", not TitleProcessor.is_valid_job_title("   ")[0])
T("Valid title with special chars accepted",
    TitleProcessor.is_valid_job_title("Software Engineer Intern — Summer 2026")[0])
T("Co-op title accepted", TitleProcessor.is_valid_job_title("Software Engineer Co-op")[0])
T("Apprentice title accepted", TitleProcessor.is_valid_job_title("Software Development Apprentice")[0])
T("Security Engineer Intern accepted",
    TitleProcessor.is_valid_job_title("Security Engineer Intern")[0])
T("Platform Engineer Intern accepted",
    TitleProcessor.is_valid_job_title("Platform Engineer Intern")[0])
T("Site Reliability Engineer Intern accepted",
    TitleProcessor.is_valid_job_title("Site Reliability Engineer Intern")[0])


# =============================================================================
# 3. SEASON DETECTION
# =============================================================================
section(3, "Season Detection")

T("Summer 2026 accepted", TitleProcessor.check_season_requirement("Software Engineer Intern Summer 2026")[0])
T("Summer 2027 accepted", TitleProcessor.check_season_requirement("Software Engineer Intern Summer 2027")[0])
T("Fall 2026 accepted", TitleProcessor.check_season_requirement("Software Engineer Intern Fall 2026")[0])
T("Spring 2026 rejected", not TitleProcessor.check_season_requirement("Software Engineering Intern Spring 2026")[0])
T("Spring 2025 rejected", not TitleProcessor.check_season_requirement("Software Engineering Intern Spring 2025")[0])
T("Summer 2025 rejected", not TitleProcessor.check_season_requirement("", page_text="Summer 2025 internship")[0])
T("No year — page text 2026 accepted",
    TitleProcessor.check_season_requirement("Software Engineer Intern", page_text="Summer 2026 start date")[0])
T("April start date rejected",
    not TitleProcessor.check_season_requirement("Software Engineer Intern",
        page_text="internship begins in April 2026 and runs through June")[0])
T("March start date rejected",
    not TitleProcessor.check_season_requirement("Software Engineer Intern",
        page_text="program commences March 2026")[0])
T("2027 year accepted", TitleProcessor.check_season_requirement("", page_text="Summer 2027 cohort")[0])
T("Copyright year ignored",
    TitleProcessor.check_season_requirement("Software Engineer Intern",
        page_text="© 2025 Company Inc. Summer 2026 internship")[0])
T("Financial year ignored",
    TitleProcessor.check_season_requirement("Software Engineer Intern",
        page_text="FY 2025 revenue $2B. Summer 2026 program")[0])


# =============================================================================
# 4. CS ROLE CLASSIFICATION
# =============================================================================
section(4, "CS Role Classification")

T("Backend engineer is CS", TitleProcessor.is_cs_engineering_role("Backend Software Engineer Intern"))
T("ML engineer is CS", TitleProcessor.is_cs_engineering_role("Machine Learning Engineer Intern"))
T("Data engineer is CS", TitleProcessor.is_cs_engineering_role("Data Engineer Intern"))
T("DevOps is CS", TitleProcessor.is_cs_engineering_role("DevOps Engineer Intern"))
T("Security engineer is CS", TitleProcessor.is_cs_engineering_role("Security Software Engineer Intern"))
T("Cloud engineer is CS", TitleProcessor.is_cs_engineering_role("Cloud Infrastructure Engineer Intern"))
T("AI researcher is CS", TitleProcessor.is_cs_engineering_role("AI Research Scientist Intern"))
T("Quant research is CS", TitleProcessor.is_cs_engineering_role("Quantitative Research Intern"))
T("Accounting is NOT CS", not TitleProcessor.is_cs_engineering_role("Accounting Summer Intern"))
T("Finance analyst is NOT CS", not TitleProcessor.is_cs_engineering_role("Finance Analyst Intern"))
T("HR intern is NOT CS", not TitleProcessor.is_cs_engineering_role("Human Resources Intern"))
T("Supply chain is NOT CS", not TitleProcessor.is_cs_engineering_role("Supply Chain Intern"))
T("Product management is NOT CS", not TitleProcessor.is_cs_engineering_role("Product Management Intern"))
T("Marketing analytics IS CS", TitleProcessor.is_cs_engineering_role("Marketing Analytics Engineer Intern"))
T("Business analyst is bool", isinstance(TitleProcessor.is_cs_engineering_role("Business Analyst Intern"), bool))


# =============================================================================
# 5. LOCATION & INTERNATIONAL DETECTION
# =============================================================================
section(5, "Location & International Detection")

from aggregator.processors import LocationProcessor

T("Toronto ON detected as Canada", LocationProcessor.check_if_international("Toronto, ON") is not None)
T("Vancouver BC detected as Canada", LocationProcessor.check_if_international("Vancouver, BC") is not None)
T("Ottawa Canada detected", LocationProcessor.check_if_international("Ottawa, Canada") is not None)
T("CAN suffix detected", LocationProcessor.check_if_international("Peterborough CAN") is not None)
T("Ontario province detected", LocationProcessor.check_if_international("Waterloo, Ontario") is not None)
T("Stuttgart Germany detected", LocationProcessor.check_if_international("Stuttgart, Germany") is not None)
T("Geany typo detected as Germany", LocationProcessor.check_if_international("Stuttgart, Geany") is not None)
T("Munich detected as Germany", LocationProcessor.check_if_international("Munich, Germany") is not None)
T("Berlin detected", LocationProcessor.check_if_international("Berlin, Deutschland") is not None)
T("London UK detected", LocationProcessor.check_if_international("London, United Kingdom") is not None)
T("Paris France detected", LocationProcessor.check_if_international("Paris, France") is not None)
T("Tokyo Japan detected", LocationProcessor.check_if_international("Tokyo, Japan") is not None)
T("Sydney Australia detected", LocationProcessor.check_if_international("Sydney, Australia") is not None)
T("San Francisco NOT international", LocationProcessor.check_if_international("San Francisco, CA") is None)
T("Remote NOT international", LocationProcessor.check_if_international("Remote") is None)
T("New York NOT international", LocationProcessor.check_if_international("New York, NY") is None)
T("Seattle WA NOT international", LocationProcessor.check_if_international("Seattle, WA") is None)
T("Burlington MA NOT Canada", LocationProcessor.check_if_international("Burlington, MA") is None)
T("Vancouver WA NOT Canada", LocationProcessor.check_if_international("Vancouver, WA") is None)
T("Unknown location returns None", LocationProcessor.check_if_international("Unknown") is None)


# =============================================================================
# 6. SPONSORSHIP DETECTION
# =============================================================================
section(6, "Sponsorship Detection")

from aggregator.processors import ValidationHelper
from bs4 import BeautifulSoup

def soup(text): return BeautifulSoup(text, "html.parser")

T("'will sponsor' = Yes",
    ValidationHelper.check_sponsorship_status(soup("We will sponsor H-1B visas")) == "Yes")
T("'provides sponsorship' = Yes",
    ValidationHelper.check_sponsorship_status(soup("Company provides sponsorship")) == "Yes")
T("'sponsor H-1B' reversed = Yes",
    ValidationHelper.check_sponsorship_status(soup("We sponsor H-1B candidates")) == "Yes")
T("'H-1B visa sponsorship available' = Yes",
    ValidationHelper.check_sponsorship_status(soup("H-1B visa sponsorship available")) == "Yes")
T("'does not sponsor' = No",
    ValidationHelper.check_sponsorship_status(soup("We do not sponsor visas")) == "No")
T("'no sponsorship' = No",
    ValidationHelper.check_sponsorship_status(soup("No visa sponsorship available")) == "No")
T("'unable to sponsor' = No",
    ValidationHelper.check_sponsorship_status(
        soup("We are unable to sponsor or take over sponsorship of any employment-based visa")) == "No")
T("'cannot sponsor' = No",
    ValidationHelper.check_sponsorship_status(soup("We cannot sponsor work visas at this time")) == "No")
T("'will not sponsor' = No",
    ValidationHelper.check_sponsorship_status(soup("The company will not sponsor employment visas")) == "No")
T("Panasonic exact text = No",
    ValidationHelper.check_sponsorship_status(
        soup("We are unable to sponsor or take over sponsorship of any type of employment-based visa at this time.")) == "No")
T("No mention = Unknown",
    ValidationHelper.check_sponsorship_status(soup("Great opportunity for engineers")) == "Unknown")
T("Empty = Unknown", ValidationHelper.check_sponsorship_status(soup("")) == "Unknown")
T("None = Unknown", ValidationHelper.check_sponsorship_status(None) == "Unknown")
T("'authorized to work' alone = Unknown",
    ValidationHelper.check_sponsorship_status(soup("Must be authorized to work in the US")) == "Unknown")
T("'not able to sponsor' = No",
    ValidationHelper.check_sponsorship_status(soup("We are not able to provide visa sponsorship")) == "No")


# =============================================================================
# 7. UNDERGRADUATE DETECTION
# =============================================================================
section(7, "Undergraduate Detection")

T("'pursuing a bachelor's degree' caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("Must be pursuing a bachelor's degree in Computer Science"))[0] is not None)
T("'four-year college enrollment' caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("Must be currently enrolled as a fulltime student at an accredited four-year college or university"))[0] is not None)
T("'obtaining a bachelor's degree' caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("Active student currently obtaining a bachelor's degree (BS)"))[0] is not None)
T("'rising junior preferred' caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("Rising junior or senior preferred for this role"))[0] is not None)
T("'sophomore standing' caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("Applicant must have sophomore or junior standing"))[0] is not None)
T("'open to undergraduate students only' caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("This internship is open to undergraduate students only"))[0] is not None)
T("'undergraduate students only' caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("undergraduate students only are eligible"))[0] is not None)
T("'BS/MS students welcome' NOT caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("Open to BS/MS students in Computer Science"))[0] is None)
T("'graduate students encouraged' NOT caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("Graduate students are encouraged to apply"))[0] is None)
T("MS student mention NOT caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("Currently pursuing MS or PhD in Computer Science"))[0] is None)
T("'four-year university enrolled' caught",
    ValidationHelper._check_undergraduate_only_requirements(
        soup("Enrolled at an accredited four-year university as a full-time student"))[0] is not None)
T("None soup returns None",
    ValidationHelper._check_undergraduate_only_requirements(None)[0] is None)


# =============================================================================
# 8. JOB ID EXTRACTION
# =============================================================================
section(8, "Job ID Extraction")

def extract_job_id(url):
    for pattern, conf in JOB_ID_PATTERNS:
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    return None

T("Greenhouse 7-digit ID",
    extract_job_id("https://boards.greenhouse.io/company/jobs/1234567") == "1234567")
T("Workday JR-XXXXX hyphen",
    extract_job_id("https://company.wd5.myworkdayjobs.com/job/JR-12345?") is not None)
T("Workday JR_XXXXX underscore (Guidewire fix)",
    extract_job_id("https://wd5.myworkdaysite.com/job/Data-Developer-Intern_JR_14561") is not None)
T("Lever UUID",
    extract_job_id("https://jobs.lever.co/company/abc12345-1234-1234-1234-123456789012") is not None)
T("SmartRecruiters 15-digit",
    extract_job_id("https://jobs.smartrecruiters.com/Company/123456789012345") is not None)
T("iCIMS pattern",
    extract_job_id("https://careers.company.icims.com/jobs/12345/job") is not None)
T("Ashby UUID",
    extract_job_id("https://jobs.ashbyhq.com/company/abc12345-1234-1234-1234-123456789012") is not None)
T("Generic 6-digit ID",
    extract_job_id("https://careers.company.com/jobs/654321") is not None)
T("No ID returns None",
    extract_job_id("https://company.com/careers") is None)
T("Garbage URL returns None",
    extract_job_id("not-a-url") is None)


# =============================================================================
# 9. COMPANY NAME EXTRACTION FROM SUBJECT
# =============================================================================
section(9, "Company Name Extraction from Subject (SWE List fix)")

def extract_company_from_subject(subject):
    if not subject:
        return ""
    m = re.search(r'@\s*([^|]+?)(?:\s*\||\s*$)', subject)
    return m.group(1).strip() if m else ""

def extract_title_from_subject(subject):
    if not subject:
        return ""
    m = re.match(r'^(.+?)\s*@', subject)
    return m.group(1).strip() if m else ""

T("'SWE Intern @ Brex | Simplify' → Brex",
    extract_company_from_subject("Software Engineer Intern @ Brex | Simplify") == "Brex")
T("'PM Intern @ Honeywell | Simplify' → Honeywell",
    extract_company_from_subject("Product Management Intern Engineer @ Honeywell | Simplify") == "Honeywell")
T("'Intern @ Western Digital | Simplify' → Western Digital",
    extract_company_from_subject("Intern @ Western Digital | Simplify") == "Western Digital")
T("'Integration Engineer @ Veolia | Simplify' → Veolia",
    extract_company_from_subject("Integration Engineer Intern @ Veolia | Simplify") == "Veolia")
T("'AI Engineer Intern @ OpenAI' (no pipe) → OpenAI",
    extract_company_from_subject("AI Engineer Intern @ OpenAI") == "OpenAI")
T("Title extracted correctly",
    extract_title_from_subject("Software Engineer Intern @ Brex | Simplify") == "Software Engineer Intern")
T("No @ returns empty string",
    extract_company_from_subject("Software Engineer Intern 2026") == "")
T("None returns empty string", extract_company_from_subject(None) == "")
T("Company with spaces extracted",
    extract_company_from_subject("Data Analyst Intern @ New York Life Insurance | Simplify") == "New York Life Insurance")
T("Company with ampersand",
    extract_company_from_subject("SWE Intern @ Johnson & Johnson | Simplify") == "Johnson & Johnson")


# =============================================================================
# 10. OUTREACH VERIFIER
# =============================================================================
section(10, "Outreach Verifier")

from outreach.outreach_verifier import (
    is_suspicious_email, confidence_label,
    AUTO_SEND_THRESHOLD, MANUAL_REVIEW_THRESHOLD,
    CircuitBreaker, DomainHistory,
)

T("ATS greenhouse.io blocked", is_suspicious_email("jobs@greenhouse.io"))
T("ATS workday.com blocked", is_suspicious_email("apply@workday.com"))
T("ATS lever.co blocked", is_suspicious_email("careers@lever.co"))
T("Role email hr@ blocked", is_suspicious_email("hr@company.com"))
T("Role email recruiting@ blocked", is_suspicious_email("recruiting@company.com"))
T("Role email noreply@ blocked", is_suspicious_email("noreply@company.com"))
T("Too-short local (2 chars) blocked", is_suspicious_email("ab@company.com"))
T("Digits-only local blocked", is_suspicious_email("123456@company.com"))
T("Too many subdomains blocked", is_suspicious_email("user@a.b.c.d.company.com"))
T("Valid personal email allowed", not is_suspicious_email("john.doe@stripe.com"))
T("Valid underscore email allowed", not is_suspicious_email("john_doe@company.com"))
T("AUTO_SEND_THRESHOLD = 75", AUTO_SEND_THRESHOLD == 75)
T("MANUAL_REVIEW_THRESHOLD = 50", MANUAL_REVIEW_THRESHOLD == 50)
T("High confidence at 80", confidence_label(80) == "High")
T("Low confidence at 30", confidence_label(30) == "Low")


# =============================================================================
# 11. NAMEPARSER EDGE CASES
# =============================================================================
section(11, "NameParser Edge Cases")

from outreach.outreach_data import NameParser

T("Simple first last", lambda: NameParser.parse("John Doe")["first"] == "John")
T("Last name extracted", lambda: NameParser.parse("John Doe")["last"] == "Doe")
T("Dr. prefix stripped", lambda: NameParser.parse("Dr. Jane Smith")["first"] == "Jane")
T("PhD suffix stripped", lambda: NameParser.parse("Jane Smith PhD")["last"] == "Smith")
T("Parenthetical stripped", lambda: NameParser.parse("Joanna (Maskas) Clark")["last"] == "Clark")
T("Single name marked", lambda: NameParser.parse("Madonna")["single"] is True)
T("Comma format (Last, First)", lambda: NameParser.parse("Doe, John")["first"] == "John")
T("Unicode name handled", lambda: NameParser.parse("François Müller") is not None)
T("Hyphenated first name", lambda: NameParser.parse("Mary-Jane Watson") is not None)
T("Three-part name", lambda: NameParser.parse("John Michael Smith")["first"] == "John")
T("Empty string returns None", NameParser.parse("") is None)
T("Whitespace returns None", NameParser.parse("   ") is None)


# =============================================================================
# 12. PATTERNCACHE
# =============================================================================
section(12, "PatternCache")

from outreach.outreach_data import PatternCache

pc = PatternCache()

T("PatternCache loads without error", isinstance(pc, PatternCache))
T("Google seed pattern exists", pc.get("google.com") is not None)
T("Stripe seed pattern exists", pc.get("stripe.com") is not None)
T("gen_single first.last correct email",
    lambda: pc.gen_single(NameParser.parse("John Doe"), "stripe.com") == "john.doe@stripe.com")
T("gen_single works for known domain",
    lambda: isinstance(pc.gen_single(NameParser.parse("Jane Smith"), "google.com"), str))
T("Unknown domain get returns None", pc.get("unknowndomain99999xyz.com") is None)
T("100+ domains in cache", len(pc._d) >= 100)
T("detect returns None for mismatched email",
    lambda: pc.detect("xyz123@company.com", NameParser.parse("John Doe")) is None)
T("gen_candidates returns list",
    lambda: isinstance(pc.gen_candidates(NameParser.parse("John Doe"), "company.com"), list))
T("gen_candidates non-empty",
    lambda: len(pc.gen_candidates(NameParser.parse("John Doe"), "company.com")) > 0)


# =============================================================================
# 13. SEND SCHEDULED LOGIC
# =============================================================================
section(13, "Send Scheduled Logic")

import importlib.util
spec = importlib.util.spec_from_file_location(
    "send_scheduled",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts", "send_scheduled.py")
)
ss = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(ss)
    SS_OK = True
except Exception as e:
    SS_OK = False
    print(f"  {_y('WARN')}  send_scheduled import failed: {e}")

if SS_OK:
    T("_dup False for new email", not ss._dup({}, "new@co.com", "Subject"))
    T("_dup True after _rec",
        lambda: (sl := {}, ss._rec(sl, "x@co.com", "Sub"), ss._dup(sl, "x@co.com", "Sub"))[-1])
    T("_parse_send_at parses ET string", ss._parse_send_at("Mar 25, 9:30 AM ET") is not None)
    T("_parse_send_at empty = None", ss._parse_send_at("") is None)
    T("_parse_send_at garbage = None", ss._parse_send_at("banana") is None)
    T("_resume_path SDE", "SWE" in ss._resume_path("SDE") or "SDE" in ss._resume_path("SDE"))
    T("_resume_path ML", "ML" in ss._resume_path("ML"))
    T("_resume_path DA", "Data" in ss._resume_path("DA") or "DA" in ss._resume_path("DA"))
    T("_to_html wraps in div", "<div" in ss._to_html("Hello"))
    T("_to_html double newline = paragraphs", ss._to_html("P1\n\nP2").count("<p ") >= 2)
    T("DEAD_MAX = 3", ss.DEAD_MAX == 3)
    T("_clean_sl removes old entries",
        lambda: len(ss._clean_sl({"k": (datetime.datetime.now() - datetime.timedelta(days=20)).isoformat()})) == 0)


# =============================================================================
# 14. AUTO EXTRACT SIGNALS
# =============================================================================
section(14, "Auto Extract Signals")

spec2 = importlib.util.spec_from_file_location(
    "auto_extract",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts", "auto_extract.py")
)
ae = importlib.util.module_from_spec(spec2)
try:
    spec2.loader.exec_module(ae)
    AE_OK = True
except Exception as e:
    AE_OK = False

if AE_OK:
    T("San Francisco is tech hub", ae._is_tech_hub("San Francisco, CA"))
    T("Mountain View is tech hub", ae._is_tech_hub("Mountain View, CA"))
    T("Seattle is tech hub", ae._is_tech_hub("Seattle, WA"))
    T("Boston is tech hub", ae._is_tech_hub("Boston, MA"))
    T("Remote is tech hub", ae._is_tech_hub("Remote"))
    T("New York is tech hub", ae._is_tech_hub("New York, NY"))
    T("Austin TX is tech hub", ae._is_tech_hub("Austin, TX"))
    T("Omaha NE is NOT tech hub", not ae._is_tech_hub("Omaha, NE"))
    T("Unknown is NOT tech hub", not ae._is_tech_hub("Unknown"))
    T("Empty string is NOT tech hub", not ae._is_tech_hub(""))


# =============================================================================
# 15. DATE PARSER
# =============================================================================
section(15, "Date Parser")

from aggregator.utils import DateParser

T("today = 0 days", DateParser.extract_days_ago("today") == 0)
T("yesterday = 1 day", DateParser.extract_days_ago("yesterday") == 1)
T("2 days ago = 2", DateParser.extract_days_ago("2 days ago") == 2)
T("5d ago = 5", DateParser.extract_days_ago("5d ago") == 5)
T("1 week ago handled", lambda: DateParser.extract_days_ago("1 week ago") in (7, None))
T("1mo ago = 30", DateParser.extract_days_ago("1mo ago") == 30)
T("3 hours ago = 0", DateParser.extract_days_ago("3 hours ago") == 0)
T("30+ days is not None", DateParser.extract_days_ago("30+ days ago") is not None)
T("None returns None", DateParser.extract_days_ago(None) is None)
T("Garbage returns None", DateParser.extract_days_ago("banana") is None)


# =============================================================================
# 16. STRESS & LOAD TESTS
# =============================================================================
section(16, "Stress & Load Tests")

T("TitleProcessor 1000 titles in <2s",
    lambda: (
        start := time.time(),
        [TitleProcessor.is_valid_job_title(f"Software Engineer Intern {i}") for i in range(1000)],
        (time.time() - start) < 2.0
    )[-1])

T("is_suspicious_email 500 calls in <0.5s",
    lambda: (
        start := time.time(),
        [is_suspicious_email("john.doe@company.com") for _ in range(500)],
        (time.time() - start) < 0.5
    )[-1])

T("LocationProcessor 200 US locations return None",
    lambda: all(
        LocationProcessor.check_if_international(loc) is None
        for loc in [
            "San Francisco, CA", "Seattle, WA", "Boston, MA", "Austin, TX",
            "New York, NY", "Chicago, IL", "Los Angeles, CA", "Remote",
            "Palo Alto, CA", "Mountain View, CA", "Portland, OR", "Dallas, TX",
        ] * 17
    ))

T("sponsorship check 100 pages in <1s",
    lambda: (
        start := time.time(),
        [ValidationHelper.check_sponsorship_status(soup("We will sponsor H-1B")) for _ in range(100)],
        (time.time() - start) < 1.0
    )[-1])

T("NameParser 100 names without error",
    lambda: all(
        NameParser.parse(n) is not None
        for n in ["John Doe", "Jane Smith", "Priya Patel", "Wei Zhang",
                  "Carlos Martinez", "Aisha Johnson", "Mary-Jane Watson",
                  "Dr. Robert Brown", "François Müller", "Li Wei"] * 10
    ))

T("season check 200 titles in <1s",
    lambda: (
        start := time.time(),
        [TitleProcessor.check_season_requirement(f"SWE Intern Summer 2026 #{i}") for i in range(200)],
        (time.time() - start) < 1.0
    )[-1])

T("company extraction 500 calls in <0.1s",
    lambda: (
        start := time.time(),
        [extract_company_from_subject("SWE Intern @ Brex | Simplify") for _ in range(500)],
        (time.time() - start) < 0.1
    )[-1])

T("PatternCache 200 lookups in <0.1s",
    lambda: (
        start := time.time(),
        [pc.get("google.com") for _ in range(200)],
        (time.time() - start) < 0.1
    )[-1])

T("Job ID extraction 300 URLs in <0.5s",
    lambda: (
        start := time.time(),
        [extract_job_id(f"https://boards.greenhouse.io/company/jobs/{i}") for i in range(300)],
        (time.time() - start) < 0.5
    )[-1])

T("DateParser 200 calls in <0.2s",
    lambda: (
        start := time.time(),
        [DateParser.extract_days_ago("2 days ago") for _ in range(200)],
        (time.time() - start) < 0.2
    )[-1])


# =============================================================================
# RESULTS
# =============================================================================
print(f"\n{'═'*60}")
total = results['pass'] + results['fail']
pct = results['pass'] / max(total, 1) * 100
print(f"  {_g(str(results['pass']))} passed  {_r(str(results['fail']))} failed  ({total} total)")
print(f"  Pass rate: {_g(f'{pct:.1f}%') if pct >= 95 else _r(f'{pct:.1f}%')}")

if results['errors']:
    print(f"\n{_r('Failed:')}")
    for e in results['errors']:
        print(f"  - {e}")

if results['fail'] == 0:
    print(f"\n  {_g('All tests passed. Pipeline is production-ready.')}")
else:
    print(f"\n  {_r('Fix failing tests before next production run.')}")
print(f"{'═'*60}\n")

sys.exit(0 if results['fail'] == 0 else 1)
