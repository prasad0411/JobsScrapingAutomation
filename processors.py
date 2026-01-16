#!/usr/bin/env python3

import re
import json
from functools import lru_cache
from collections import Counter

from config import (
    US_ZIPCODE_AVAILABLE,
    VALIDATORS_AVAILABLE,
    DATEUTIL_AVAILABLE,
    TLDEXTRACT_AVAILABLE,
    CANADA_PROVINCES,
    CANADA_PROVINCE_NAMES,
    MAJOR_CANADIAN_CITIES,
    AMBIGUOUS_CITIES,
    US_CONTEXT_KEYWORDS,
    CANADA_CONTEXT_KEYWORDS,
    PLATFORM_CONFIGS,
    JOB_ID_PATTERNS,
    LOCATION_SELECTORS,
    LOCATION_METADATA_PATTERNS,
    HTML_ARTIFACT_PATTERNS,
    INVALID_LOCATION_KEYWORDS,
    DEPARTMENT_KEYWORDS,
    WORKDAY_HQ_CODES,
    URL_TO_COMPANY_MAPPING,
    COMPANY_SLUG_MAPPING,
    COMPANY_PLACEHOLDERS,
    MIN_CONFIDENCE_JOB_ID,
    MIN_CONFIDENCE_LOCATION,
    MIN_CONFIDENCE_COMPANY,
    get_state_for_city,
    validate_us_state_code,
    get_canadian_province,
    extract_domain_and_subdomain,
    is_valid_url,
    normalize_unicode,
)

from utils import (
    ExtractionResult,
    ExtractionVoter,
    CompanyNormalizer,
    CompanyValidator,
    PlatformDetector,
)

_COMPILED_JOB_ID_PATTERNS = [(re.compile(p, re.I), c) for p, c in JOB_ID_PATTERNS]
_COMPILED_URL_COMPANY_PATTERNS = [
    (re.compile(p, re.I), c) for p, c in URL_TO_COMPANY_MAPPING.items()
]
_COMPILED_METADATA_PATTERNS = [re.compile(p, re.I) for p in LOCATION_METADATA_PATTERNS]
_COMPILED_ARTIFACT_PATTERNS = [re.compile(p) for p in HTML_ARTIFACT_PATTERNS]

_TITLE_CLEAN_PATTERN = re.compile(r"\s*[\(\[].+?[\)\]]|\s*\([^)]*$|\s*\[[^\]]*$")
_SEASON_PATTERN = re.compile(r"\s*-?\s*(Summer|Fall|Spring|Winter)\s*20\d{2}", re.I)
_YEAR_PATTERN = re.compile(r"\s*-?\s*20\d{2}\s*-?\s*")
_DEGREE_PATTERN = re.compile(
    r"\s*[\(\[]\s*(BS/MS|MS|PhD|Bachelor|Master).*?[\)\]]", re.I
)
_CITY_STATE_PATTERN = re.compile(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s*([A-Z]{2})\b")
_LOCATION_LABEL_PATTERN = re.compile(
    r"Location\s*:?\s*([A-Za-z\s,]+(?:,\s*[A-Z]{2})?)", re.I
)


class TitleProcessor:
    @staticmethod
    @lru_cache(maxsize=512)
    def clean_title_aggressive(title):
        if not title or len(title) < 5:
            return title

        original = title
        title = _TITLE_CLEAN_PATTERN.sub("", title)
        title = _SEASON_PATTERN.sub("", title)
        title = _YEAR_PATTERN.sub(" ", title)
        title = _DEGREE_PATTERN.sub("", title)
        title = re.sub(r"\s+", " ", title).strip().strip("-")

        return title if len(title) >= 5 else original

    @staticmethod
    @lru_cache(maxsize=256)
    def is_valid_job_title(title):
        if not title or len(title) < 5:
            return False, "Title too short"

        excluded = {"application", "click here", "apply now", "view job", "submit your"}
        if any(phrase in title.lower() for phrase in excluded):
            return False, "Invalid title pattern"

        return True, None

    @staticmethod
    @lru_cache(maxsize=256)
    def is_cs_engineering_role(title):
        keywords = {
            "software",
            "engineer",
            "developer",
            "data",
            "ml",
            "ai",
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
            "automation",
        }
        return any(kw in title.lower() for kw in keywords)

    @staticmethod
    @lru_cache(maxsize=256)
    def is_internship_role(title):
        title_lower = title.lower()

        if not any(kw in title_lower for kw in ["intern", "co-op", "coop"]):
            return False, "Not internship/co-op role"

        excluded = {
            "senior",
            "sr.",
            "sr ",
            "staff",
            "principal",
            "lead",
            "architect",
            "director",
            "manager",
        }
        for level in excluded:
            if level in title_lower:
                return False, f"Senior/experienced role: contains '{level}'"

        return True, None

    @staticmethod
    def check_season_requirement(title, page_text=""):
        combined = (title + " " + page_text).lower()

        multi_season = re.search(
            r"(spring.*summer|summer.*spring|spring/summer)", combined, re.I
        )
        if multi_season:
            return True, ""

        wrong_patterns = [
            (r"fall\s*20\d{2}", "Fall"),
            (r"winter\s*20\d{2}", "Winter"),
            (r"spring\s*20(?:2[0-5])", "Spring"),
        ]

        for pattern, season_name in wrong_patterns:
            if re.search(pattern, combined, re.I):
                if "winter" in season_name.lower() and re.search(
                    r"winter.*2025/2026", combined, re.I
                ):
                    return True, ""
                if "spring" in season_name.lower() and re.search(
                    r"summer", combined, re.I
                ):
                    return True, ""
                return False, f"Wrong season: {season_name}"

        return True, ""


class JobIDExtractor:
    @staticmethod
    def extract_from_url(url):
        if not url:
            return ExtractionResult(None, 0.0, "url_extract")

        for pattern, confidence in _COMPILED_JOB_ID_PATTERNS:
            match = pattern.search(url)
            if match:
                job_id = match.group(1).strip()
                if JobIDExtractor._is_valid_id(job_id):
                    return ExtractionResult(job_id, confidence, "url_pattern")

        return ExtractionResult(None, 0.0, "url_extract")

    @staticmethod
    def extract_from_html_meta(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "html_meta")

        meta_selectors = [
            ("meta", {"property": "og:job:id"}),
            ("meta", {"name": "job-id"}),
            ("meta", {"property": "job:id"}),
        ]

        for tag_name, attrs in meta_selectors:
            elem = soup.find(tag_name, attrs)
            if elem:
                value = elem.get("content") or elem.get("value")
                if value and JobIDExtractor._is_valid_id(value):
                    return ExtractionResult(value, 0.95, "html_meta")

        data_elems = soup.find_all(attrs={"data-job-id": True})
        for elem in data_elems:
            value = elem.get("data-job-id")
            if value and JobIDExtractor._is_valid_id(value):
                return ExtractionResult(value, 0.90, "html_data_attr")

        return ExtractionResult(None, 0.0, "html_meta")

    @staticmethod
    def extract_from_json_ld(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "json_ld")

        json_ld = soup.find("script", {"type": "application/ld+json"})
        if json_ld:
            try:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    identifier = data.get("identifier", {})
                    if isinstance(identifier, dict):
                        value = identifier.get("value", "")
                        if value and JobIDExtractor._is_valid_id(value):
                            return ExtractionResult(value, 0.95, "json_ld")
            except:
                pass

        return ExtractionResult(None, 0.0, "json_ld")

    @staticmethod
    def extract_from_page_text(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "page_text")

        page_text = soup.get_text()[:5000]
        labeled_patterns = [
            (r"Job\s*Code\s*:?\s*([A-Z0-9]{4,15})\b", 0.90),
            (r"Job\s*ID\s*:?\s*([A-Z0-9\-]{4,15})\b", 0.85),
            (r"Req(?:uisition)?\s*ID\s*:?\s*([A-Z0-9\-]{4,20})\b", 0.85),
        ]

        for pattern, confidence in labeled_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                job_id = match.group(1).strip()
                if JobIDExtractor._is_valid_id(job_id):
                    return ExtractionResult(job_id, confidence, "page_text_labeled")

        return ExtractionResult(None, 0.0, "page_text")

    @staticmethod
    @lru_cache(maxsize=512)
    def _is_valid_id(job_id):
        if not job_id or len(job_id) < 4 or len(job_id) > 20:
            return False
        if not re.match(r"^[A-Z0-9\-_]+$", job_id, re.I):
            return False
        false_positives = {
            "APPLY",
            "NOW",
            "HERE",
            "JOIN",
            "CLICK",
            "VIEW",
            "SOFTWARE",
            "ENGINEER",
        }
        return job_id.upper() not in false_positives

    @staticmethod
    def extract_all_methods(url, soup):
        results = [
            JobIDExtractor.extract_from_url(url),
            JobIDExtractor.extract_from_html_meta(soup),
            JobIDExtractor.extract_from_json_ld(soup),
            JobIDExtractor.extract_from_page_text(soup),
        ]

        best_result = ExtractionVoter.vote(
            results, min_confidence=MIN_CONFIDENCE_JOB_ID
        )

        if best_result:
            return best_result.value

        import hashlib

        return f"HASH_{hashlib.md5(url.encode()).hexdigest()[:10]}"


class LocationExtractor:
    @staticmethod
    def extract_from_title(title):
        if not title:
            return ExtractionResult(None, 0.0, "title_parse")

        match = re.search(r"[-–]\s*([A-Za-z\s]+,\s*[A-Z]{2})(?:\s|$)", title)
        if match:
            return ExtractionResult(match.group(1).strip(), 0.80, "title_city_state")

        if re.search(r"\(remote\)", title, re.I):
            return ExtractionResult("Remote", 0.90, "title_remote")

        return ExtractionResult(None, 0.0, "title_parse")

    @staticmethod
    def extract_from_html_selectors(soup, platform="generic"):
        if not soup:
            return ExtractionResult(None, 0.0, "html_selectors")

        selectors = PLATFORM_CONFIGS.get(platform, {}).get(
            "location_selectors", LOCATION_SELECTORS
        )

        for selector, confidence in selectors:
            try:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if 2 < len(text) < 100:
                        cleaned = LocationProcessor.clean_location_aggressive(text)
                        if cleaned and cleaned != "Unknown":
                            return ExtractionResult(
                                cleaned, confidence, "html_selector"
                            )
            except:
                continue

        return ExtractionResult(None, 0.0, "html_selectors")

    @staticmethod
    def extract_from_json_ld(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "json_ld")

        json_ld = soup.find("script", {"type": "application/ld+json"})
        if json_ld:
            try:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    job_location = data.get("jobLocation", {})
                    if isinstance(job_location, dict):
                        address = job_location.get("address", {})
                        if isinstance(address, dict):
                            city = address.get("addressLocality", "")
                            state = address.get("addressRegion", "")
                            if city and state and len(state) == 2:
                                return ExtractionResult(
                                    f"{city}, {state}", 0.95, "json_ld"
                                )
            except:
                pass

        return ExtractionResult(None, 0.0, "json_ld")

    @staticmethod
    def extract_from_page_text(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "page_text")

        page_text = soup.get_text()[:3000]
        match = _LOCATION_LABEL_PATTERN.search(page_text)
        if match:
            location = match.group(1).strip()
            if len(location) < 50:
                cleaned = LocationProcessor.clean_location_aggressive(location)
                if cleaned and cleaned != "Unknown":
                    return ExtractionResult(cleaned, 0.75, "page_text_labeled")

        return ExtractionResult(None, 0.0, "page_text")

    @staticmethod
    def extract_from_url(url):
        if not url:
            return ExtractionResult(None, 0.0, "url_parse")

        patterns = [
            r"/job/([A-Z][a-z]+(?:-[A-Z][a-z]+)*)-([A-Z]{2})(?:-USA)?/",
            r"/([A-Z][a-z]+(?:-[A-Z][a-z]+)*)-([A-Z]{2})/",
            r"/job/([A-Z][a-z]+-[A-Z]{2})/",
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                if len(match.groups()) == 2:
                    city, state = match.groups()
                    city = city.replace("-", " ")
                    if validate_us_state_code(state):
                        return ExtractionResult(f"{city}, {state}", 0.75, "url_path")
                else:
                    location = match.group(1).replace("-", ", ")
                    return ExtractionResult(location, 0.65, "url_path")

        return ExtractionResult(None, 0.0, "url_parse")

    @staticmethod
    def extract_all_methods(url, soup, title="", platform="generic"):
        results = [
            LocationExtractor.extract_from_json_ld(soup),
            LocationExtractor.extract_from_html_selectors(soup, platform),
            LocationExtractor.extract_from_title(title),
            LocationExtractor.extract_from_page_text(soup),
            LocationExtractor.extract_from_url(url),
        ]

        best_result = ExtractionVoter.vote(
            results, min_confidence=MIN_CONFIDENCE_LOCATION
        )

        if best_result:
            return LocationProcessor.format_location_clean(best_result.value)

        return "Unknown"


class LocationProcessor:
    @staticmethod
    @lru_cache(maxsize=512)
    def clean_location_aggressive(location_text):
        if not location_text or len(location_text) < 2:
            return "Unknown"

        location = location_text.strip()

        for pattern in _COMPILED_METADATA_PATTERNS:
            location = pattern.sub("", location)

        for pattern in _COMPILED_ARTIFACT_PATTERNS:
            location = pattern.sub("", location)

        location = re.sub(r"\s*\([^)]*\)", "", location)
        location = re.sub(r"\s*-\s*[A-Z]{2,4}$", "", location)

        stop_keywords = [
            "Employment",
            "Type",
            "Details",
            "Program",
            "Internship",
            "Job",
            "Position",
            "Careers",
        ]
        for keyword in stop_keywords:
            if keyword in location:
                location = location.split(keyword)[0].strip()
                break

        location = re.sub(
            r",?\s*\d{3,5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?", "", location
        )
        location = re.sub(r"\s*\d{5}(-\d{4})?\s*", "", location)
        location = re.sub(r",?\s*(?:USA|United States)\s*", "", location, flags=re.I)

        match = _CITY_STATE_PATTERN.search(location)
        if match:
            city, state = match.group(1).strip(), match.group(2).upper()
            if validate_us_state_code(state):
                return f"{city}, {state}"

        location = re.sub(r"\s+", " ", location).strip()

        if not location or len(location) < 2:
            return "Unknown"

        if location.lower() in DEPARTMENT_KEYWORDS:
            return "Unknown"

        if any(kw in location.lower() for kw in INVALID_LOCATION_KEYWORDS):
            return "Unknown"

        if US_ZIPCODE_AVAILABLE:
            state = get_state_for_city(location)
            if state:
                return f"{location.title()}, {state}"

        return location

    @staticmethod
    @lru_cache(maxsize=512)
    def format_location_clean(location):
        if not location or location == "Unknown":
            return "Unknown"

        if location in ["Remote", "Hybrid"]:
            return location

        if location in WORKDAY_HQ_CODES:
            city, state = WORKDAY_HQ_CODES[location]
            if state != "UNKNOWN":
                return f"{city}, {state}"

        location = LocationProcessor.clean_location_aggressive(location)

        match = re.search(r"^([A-Z]{2})\s+(.+)$", location)
        if match:
            state, rest = match.groups()
            if validate_us_state_code(state):
                return f"{rest.strip()}, {state}"

        location = re.sub(r"^[A-Z]{2,4}[:_-]\s*", "", location)

        if location in {"Headquarters", "Office", "Campus"}:
            return "Unknown"

        match = _CITY_STATE_PATTERN.search(location)
        if match:
            city, state = match.group(1).strip(), match.group(2).upper()
            if validate_us_state_code(state):
                return f"{city}, {state}"

        return location

    @staticmethod
    def extract_remote_status_enhanced(soup, location, url):
        if not soup:
            return "Unknown"

        if location:
            location_lower = location.lower()
            if "remote" in location_lower:
                return "Remote"
            if "hybrid" in location_lower:
                return "Hybrid"

        page_text = soup.get_text()[:2000].lower()

        if "100% remote" in page_text or "fully remote" in page_text:
            return "Remote"
        if "remote" in page_text[:500]:
            return "Remote"
        if "hybrid" in page_text[:500]:
            return "Hybrid"
        if "on-site" in page_text[:500] or "onsite" in page_text[:500]:
            return "On Site"

        return "Unknown"

    @staticmethod
    def check_if_international(location, soup=None):
        if not location or location == "Unknown":
            if soup:
                return LocationProcessor._check_page_for_canada(soup)
            return None

        location_lower = location.lower()
        normalized = (
            normalize_unicode(location_lower)
            if hasattr(LocationProcessor, "normalize_unicode")
            else location_lower
        )

        for full_name, code in CANADA_PROVINCE_NAMES.items():
            if re.search(rf"\b{full_name}\b", location_lower, re.I):
                if full_name == "ontario" and ", ca" in location_lower:
                    continue
                return f"Location: Canada ({full_name.title()})"

        for province in CANADA_PROVINCES:
            patterns = [rf",\s*{province}\b", rf"\s+-\s+{province}\b"]
            for pattern in patterns:
                if re.search(pattern, location):
                    if province == "ON" and ", ca" in location_lower:
                        continue
                    return f"Location: Canada (province: {province})"

        if ", CA" in location or " - CA" in location:
            canada_indicators = ["toronto", "ottawa", "montreal", "canada"]
            if any(ind in location_lower for ind in canada_indicators):
                if "ontario, ca" not in location_lower:
                    return "Location: Canada (CA in Canadian context)"

        for city, label in MAJOR_CANADIAN_CITIES.items():
            if city in normalized:
                return f"Location: Canada ({city.title()}, {label})"

        if "canada" in location_lower:
            return "Location: Canada"

        if any(kw in location_lower for kw in ["uk", "london", "india", "china"]):
            return "Location: International"

        if soup:
            return LocationProcessor._check_page_for_canada(soup)

        return None

    @staticmethod
    def _check_page_for_canada(soup):
        if not soup:
            return None

        meta_desc = soup.find("meta", {"property": "og:description"})
        if meta_desc:
            content = meta_desc.get("content", "").lower()
            if content == "canada" or "canada only" in content:
                return "Location: Canada (from meta tag)"

        page_text = soup.get_text()[:10000]
        canadian_patterns = [
            (r"Ottawa\s*,?\s*Ontario", "Ottawa, Ontario"),
            (r"Toronto\s*,?\s*Ontario", "Toronto, Ontario"),
            (r"Montreal\s*,?\s*Quebec", "Montreal, Quebec"),
        ]

        for pattern, city_name in canadian_patterns:
            if re.search(pattern, page_text, re.I):
                return f"Location: Canada ({city_name})"

        return None


class ValidationHelper:
    @staticmethod
    @lru_cache(maxsize=512)
    def is_valid_job_url(url):
        if not url or not url.startswith("http"):
            return False, "Invalid URL format"

        if VALIDATORS_AVAILABLE:
            if not is_valid_url(url):
                return False, "Invalid URL format"

        url_lower = url.lower()

        if "jobright.ai" in url_lower and "/jobs/info/" not in url_lower:
            return False, "Invalid Jobright URL"

        excluded = [
            "/unsubscribe",
            "/my-alerts",
            "/blog",
            "/terms",
            "/privacy",
            "chromewebstore.google.com",
        ]
        for pattern in excluded:
            if pattern in url_lower:
                return False, f"{pattern.split('/')[-1].title()} link"

        return True, None

    @staticmethod
    def check_url_for_international(url):
        if not url:
            return None

        url_lower = url.lower()
        canada_patterns = [
            "/montreal-quebec-can/",
            "/toronto-ontario/",
            "/ottawa-ontario/",
            "-quebec-can",
            "-ontario-can",
            "/can/",
            "canada/",
            ".ca/",
        ]

        for pattern in canada_patterns:
            if pattern in url_lower:
                return "International: Canada (from URL)"

        return None

    @staticmethod
    def check_page_restrictions(soup):
        if not soup:
            return None, None, []

        degree_decision, degree_reason = (
            ValidationHelper._check_degree_requirements_strict(soup)
        )
        if degree_decision == "REJECT":
            return degree_decision, degree_reason, []

        page_text = soup.get_text()[:15000].lower()

        citizenship_patterns = [
            r"u\.?s\.?\s+citizenship\s+(?:is\s+)?required",
            r"must be a u\.?s\.?\s+citizen",
            r"only u\.?s\.?\s+citizens\s+(?:are\s+)?eligible",
        ]

        for pattern in citizenship_patterns:
            if re.search(pattern, page_text, re.I):
                return "REJECT", "US citizenship required", []

        if re.search(r"us work authorization required", page_text, re.I):
            return "REJECT", "US work authorization required", []

        if re.search(
            r"(?:must be able to obtain|clearance.*required)", page_text, re.I
        ):
            return "REJECT", "Security clearance required", []

        return None, None, []

    @staticmethod
    def _check_degree_requirements_strict(soup):
        if not soup:
            return None, None

        page_text = soup.get_text()[:15000].lower()

        bs_patterns = [
            r"bachelor'?s?\s+(?:students?|degree|candidates?)\s+only",
            r"undergraduate\s+(?:students?|only)",
            r"currently\s+pursuing\s+a?\s+bachelor",
        ]

        for pattern in bs_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                start = max(0, match.start() - 300)
                end = min(len(page_text), match.end() + 300)
                context = page_text[start:end]

                if not any(
                    kw in context for kw in ["master", "ms/phd", "graduate student"]
                ):
                    return "REJECT", "Bachelor's students only"

        return None, None

    @staticmethod
    @lru_cache(maxsize=256)
    def validate_company_field(company, title, url):
        if not company or company == "Unknown" or not company.strip():
            return True, ValidationHelper.extract_company_from_domain(url), None

        company = company.strip()

        if not CompanyValidator.is_valid(company):
            return True, ValidationHelper.extract_company_from_domain(url), None

        if len(company) > 100:
            return False, company, "Company name too long"

        job_keywords = {"intern", "software", "engineer", "developer"}
        if sum(1 for kw in job_keywords if kw in company.lower()) >= 2:
            return False, company, "Company field contains job title"

        return True, company, None

    @staticmethod
    def clean_legal_entity(company):
        if not company:
            return company

        company = re.sub(r"^\d+\s+", "", company)
        company = re.sub(r"^[A-Z]{2,4}[-\s]", "", company)
        company = re.sub(
            r",?\s+(Inc\.?|LLC\.?|Corp\.?|Ltd\.?|Corporation)$", "", company, flags=re.I
        )

        return company.strip()

    @staticmethod
    @lru_cache(maxsize=256)
    def extract_company_from_domain(url):
        if not url:
            return "Unknown"

        if TLDEXTRACT_AVAILABLE:
            subdomain, domain = extract_domain_and_subdomain(url)
            if domain:
                domain_name = domain.split(".")[0]
                if domain_name in COMPANY_SLUG_MAPPING:
                    return COMPANY_SLUG_MAPPING[domain_name]
                return domain_name.replace("-", " ").title()

        match = re.search(r"https?://(?:www\.)?([^/]+)", url, re.I)
        if not match:
            return "Unknown"

        domain = match.group(1).lower()
        domain = re.sub(r"\.(com|org|net|io|ai|co|jobs|careers)$", "", domain)

        parts = domain.split(".")
        company = parts[-1] if len(parts) > 1 else parts[0]

        if company in COMPANY_SLUG_MAPPING:
            return COMPANY_SLUG_MAPPING[company]

        return company.replace("-", " ").replace("_", " ").title()

    @staticmethod
    def check_sponsorship_status(soup):
        if not soup:
            return "Unknown"

        page_text = soup.get_text()[:3000]

        if re.search(
            r"(?:will|does|provides?)\s+sponsor|h-?1b.*sponsor", page_text, re.I
        ):
            return "Yes"

        if re.search(r"(?:no|not|doesn't)\s+sponsor|no h-?1b", page_text, re.I):
            return "No"

        if re.search(
            r"(?:must have|requires?)\s+(?:us|u\.s\.)\s+work authorization",
            page_text,
            re.I,
        ):
            return "No"

        return "Unknown"


class CompanyExtractor:
    @staticmethod
    def extract_from_url_mapping(url):
        if not url:
            return ExtractionResult(None, 0.0, "url_mapping")

        for pattern, company_name in _COMPILED_URL_COMPANY_PATTERNS:
            if pattern.search(url):
                return ExtractionResult(company_name, 0.95, "url_mapping")

        return ExtractionResult(None, 0.0, "url_mapping")

    @staticmethod
    def extract_from_json_ld(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "json_ld")

        json_ld = soup.find("script", {"type": "application/ld+json"})
        if json_ld:
            try:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    org = data.get("hiringOrganization", {})
                    if isinstance(org, dict):
                        name = org.get("name", "")
                        if name and len(name) < 100:
                            cleaned = ValidationHelper.clean_legal_entity(name)
                            normalized = CompanyNormalizer.normalize(cleaned, "")
                            if normalized and CompanyValidator.is_valid(normalized):
                                return ExtractionResult(normalized, 0.93, "json_ld")
            except:
                pass

        return ExtractionResult(None, 0.0, "json_ld")

    @staticmethod
    def extract_from_meta_tags(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "meta_tags")

        meta = soup.find("meta", {"property": "og:site_name"})
        if meta and meta.get("content"):
            company = meta.get("content").strip()
            company = re.sub(r"\s*[-|]\s*(careers|jobs).*$", "", company, flags=re.I)
            if company and len(company) < 50:
                normalized = CompanyNormalizer.normalize(company, "")
                if normalized and CompanyValidator.is_valid(normalized):
                    return ExtractionResult(normalized, 0.90, "meta_tags")

        return ExtractionResult(None, 0.0, "meta_tags")

    @staticmethod
    def extract_from_visible_elements(soup, url):
        if not soup:
            return ExtractionResult(None, 0.0, "visible_elements")

        url_lower = url.lower() if url else ""

        if "icims.com" in url_lower:
            mobile_header = soup.find("div", id="mobile-header-container")
            if mobile_header:
                h1 = mobile_header.find("h1")
                if h1:
                    text = h1.get_text().strip()
                    normalized = CompanyNormalizer.normalize(text, url)
                    if normalized and CompanyValidator.is_valid(normalized):
                        return ExtractionResult(normalized, 0.88, "visible_icims")

        if "smartrecruiters" in url_lower:
            logo = soup.find("img", alt=re.compile(r"logo", re.I))
            if logo:
                alt = (
                    logo.get("alt", "")
                    .replace(" logo", "")
                    .replace(" Logo", "")
                    .strip()
                )
                if alt and len(alt) > 2:
                    normalized = CompanyNormalizer.normalize(alt, url)
                    if normalized and CompanyValidator.is_valid(normalized):
                        return ExtractionResult(normalized, 0.88, "visible_logo")

        header = soup.find(["header", "nav"])
        if header:
            h1 = header.find("h1")
            if h1:
                text = h1.get_text().strip()
                if text and not CompanyExtractor._looks_like_title(text):
                    normalized = CompanyNormalizer.normalize(text, url)
                    if normalized and CompanyValidator.is_valid(normalized):
                        return ExtractionResult(normalized, 0.82, "visible_header")

        return ExtractionResult(None, 0.0, "visible_elements")

    @staticmethod
    def extract_from_url_path(url, platform):
        if not url:
            return ExtractionResult(None, 0.0, "url_path")

        path_company = CompanyNormalizer.extract_from_url_path(url, platform)
        if path_company and CompanyValidator.is_valid(path_company):
            if not CompanyValidator.is_junk_subdomain(path_company.lower()):
                return ExtractionResult(path_company, 0.70, "url_path")

        return ExtractionResult(None, 0.0, "url_path")

    @staticmethod
    def extract_from_subdomain(url):
        if not url:
            return ExtractionResult(None, 0.0, "subdomain")

        subdomain, _ = extract_domain_and_subdomain(url)

        if subdomain and not CompanyValidator.is_junk_subdomain(subdomain):
            normalized = CompanyNormalizer.normalize(subdomain, url)
            if normalized and CompanyValidator.is_valid(normalized):
                return ExtractionResult(normalized, 0.40, "subdomain")

        return ExtractionResult(None, 0.0, "subdomain")

    @staticmethod
    @lru_cache(maxsize=256)
    def _looks_like_title(text):
        if not text:
            return False
        text_lower = text.lower()
        if any(p in text_lower for p in ["submit your", "sign in", "apply now"]):
            return True
        title_kw = {"intern", "co-op", "engineer", "developer", "software"}
        return sum(1 for kw in title_kw if kw in text_lower) >= 2

    @staticmethod
    def extract_all_methods(url, soup):
        platform = PlatformDetector.detect(url)

        results = [
            CompanyExtractor.extract_from_url_mapping(url),
            CompanyExtractor.extract_from_json_ld(soup),
            CompanyExtractor.extract_from_meta_tags(soup),
            CompanyExtractor.extract_from_visible_elements(soup, url),
            CompanyExtractor.extract_from_url_path(url, platform),
            CompanyExtractor.extract_from_subdomain(url),
        ]

        best_result = ExtractionVoter.vote(
            results, min_confidence=MIN_CONFIDENCE_COMPANY
        )

        if best_result:
            return best_result.value

        return ValidationHelper.extract_company_from_domain(url)


class QualityScorer:
    @staticmethod
    def calculate_score(job_data):
        score = 0

        company = job_data.get("company", "Unknown")
        if company and company not in ["Unknown", "N/A"] and len(company) > 2:
            score += 3

        location = job_data.get("location", "Unknown")
        if location and location != "Unknown":
            score += 2

        job_id = job_data.get("job_id", "N/A")
        if job_id and job_id != "N/A" and not job_id.startswith("HASH_"):
            score += 1

        title = job_data.get("title", "")
        if 15 < len(title) < 120:
            score += 1

        return score

    @staticmethod
    def is_acceptable_quality(score, min_score=4):
        return score >= min_score
