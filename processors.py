#!/usr/bin/env python3

import re
import json
import logging
from functools import lru_cache

from config import (
    CANADA_PROVINCES,
    CANADA_PROVINCE_NAMES,
    MAJOR_CANADIAN_CITIES,
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
    MIN_CONFIDENCE_JOB_ID,
    MIN_CONFIDENCE_LOCATION,
    MIN_CONFIDENCE_COMPANY,
    get_state_for_city,
    validate_us_state_code,
    normalize_unicode,
    extract_domain_and_subdomain,
    FULL_STATE_NAMES,
    CITY_ABBREVIATIONS,
    LOCATION_SUFFIXES,
    AMBIGUOUS_CITIES,
    US_CONTEXT_KEYWORDS,
    CANADA_CONTEXT_KEYWORDS,
)

from utils import (
    ExtractionResult,
    ExtractionVoter,
    CompanyNormalizer,
    CompanyValidator,
    PlatformDetector,
)

# ============================================================================
# Compiled Patterns (Performance Optimization)
# ============================================================================

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

# ============================================================================
# Title Processing
# ============================================================================


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

        if re.search(r"(spring.*summer|summer.*spring|spring/summer)", combined, re.I):
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


# ============================================================================
# Job ID Extraction
# ============================================================================


class JobIDExtractor:
    @staticmethod
    def extract_from_url(url):
        if not url:
            return ExtractionResult(None, 0.0, "url_extract")

        for pattern, confidence in _COMPILED_JOB_ID_PATTERNS:
            try:
                match = pattern.search(url)
                if match:
                    job_id = match.group(1).strip()
                    if JobIDExtractor._is_valid_id(job_id):
                        return ExtractionResult(job_id, confidence, "url_pattern")
            except Exception as e:
                logging.debug(f"Job ID pattern failed on URL {url}: {e}")
                continue

        return ExtractionResult(None, 0.0, "url_extract")

    @staticmethod
    def extract_from_html_meta(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "html_meta")

        try:
            for tag_name, attrs in [
                ("meta", {"property": "og:job:id"}),
                ("meta", {"name": "job-id"}),
            ]:
                elem = soup.find(tag_name, attrs)
                if elem:
                    value = elem.get("content") or elem.get("value")
                    if value and JobIDExtractor._is_valid_id(value):
                        return ExtractionResult(value, 0.95, "html_meta")

            for elem in soup.find_all(attrs={"data-job-id": True}):
                value = elem.get("data-job-id")
                if value and JobIDExtractor._is_valid_id(value):
                    return ExtractionResult(value, 0.90, "html_data_attr")
        except Exception as e:
            logging.debug(f"HTML meta extraction failed: {e}")

        return ExtractionResult(None, 0.0, "html_meta")

    @staticmethod
    def extract_from_json_ld(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "json_ld")

        try:
            json_ld = soup.find("script", {"type": "application/ld+json"})
            if json_ld:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    identifier = data.get("identifier", {})
                    if isinstance(identifier, dict) and identifier.get("value"):
                        value = identifier["value"]
                        if JobIDExtractor._is_valid_id(value):
                            return ExtractionResult(value, 0.95, "json_ld")
        except Exception as e:
            logging.debug(f"JSON-LD job ID extraction failed: {e}")

        return ExtractionResult(None, 0.0, "json_ld")

    @staticmethod
    def extract_from_page_text(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "page_text")

        try:
            page_text = soup.get_text()[:5000]
            for pattern, confidence in [
                (r"Job\s*Code\s*:?\s*([A-Z0-9]{4,15})\b", 0.90),
                (r"Job\s*ID\s*:?\s*([A-Z0-9\-]{4,15})\b", 0.85),
                (r"Req(?:uisition)?\s*ID\s*:?\s*([A-Z0-9\-]{4,20})\b", 0.85),
                (r"Role\s*ID\s*:?\s*([A-Z0-9\-]{4,20})\b", 0.85),
            ]:
                match = re.search(pattern, page_text, re.I)
                if match:
                    job_id = match.group(1).strip()
                    if JobIDExtractor._is_valid_id(job_id):
                        return ExtractionResult(job_id, confidence, "page_text_labeled")
        except Exception as e:
            logging.debug(f"Page text job ID extraction failed: {e}")

        return ExtractionResult(None, 0.0, "page_text")

    @staticmethod
    @lru_cache(maxsize=512)
    def _is_valid_id(job_id):
        if not job_id or not (4 <= len(job_id) <= 40):
            return False
        if not re.match(r"^[A-Z0-9\-_]+$", job_id, re.I):
            return False
        return job_id.upper() not in {
            "APPLY",
            "NOW",
            "HERE",
            "JOIN",
            "CLICK",
            "VIEW",
            "SOFTWARE",
            "ENGINEER",
        }

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

        # Fallback: Generate hash-based ID
        import hashlib

        return f"HASH_{hashlib.md5(url.encode()).hexdigest()[:10]}"


# ============================================================================
# Location Extraction - ENHANCED WITH 5 METHODS
# ============================================================================


class LocationExtractor:
    @staticmethod
    def extract_from_title(title):
        if not title:
            return ExtractionResult(None, 0.0, "title_parse")

        try:
            match = re.search(r"[-–]\s*([A-Za-z\s]+,\s*[A-Z]{2})(?:\s|$)", title)
            if match:
                return ExtractionResult(
                    match.group(1).strip(), 0.80, "title_city_state"
                )

            if re.search(r"\(remote\)", title, re.I):
                return ExtractionResult("Remote", 0.90, "title_remote")
        except Exception as e:
            logging.debug(f"Title location extraction failed: {e}")

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
            except Exception as e:
                logging.debug(f"Selector {selector} failed: {e}")
                continue

        return ExtractionResult(None, 0.0, "html_selectors")

    @staticmethod
    def extract_from_json_ld(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "json_ld")

        try:
            json_ld = soup.find("script", {"type": "application/ld+json"})
            if json_ld:
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
        except Exception as e:
            logging.debug(f"JSON-LD location extraction failed: {e}")

        return ExtractionResult(None, 0.0, "json_ld")

    @staticmethod
    def extract_from_page_text(soup):
        """NEW: Enhanced page text extraction with multiple patterns"""
        if not soup:
            return ExtractionResult(None, 0.0, "page_text")

        try:
            page_text = soup.get_text()[:5000]

            # Pattern 1: "Location: City, State"
            match = _LOCATION_LABEL_PATTERN.search(page_text)
            if match:
                location = match.group(1).strip()
                if len(location) < 50:
                    cleaned = LocationProcessor.clean_location_aggressive(location)
                    if cleaned and cleaned != "Unknown":
                        return ExtractionResult(cleaned, 0.75, "page_text_labeled")

            # Pattern 2: "Locations: City, State, Country" (EA format)
            ea_pattern = r"Locations?:\s*([^,\n]+(?:,\s*[^,\n]+){1,2})"
            match = re.search(ea_pattern, page_text, re.I)
            if match:
                location = match.group(1).strip()
                if len(location) < 100:
                    cleaned = LocationProcessor.clean_location_aggressive(location)
                    if cleaned and cleaned != "Unknown":
                        return ExtractionResult(cleaned, 0.80, "page_text_ea_format")

        except Exception as e:
            logging.debug(f"Page text location extraction failed: {e}")

        return ExtractionResult(None, 0.0, "page_text")

    @staticmethod
    def extract_from_url(url):
        if not url:
            return ExtractionResult(None, 0.0, "url_parse")

        try:
            # Pattern 1: Workday-style with full state name
            match = re.search(r"/job/([A-Za-z-]+)-([A-Za-z\s]+)/", url)
            if match:
                city = match.group(1).replace("-", " ").title()
                state_text = match.group(2).replace("-", " ").title()

                # Try to convert full state name to code
                state_code = LocationProcessor.convert_state_name_to_code(state_text)
                if state_code:
                    return ExtractionResult(
                        f"{city}, {state_code}", 0.80, "url_path_full_state"
                    )

            # Pattern 2: Standard City-StateCode format
            patterns = [
                r"/job/([A-Z][a-z]+(?:-[A-Z][a-z]+)*)-([A-Z]{2})(?:-USA)?/",
                r"/([A-Z][a-z]+(?:-[A-Z][a-z]+)*)-([A-Z]{2})/",
            ]

            for pattern in patterns:
                match = re.search(pattern, url)
                if match and len(match.groups()) == 2:
                    city, state = match.groups()
                    city = city.replace("-", " ")
                    if validate_us_state_code(state):
                        return ExtractionResult(f"{city}, {state}", 0.75, "url_path")
        except Exception as e:
            logging.debug(f"URL location extraction failed: {e}")

        return ExtractionResult(None, 0.0, "url_parse")

    @staticmethod
    def extract_all_methods(url, soup, title="", platform="generic"):
        """Enhanced with 5 extraction methods"""
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
        return (
            LocationProcessor.format_location_clean(best_result.value)
            if best_result
            else "Unknown"
        )


# ============================================================================
# Location Processing - SIGNIFICANTLY ENHANCED
# ============================================================================


class LocationProcessor:
    @staticmethod
    @lru_cache(maxsize=512)
    def clean_location_aggressive(location_text):
        if not location_text or len(location_text) < 2:
            return "Unknown"

        try:
            location = location_text.strip()

            # Step 1: Remove metadata patterns
            for pattern in _COMPILED_METADATA_PATTERNS:
                location = pattern.sub("", location)

            # Step 2: Remove HTML artifacts
            for pattern in _COMPILED_ARTIFACT_PATTERNS:
                location = pattern.sub("", location)

            # Step 3: Remove parenthetical content
            location = re.sub(r"\s*\([^)]*\)", "", location)
            location = re.sub(r"\s*-\s*[A-Z]{2,4}$", "", location)

            # Step 4: Remove department keywords
            for keyword in [
                "Employment",
                "Type",
                "Details",
                "Program",
                "Internship",
                "Job",
                "Position",
            ]:
                if keyword in location:
                    location = location.split(keyword)[0].strip()
                    break

            # Step 5: Remove addresses/zipcodes
            location = re.sub(r",?\s*\d{3,5}\s+[A-Z][a-z]+", "", location)
            location = re.sub(r"\s*\d{5}(-\d{4})?\s*", "", location)

            # Step 6: Remove country suffixes
            location = re.sub(
                r",?\s*(?:USA|United States)\s*", "", location, flags=re.I
            )

            # Step 7: Extract City, State pattern
            match = _CITY_STATE_PATTERN.search(location)
            if match:
                city, state = match.group(1).strip(), match.group(2).upper()
                if validate_us_state_code(state):
                    return f"{city}, {state}"

            # Step 8: Normalize whitespace
            location = re.sub(r"\s+", " ", location).strip()

            if not location or len(location) < 2:
                return "Unknown"

            # Step 9: Reject if matches department keywords
            if location.lower() in DEPARTMENT_KEYWORDS:
                return "Unknown"

            # Step 10: Reject invalid location keywords
            if any(kw in location.lower() for kw in INVALID_LOCATION_KEYWORDS):
                return "Unknown"

            # Step 11: Try to infer state from city name
            state = get_state_for_city(location)
            if state:
                return f"{location.title()}, {state}"

            return location
        except Exception as e:
            logging.debug(f"Location cleaning failed for '{location_text}': {e}")
            return "Unknown"

    @staticmethod
    @lru_cache(maxsize=512)
    def format_location_clean(location):
        """Enhanced with suffix stripping, abbreviation expansion, state conversion"""
        if not location or location == "Unknown":
            return "Unknown"

        try:
            if location in ["Remote", "Hybrid"]:
                return location

            # Step 1: Handle Workday HQ codes
            if location in WORKDAY_HQ_CODES:
                city, state = WORKDAY_HQ_CODES[location]
                return f"{city}, {state}" if state != "UNKNOWN" else "Unknown"

            # Step 2: Clean the location
            location = LocationProcessor.clean_location_aggressive(location)

            # Step 3: Strip location suffixes (NEW)
            for suffix in LOCATION_SUFFIXES:
                if location.endswith(suffix):
                    location = location[: -len(suffix)].strip()
                    break

            # Step 4: Expand city abbreviations (NEW)
            location_lower = location.lower()
            if location_lower in CITY_ABBREVIATIONS:
                return CITY_ABBREVIATIONS[location_lower]

            # Step 5: Convert full state names to codes (NEW)
            location = LocationProcessor.convert_state_name_to_code(location)

            # Step 6: Handle state code prefix (MA Cambridge -> Cambridge, MA)
            match = re.search(r"^([A-Z]{2})\s+(.+)$", location)
            if match:
                state, rest = match.groups()
                if validate_us_state_code(state):
                    return f"{rest.strip()}, {state}"

            # Step 7: Remove technical prefixes
            location = re.sub(r"^[A-Z]{2,4}[:_-]\s*", "", location)

            # Step 8: Reject generic locations (NEW)
            if not LocationProcessor.is_valid_location_text(location):
                return "Unknown"

            return location
        except Exception as e:
            logging.debug(f"Location formatting failed for '{location}': {e}")
            return "Unknown"

    @staticmethod
    @lru_cache(maxsize=256)
    def convert_state_name_to_code(location):
        """NEW: Convert full state/province names to codes"""
        if not location:
            return location

        location_lower = location.lower()

        # Check US states
        for full_name, code in FULL_STATE_NAMES.items():
            if full_name in location_lower:
                # Replace "Vancouver, British Columbia" -> "Vancouver, BC"
                location = re.sub(rf"\b{full_name}\b", code, location, flags=re.I)
                return location

        # Check Canadian provinces
        for full_name, code in CANADA_PROVINCE_NAMES.items():
            if full_name in location_lower:
                location = re.sub(rf"\b{full_name}\b", code, location, flags=re.I)
                return location

        return location

    @staticmethod
    @lru_cache(maxsize=256)
    def is_valid_location_text(text):
        """NEW: Validate location isn't garbage or too generic"""
        if not text or text == "Unknown":
            return False

        if len(text) > 50:
            return False
        if len(text) < 3:
            return False

        # Reject garbage phrases
        garbage_phrases = [
            "as well as",
            "in accordance with",
            "equal opportunity",
            "without regard to",
            "applicants",
            "candidates",
        ]
        text_lower = text.lower()
        if any(phrase in text_lower for phrase in garbage_phrases):
            return False

        # Reject too generic
        if text_lower in [
            "united states",
            "us",
            "usa",
            "headquarters",
            "office",
            "campus",
        ]:
            return False

        # Reject state-only (no city)
        if text.upper() in US_STATES_FALLBACK:
            return False

        return True

    @staticmethod
    def extract_remote_status_enhanced(soup, location, url):
        if not soup:
            return "Unknown"

        try:
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
        except Exception as e:
            logging.debug(f"Remote status extraction failed: {e}")

        return "Unknown"

    @staticmethod
    def check_if_international(location, soup=None, url=None):
        """ENHANCED: Multi-method Canadian detection that ALWAYS runs"""

        # Method 1: Check extracted location
        if location and location != "Unknown":
            location_lower = location.lower()
            normalized = normalize_unicode(location_lower)

            # Check for "Canada" keyword
            if "canada" in location_lower:
                return "Location: Canada"

            # Check for Canadian provinces (full names)
            for full_name, code in CANADA_PROVINCE_NAMES.items():
                if re.search(rf"\b{full_name}\b", location_lower, re.I):
                    # Exclude "Ontario, CA" (California)
                    if full_name == "ontario" and ", ca" in location_lower:
                        continue
                    return f"Location: Canada ({full_name.title()})"

            # Check for province codes
            for province in CANADA_PROVINCES:
                if re.search(rf",\s*{province}\b", location):
                    # Exclude "ON" in "Ontario, CA"
                    if province == "ON" and ", ca" in location_lower:
                        continue
                    return f"Location: Canada (province: {province})"

            # Check for ", CA" with Canadian city indicators
            if (", CA" in location or " - CA" in location) and any(
                ind in location_lower for ind in ["toronto", "ottawa", "montreal"]
            ):
                if "ontario, ca" not in location_lower:
                    return "Location: Canada"

            # Check for major Canadian cities
            for city, province in MAJOR_CANADIAN_CITIES.items():
                if city in normalized:
                    # Resolve ambiguous cities
                    if city in AMBIGUOUS_CITIES:
                        resolved = LocationProcessor._resolve_ambiguous_city(
                            city, location, soup, url
                        )
                        if resolved == "Canada":
                            return f"Location: Canada ({city.title()})"
                    else:
                        return f"Location: Canada ({city.title()})"

        # Method 2: Check page text (BACKUP - always runs even if location fails)
        canadian_result = LocationProcessor._check_page_for_canada(soup)
        if canadian_result:
            return canadian_result

        # Method 3: Check URL
        if url:
            url_result = LocationProcessor._check_url_for_canada(url)
            if url_result:
                return url_result

        return None

    @staticmethod
    def _resolve_ambiguous_city(city, location, soup, url):
        """NEW: Resolve ambiguous cities using context"""
        if not city in AMBIGUOUS_CITIES:
            return "Unknown"

        context_text = ""

        # Gather context from location string
        if location:
            context_text += location.lower() + " "

        # Gather context from page text
        if soup:
            context_text += soup.get_text()[:3000].lower() + " "

        # Gather context from URL
        if url:
            context_text += url.lower() + " "

        # Check for Canadian indicators
        canada_score = sum(1 for kw in CANADA_CONTEXT_KEYWORDS if kw in context_text)
        us_score = sum(1 for kw in US_CONTEXT_KEYWORDS if kw in context_text)

        # Check domain
        if url and ".ca" in url.lower():
            canada_score += 2

        # Check for explicit province/state mention
        for province in CANADA_PROVINCES:
            if f", {province}" in location:
                canada_score += 3

        if canada_score > us_score:
            return "Canada"
        elif us_score > canada_score:
            return "US"

        # Default: assume US for ambiguous cases
        return "US"

    @staticmethod
    def _check_page_for_canada(soup):
        if not soup:
            return None

        try:
            # Check meta description
            meta_desc = soup.find("meta", {"property": "og:description"})
            if meta_desc and meta_desc.get("content"):
                content = meta_desc["content"].lower()
                if content == "canada" or "canada only" in content:
                    return "Location: Canada (from meta tag)"

            # Check page text
            page_text = soup.get_text()[:10000].lower()

            # Pattern 1: City, Province, Canada
            canadian_city_patterns = [
                r"ottawa\s*,?\s*ontario",
                r"toronto\s*,?\s*ontario",
                r"montreal\s*,?\s*quebec",
                r"vancouver\s*,?\s*british\s*columbia",
                r"calgary\s*,?\s*alberta",
            ]
            for pattern in canadian_city_patterns:
                if re.search(pattern, page_text, re.I):
                    match = re.search(pattern, page_text, re.I)
                    city_province = match.group(0) if match else "Canada"
                    return f"Location: Canada ({city_province})"

            # Pattern 2: "work in Canada", "located in Canada"
            if re.search(r"(work|located|based)\s+in\s+canada", page_text, re.I):
                return "Location: Canada (page text indicator)"

        except Exception as e:
            logging.debug(f"Canada page check failed: {e}")

        return None

    @staticmethod
    def _check_url_for_canada(url):
        """NEW: Check URL for Canadian indicators"""
        if not url:
            return None

        url_lower = url.lower()

        # Check domain
        if ".ca/" in url_lower or url_lower.endswith(".ca"):
            return "Location: Canada (domain .ca)"

        # Check path
        canada_patterns = [
            "/montreal-quebec-can/",
            "/toronto-ontario/",
            "-ontario-can",
            "/can/",
            "/canada/",
            "-canada-",
            "vancouver-british-columbia",
        ]
        for pattern in canada_patterns:
            if pattern in url_lower:
                return "Location: Canada (from URL path)"

        return None


# ============================================================================
# Validation Helpers
# ============================================================================


class ValidationHelper:
    @staticmethod
    @lru_cache(maxsize=512)
    def is_valid_job_url(url):
        if not url or not url.startswith("http"):
            return False, "Invalid URL format"

        url_lower = url.lower()

        if "jobright.ai" in url_lower and "/jobs/info/" not in url_lower:
            return False, "Invalid Jobright URL"

        for pattern in [
            "/unsubscribe",
            "/my-alerts",
            "/blog",
            "/terms",
            "/privacy",
            "chromewebstore",
        ]:
            if pattern in url_lower:
                return False, f"{pattern.split('/')[-1].title()} link"

        return True, None

    @staticmethod
    def check_url_for_international(url):
        """Delegates to LocationProcessor._check_url_for_canada"""
        return LocationProcessor._check_url_for_canada(url)

    @staticmethod
    def check_page_restrictions(soup):
        if not soup:
            return None, None, []

        try:
            degree_decision, degree_reason = (
                ValidationHelper._check_degree_requirements_strict(soup)
            )
            if degree_decision == "REJECT":
                return degree_decision, degree_reason, []

            page_text = soup.get_text()[:15000].lower()

            patterns = {
                r"u\.?s\.?\s+citizenship\s+(?:is\s+)?required": "US citizenship required",
                r"us work authorization required": "US work authorization required",
                r"(?:clearance.*required)": "Security clearance required",
            }

            for pattern, reason in patterns.items():
                if re.search(pattern, page_text, re.I):
                    return "REJECT", reason, []
        except Exception as e:
            logging.debug(f"Page restrictions check failed: {e}")

        return None, None, []

    @staticmethod
    def _check_degree_requirements_strict(soup):
        if not soup:
            return None, None

        try:
            page_text = soup.get_text()[:15000].lower()

            for pattern in [
                r"bachelor'?s?\s+(?:students?|degree|candidates?)\s+only",
                r"undergraduate\s+(?:students?|only)",
                r"currently\s+pursuing\s+a?\s+bachelor",
            ]:
                match = re.search(pattern, page_text, re.I)
                if match:
                    context = page_text[
                        max(0, match.start() - 300) : min(
                            len(page_text), match.end() + 300
                        )
                    ]
                    if not any(
                        kw in context for kw in ["master", "ms/phd", "graduate"]
                    ):
                        return "REJECT", "Bachelor's students only"
        except Exception as e:
            logging.debug(f"Degree requirements check failed: {e}")

        return None, None

    @staticmethod
    def validate_company_field(company, title, url):
        if not company or company == "Unknown" or not company.strip():
            return True, ValidationHelper.extract_company_from_domain(url), None

        company = company.strip()

        if not CompanyValidator.is_valid(company):
            return True, ValidationHelper.extract_company_from_domain(url), None

        if len(company) > 100:
            return False, company, "Company name too long"

        if (
            sum(
                1
                for kw in ["intern", "software", "engineer", "developer"]
                if kw in company.lower()
            )
            >= 2
        ):
            return False, company, "Company field contains job title"

        return True, company, None

    @staticmethod
    def clean_legal_entity(company):
        if not company:
            return company
        company = re.sub(r"^\d+\s+|^[A-Z]{2,4}[-\s]", "", company)
        return re.sub(
            r",?\s+(Inc\.?|LLC\.?|Corp\.?|Ltd\.?)$", "", company, flags=re.I
        ).strip()

    @staticmethod
    @lru_cache(maxsize=256)
    def extract_company_from_domain(url):
        if not url:
            return "Unknown"

        try:
            subdomain, domain = extract_domain_and_subdomain(url)
            if domain:
                domain_name = domain.split(".")[0]
                if domain_name in COMPANY_SLUG_MAPPING:
                    return COMPANY_SLUG_MAPPING[domain_name]
                return domain_name.replace("-", " ").title()
        except Exception as e:
            logging.debug(f"Domain extraction failed for {url}: {e}")

        return "Unknown"

    @staticmethod
    def check_sponsorship_status(soup):
        if not soup:
            return "Unknown"

        try:
            page_text = soup.get_text()[:3000]

            if re.search(
                r"(?:will|does|provides?)\s+sponsor|h-?1b.*sponsor", page_text, re.I
            ):
                return "Yes"
            if re.search(r"(?:no|not|doesn't)\s+sponsor", page_text, re.I):
                return "No"
        except Exception as e:
            logging.debug(f"Sponsorship check failed: {e}")

        return "Unknown"


# ============================================================================
# Company Extraction
# ============================================================================


class CompanyExtractor:
    @staticmethod
    def extract_from_url_mapping(url):
        if not url:
            return ExtractionResult(None, 0.0, "url_mapping")

        try:
            for pattern, company_name in _COMPILED_URL_COMPANY_PATTERNS:
                if pattern.search(url):
                    return ExtractionResult(company_name, 0.95, "url_mapping")
        except Exception as e:
            logging.debug(f"URL company mapping failed: {e}")

        return ExtractionResult(None, 0.0, "url_mapping")

    @staticmethod
    def extract_from_json_ld(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "json_ld")

        try:
            json_ld = soup.find("script", {"type": "application/ld+json"})
            if json_ld:
                data = json.loads(json_ld.string)
                org = (
                    data.get("hiringOrganization", {}) if isinstance(data, dict) else {}
                )
                if isinstance(org, dict) and org.get("name"):
                    name = org["name"]
                    if len(name) < 100:
                        cleaned = ValidationHelper.clean_legal_entity(name)
                        normalized = CompanyNormalizer.normalize(cleaned, "")
                        if normalized and CompanyValidator.is_valid(normalized):
                            return ExtractionResult(normalized, 0.93, "json_ld")
        except Exception as e:
            logging.debug(f"JSON-LD company extraction failed: {e}")

        return ExtractionResult(None, 0.0, "json_ld")

    @staticmethod
    def extract_from_meta_tags(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "meta_tags")

        try:
            meta = soup.find("meta", {"property": "og:site_name"})
            if meta and meta.get("content"):
                company = re.sub(
                    r"\s*[-|]\s*(careers|jobs).*$",
                    "",
                    meta["content"].strip(),
                    flags=re.I,
                )
                if company and len(company) < 50:
                    normalized = CompanyNormalizer.normalize(company, "")
                    if normalized and CompanyValidator.is_valid(normalized):
                        return ExtractionResult(normalized, 0.90, "meta_tags")
        except Exception as e:
            logging.debug(f"Meta tag company extraction failed: {e}")

        return ExtractionResult(None, 0.0, "meta_tags")

    @staticmethod
    def extract_from_visible_elements(soup, url):
        if not soup:
            return ExtractionResult(None, 0.0, "visible_elements")

        try:
            url_lower = url.lower() if url else ""

            if "icims.com" in url_lower:
                mobile_header = soup.find("div", id="mobile-header-container")
                if mobile_header:
                    h1 = mobile_header.find("h1")
                    if h1:
                        normalized = CompanyNormalizer.normalize(
                            h1.get_text().strip(), url
                        )
                        if normalized and CompanyValidator.is_valid(normalized):
                            return ExtractionResult(normalized, 0.88, "visible_icims")

            if "smartrecruiters" in url_lower:
                logo = soup.find("img", alt=re.compile(r"logo", re.I))
                if logo:
                    alt = logo.get("alt", "").replace(" logo", "").strip()
                    if len(alt) > 2:
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
        except Exception as e:
            logging.debug(f"Visible elements extraction failed: {e}")

        return ExtractionResult(None, 0.0, "visible_elements")

    @staticmethod
    def extract_from_url_path(url, platform):
        if not url:
            return ExtractionResult(None, 0.0, "url_path")

        try:
            path_company = CompanyNormalizer.extract_from_url_path(url, platform)
            if path_company and CompanyValidator.is_valid(path_company):
                if not CompanyValidator.is_junk_subdomain(path_company.lower()):
                    return ExtractionResult(path_company, 0.70, "url_path")
        except Exception as e:
            logging.debug(f"URL path company extraction failed: {e}")

        return ExtractionResult(None, 0.0, "url_path")

    @staticmethod
    def extract_from_subdomain(url):
        if not url:
            return ExtractionResult(None, 0.0, "subdomain")

        try:
            subdomain, _ = extract_domain_and_subdomain(url)

            if subdomain and not CompanyValidator.is_junk_subdomain(subdomain):
                normalized = CompanyNormalizer.normalize(subdomain, url)
                if normalized and CompanyValidator.is_valid(normalized):
                    return ExtractionResult(normalized, 0.40, "subdomain")
        except Exception as e:
            logging.debug(f"Subdomain extraction failed: {e}")

        return ExtractionResult(None, 0.0, "subdomain")

    @staticmethod
    @lru_cache(maxsize=256)
    def _looks_like_title(text):
        if not text:
            return False
        text_lower = text.lower()
        if any(p in text_lower for p in ["submit your", "sign in", "apply now"]):
            return True
        return (
            sum(
                1
                for kw in ["intern", "co-op", "engineer", "developer", "software"]
                if kw in text_lower
            )
            >= 2
        )

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
        return (
            best_result.value
            if best_result
            else ValidationHelper.extract_company_from_domain(url)
        )


# ============================================================================
# Quality Scoring
# ============================================================================


class QualityScorer:
    @staticmethod
    def calculate_score(job_data):
        score = 0

        if (
            job_data.get("company") not in [None, "Unknown", "N/A"]
            and len(job_data.get("company", "")) > 2
        ):
            score += 3

        if job_data.get("location") not in [None, "Unknown"]:
            score += 2

        job_id = job_data.get("job_id", "N/A")
        if job_id not in ["N/A", None] and not job_id.startswith("HASH_"):
            score += 1

        title_len = len(job_data.get("title", ""))
        if 15 < title_len < 120:
            score += 1

        return score

    @staticmethod
    def is_acceptable_quality(score, min_score=4):
        return score >= min_score
