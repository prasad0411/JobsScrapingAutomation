#!/usr/bin/env python3

import re
import json
import logging
from functools import lru_cache

from config import (
    CANADA_PROVINCES,
    CANADA_PROVINCE_NAMES,
    MAJOR_CANADIAN_CITIES,
    CANADIAN_COMPANIES,
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
    COMPANY_NAME_PREFIXES,
    COMPANY_NAME_STOPWORDS,
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
    TECHNICAL_ROLE_KEYWORDS,
    NON_TECHNICAL_PURE,
    SPONSORSHIP_REJECT_PATTERNS,
    BLACKLIST_DOMAINS,
    MAX_REASONABLE_AGE_DAYS,
    US_STATES_FALLBACK,
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
_COMPILED_SPONSORSHIP_PATTERNS = [
    re.compile(p, re.I) for p in SPONSORSHIP_REJECT_PATTERNS
]

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
# Title Processing - ORIGINAL
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

        excluded = {"click here", "apply now", "view job", "submit your", "click to"}
        if any(phrase in title.lower() for phrase in excluded):
            return False, "Invalid title pattern"

        return True, None

    @staticmethod
    @lru_cache(maxsize=256)
    def is_cs_engineering_role(title, description=""):
        """ENHANCED: Expanded keywords + description scanning"""
        combined_text = (title + " " + description).lower()

        # Check expanded technical keywords
        if any(kw in combined_text for kw in TECHNICAL_ROLE_KEYWORDS):
            # Context check for ambiguous keywords
            non_tech_pure = sum(1 for kw in NON_TECHNICAL_PURE if kw in combined_text)
            tech_count = sum(1 for kw in TECHNICAL_ROLE_KEYWORDS if kw in combined_text)

            # If more technical signals than non-technical, accept
            if tech_count > non_tech_pure:
                return True

        return False

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
# Job ID Extraction - ORIGINAL
# ============================================================================


class JobIDExtractor:
    @staticmethod
    def extract_from_url(url, platform="generic"):
        """ENHANCED: Platform-specific handling (Glassdoor N/A)"""
        if not url:
            return ExtractionResult(None, 0.0, "url_extract")

        # NEW: Glassdoor special handling
        if platform == "glassdoor" or "glassdoor" in url.lower():
            return ExtractionResult("N/A", 0.98, "glassdoor_na")

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
                (r"Role\s*ID\s*:?\s*([A-Z0-9\-]{4,20})\b", 0.85),  # NEW: EA format
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
        if not job_id or not (4 <= len(job_id) <= 20):
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
    def extract_all_methods(url, soup, platform="generic"):
        """ORIGINAL: All methods execute, vote on non-None"""
        results = [
            JobIDExtractor.extract_from_url(url, platform),
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
# Location Extraction - ORIGINAL + ENHANCED
# ============================================================================


class LocationExtractor:
    @staticmethod
    def extract_from_title(title):
        """NEW PRIORITY 1: Extract from title (catches Boomi, EA cases)"""
        if not title:
            return ExtractionResult(None, 0.0, "title_parse")

        try:
            # Pattern 1: (City, ST) or (City, Province)
            match = re.search(r"\(([A-Za-z\s]+),\s*([A-Z]{2})\)", title)
            if match:
                city, code = match.groups()
                return ExtractionResult(
                    f"{city.strip()}, {code}", 0.96, "title_parentheses"
                )

            # Pattern 2: - City, ST (original)
            match = re.search(r"[-–]\s*([A-Za-z\s]+,\s*[A-Z]{2})(?:\s|$)", title)
            if match:
                return ExtractionResult(
                    match.group(1).strip(), 0.90, "title_city_state"
                )

            # Pattern 3: (remote)
            if re.search(r"\(remote\)", title, re.I):
                return ExtractionResult("Remote", 0.92, "title_remote")
        except Exception as e:
            logging.debug(f"Title location extraction failed: {e}")

        return ExtractionResult(None, 0.0, "title_parse")

    @staticmethod
    def extract_from_html_selectors(soup, platform="generic"):
        """ORIGINAL: Platform-specific selectors"""
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
        """ORIGINAL"""
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
        """ORIGINAL + ENHANCED"""
        if not soup:
            return ExtractionResult(None, 0.0, "page_text")

        try:
            page_text = soup.get_text()[:5000]

            # Original pattern
            match = _LOCATION_LABEL_PATTERN.search(page_text)
            if match:
                location = match.group(1).strip()
                if len(location) < 50:
                    cleaned = LocationProcessor.clean_location_aggressive(location)
                    if cleaned and cleaned != "Unknown":
                        return ExtractionResult(cleaned, 0.75, "page_text_labeled")

            # NEW: EA-style pattern
            ea_pattern = (
                r"Locations?:\s*([A-Za-z\s]+,\s*[A-Za-z\s]+(?:,\s*[A-Za-z\s]+)?)"
            )
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
        """ORIGINAL + ENHANCED Workday parser"""
        if not url:
            return ExtractionResult(None, 0.0, "url_parse")

        try:
            # NEW: Enhanced Workday URL parser
            if "workday" in url.lower():
                return LocationExtractor._extract_workday_url_enhanced(url)

            # ORIGINAL: Standard patterns
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
    def _extract_workday_url_enhanced(url):
        """NEW: Ultimate Workday URL parser - handles ALL edge cases"""
        try:
            match = re.search(r"/job/([^/]+)/", url)
            if not match:
                return ExtractionResult(None, 0.0, "url_workday")

            segment = match.group(1)

            # Step 1: Remove country suffixes (order matters - longest first)
            country_suffixes = [
                "-United-States-of-America",
                "-United-States",
                "-USA",
                "-US",
            ]
            for suffix in country_suffixes:
                segment = segment.replace(suffix, "")

            # Step 2: Remove address numbers (e.g., -307-Legget-Dr, -1800-Arch-St)
            segment = re.sub(r"-\d{3,}-.*$", "", segment)

            # Step 3: Split by hyphen and filter empty strings + "US"/"USA" tokens
            parts = [
                p for p in segment.split("-") if p and p.upper() not in ["US", "USA"]
            ]

            if len(parts) < 2:
                # Single part - check if Canadian city
                if len(parts) == 1 and parts[0].lower() in MAJOR_CANADIAN_CITIES:
                    return ExtractionResult(
                        f"CANADIAN_CITY_{parts[0]}", 0.88, "url_canadian_city"
                    )
                return ExtractionResult(None, 0.0, "url_workday")

            # Step 4: Try REVERSED format first (PA-Philadelphia)
            if len(parts[0]) == 2 and validate_us_state_code(parts[0]):
                state_code = parts[0].upper()
                city_parts = parts[1:]
                # Remove trailing numbers if any
                city_parts = [p for p in city_parts if not p.isdigit()]
                city = " ".join(city_parts)
                return ExtractionResult(
                    f"{city}, {state_code}", 0.93, "url_workday_reversed"
                )

            # Step 5: Try RIGHT-TO-LEFT state matching (normal format)
            state_code = None
            state_end_index = len(parts)

            # Try 2-word states (e.g., "South Dakota")
            for i in range(len(parts) - 1, 0, -1):
                if i < 1:
                    break
                two_word = f"{parts[i-1]} {parts[i]}".lower()
                if two_word in FULL_STATE_NAMES:
                    state_code = FULL_STATE_NAMES[two_word]
                    state_end_index = i - 1
                    break

            # Try 1-word states
            if not state_code:
                last_part = parts[-1]
                # Check 2-letter code
                if len(last_part) == 2 and validate_us_state_code(last_part):
                    state_code = last_part.upper()
                    state_end_index = len(parts) - 1
                # Check full state name
                elif last_part.lower() in FULL_STATE_NAMES:
                    state_code = FULL_STATE_NAMES[last_part.lower()]
                    state_end_index = len(parts) - 1

            if state_code:
                # Extract city (everything before state)
                city_parts = parts[:state_end_index]
                city = " ".join(city_parts)
                return ExtractionResult(
                    f"{city}, {state_code}", 0.92, "url_workday_ultimate"
                )

            # No US state found - check if Canadian city
            first_word = parts[0].lower()
            if first_word in MAJOR_CANADIAN_CITIES:
                return ExtractionResult(
                    f"CANADIAN_CITY_{parts[0]}", 0.88, "url_canadian_city"
                )

        except Exception as e:
            logging.debug(f"Workday URL parsing failed: {e}")

        return ExtractionResult(None, 0.0, "url_workday")

    @staticmethod
    def extract_all_methods(url, soup, title="", platform="generic", page_source=""):
        """
        CRITICAL ENHANCEMENT: ALL methods execute (no short-circuit)
        Title extraction is PRIORITY 1 (highest confidence)
        page_source parameter added for Selenium text extraction
        """
        results = [
            LocationExtractor.extract_from_title(title),  # NEW PRIORITY 1
            LocationExtractor.extract_from_json_ld(soup),
            LocationExtractor.extract_from_html_selectors(soup, platform),
            LocationExtractor.extract_from_page_text(soup),
            LocationExtractor.extract_from_url(url),  # ENHANCED Workday parser
        ]

        # NEW: If page_source available (from Selenium), try text extraction
        if page_source:
            text_result = LocationExtractor._extract_from_selenium_text(page_source)
            if text_result:
                results.append(text_result)

        best_result = ExtractionVoter.vote(
            results, min_confidence=MIN_CONFIDENCE_LOCATION
        )
        return (
            LocationProcessor.format_location_clean(best_result.value)
            if best_result
            else "Unknown"
        )

    @staticmethod
    def _extract_from_selenium_text(page_source):
        """NEW: Extract from raw Selenium page source (selector-independent)"""
        if not page_source:
            return None

        try:
            patterns = [
                (r"📍\s*([A-Za-z\s]+,\s*[A-Z]{2})", 0.90),
                (r">([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?,\s*[A-Z]{2,15})<", 0.82),
            ]

            for pattern, conf in patterns:
                match = re.search(pattern, page_source, re.I)
                if match:
                    location = match.group(1).strip()
                    if 3 < len(location) < 100:
                        return ExtractionResult(location, conf, "selenium_text")
        except:
            pass

        return None


# ============================================================================
# Location Processing - ORIGINAL + ENHANCED
# ============================================================================


class LocationProcessor:
    @staticmethod
    @lru_cache(maxsize=512)
    def clean_location_aggressive(location_text):
        """ORIGINAL cleaning logic"""
        if not location_text or len(location_text) < 2:
            return "Unknown"

        try:
            location = location_text.strip()

            location = re.sub(r"^locations", "", location, flags=re.I)

            for pattern in _COMPILED_METADATA_PATTERNS:
                location = pattern.sub("", location)

            for pattern in _COMPILED_ARTIFACT_PATTERNS:
                location = pattern.sub("", location)

            location = re.sub(r"\s*\([^)]*\)", "", location)
            location = re.sub(r"\s*-\s*[A-Z]{2,4}$", "", location)

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

            location = re.sub(r",?\s*\d{3,5}\s+[A-Z][a-z]+", "", location)
            location = re.sub(r"\s*\d{5}(-\d{4})?\s*", "", location)
            location = re.sub(
                r",?\s*(?:USA|United States)\s*", "", location, flags=re.I
            )

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
        """ORIGINAL + ENHANCED with suffix stripping, abbreviation expansion"""
        if not location or location == "Unknown":
            return "Unknown"

        try:
            # NEW: Handle Canadian city marker from URL extraction
            if location.startswith("CANADIAN_CITY_"):
                city = location.replace("CANADIAN_CITY_", "")
                return f"{city}, CANADA"  # Signal for rejection

            # ORIGINAL: Special values
            if location in ["Remote", "Hybrid"]:
                return location

            if location in WORKDAY_HQ_CODES:
                city, state = WORKDAY_HQ_CODES[location]
                return f"{city}, {state}" if state != "UNKNOWN" else "Unknown"

            # ORIGINAL: Clean
            location = LocationProcessor.clean_location_aggressive(location)
            if not location:
                return "Unknown"

            # NEW: Strip location suffixes
            for suffix in LOCATION_SUFFIXES:
                if location.endswith(suffix):
                    location = location[: -len(suffix)].strip()
                    break

            # NEW: Expand city abbreviations
            location_lower = location.lower()
            if location_lower in CITY_ABBREVIATIONS:
                return CITY_ABBREVIATIONS[location_lower]

            # NEW: Convert full state names to codes
            location = LocationProcessor.convert_state_name_to_code(location)

            # ORIGINAL: State code prefix handling
            match = re.search(r"^([A-Z]{2})\s+(.+)$", location)
            if match:
                state, rest = match.groups()
                if validate_us_state_code(state):
                    return f"{rest.strip()}, {state}"

            location = re.sub(r"^[A-Z]{2,4}[:_-]\s*", "", location)

            # NEW: Validate isn't garbage
            if not LocationProcessor.is_valid_location_text(location):
                return "Unknown"

            # ORIGINAL: Check against bad values
            if location in {"Headquarters", "Office", "Campus"}:
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

        # Try 2-word states first (more specific)
        for full_name, code in FULL_STATE_NAMES.items():
            if len(full_name.split()) == 2 and full_name in location_lower:
                location = re.sub(rf"\b{full_name}\b", code, location, flags=re.I)
                return location

        # Then 1-word states
        for full_name, code in FULL_STATE_NAMES.items():
            if len(full_name.split()) == 1 and full_name in location_lower:
                location = re.sub(rf"\b{full_name}\b", code, location, flags=re.I)
                return location

        # Canadian provinces
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
    def extract_remote_status_enhanced(soup, location, url, description=""):
        if not soup:
            return "Unknown"

        try:
            if location:
                location_lower = location.lower()
                if "remote" in location_lower:
                    return "Remote"
                if "hybrid" in location_lower:
                    return "Hybrid"

            if description:
                desc_lower = description.lower()
                if "100% remote" in desc_lower or "fully remote" in desc_lower:
                    return "Remote"
                if "hybrid" in desc_lower:
                    return "Hybrid"
                if "on-site" in desc_lower or "onsite" in desc_lower:
                    return "On Site"

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
    def check_if_international(location, soup=None, url=None, title=""):
        """
        ORIGINAL + ENHANCED: Multi-method Canadian detection
        NEW: Title parameter for priority checking
        """

        # NEW METHOD 1: Check title first (highest priority for Boomi cases)
        if title:
            title_location = LocationExtractor.extract_from_title(title)
            if title_location and title_location.value:
                loc_upper = title_location.value.upper()
                # Check for Canadian province codes in title
                for province in CANADA_PROVINCES:
                    if f", {province}" in loc_upper:
                        # Exclude Ontario, CA (California)
                        if province == "ON" and ", CA" not in loc_upper:
                            return f"Location: Canada (from title)"
                        elif province != "ON":
                            return f"Location: Canada (from title: {province})"

        # ORIGINAL METHOD 2: Check extracted location
        if location and location not in ["Unknown", ""]:
            location_lower = location.lower()
            normalized = normalize_unicode(location_lower)

            # NEW: Check for CANADA marker
            if ", CANADA" in location.upper() or "CANADIAN_CITY_" in location:
                return "Location: Canada (from location field)"

            # ORIGINAL: Check for "Canada" keyword
            if "canada" in location_lower:
                return "Location: Canada"

            # ORIGINAL: Check provinces (full names)
            for full_name, code in CANADA_PROVINCE_NAMES.items():
                if re.search(rf"\b{full_name}\b", location_lower, re.I):
                    if full_name == "ontario" and ", ca" in location_lower:
                        continue
                    return f"Location: Canada ({full_name.title()})"

            # ORIGINAL: Check province codes
            for province in CANADA_PROVINCES:
                if re.search(rf",\s*{province}\b", location):
                    if province == "ON" and ", ca" in location_lower:
                        continue
                    return f"Location: Canada (province: {province})"

            # ORIGINAL: Check ", CA" with Canadian city indicators
            if (", CA" in location or " - CA" in location) and any(
                ind in location_lower for ind in ["toronto", "ottawa", "montreal"]
            ):
                if "ontario, ca" not in location_lower:
                    return "Location: Canada"

            # EXPANDED: Check major Canadian cities
            for city in MAJOR_CANADIAN_CITIES:
                if city in normalized:
                    # NEW: Handle ambiguous cities
                    if city in AMBIGUOUS_CITIES:
                        resolved = LocationProcessor._resolve_ambiguous_city(
                            city, location, soup, url
                        )
                        if resolved == "Canada":
                            return f"Location: Canada ({city.title()})"
                    else:
                        return f"Location: Canada ({city.title()})"

        # NEW METHOD 3: Extract city from URL (works even if location = "Unknown")
        if url:
            city_from_url = LocationProcessor._extract_city_from_url(url)
            if city_from_url:
                city_lower = city_from_url.lower()
                if city_lower in MAJOR_CANADIAN_CITIES:
                    if city_lower not in AMBIGUOUS_CITIES:
                        return f"Location: Canada (URL city: {city_from_url})"
                    else:
                        resolved = LocationProcessor._resolve_ambiguous_city(
                            city_from_url, location, soup, url
                        )
                        if resolved == "Canada":
                            return f"Location: Canada (URL city: {city_from_url})"

        # ORIGINAL METHOD: Check page text
        canada_page_check = LocationProcessor._check_page_for_canada(soup)
        if canada_page_check:
            return canada_page_check

        # ORIGINAL METHOD: Check URL
        canada_url_check = LocationProcessor._check_url_for_canada(url)
        if canada_url_check:
            return canada_url_check

        # ORIGINAL: Check other international (UK, India, China)
        if location and location != "Unknown":
            location_lower = location.lower()
            if any(kw in location_lower for kw in ["uk", "london", "india", "china"]):
                return "Location: International"

        return None

    @staticmethod
    def _extract_city_from_url(url):
        """NEW: Extract first city name from URL path"""
        if not url:
            return None
        try:
            # Pattern 1: /job/CityName-...
            match = re.search(r"/job/([A-Z][a-z]+)(?:-|/)", url)
            if match:
                return match.group(1)

            # Pattern 2: Query parameter
            match = re.search(r"[?&]city=([A-Za-z]+)", url, re.I)
            if match:
                return match.group(1)
        except:
            pass
        return None

    @staticmethod
    def _resolve_ambiguous_city(city, location, soup, url):
        """NEW: Resolve ambiguous cities using context"""
        if city.lower() not in AMBIGUOUS_CITIES:
            return "Unknown"

        context_text = ""
        if location:
            context_text += location.lower() + " "
        if soup:
            context_text += soup.get_text()[:3000].lower() + " "
        if url:
            context_text += url.lower() + " "

        canada_score = sum(1 for kw in CANADA_CONTEXT_KEYWORDS if kw in context_text)
        us_score = sum(1 for kw in US_CONTEXT_KEYWORDS if kw in context_text)

        if url and ".ca" in url.lower():
            canada_score += 3

        for province in CANADA_PROVINCES:
            if f", {province}" in context_text:
                canada_score += 2

        if canada_score > us_score:
            return "Canada"
        elif us_score > 0:
            return "US"

        # Default to US for truly ambiguous
        return "US"

    @staticmethod
    def _check_page_for_canada(soup):
        """ORIGINAL"""
        if not soup:
            return None

        try:
            meta_desc = soup.find("meta", {"property": "og:description"})
            if meta_desc and meta_desc.get("content"):
                content = meta_desc["content"].lower()
                if content == "canada" or "canada only" in content:
                    return "Location: Canada (from meta tag)"

            page_text = soup.get_text()[:10000]
            for pattern, city_name in [
                (r"Ottawa\s*,?\s*Ontario", "Ottawa, ON"),
                (r"Toronto\s*,?\s*Ontario", "Toronto, ON"),
                (r"Montreal\s*,?\s*Quebec", "Montreal, QC"),
                (r"Vancouver\s*,?\s*British\s*Columbia", "Vancouver, BC"),  # NEW
                (r"Calgary\s*,?\s*Alberta", "Calgary, AB"),  # NEW
            ]:
                if re.search(pattern, page_text, re.I):
                    return f"Location: Canada ({city_name})"

            # NEW: Generic "work in Canada" pattern
            if re.search(r"(work|located|based)\s+in\s+canada", page_text, re.I):
                return "Location: Canada (page text indicator)"

        except Exception as e:
            logging.debug(f"Canada page check failed: {e}")

        return None

    @staticmethod
    def _check_url_for_canada(url):
        """ORIGINAL"""
        if not url:
            return None

        url_lower = url.lower()

        if ".ca/" in url_lower or url_lower.endswith(".ca"):
            return "Location: Canada (domain .ca)"

        canada_patterns = [
            "/montreal-quebec-can/",
            "/toronto-ontario/",
            "-ontario-can",
            "/can/",
            "canada/",
            "/vancouver-british-columbia",  # NEW
            "/calgary-alberta",  # NEW
        ]

        if any(p in url_lower for p in canada_patterns):
            return "Location: Canada (from URL path)"

        return None


# ============================================================================
# Validation Helpers - ORIGINAL + ENHANCED
# ============================================================================


class ValidationHelper:
    @staticmethod
    @lru_cache(maxsize=512)
    def is_valid_job_url(url):
        """ORIGINAL + NEW blacklist check"""
        if not url or not url.startswith("http"):
            return False, "Invalid URL format"

        url_lower = url.lower()

        # NEW: Check blacklist
        if any(domain in url_lower for domain in BLACKLIST_DOMAINS):
            return False, "Blacklisted domain"

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
        return LocationProcessor._check_url_for_canada(url)

    @staticmethod
    def _check_clearance_requirements(soup):
        if not soup:
            return None, None

        try:
            page_text = soup.get_text()[:15000].lower()
            clearance_patterns = [
                r"(?:security\s+)?clearance.*(?:required|preferred)",
                r"must\s+(?:be\s+)?(?:able\s+to\s+)?(?:obtain|get|acquire).*clearance",
                r"(?:eligible|eligibility)\s+for.*(?:security\s+)?clearance",
                r"able\s+to\s+obtain.*clearance",
                r"clearance\s+(?:eligibility|required|preferred)",
                r"u\.?s\.?\s+citizen.*clearance",
                r"citizenship.*clearance",
                r"dod\s+(?:secret|top\s+secret)",
                r"ts/sci",
                r"polygraph",
            ]
            for pattern in clearance_patterns:
                if re.search(pattern, page_text, re.I):
                    return "REJECT", "Security clearance required"
        except Exception as e:
            logging.debug(f"Clearance check failed: {e}")

        return None, None

    @staticmethod
    def _check_graduation_year_requirements(soup):
        if not soup:
            return None, None

        try:
            page_text = soup.get_text()[:15000]

            if re.search(
                r"(?:graduation|graduating).*(?:or\s+later|and\s+beyond)",
                page_text,
                re.I,
            ):
                logging.debug("Grad year: 'or later' found - flexible")
                return None, None

            text_cleaned = re.sub(
                r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b",
                "",
                page_text,
                flags=re.I,
            )

            graduation_years = set()

            range_patterns = [
                r"(?:between\s+)?(\d{1,2})/(\d{4})\s*-\s*(\d{1,2})/(\d{4})",
                r"between\s+(\d{4})\s+(?:and|to)\s+(\d{4})",
                r"(\d{4})\s+(?:and|to|-)\s+(\d{4}).*(?:graduat|class)",
            ]

            for pattern in range_patterns:
                for match in re.finditer(pattern, text_cleaned, re.I):
                    years = [
                        g for g in match.groups() if g and g.isdigit() and len(g) == 4
                    ]
                    for y in years:
                        year_int = int(y)
                        if 2020 <= year_int <= 2030:
                            graduation_years.add(year_int)

            if graduation_years:
                min_year = min(graduation_years)
                max_year = max(graduation_years)
                if min_year <= 2027 <= max_year:
                    logging.debug(
                        f"Grad year: Range {min_year}-{max_year} includes 2027"
                    )
                    return None, None

            specific_patterns = [
                r"(?:anticipated\s+)?graduation(?:\s+of)?(?:\s+date)?[:\s]+(?:in\s+)?(?:[A-Za-z]+\s+)?(\d{4})",
                r"graduating\s+(?:in\s+)?(?:[A-Za-z]+\s+)?(\d{4})",
                r"class\s+of\s+(\d{4})",
                r"expected\s+graduation[:\s]+(?:[A-Za-z]+\s+)?(\d{4})",
            ]

            for pattern in specific_patterns:
                for match in re.finditer(pattern, text_cleaned, re.I):
                    year_str = match.group(1)
                    if year_str and year_str.isdigit():
                        year_int = int(year_str)
                        if 2020 <= year_int <= 2030:
                            graduation_years.add(year_int)

            if not graduation_years:
                logging.debug("Grad year: No years found")
                return None, None

            if 2027 in graduation_years:
                logging.debug(f"Grad year: {graduation_years} includes 2027")
                return None, None

            if all(y < 2027 or y > 2027 for y in graduation_years):
                years_str = ", ".join(str(y) for y in sorted(graduation_years))
                logging.debug(f"Grad year: {years_str} excludes 2027")
                return "REJECT", f"Graduation year mismatch: {years_str}"

        except Exception as e:
            logging.debug(f"Graduation year check failed: {e}")

        return None, None

    @staticmethod
    def _check_bs_only_restrictions(soup):
        if not soup:
            return None, None

        try:
            page_text = soup.get_text()[:15000]
            page_lower = page_text.lower()

            flexibility_phrases = [
                r"bachelor'?s?\s+or\s+master'?s?",
                r"bs\s*/\s*ms",
                r"b\.s\.\s*/\s*m\.s\.",
                r"b\.s\.\s+or\s+m\.s\.",
                r"undergraduate\s+or\s+graduate",
                r"or\s+equivalent\s+experience",
                r"or\s+advanced\s+degree",
                r"bachelor'?s?\s+degree\s+or\s+equivalent",
                r"bs\s+or\s+ms\s+degree",
                r"graduate\s+students?.*(?:welcome|encouraged|may\s+apply)",
            ]

            for phrase in flexibility_phrases:
                if re.search(phrase, page_lower):
                    logging.debug(f"BS check: Found flexibility phrase: {phrase}")
                    return None, None

            bs_indicators = [
                r"\bb\.?s\.?\b.*(?:in\s+)?computer\s+science",
                r"bachelor'?s?\s+degree",
                r"undergraduate\s+(?:degree|student|program)",
                r"(?:sophomore|junior)\s+standing",
                r"pursuing\s+a?\s+bachelor",
            ]

            has_bs_requirement = False
            for pattern in bs_indicators:
                if re.search(pattern, page_lower):
                    has_bs_requirement = True
                    logging.debug(f"BS check: Found BS requirement: {pattern}")
                    break

            if not has_bs_requirement:
                return None, None

            ms_indicators = [
                r"\bm\.?s\.?\b",
                r"master'?s?\s+degree",
                r"graduate\s+(?:degree|student|program)",
                r"ph\.?d\.?",
                r"advanced\s+degree",
            ]

            has_ms = any(re.search(pattern, page_lower) for pattern in ms_indicators)

            if has_ms:
                logging.debug("BS check: Found MS indicators - flexible")
                return None, None

            context_window = 200
            for match in re.finditer(
                r"\bb\.?s\.?\b|bachelor'?s?|undergraduate", page_lower
            ):
                start = max(0, match.start() - context_window)
                end = min(len(page_lower), match.end() + context_window)
                context = page_lower[start:end]

                if "preferred" in context and "required" not in context:
                    logging.debug("BS check: Found 'preferred' context - not strict")
                    return None, None

            logging.debug("BS check: BS-only confirmed")
            return "REJECT", "Bachelor's students only"

        except Exception as e:
            logging.debug(f"BS-only check failed: {e}")

        return None, None

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

            bs_decision, bs_reason = ValidationHelper._check_bs_only_restrictions(soup)
            if bs_decision == "REJECT":
                return bs_decision, bs_reason, []

            year_decision, year_reason = (
                ValidationHelper._check_graduation_year_requirements(soup)
            )
            if year_decision == "REJECT":
                return year_decision, year_reason, []

            clearance_decision, clearance_reason = (
                ValidationHelper._check_clearance_requirements(soup)
            )
            if clearance_decision == "REJECT":
                return clearance_decision, clearance_reason, []

        except Exception as e:
            logging.debug(f"Page restrictions check failed: {e}")

        return None, None, []

    @staticmethod
    def _check_degree_requirements_strict(soup):
        """ORIGINAL"""
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
        """ORIGINAL"""
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
        import unicodedata

        company = "".join(
            c
            for c in company
            if c.isprintable() and unicodedata.category(c)[0] not in ["C", "So"]
        )
        company = company.strip()
        if not company or len(company) < 2:
            return "Unknown"
        company = re.sub(r"^LE\d{4}\s+", "", company)
        company = re.sub(r"^Company\s+\d+\s+-\s+", "", company)
        company = re.sub(r"^\d+\s+|^[A-Z]{2,4}[-\s]", "", company)
        company = re.sub(r"\s*\([^)]*U\.S\.A\.\)", "", company)
        company = re.sub(r"\s*[+|]\s+[^,]+$", "", company)
        company = re.sub(r"\s+USA$", "", company)
        company = re.sub(r"\s+ODA$", "", company)
        return re.sub(
            r",?\s+(Inc\.?|LLC\.?|Corp\.?|Ltd\.?|Corporation)$", "", company, flags=re.I
        ).strip()

    @staticmethod
    @lru_cache(maxsize=256)
    def extract_company_from_domain(url):
        """ORIGINAL"""
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
        """ORIGINAL"""
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
# Company Extraction - ORIGINAL (ALL 6 METHODS)
# ============================================================================


class CompanyExtractor:
    @staticmethod
    def extract_from_url_mapping(url):
        """ORIGINAL"""
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
        """ORIGINAL"""
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
        """ORIGINAL"""
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
        """ORIGINAL"""
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
        """ORIGINAL"""
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
        """ORIGINAL"""
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
        """ORIGINAL"""
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
        """ORIGINAL: ALL 6 methods"""
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
# Quality Scoring - ORIGINAL
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
