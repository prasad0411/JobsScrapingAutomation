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
# Compiled Patterns
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
        excluded = {"application", "click here", "apply now", "view job"}
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
            "cloud",
            "devops",
            "platform",
            "security",
            "qa",
            "test",
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
            "staff",
            "principal",
            "lead",
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
                return False, f"Wrong season: {season_name}"
        return True, ""


# ============================================================================
# Job ID Extraction
# ============================================================================


class JobIDExtractor:
    @staticmethod
    def extract_from_url(url, platform="generic"):
        """ENHANCED: Platform-specific extraction with Glassdoor handling"""
        if not url:
            return ExtractionResult(None, 0.0, "url_extract")

        # Glassdoor: Don't extract job ID
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
                logging.debug(f"Job ID pattern failed: {e}")
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
        except:
            pass
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
        except:
            pass
        return ExtractionResult(None, 0.0, "json_ld")

    @staticmethod
    def extract_from_page_text(soup):
        if not soup:
            return ExtractionResult(None, 0.0, "page_text")
        try:
            page_text = soup.get_text()[:5000]
            patterns = [
                (r"Role\s*ID\s*:?\s*([A-Z0-9\-]{4,20})\b", 0.90),
                (r"Job\s*ID\s*:?\s*([A-Z0-9\-]{4,15})\b", 0.85),
                (r"Req(?:uisition)?\s*ID\s*:?\s*([A-Z0-9\-]{4,20})\b", 0.85),
            ]
            for pattern, confidence in patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    job_id = match.group(1).strip()
                    if JobIDExtractor._is_valid_id(job_id):
                        return ExtractionResult(job_id, confidence, "page_text")
        except:
            pass
        return ExtractionResult(None, 0.0, "page_text")

    @staticmethod
    @lru_cache(maxsize=512)
    def _is_valid_id(job_id):
        if not job_id or not (4 <= len(job_id) <= 40):
            return False
        if not re.match(r"^[A-Z0-9\-_]+$", job_id, re.I):
            return False
        return job_id.upper() not in {"APPLY", "NOW", "HERE", "VIEW", "N/A"}

    @staticmethod
    def extract_all_methods(url, soup, platform="generic"):
        """All methods execute, then vote on non-None results"""
        results = [
            JobIDExtractor.extract_from_url(url, platform),
            JobIDExtractor.extract_from_html_meta(soup),
            JobIDExtractor.extract_from_json_ld(soup),
            JobIDExtractor.extract_from_page_text(soup),
        ]

        # Filter: keep only results with actual values (not None)
        valid_results = [r for r in results if r.value not in [None, ""]]

        if valid_results:
            best_result = ExtractionVoter.vote(
                valid_results, min_confidence=MIN_CONFIDENCE_JOB_ID
            )
            if best_result and best_result.value != "N/A":
                return best_result.value

        # Fallback: Generate hash
        import hashlib

        return f"HASH_{hashlib.md5(url.encode()).hexdigest()[:10]}"


# ============================================================================
# Location Extraction - MULTI-METHOD WITH MANDATORY FALLBACK
# ============================================================================


class LocationExtractor:
    @staticmethod
    def extract_from_json_ld(soup):
        """Method 1: JSON-LD structured data"""
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
                            if city and state:
                                return ExtractionResult(
                                    f"{city}, {state}", 0.95, "json_ld"
                                )
        except:
            pass
        return ExtractionResult(None, 0.0, "json_ld")

    @staticmethod
    def extract_from_html_selectors(soup, platform="generic"):
        """Method 2: Platform-specific HTML selectors"""
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
                        if cleaned and LocationProcessor.is_valid_location_text(
                            cleaned
                        ):
                            return ExtractionResult(
                                cleaned, confidence, "html_selector"
                            )
            except:
                continue

        return ExtractionResult(None, 0.0, "html_selectors")

    @staticmethod
    def extract_from_page_text_patterns(soup):
        """Method 3: Text pattern matching (selector-independent)"""
        if not soup:
            return ExtractionResult(None, 0.0, "text_patterns")

        try:
            page_text = soup.get_text()[:5000]

            # Pattern 1: "Location: City, State" or "Locations: City, State, Country"
            patterns = [
                (
                    r"Locations?:\s*([A-Za-z\s]+,\s*[A-Za-z\s]+(?:,\s*[A-Za-z\s]+)?)",
                    0.85,
                ),
                (r"📍\s*([A-Za-z\s]+,\s*[A-Z]{2})", 0.88),
                (r">([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?,\s*[A-Z]{2})<", 0.75),
            ]

            for pattern, confidence in patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    location = match.group(1).strip()
                    if len(location) < 100:
                        cleaned = LocationProcessor.clean_location_aggressive(location)
                        if cleaned and LocationProcessor.is_valid_location_text(
                            cleaned
                        ):
                            return ExtractionResult(cleaned, confidence, "text_pattern")
        except:
            pass

        return ExtractionResult(None, 0.0, "text_patterns")

    @staticmethod
    def extract_from_url_workday_ultimate(url):
        """Method 4: ULTIMATE Workday URL parser with RIGHT-TO-LEFT state matching"""
        if not url or "workday" not in url.lower():
            return ExtractionResult(None, 0.0, "url_workday")

        try:
            # Extract location segment from URL
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

            # Step 2: Remove address numbers (e.g., "-307-Legget-Dr")
            segment = re.sub(r"-\d{3,}-.*$", "", segment)

            # Step 3: Split by hyphen
            parts = segment.split("-")
            if len(parts) < 2:
                return ExtractionResult(None, 0.0, "url_workday")

            # Step 4: Find state using RIGHT-TO-LEFT matching
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
                # Check if it's a 2-letter state code
                if len(last_part) == 2 and validate_us_state_code(last_part):
                    state_code = last_part.upper()
                    state_end_index = len(parts) - 1
                # Check if it's a full state name
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

            # No state found - check if it's a Canadian city
            first_word = parts[0].lower()
            if first_word in MAJOR_CANADIAN_CITIES:
                # Return special marker for Canadian detection
                return ExtractionResult(
                    f"CANADIAN_CITY_{parts[0]}", 0.88, "url_canadian_city"
                )

        except:
            pass

        return ExtractionResult(None, 0.0, "url_workday")

    @staticmethod
    def extract_from_url_generic(url):
        """Method 5: Generic URL path extraction"""
        if not url:
            return ExtractionResult(None, 0.0, "url_generic")

        try:
            # Pattern: City-State format
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
                        return ExtractionResult(f"{city}, {state}", 0.75, "url_generic")
        except:
            pass

        return ExtractionResult(None, 0.0, "url_generic")

    @staticmethod
    def extract_from_title(title):
        """Method 6: Extract from job title"""
        if not title:
            return ExtractionResult(None, 0.0, "title")
        try:
            match = re.search(r"[-–]\s*([A-Za-z\s]+,\s*[A-Z]{2})(?:\s|$)", title)
            if match:
                return ExtractionResult(match.group(1).strip(), 0.80, "title")
            if re.search(r"\(remote\)", title, re.I):
                return ExtractionResult("Remote", 0.90, "title_remote")
        except:
            pass
        return ExtractionResult(None, 0.0, "title")

    @staticmethod
    def extract_all_methods(url, soup, title="", platform="generic"):
        """
        CRITICAL FIX: ALL methods execute, then vote on non-None results
        URL parsing ALWAYS runs as mandatory fallback
        """
        # Execute ALL extraction methods (no short-circuit)
        results = [
            LocationExtractor.extract_from_json_ld(soup),
            LocationExtractor.extract_from_html_selectors(soup, platform),
            LocationExtractor.extract_from_page_text_patterns(soup),
            LocationExtractor.extract_from_url_workday_ultimate(url),  # MANDATORY
            LocationExtractor.extract_from_url_generic(url),  # MANDATORY
            LocationExtractor.extract_from_title(title),
        ]

        # Filter: Keep only results with actual values (not None)
        valid_results = [r for r in results if r.value not in [None, ""]]

        # Vote on valid results
        if valid_results:
            best_result = ExtractionVoter.vote(
                valid_results, min_confidence=MIN_CONFIDENCE_LOCATION
            )
            if best_result:
                return LocationProcessor.format_location_clean(best_result.value)

        return "Unknown"


# ============================================================================
# Location Processing - ENHANCED
# ============================================================================


class LocationProcessor:
    @staticmethod
    @lru_cache(maxsize=512)
    def clean_location_aggressive(location_text):
        if not location_text or len(location_text) < 2:
            return None

        try:
            location = location_text.strip()

            # Remove metadata
            for pattern in _COMPILED_METADATA_PATTERNS:
                location = pattern.sub("", location)

            # Remove HTML artifacts
            for pattern in _COMPILED_ARTIFACT_PATTERNS:
                location = pattern.sub("", location)

            # Remove parentheticals
            location = re.sub(r"\s*\([^)]*\)", "", location)

            # Remove country suffixes
            location = re.sub(
                r",?\s*(?:USA|United States)\s*", "", location, flags=re.I
            )

            # Extract City, State pattern
            match = _CITY_STATE_PATTERN.search(location)
            if match:
                city, state = match.group(1).strip(), match.group(2).upper()
                if validate_us_state_code(state):
                    return f"{city}, {state}"

            location = re.sub(r"\s+", " ", location).strip()

            if not location or len(location) < 2:
                return None

            # Reject department keywords
            if location.lower() in DEPARTMENT_KEYWORDS:
                return None

            if any(kw in location.lower() for kw in INVALID_LOCATION_KEYWORDS):
                return None

            # Try to infer state from city
            state = get_state_for_city(location)
            if state:
                return f"{location.title()}, {state}"

            return location
        except:
            return None

    @staticmethod
    @lru_cache(maxsize=512)
    def format_location_clean(location):
        """ENHANCED: Multi-step normalization with validation"""
        if not location or location == "Unknown":
            return "Unknown"

        try:
            # Handle Canadian city marker from URL extraction
            if location.startswith("CANADIAN_CITY_"):
                city = location.replace("CANADIAN_CITY_", "")
                return f"{city}, CANADA"  # Signal for rejection

            # Handle standard special values
            if location in ["Remote", "Hybrid"]:
                return location

            # Handle Workday HQ codes
            if location in WORKDAY_HQ_CODES:
                city, state = WORKDAY_HQ_CODES[location]
                return f"{city}, {state}" if state != "UNKNOWN" else "Unknown"

            # Clean the location
            location = LocationProcessor.clean_location_aggressive(location)
            if not location:
                return "Unknown"

            # Strip location suffixes
            for suffix in LOCATION_SUFFIXES:
                if location.endswith(suffix):
                    location = location[: -len(suffix)].strip()
                    break

            # Expand city abbreviations
            if location.lower() in CITY_ABBREVIATIONS:
                return CITY_ABBREVIATIONS[location.lower()]

            # Convert full state names to codes (critical for Workday)
            location = LocationProcessor.convert_state_name_to_code(location)

            # Validate final result
            if not LocationProcessor.is_valid_location_text(location):
                return "Unknown"

            return location
        except:
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

        # Then try 1-word states
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

        if len(text) > 50 or len(text) < 3:
            return False

        # Reject garbage phrases
        garbage = [
            "as well as",
            "in accordance",
            "equal opportunity",
            "without regard",
            "applicants",
            "candidates",
        ]
        text_lower = text.lower()
        if any(phrase in text_lower for phrase in garbage):
            return False

        # Reject too generic
        if text_lower in ["united states", "us", "usa", "headquarters", "office"]:
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
            if "hybrid" in page_text[:500]:
                return "Hybrid"
            if "on-site" in page_text[:500] or "onsite" in page_text[:500]:
                return "On Site"
        except:
            pass
        return "Unknown"

    @staticmethod
    def check_if_international(location, soup=None, url=None):
        """
        ENHANCED: Multi-method Canadian detection
        CRITICAL: Always runs even if location = "Unknown"
        """

        # Method 1: Check extracted location
        if location and location not in ["Unknown", ""]:
            location_lower = location.lower()

            # Check for "CANADA" marker from URL extraction
            if ", CANADA" in location or "CANADIAN_CITY_" in location:
                return "Location: Canada (from URL city)"

            # Explicit Canada keyword
            if "canada" in location_lower:
                return "Location: Canada"

            # Canadian provinces (full names)
            for full_name, code in CANADA_PROVINCE_NAMES.items():
                if re.search(rf"\b{full_name}\b", location_lower, re.I):
                    if not (full_name == "ontario" and ", ca" in location_lower):
                        return f"Location: Canada ({full_name.title()})"

            # Province codes
            for province in CANADA_PROVINCES:
                if re.search(rf",\s*{province}\b", location):
                    if not (province == "ON" and ", ca" in location_lower):
                        return f"Location: Canada (province: {province})"

            # Major Canadian cities
            for city, province in MAJOR_CANADIAN_CITIES.items():
                if city in location_lower:
                    if city in AMBIGUOUS_CITIES:
                        resolved = LocationProcessor._resolve_ambiguous_city(
                            city, location, soup, url
                        )
                        if resolved == "Canada":
                            return f"Location: Canada ({city.title()})"
                    else:
                        return f"Location: Canada ({city.title()})"

        # Method 2: CRITICAL - Extract city from URL (works even if location = "Unknown")
        city_from_url = LocationProcessor._extract_city_from_url(url)
        if city_from_url:
            city_lower = city_from_url.lower()
            if city_lower in MAJOR_CANADIAN_CITIES:
                if city_lower not in AMBIGUOUS_CITIES:
                    return f"Location: Canada ({city_from_url})"
                else:
                    resolved = LocationProcessor._resolve_ambiguous_city(
                        city_from_url, location, soup, url
                    )
                    if resolved == "Canada":
                        return f"Location: Canada ({city_from_url})"

        # Method 3: Page text patterns
        page_result = LocationProcessor._check_page_for_canada(soup)
        if page_result:
            return page_result

        # Method 4: URL domain/path
        url_result = LocationProcessor._check_url_for_canada(url)
        if url_result:
            return url_result

        return None

    @staticmethod
    def _extract_city_from_url(url):
        """NEW: Extract first city name from URL path"""
        if not url:
            return None
        try:
            match = re.search(r"/job/([A-Z][a-z]+)(?:-|/)", url)
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

        return "US"  # Default to US for truly ambiguous

    @staticmethod
    def _check_page_for_canada(soup):
        """Check page text for Canadian indicators"""
        if not soup:
            return None
        try:
            page_text = soup.get_text()[:10000].lower()

            patterns = [
                (r"vancouver\s*,?\s*british\s*columbia", "vancouver, british columbia"),
                (r"ottawa\s*,?\s*ontario", "ottawa, ontario"),
                (r"toronto\s*,?\s*ontario", "toronto, ontario"),
                (r"montreal\s*,?\s*quebec", "montreal, quebec"),
                (r"(work|located|based)\s+in\s+canada", "page text indicator"),
            ]

            for pattern, desc in patterns:
                if re.search(pattern, page_text, re.I):
                    return f"Location: Canada ({desc})"
        except:
            pass
        return None

    @staticmethod
    def _check_url_for_canada(url):
        """Check URL for Canadian indicators"""
        if not url:
            return None

        url_lower = url.lower()

        if ".ca/" in url_lower or url_lower.endswith(".ca"):
            return "Location: Canada (domain .ca)"

        patterns = [
            "/montreal-quebec",
            "/toronto-ontario",
            "/ottawa-ontario",
            "/vancouver-british-columbia",
            "/calgary-alberta",
            "/canada/",
        ]

        for pattern in patterns:
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
        for pattern in ["/unsubscribe", "/my-alerts", "/blog", "/terms", "/privacy"]:
            if pattern in url_lower:
                return False, f"{pattern.split('/')[-1].title()} link"
        return True, None

    @staticmethod
    def check_url_for_international(url):
        return LocationProcessor._check_url_for_canada(url)

    @staticmethod
    def check_page_restrictions(soup):
        if not soup:
            return None, None, []
        try:
            page_text = soup.get_text()[:15000].lower()
            patterns = {
                r"u\.?s\.?\s+citizenship\s+(?:is\s+)?required": "US citizenship required",
                r"(?:clearance.*required)": "Security clearance required",
            }
            for pattern, reason in patterns.items():
                if re.search(pattern, page_text, re.I):
                    return "REJECT", reason, []
        except:
            pass
        return None, None, []

    @staticmethod
    def validate_company_field(company, title, url):
        if not company or company == "Unknown" or not company.strip():
            return True, ValidationHelper.extract_company_from_domain(url), None
        company = company.strip()
        if not CompanyValidator.is_valid(company):
            return True, ValidationHelper.extract_company_from_domain(url), None
        if len(company) > 100:
            return False, company, "Company name too long"
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
        except:
            pass
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
        except:
            pass
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
        except:
            pass
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
        except:
            pass
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
        except:
            pass
        return ExtractionResult(None, 0.0, "meta_tags")

    @staticmethod
    def extract_all_methods(url, soup):
        platform = PlatformDetector.detect(url)
        results = [
            CompanyExtractor.extract_from_url_mapping(url),
            CompanyExtractor.extract_from_json_ld(soup),
            CompanyExtractor.extract_from_meta_tags(soup),
        ]
        valid_results = [r for r in results if r.value not in [None, ""]]
        if valid_results:
            best_result = ExtractionVoter.vote(
                valid_results, min_confidence=MIN_CONFIDENCE_COMPANY
            )
            if best_result:
                return best_result.value
        return ValidationHelper.extract_company_from_domain(url)


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
