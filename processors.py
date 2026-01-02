#!/usr/bin/env python3
# cSpell:disable
"""
Processing module for job data validation, cleaning, and quality scoring - FINAL VERSION
Handles title cleaning, location formatting, international filtering, and all validation checks.
"""

import re
import datetime
from config import (
    US_STATES,
    CANADA_PROVINCES,
    CITY_TO_STATE,
    CANADA_CITIES,
    MIN_QUALITY_SCORE,
    SPECIAL_COMPANY_NAMES,
)


class TitleProcessor:
    """Processes and validates job titles."""

    @staticmethod
    def clean_title_aggressive(title):
        """Aggressively clean title by removing years, seasons, brackets, etc."""
        if not title or len(title) < 5:
            return title

        original = title

        # Remove parentheses and brackets
        title = re.sub(r"\s*[\(\[].+?[\)\]]", "", title)

        # Remove seasons + years
        title = re.sub(
            r"\s*(Summer|Fall|Winter|Spring)\s*20\d{2}\s*", " ", title, flags=re.I
        )

        # Remove just years
        title = re.sub(r"\s*20\d{2}\s*", " ", title)

        # Remove "or X months"
        title = re.sub(r",?\s*[Oo]r\s+\d+\s+months", "", title)

        # Remove location suffixes
        title = re.sub(
            r"\s*[-,]\s*(Canada|UK|India|Remote|Hybrid).*$", "", title, flags=re.I
        )
        title = re.sub(r"\s*[-,]\s*[A-Z][a-z]+,\s*[A-Z]{2}.*$", "", title)

        # Remove work type suffixes
        title = re.sub(
            r"\s*[-–]\s*(Remote|Hybrid|On-?site|Full[- ]time|Part[- ]time)\s*$",
            "",
            title,
            flags=re.I,
        )

        # Clean up
        title = re.sub(r"\s*[-–—]\s*$", "", title)
        title = re.sub(r"\s+", " ", title).strip()

        # If we removed too much, return original
        if len(title) < 10 and len(original) > 15:
            return original

        return title

    @staticmethod
    def is_valid_job_title(title):
        """Check if title is a valid job posting."""
        if not title or title == "Unknown" or len(title) < 10:
            return False, "Invalid title"

        title_lower = title.lower()

        # Check for marketing phrases
        marketing = [
            "meet your",
            "join our team",
            "learn more",
            "discover how",
            "explore our",
            "contact us",
            "get started",
            "welcome to",
        ]
        for phrase in marketing:
            if phrase in title_lower:
                return False, f"Marketing: '{phrase}'"

        # Must contain job keywords
        job_keywords = [
            "intern",
            "engineer",
            "developer",
            "analyst",
            "scientist",
            "designer",
            "manager",
            "specialist",
            "coordinator",
            "associate",
        ]
        if not any(kw in title_lower for kw in job_keywords):
            return False, "No job keywords"

        return True, None

    @staticmethod
    def is_cs_engineering_role(title):
        """Check if role is CS/Engineering related."""
        title_lower = title.lower()

        # Exclude non-CS roles
        excluded = ["product management", "marketing", "sales", "hr", "finance"]
        if any(kw in title_lower for kw in excluded):
            return False

        # Require CS keywords
        required = [
            "software",
            "swe",
            "engineer",
            "developer",
            "data",
            "tech",
            "algorithm",
            "ml",
            "ai",
        ]
        return any(kw in title_lower for kw in required)

    @staticmethod
    def is_internship_role(title):
        """Check if role is internship/co-op (not senior/experienced/full-time)."""
        title_lower = title.lower()

        # Must contain internship indicators
        internship_keywords = ["intern", "co-op", "coop"]
        if not any(kw in title_lower for kw in internship_keywords):
            return False, "Not internship/co-op role"

        # Exclude senior/experienced roles
        excluded_levels = [
            "senior",
            "sr.",
            "sr ",
            "staff",
            "principal",
            "lead",
            "experienced",
            "expert",
            "architect",
            "director",
            "manager",
        ]
        for level in excluded_levels:
            if level in title_lower:
                return False, f"Senior/experienced role: contains '{level}'"

        return True, None

    @staticmethod
    def check_season_requirement(title, page_text=""):
        """Check if job is for acceptable season (Summer 2026+, Fall 2026+, no Spring/Winter)."""
        combined_text = (title + " " + page_text).lower()

        # Reject Spring 2026
        if re.search(r"spring\s*2026", combined_text, re.I):
            return False, "Spring 2026 posting (too early)"

        # Reject Winter 2026
        if re.search(r"winter\s*2026", combined_text, re.I):
            return False, "Winter 2026 posting (too early)"

        # Reject all 2025 postings
        if re.search(r"(summer|fall|winter|spring)\s*2025", combined_text, re.I):
            return False, "2025 posting (too old)"

        # Accept Summer 2026+ and Fall 2026+
        return True, None


class LocationProcessor:
    """Processes and validates job locations."""

    @staticmethod
    def extract_location_enhanced(soup, url):
        """Enhanced location extraction with Simplify.jobs special handling."""

        # Special handling for Simplify.jobs pages (CRITICAL FIX!)
        if "simplify.jobs" in url.lower():
            simplify_loc = LocationProcessor._extract_from_simplify_page(soup)
            if simplify_loc and simplify_loc != "Unknown":
                print(f"    [Simplify] Found location: '{simplify_loc}'")
                return simplify_loc

        # Method 1: JSON-LD structured data
        json_loc = LocationProcessor._extract_from_json_ld(soup)
        if json_loc and LocationProcessor.is_valid_us_location(json_loc):
            return json_loc

        # Method 2: Labeled fields
        labeled_loc = LocationProcessor._extract_from_labels(soup)
        if labeled_loc and LocationProcessor.is_valid_us_location(labeled_loc):
            return labeled_loc

        # Method 3: Workday URL parsing
        if "workday" in url.lower():
            match = re.search(r"/job/([^/]+)/", url)
            if match:
                location_raw = match.group(1)
                if not location_raw.lower().startswith("remote"):
                    workday_loc = LocationProcessor._parse_workday_location(
                        location_raw
                    )
                    if workday_loc != "Unknown":
                        return workday_loc

        # Method 4: Page scanning
        scanned_loc = LocationProcessor._scan_page(soup)
        if scanned_loc and LocationProcessor.is_valid_us_location(scanned_loc):
            return scanned_loc

        # Method 5: Meta tags
        meta_loc = LocationProcessor._extract_from_meta(soup)
        if meta_loc and LocationProcessor.is_valid_us_location(meta_loc):
            return meta_loc

        return "Unknown"

    @staticmethod
    def _extract_from_simplify_page(soup):
        """Extract location from Simplify.jobs page specifically - ENHANCED VERSION."""
        try:
            # Strategy 1: Look for location text patterns in entire page
            page_text = soup.get_text()

            # Pattern 1: "Montreal, QC, Canada" (most explicit)
            match = re.search(
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2}),\s*(Canada|USA)\b",
                page_text[:3000],
            )
            if match:
                city, state, country = match.group(1), match.group(2), match.group(3)
                return f"{city}, {state}, {country}"

            # Pattern 2: "City, ST" where ST could be province or state
            matches = re.findall(
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b",
                page_text[:3000],
            )

            for city, state in matches:
                state_upper = state.upper()

                # Check if it's a Canadian province
                if state_upper in CANADA_PROVINCES:
                    # Verify it's actually Canada by checking nearby context
                    match_pos = page_text.find(f"{city}, {state}")
                    if match_pos != -1:
                        context = page_text[
                            max(0, match_pos - 100) : min(
                                len(page_text), match_pos + 100
                            )
                        ]
                        # If "Canada" appears in context, it's definitely Canadian
                        if "canada" in context.lower():
                            return f"{city}, {state_upper}, Canada"
                        # If no US states in context, assume Canada
                        if not any(us_st in context for us_st in US_STATES.values()):
                            return f"{city}, {state_upper}, Canada"

                # Check if it's a US state
                if state_upper in US_STATES.values():
                    return f"{city}, {state_upper}"

            # Strategy 2: Look in specific HTML elements
            # Check for elements containing "Montreal", "Toronto", etc.
            location_keywords = [
                "Montreal",
                "Toronto",
                "Vancouver",
                "Ottawa",
                "Calgary",
            ]
            for keyword in location_keywords:
                elements = soup.find_all(
                    ["p", "div", "span", "li"], string=re.compile(keyword, re.I)
                )
                for elem in elements:
                    text = elem.get_text().strip()
                    # Extract full location from this element
                    match = re.search(
                        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})(?:,\s*([A-Za-z]+))?\b",
                        text,
                    )
                    if match:
                        city, state, country = (
                            match.group(1),
                            match.group(2),
                            match.group(3),
                        )
                        if country and country.lower() == "canada":
                            return f"{city}, {state}, Canada"
                        if state.upper() in CANADA_PROVINCES:
                            return f"{city}, {state.upper()}, Canada"

            return "Unknown"
        except Exception as e:
            print(f"    [Simplify extraction error]: {e}")
            return "Unknown"

    @staticmethod
    def _extract_from_json_ld(soup):
        """Extract location from JSON-LD structured data."""
        try:
            json_ld_tags = soup.find_all("script", type="application/ld+json")
            for json_ld in json_ld_tags:
                try:
                    import json

                    data = json.loads(json_ld.string)
                    if isinstance(data, dict):
                        job_loc = data.get("jobLocation", {})
                        if isinstance(job_loc, dict):
                            addr = job_loc.get("address", {})
                            if isinstance(addr, dict):
                                city = addr.get("addressLocality", "")
                                state = addr.get("addressRegion", "")
                                if (
                                    city
                                    and state
                                    and state.upper() in US_STATES.values()
                                ):
                                    return f"{city}, {state.upper()}"
                except:
                    continue
            return None
        except:
            return None

    @staticmethod
    def _extract_from_labels(soup):
        """Extract from labeled fields."""
        try:
            labels = [
                "location:",
                "office location:",
                "work location:",
                "job location:",
            ]

            for label in labels:
                # Check dt/dd pairs
                dt = soup.find("dt", string=re.compile(label, re.I))
                if dt:
                    dd = dt.find_next_sibling("dd")
                    if dd:
                        location = dd.get_text().strip()
                        if len(location) < 100 and "," in location:
                            return location

                # Check spans/divs
                for tag in soup.find_all(
                    ["span", "div", "p"], string=re.compile(label, re.I)
                ):
                    parent = tag.parent
                    if parent:
                        siblings = parent.find_next_siblings()
                        if siblings:
                            location = siblings[0].get_text().strip()
                            if len(location) < 100:
                                return location

            return None
        except:
            return None

    @staticmethod
    def _parse_workday_location(location_str):
        """Parse location from Workday URL format."""
        try:
            location_str = re.sub(r"^[0-9]+[A-Z]+\s*[-–]?\s*", "", location_str)
            location_str = location_str.replace("-", " ").replace("_", " ")
            parts = [p.strip() for p in location_str.split() if p.strip()]

            if not parts:
                return "Unknown"

            state = None
            city_words = []

            for i in range(len(parts) - 1, -1, -1):
                word_upper = parts[i].upper()
                if not state and word_upper in US_STATES.values():
                    state = word_upper
                    continue
                if state:
                    city_words.insert(0, parts[i])
                    potential_city = " ".join(city_words).lower()
                    if potential_city in CITY_TO_STATE:
                        if CITY_TO_STATE[potential_city] == state:
                            return f"{' '.join(city_words).title()} - {state}"

            if state and city_words:
                facility = ["hospital", "building", "pkwy", "patient"]
                cleaned = [w for w in city_words if w.lower() not in facility]
                if cleaned:
                    return f"{' '.join(cleaned).title()} - {state}"

            return "Unknown"
        except:
            return "Unknown"

    @staticmethod
    def _scan_page(soup):
        """Scan page text for location patterns."""
        try:
            page_text = soup.get_text()[:6000]

            # City, State pattern
            pattern = r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b"
            matches = re.findall(pattern, page_text)

            for city, state in matches:
                if state.upper() in US_STATES.values():
                    full_match = f"{city}, {state}"
                    context = page_text[
                        max(0, page_text.find(full_match) - 50) : page_text.find(
                            full_match
                        )
                        + 100
                    ]

                    if not any(
                        skip in context.lower() for skip in ["copyright", "reserved"]
                    ):
                        return f"{city}, {state.upper()}"

            # Known cities
            for city, state in list(CITY_TO_STATE.items())[:100]:
                if re.search(r"\b" + city + r"\b", page_text, re.I):
                    return f"{city.title()}, {state}"

            return None
        except:
            return None

    @staticmethod
    def _extract_from_meta(soup):
        """Extract location from meta tags."""
        try:
            meta_tags = [
                ("property", "og:locality"),
                ("property", "og:region"),
                ("name", "location"),
                ("name", "geo.placename"),
            ]

            for attr, value in meta_tags:
                meta = soup.find("meta", {attr: value})
                if meta and meta.get("content"):
                    return meta.get("content").strip()

            return None
        except:
            return None

    @staticmethod
    def format_location_clean(location):
        """Clean and format location to City - State format."""
        if not location or location == "Unknown":
            return "Unknown"

        location = location.strip()

        # Multiple locations
        if "|" in location or location.count(",") > 2:
            us_count = sum(1 for state in US_STATES.values() if state in location)
            if us_count >= 2:
                return "Multiple US Locations"
            return location

        # Remove USA suffix
        location = re.sub(r",?\s*(USA?|United States)$", "", location, flags=re.I)

        # Extract City, State
        match = re.search(r"([^,]+),\s*([A-Z]{2})(?:,|\s|$)", location)
        if match:
            city, state = match.group(1).strip(), match.group(2).upper()
            if state in US_STATES.values():
                return f"{city} - {state}"
            elif state in CANADA_PROVINCES:
                return f"{city}, {state}"

        # Just state code
        if location.upper() in US_STATES.values():
            return location.upper()

        return re.sub(r"\s+", " ", location).strip()

    @staticmethod
    def is_valid_us_location(location):
        """Check if location is valid US location (not Canada or other countries)."""
        if not location or location == "Unknown":
            return False

        location_lower = location.lower()

        # Explicitly reject if contains "Canada"
        if "canada" in location_lower:
            return False

        # Reject if has Canadian province (with context check)
        for prov in CANADA_PROVINCES:
            if prov in location:
                # Check if it's after a comma or at end (real province code)
                if re.search(r",\s*" + prov + r"\b", location) or location.endswith(
                    prov
                ):
                    return False

        # Reject if has Canadian city
        has_canadian_city = any(city in location_lower for city in CANADA_CITIES.keys())
        if has_canadian_city:
            # Double-check it's not also a US location
            has_us_state = any(state in location for state in US_STATES.values())
            if not has_us_state:
                return False

        has_us_state = any(state in location for state in US_STATES.values())
        has_us_city = any(
            city in location_lower for city in list(CITY_TO_STATE.keys())[:50]
        )

        return has_us_state or has_us_city

    @staticmethod
    def check_if_international(location, soup):
        """Check if location is international (non-US). Returns reason string or None."""
        if not location or location == "Unknown":
            if soup:
                country = LocationProcessor._detect_country_from_page(soup)
                if country and country not in ["USA", "United States", "US"]:
                    return f"Location: {country}"
            return None

        location_lower = location.lower()

        # PRIORITY CHECK 1: Explicit "Canada" in location string
        if "canada" in location_lower:
            return "Location: Canada"

        # PRIORITY CHECK 2: Canadian cities (very specific)
        canadian_cities_definite = [
            "toronto",
            "montreal",
            "vancouver",
            "ottawa",
            "calgary",
            "edmonton",
            "winnipeg",
            "quebec",
            "markham",
            "mississauga",
            "hamilton",
            "kitchener",
            "waterloo",
            "halifax",
            "victoria",
            "oakville",
            "burlington",
            "brampton",
            "windsor",
            "london",
        ]

        for city in canadian_cities_definite:
            if city in location_lower:
                # Make absolutely sure it's not a US location
                if not any(us_state in location for us_state in US_STATES.values()):
                    return "Location: Canada"

        # PRIORITY CHECK 3: Canadian provinces (strict matching)
        for prov in CANADA_PROVINCES:
            # Must be after comma or at end: ", ON" or "ON$"
            if re.search(r",\s*" + prov + r"\b", location) or re.search(
                r"\b" + prov + r"$", location
            ):
                return "Location: Canada"

        # Other international locations
        if re.search(r"\b(uk|united kingdom)\b", location_lower):
            return "Location: UK"

        countries = {
            "australia": "Australia",
            "india": "India",
            "singapore": "Singapore",
            "china": "China",
            "japan": "Japan",
            "germany": "Germany",
        }
        for key, name in countries.items():
            if re.search(r"\b" + key + r"\b", location_lower):
                return f"Location: {name}"

        return None

    @staticmethod
    def _detect_country_from_page(soup):
        """Detect country from page content with strict patterns."""
        try:
            page_text = soup.get_text()[:5000]

            # Very specific Canada patterns
            canada_patterns = [
                r"\b(?:Toronto|Montreal|Vancouver|Ottawa|Calgary),\s*(?:ON|QC|BC|AB),?\s*Canada\b",
                r"\bCanada\s+only\b",
                r"\bMust\s+reside\s+in\s+Canada\b",
                r"\bauthorized\s+to\s+work\s+in\s+Canada\b",
                r"\bCanadian\s+work\s+authorization\b",
                r"\brelocation\s+to\s+(?:a\s+)?Toronto\b",
            ]

            for pattern in canada_patterns:
                if re.search(pattern, page_text, re.I):
                    return "Canada"

            # Don't flag if multiple US cities mentioned
            us_mentions = sum(
                1
                for city in list(CITY_TO_STATE.keys())[:20]
                if city in page_text.lower()
            )
            if us_mentions >= 2:
                return None

            return None
        except:
            return None

    @staticmethod
    def extract_remote_status_enhanced(soup, location, url):
        """Extract remote status from multiple sources."""
        if "remote" in url.lower():
            return "Remote"

        if location:
            location_lower = location.lower()
            if "remote" in location_lower:
                return "Remote"
            if "hybrid" in location_lower:
                return "Hybrid"

        try:
            page_text = soup.get_text()[:3000].lower()

            if re.search(
                r"\b(100%\s*remote|fully\s*remote|remote\s*work)\b", page_text
            ):
                return "Remote"
            if re.search(r"\bhybrid\b", page_text):
                return "Hybrid"
            if re.search(r"\b(on[-\s]?site|in[-\s]?person)\b", page_text):
                return "On Site"

            if location and location != "Unknown":
                return "On Site"

            return "Unknown"
        except:
            return "Unknown"

    @staticmethod
    def extract_location_from_url(url):
        url_lower = url.lower()
        if "workday" in url_lower or "myworkdayjobs" in url_lower:
            if (
                "-can/" in url_lower
                or "-can-" in url_lower
                or "canada-" in url_lower
                or "/canada" in url_lower
            ):
                canada_match = re.search(
                    r"/([A-Za-z-]+)(?:-ON-CAN|-QC-CAN|-BC-CAN|-AB-CAN|---[A-Za-z-]+)",
                    url,
                    re.I,
                )
                if canada_match:
                    city = canada_match.group(1).replace("-", " ").title()
                    return f"{city}, Canada"
                return "Canada"
            canadian_cities = [
                "toronto",
                "ottawa",
                "montreal",
                "vancouver",
                "calgary",
                "edmonton",
            ]
            for city in canadian_cities:
                if city in url_lower:
                    return f"{city.title()}, Canada"
        if "oraclecloud.com" in url_lower:
            if any(c in url_lower for c in ["canada", "toronto", "ottawa", "montreal"]):
                return "Canada"
        return None


class ValidationHelper:
    """Validates company names, URLs, and checks page restrictions."""

    @staticmethod
    def validate_company_field(company, title, url):
        """Validate and fix company field."""
        if not company or company == "Unknown":
            fixed = ValidationHelper.extract_company_from_domain(url)
            return True, fixed if fixed != "Unknown" else company, None

        company_lower = company.lower().strip()
        title_lower = title.lower().strip()

        # Company == Title is bad data
        if company_lower == title_lower:
            fixed = ValidationHelper.extract_company_from_domain(url)
            if fixed != "Unknown" and fixed.lower() != title_lower:
                return True, fixed, None
            return False, company, "Bad data: Company=Title"

        # Year in company name
        if re.search(r"20\d{2}", company):
            return False, company, "Bad data: Year in company"

        # "Intern" in company name
        if re.search(r"\bintern(ship)?\b", company, re.I):
            return False, company, "Bad data: Intern in company"

        return True, company, None

    @staticmethod
    def extract_company_from_domain(url):
        """Extract company name from domain."""
        try:
            if "workday" in url.lower():
                match = re.search(r"https?://([^.]+)\.(?:wd\d+\.)?myworkdayjobs", url)
                if match:
                    slug = match.group(1).lower().replace(" ", "")
                    if slug in SPECIAL_COMPANY_NAMES:
                        return SPECIAL_COMPANY_NAMES[slug]
                    return match.group(1).replace("-", " ").title()

            match = re.search(r"https?://(?:www\.)?([^./]+)", url)
            if match:
                slug = (
                    match.group(1)
                    .lower()
                    .replace("-", " ")
                    .replace("_", " ")
                    .replace(" ", "")
                )
                if slug in SPECIAL_COMPANY_NAMES:
                    return SPECIAL_COMPANY_NAMES[slug]
                return match.group(1).replace("-", " ").replace("_", " ").title()

            return "Unknown"
        except:
            return "Unknown"

    @staticmethod
    def check_page_restrictions(soup):
        """Check for citizenship, clearance, or degree requirements that disqualify."""
        try:
            page_text = soup.get_text().lower()

            # Security clearance
            if "security clearance" in page_text:
                return "Security clearance required"

            # US citizenship
            if "us citizen only" in page_text or "must be a us citizen" in page_text:
                return "US citizenship required"

            # Bachelor's degree requirement (COMPREHENSIVE CHECK)
            bachelor_only_patterns = [
                r"undergraduate\s+students?\s+only",
                r"bachelor'?s?\s+degree\s+in\s+progress",
                r"currently\s+pursuing\s+a\s+bachelor'?s?(?!\s+(or|and)\s+master)",  # "bachelor's" but NOT "bachelor's or master's"
                r"enrolled\s+in\s+(?:a|an)\s+undergraduate\s+degree",
                r"must\s+be\s+enrolled\s+in\s+(?:a|an)\s+undergraduate",
            ]

            for pattern in bachelor_only_patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    # Check 200 chars before and after for "master" or "graduate"
                    context_start = max(0, match.start() - 200)
                    context_end = min(len(page_text), match.end() + 200)
                    context = page_text[context_start:context_end]

                    # If master's/graduate mentioned nearby, it's okay
                    if not re.search(
                        r"(master'?s?|graduate|grad\s+student)", context, re.I
                    ):
                        return "Bachelor's degree requirement (undergrad only)"

            return None
        except:
            return None

    @staticmethod
    def check_sponsorship_status(soup):
        """Check H1B sponsorship status from page."""
        try:
            page_text = soup.get_text().lower()

            positive = [
                "visa sponsorship available",
                "h1b sponsorship",
                "will sponsor",
                "opt eligible",
            ]
            for indicator in positive:
                if indicator in page_text:
                    return "Yes"

            negative = ["no visa sponsorship", "does not sponsor", "cannot sponsor"]
            for indicator in negative:
                if indicator in page_text:
                    return "No"

            return "Unknown"
        except:
            return "Unknown"

    @staticmethod
    def is_valid_job_url(url):
        """Validate that URL is a specific job posting, not a list/recommendation page."""
        if not url or not url.startswith("http"):
            return False, "Invalid URL format"

        url_lower = url.lower()

        # Reject Jobright recommendation/list pages
        if "jobright.ai" in url_lower:
            if "/jobs/recommend" in url_lower:
                return False, "Jobright recommendation page (not specific job)"
            if "/jobs/info/" not in url_lower:
                return False, "Invalid Jobright URL (not specific job)"

        # Reject Chrome Web Store and other non-job URLs
        if "chromewebstore.google.com" in url_lower:
            return False, "Chrome Web Store link (not a job)"

        # Reject other listing/search pages
        invalid_patterns = [
            "/jobs/search",
            "/job-search",
            "/search?",
            "/jobs?",
            "/opportunities",
            "/job-board",
            "/careers?",
            "/job-listing",
        ]

        for pattern in invalid_patterns:
            if pattern in url_lower:
                return False, f"Job listing page: {pattern}"

        return True, None

    @staticmethod
    def check_url_for_canada(url):
        url_lower = url.lower()
        canada_markers = [
            "-can/",
            "-can-",
            "/canada",
            "canada-",
            "canada/",
            "/toronto",
            "/ottawa",
            "/montreal",
            "/vancouver",
            "toronto-on",
            "ottawa-on",
            "montreal-qc",
        ]
        for marker in canada_markers:
            if marker in url_lower:
                return "URL→Canada"
        if ".ca/" in url or url.endswith(".ca"):
            return "Domain→Canada"
        return None


class QualityScorer:
    """Scores job quality based on available data."""

    @staticmethod
    def calculate_score(job_data):
        """Calculate quality score (0-7)."""
        score = 0

        if job_data.get("company") and job_data["company"] != "Unknown":
            score += 2

        if job_data.get("location") and job_data["location"] != "Unknown":
            score += 2

        if job_data.get("job_id") and job_data["job_id"] != "N/A":
            score += 1

        if job_data.get("title") and 15 < len(job_data["title"]) < 120:
            score += 1

        if job_data.get("sponsorship") not in ["Unknown", "Unknown (Email)"]:
            score += 1

        return score

    @staticmethod
    def is_acceptable_quality(score):
        """Check if quality meets minimum threshold."""
        return score >= MIN_QUALITY_SCORE
