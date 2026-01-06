#!/usr/bin/env python3
# cSpell:disable
"""
Processing module - PRODUCTION v5.0 FINAL
DUAL LIBRARIES: us + pgeocode for comprehensive city/state mapping
ENHANCED DETECTION: Security clearance, Bachelor's requirements
"""

import re
import datetime

# ✅ Import dual libraries for city/state mapping
try:
    import us as us_library

    US_LIBRARY_AVAILABLE = True
except ImportError:
    US_LIBRARY_AVAILABLE = False
    print("⚠️  'us' library not installed - state name conversion disabled")

try:
    import pgeocode

    PGEOCODE_AVAILABLE = True
    # Initialize once
    _pgeocode_nomi = pgeocode.Nominatim("us")
except ImportError:
    PGEOCODE_AVAILABLE = False
    print("⚠️  'pgeocode' library not installed - city lookup disabled")

from config import (
    US_STATES,
    CANADA_PROVINCES,
    CITY_TO_STATE,
    CANADA_CITIES,
    MIN_QUALITY_SCORE,
    SPECIAL_COMPANY_NAMES,
    MAX_JOB_AGE_DAYS,
)


class TitleProcessor:
    """Processes and validates job titles."""

    @staticmethod
    def clean_title_aggressive(title):
        """Aggressively clean title."""
        if not title or len(title) < 5:
            return title

        original = title

        title = re.sub(r"\s*[\(\[].+?[\)\]]", "", title)
        title = re.sub(r"\s*\([^)]*$", "", title)
        title = re.sub(r"\s*\[[^\]]*$", "", title)

        title = re.sub(
            r"\s*-?\s*(Summer|Fall|Spring|Winter)\s*20\d{2}", "", title, flags=re.I
        )
        title = re.sub(
            r"\s*-?\s*20\d{2}\s*(Summer|Fall|Spring|Winter)", "", title, flags=re.I
        )
        title = re.sub(r"\s*-?\s*20\d{2}\s*-?\s*", " ", title)

        title = re.sub(
            r"\s*[\(\[]\s*(BS/MS|MS|PhD|Bachelor|Master).*?[\)\]]",
            "",
            title,
            flags=re.I,
        )

        title = re.sub(r"\s+", " ", title).strip()
        title = re.sub(r"\s*-\s*$", "", title)
        title = re.sub(r"^\s*-\s*", "", title)

        if len(title) < 5:
            return original

        return title

    @staticmethod
    def is_valid_job_title(title):
        """Check if title is valid."""
        if not title or len(title) < 5:
            return False, "Title too short"

        title_lower = title.lower()

        excluded_phrases = [
            "application",
            "click here",
            "apply now",
            "view job",
            "see more",
            "show all",
            "sign in",
            "submit your",
        ]

        for phrase in excluded_phrases:
            if phrase in title_lower:
                return False, f"Invalid title pattern: {phrase}"

        return True, None

    @staticmethod
    def is_cs_engineering_role(title):
        """Check if role is CS/engineering."""
        title_lower = title.lower()

        cs_keywords = [
            "software",
            "engineer",
            "developer",
            "programming",
            "data",
            "machine learning",
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
            "infrastructure",
            "security",
            "qa",
            "test",
            "automation",
            "embedded",
            "firmware",
            "systems",
            "network",
            "database",
            "analytics",
            "computer",
            "tech",
            "it ",
            "cyber",
            "application",
            "app ",
        ]

        return any(kw in title_lower for kw in cs_keywords)

    @staticmethod
    def is_internship_role(title):
        """Check if role is internship/co-op."""
        title_lower = title.lower()

        internship_keywords = ["intern", "co-op", "coop"]
        if not any(kw in title_lower for kw in internship_keywords):
            return False, "Not internship/co-op role"

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
        """✅ COMPREHENSIVE: Season validation with all exceptions."""
        combined_text = (title + " " + page_text).lower()

        wrong_patterns = [
            (r"fall\s*20\d{2}", "Fall"),
            (r"fall\s*semester", "Fall semester"),
            (r"winter\s*20\d{2}", "Winter"),
            (r"winter\s*semester", "Winter semester"),
            (r"spring\s*20\d{2}", "Spring"),
            (r"spring\s*semester", "Spring semester"),
        ]

        for pattern, season_name in wrong_patterns:
            match = re.search(pattern, combined_text, re.I)
            if match:
                season = match.group(0)

                # ✅ EXCEPTION 1: Multiple seasons including Summer
                multi_season_patterns = [
                    r"spring.*summer",
                    r"summer.*spring",
                    r"spring/summer",
                    r"summer/spring",
                    r"spring,\s*summer",
                    r"summer,\s*spring",
                ]

                for multi_pattern in multi_season_patterns:
                    if re.search(multi_pattern, combined_text, re.I):
                        return True, ""

                # ✅ EXCEPTION 2: Winter 2025/2026 (ends Summer 2026)
                if "winter" in season.lower():
                    if re.search(r"(winter.*2025/2026|2025/2026)", combined_text, re.I):
                        return True, ""

                # Reject ONLY wrong season
                if "fall" in season.lower():
                    return False, f"Wrong season: {season_name}"
                elif "winter" in season.lower():
                    return False, f"Wrong season: {season_name}"
                elif "spring" in season.lower():
                    if not re.search(r"summer", combined_text, re.I):
                        return False, f"Wrong season: Spring only"

        # Reject 2025 or earlier (with Winter 2025/2026 exception)
        year_match = re.search(r"(fall|spring|winter)\s*202[0-5]", combined_text, re.I)
        if year_match:
            if not re.search(r"(winter.*2025/2026|2025/2026)", combined_text, re.I):
                return False, f"Wrong season: {year_match.group(0).title()}"

        return True, ""


class LocationProcessor:
    """✅ PRODUCTION: Processes locations with dual library support."""

    @staticmethod
    def extract_location_enhanced(soup, url):
        """Enhanced location extraction."""

        url_location = LocationProcessor._extract_from_url(url)
        if url_location and url_location != "Unknown":
            return url_location

        if "simplify.jobs" in url.lower():
            simplify_loc = LocationProcessor._extract_from_simplify_page(soup)
            if simplify_loc and simplify_loc != "Unknown":
                return simplify_loc

        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                import json

                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    job_location = data.get("jobLocation", {})
                    if isinstance(job_location, dict):
                        address = job_location.get("address", {})
                        if isinstance(address, dict):
                            city = address.get("addressLocality", "")
                            state = address.get("addressRegion", "")
                            if city and state:
                                return f"{city}, {state}"
                            elif city:
                                return city
            except:
                pass

        location_labels = [
            "Location:",
            "Office Location:",
            "Job Location:",
            "Primary Location:",
            "Work Location:",
        ]

        page_text = soup.get_text()
        for label in location_labels:
            pattern = re.escape(label) + r"\s*([A-Za-z\s,]+(?:,\s*[A-Z]{2})?)"
            match = re.search(pattern, page_text[:3000])
            if match:
                location = match.group(1).strip()
                location = re.sub(r"\s*\(.*?\)", "", location)
                if 3 < len(location) < 100:
                    return location

        location_pattern = r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b"
        matches = re.findall(location_pattern, page_text[:2000])

        if matches:
            city, state = matches[0]
            if state in US_STATES.values():
                return f"{city}, {state}"

        if "Remote" in page_text[:1500]:
            return "Remote"

        return "Unknown"

    @staticmethod
    def _extract_from_url(url):
        """Extract location from URL."""
        if not url:
            return None

        url_lower = url.lower()

        if "workday" in url_lower or "myworkdayjobs" in url_lower:
            match = re.search(
                r"/([A-Z][a-z]+(?:-[A-Z][a-z]+)*)-([A-Z]{2})-(?:United-States|USA)/",
                url,
                re.I,
            )
            if match:
                city = match.group(1).replace("-", " ")
                state = match.group(2).upper()
                if state in US_STATES.values():
                    return f"{city}, {state}"

            match = re.search(r"/([A-Z][a-z]+(?:-[A-Z][a-z]+)*)-([A-Z]{2})/", url, re.I)
            if match:
                city = match.group(1).replace("-", " ")
                state = match.group(2).upper()
                if state in US_STATES.values():
                    return f"{city}, {state}"

        if "/remote" in url_lower or "remote-" in url_lower:
            return "Remote"

        return None

    @staticmethod
    def _extract_from_simplify_page(soup):
        """Extract from Simplify.jobs page."""
        try:
            page_text = soup.get_text()

            match = re.search(r"Location:\s*([A-Za-z\s,]+,\s*[A-Z]{2})", page_text)
            if match:
                return match.group(1).strip()

            match = re.search(
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b", page_text[:1000]
            )
            if match:
                city, state = match.group(1), match.group(2)
                if state in US_STATES.values():
                    return f"{city}, {state}"

            return None
        except:
            return None

    @staticmethod
    def extract_remote_status_enhanced(soup, location, url):
        """Extract remote work status."""
        if not soup:
            return "Unknown"

        if location:
            location_lower = location.lower()
            if "remote" in location_lower:
                return "Remote"
            if "hybrid" in location_lower:
                return "Hybrid"

        page_text = soup.get_text()[:2000]
        page_lower = page_text.lower()

        if "100% remote" in page_lower or "fully remote" in page_lower:
            return "Remote"
        if "remote" in page_lower[:500]:
            return "Remote"
        if "hybrid" in page_lower[:500]:
            return "Hybrid"
        if "on-site" in page_lower[:500] or "onsite" in page_lower[:500]:
            return "On Site"

        return "Unknown"

    @staticmethod
    def format_location_clean(location):
        """✅ BULLETPROOF: 6-pattern extraction + dual library fallback."""
        if not location or location == "Unknown":
            return "Unknown"

        if location in ["Remote", "Hybrid"]:
            return location

        original = location.strip()

        # ✅ FIX: Clean common suffixes first (ByteDance "San JoseTeam")
        location = re.sub(
            r"(Team|Department|Division|Group|Office|Building|Campus).*$",
            "",
            original,
            flags=re.I,
        )
        location = location.strip()

        # ✅ STEP 1: Aggressive cleaning
        location = re.sub(r"\s*\d{5}(-\d{4})?\s*", " ", location)
        location = re.sub(
            r",?\s*(?:USA|U\.S\.A\.|United States)\s*", "", location, flags=re.I
        )
        location = re.sub(r"^[A-Z]{2}\d+:\s*", "", location)
        location = re.sub(
            r"\s*(?:Bldg|Building|Office|Suite|Floor|Drive|Street|Road|Avenue|Concord).*$",
            "",
            location,
            flags=re.I,
        )
        location = re.sub(r"\s*-\s*Building\s+\d+.*$", "", location, flags=re.I)

        # ✅ STEP 2: Six-pattern extraction

        # Pattern 1: Standard "City, ST"
        match = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b", location)
        if match:
            city = match.group(1).strip()
            state = match.group(2).upper()
            if state in US_STATES.values():
                return f"{city} - {state}"

        # Pattern 2: "ST - City"
        match = re.search(
            r"([A-Z]{2})\s*-\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", location
        )
        if match:
            state = match.group(1).upper()
            city = match.group(2).strip().title()
            if state in US_STATES.values():
                return f"{city} - {state}"

        # Pattern 3: "US, ST - City" or "US - ST - City"
        match = re.search(
            r"(?:US\s*,?\s*)?([A-Z]{2})\s*-\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            location,
            re.I,
        )
        if match:
            state = match.group(1).upper()
            city = match.group(2).strip().title()
            if state in US_STATES.values():
                return f"{city} - {state}"

        # Pattern 4: Full state name "Washington - Pullman"
        if US_LIBRARY_AVAILABLE:
            match = re.search(
                r"(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming)\s*-\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
                location,
                re.I,
            )
            if match:
                state_name = match.group(1).strip()
                city = match.group(2).strip().title()

                try:
                    state_obj = us_library.states.lookup(state_name)
                    if state_obj:
                        return f"{city} - {state_obj.abbr}"
                except:
                    pass

        # Pattern 5: Multiple commas "City, State, ZIP"
        location_clean = re.sub(r"\s+", " ", location).strip()
        if "," in location_clean:
            parts = [p.strip() for p in location_clean.split(",") if p.strip()]
            if len(parts) >= 2:
                potential_state = parts[-1].upper()

                # Map full state name to abbreviation
                for state_name, abbr in US_STATES.items():
                    if potential_state.lower() == state_name:
                        potential_state = abbr
                        break

                if potential_state in US_STATES.values():
                    city = parts[0].title()
                    return f"{city} - {potential_state}"

        # Pattern 6: City only - infer state from CITY_TO_STATE
        location_lower = location_clean.lower()
        if location_lower in CITY_TO_STATE:
            state = CITY_TO_STATE[location_lower]
            city = location_clean.title()
            return f"{city} - {state}"

        # ✅ FIX: Known tech cities (ByteDance "San Jose" without state)
        known_tech_cities = {
            "san jose": "CA",
            "foster city": "CA",
            "san francisco": "CA",
            "seattle": "WA",
            "pullman": "WA",
            "boston": "MA",
            "tewksbury": "MA",
        }

        if location_lower in known_tech_cities:
            state = known_tech_cities[location_lower]
            city = location_clean.title()
            return f"{city} - {state}"

        # ✅ STEP 3: Dual library fallback

        # Library 1: pgeocode lookup
        if PGEOCODE_AVAILABLE and len(location_clean) > 2 and len(location_clean) < 50:
            try:
                # Query city name
                result = _pgeocode_nomi.query_location(location_clean, top_k=1)
                if not result.empty and result.iloc[0]["state_code"]:
                    state_code = result.iloc[0]["state_code"]
                    if state_code in US_STATES.values():
                        return f"{location_clean.title()} - {state_code}"
            except:
                pass

        # Library 2: us library for state validation
        if US_LIBRARY_AVAILABLE:
            try:
                state_obj = us_library.states.lookup(location_clean)
                if state_obj:
                    # It's a state name, not a city
                    return location_clean
            except:
                pass

        # Fallback: return cleaned version
        return location_clean

    @staticmethod
    def check_if_international(location, soup=None):
        """✅ International check with US validation bypass."""
        if not location or location == "Unknown":
            return None

        location_lower = location.lower()

        # Canadian provinces
        for province in CANADA_PROVINCES:
            if f", {province}" in location or f" {province}" in location.upper():
                if province == "ON" and ", ca" in location_lower:
                    continue
                return f"Location: Canada (province: {province})"

        # Canadian cities
        for city, province in CANADA_CITIES.items():
            if city in location_lower:
                return f"Location: Canada ({city.title()})"

        if "canada" in location_lower:
            return "Location: Canada"

        # UK
        uk_indicators = ["uk", "united kingdom", "london", "manchester", "edinburgh"]
        if any(indicator in location_lower for indicator in uk_indicators):
            return "Location: UK"

        # Other countries
        other_countries = [
            "india",
            "china",
            "australia",
            "singapore",
            "germany",
            "france",
            "japan",
            "ireland",
            "netherlands",
            "israel",
        ]

        for country in other_countries:
            if country in location_lower:
                return f"Location: {country.title()}"

        # ✅ BYPASS: If "City - ST" format → Validated US
        if re.search(r"^[A-Z][a-z]+(?: [A-Z][a-z]+)?\s+-\s+[A-Z]{2}$", location):
            state = location.split("-")[1].strip()
            if state in US_STATES.values():
                return None

        # Only scan soup if location is vague
        if soup and location in [
            "Unknown",
            "Remote",
            "Hybrid",
            "United States",
            "USA",
            "US",
        ]:
            country = LocationProcessor._aggressive_country_scan(soup)
            if country and country not in ["USA", "United States", "US"]:
                return f"Location: {country}"

        return None

    @staticmethod
    def _aggressive_country_scan(soup):
        """Scan page for country mentions."""
        if not soup:
            return None

        page_text = soup.get_text()[:3000]

        country_patterns = [
            (r"\bCanada\b", "Canada"),
            (r"\bUnited Kingdom\b", "UK"),
            (r"\bU\.?K\.?\b", "UK"),
            (r"\bIndia\b", "India"),
            (r"\bChina\b", "China"),
            (r"\bAustralia\b", "Australia"),
            (r"\bSingapore\b", "Singapore"),
        ]

        for pattern, country_name in country_patterns:
            if re.search(pattern, page_text, re.I):
                return country_name

        return None


class ValidationHelper:
    """Helper methods for validation."""

    @staticmethod
    def is_valid_job_url(url):
        """Check if URL is valid."""
        if not url or not url.startswith("http"):
            return False, "Invalid URL format"

        url_lower = url.lower()

        if "jobright.ai" in url_lower:
            if "/jobs/info/" not in url_lower:
                return False, "Invalid Jobright URL"
            if "/jobs/recommend" in url_lower:
                return False, "Jobright recommendation page"

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
        """Check URL for international indicators."""
        if not url:
            return None

        url_lower = url.lower()

        canadian_patterns = [
            "/en-ca/",
            "/canada/",
            "/toronto/",
            "/vancouver/",
            "-canada-",
        ]

        for pattern in canadian_patterns:
            if pattern in url_lower:
                return f"International: Canada (from URL)"

        return None

    @staticmethod
    def check_page_restrictions(soup):
        """✅ COMPREHENSIVE: Multi-layer restriction detection."""
        if not soup:
            return None

        # ✅ FIX: Scan more of page (15000 chars instead of 5000)
        page_text = soup.get_text()[:15000]
        page_lower = page_text.lower()

        # ✅ LAYER 1: Security Clearance (Dual Detection)

        # Method 1: Citizenship requirement (HIGH CONFIDENCE)
        citizenship_patterns = [
            r"u\.?s\.?\s+citizen(?:ship)?\s+(?:required|only|is required)",
            r"must be a u\.?s\.?\s+citizen",
            r"only u\.?s\.?\s+citizens\s+(?:are\s+)?eligible",
            r"citizenship:\s*u\.?s\.?\s+(?:required|only)",
        ]

        for pattern in citizenship_patterns:
            if re.search(pattern, page_lower, re.I):
                return "US citizenship required"

        # Method 2: Context window analysis
        clearance_mentions = [
            m.start() for m in re.finditer(r"\bclearance\b", page_lower, re.I)
        ]

        for mention_pos in clearance_mentions:
            # Extract 500-char window
            context_start = max(0, mention_pos - 250)
            context_end = min(len(page_lower), mention_pos + 250)
            context = page_lower[context_start:context_end]

            # Requirement indicators
            requirement_keywords = [
                "required",
                "must have",
                "must obtain",
                "necessary",
                "prerequisite",
            ]
            soft_keywords = [
                "preferred",
                "nice to have",
                "help you obtain",
                "opportunity to",
            ]

            has_requirement = any(kw in context for kw in requirement_keywords)
            has_soft = any(kw in context for kw in soft_keywords)

            # Additional check for "eligible for" + "only" pattern (RTX case)
            if "eligible" in context and "only" in context:
                has_requirement = True

            if has_requirement and not has_soft:
                return "Security clearance required"

        # ✅ LAYER 2: Bachelor's Requirement (COMPREHENSIVE - 15+ patterns)

        # First check for ACCEPTANCE patterns
        accept_patterns = [
            r"bachelor'?s?\s+or\s+master'?s?",
            r"bachelor'?s?\s+and\s+master'?s?",
            r"bachelor'?s?\s+(?:or|and)\s+above",
            r"bachelor'?s?\s+(?:or|and)\s+higher",
            r"bachelor'?s?\s+degree\s+or\s+higher",
            r"bachelor\s+or\s+master\s+student",  # No apostrophe
            r"bachelor\s+or\s+higher",  # No apostrophe
            r"bachelors?\s+and\s+above",  # With/without apostrophe
            r"undergraduate\s+or\s+graduate",
            r"all\s+degree\s+levels?",
            r"any\s+degree\s+level",
            r"junior,?\s+senior,?\s+(?:or\s+)?graduate",
            r"(?:ms|m\.s\.|master'?s?)\s+(?:student|candidate|degree)",
            r"graduate\s+student",
            r"currently\s+pursuing\s+(?:a|your)\s+degree",  # No level specified = all welcome
        ]

        has_acceptance = any(
            re.search(pattern, page_lower, re.I) for pattern in accept_patterns
        )

        # Then check for REJECTION patterns (only if no acceptance found)
        if not has_acceptance:
            reject_patterns = [
                r"bachelor'?s?\s+degree.*(?:required|pursuing|only)",
                r"undergraduate.*degree.*required",
                r"must be.*pursuing.*bachelor'?s?",
                r"undergraduate.*only",
                r"4-year\s+degree\s+only",
                r"bachelor'?s?\s+(?:student|candidate).*only",
            ]

            for pattern in reject_patterns:
                if re.search(pattern, page_lower, re.I):
                    return "Bachelor's degree requirement (undergrad only)"

        # ✅ LAYER 3: Graduation Year (User graduates May 2027)

        # Extract graduation dates
        grad_patterns = [
            r"expected\s+graduation[:\s]+([a-z]+)?\s*(20\d{2})",
            r"graduation\s+date[:\s]+([a-z]+)?\s*(20\d{2})",
            r"graduating\s+([a-z]+)?\s*(20\d{2})",
        ]

        for pattern in grad_patterns:
            match = re.search(pattern, page_lower, re.I)
            if match:
                month = match.group(1) if match.lastindex >= 1 else None
                year = int(match.group(2) if match.lastindex >= 2 else match.group(1))

                # Before May 2027 → REJECT
                if year < 2027:
                    return f"Graduation requirement: {month.title() if month else ''} {year} (before May 2027)"

                if year == 2027:
                    # Check month if specified
                    if month:
                        reject_months = [
                            "january",
                            "february",
                            "march",
                            "april",
                            "jan",
                            "feb",
                            "mar",
                            "apr",
                        ]
                        if month.lower() in reject_months:
                            return f"Graduation requirement: {month.title()} 2027 (before May 2027)"
                    # If month not specified or May+, accept

                # 2027 (May+) or 2028+ → ACCEPT

        # ✅ LAYER 4: PhD requirement
        if re.search(
            r"phd|doctoral.*(?:required|pursuing|candidates?)", page_lower, re.I
        ):
            context_match = re.search(
                r"(?:phd|doctoral).{0,100}(?:required|pursuing)", page_lower, re.I
            )
            if context_match:
                context = context_match.group(0)
                if not re.search(
                    r"(?:bachelor|master|bs|ms).{0,30}(?:or|and).{0,30}phd",
                    context,
                    re.I,
                ):
                    return "PhD requirement"

        return None

    @staticmethod
    def validate_company_field(company, title, url):
        """Validate company name."""
        if not company or company == "Unknown":
            company_from_url = ValidationHelper.extract_company_from_domain(url)
            return True, company_from_url, None

        company = company.strip()

        if len(company) > 100:
            return False, company, "Company name too long"

        # Check if company contains job keywords
        job_keywords = [
            "intern",
            "software",
            "engineer",
            "developer",
            "position",
            "role",
        ]
        keyword_count = sum(1 for kw in job_keywords if kw in company.lower())

        if keyword_count >= 2:
            return False, company, "Company field contains job title"

        invalid_companies = [
            "apply",
            "careers",
            "jobs",
            "job board",
            "simplify",
            "multiple companies",
            "sign in",
            "submit your",
        ]

        if company.lower() in invalid_companies:
            company_from_url = ValidationHelper.extract_company_from_domain(url)
            return True, company_from_url, None

        return True, company, None

    @staticmethod
    def extract_company_from_domain(url):
        """Extract company from domain."""
        if not url:
            return "Unknown"

        try:
            domain_match = re.search(r"https?://(?:www\.)?([^/]+)", url, re.I)
            if not domain_match:
                return "Unknown"

            domain = domain_match.group(1).lower()
            domain = re.sub(r"\.(com|org|net|io|ai|co|jobs|careers)$", "", domain)

            parts = domain.split(".")
            company = parts[-1] if len(parts) > 1 else parts[0]

            if company in SPECIAL_COMPANY_NAMES:
                return SPECIAL_COMPANY_NAMES[company]

            company = company.replace("-", " ").replace("_", " ")
            return company.title()

        except:
            return "Unknown"

    @staticmethod
    def check_sponsorship_status(soup):
        """Check H1B sponsorship."""
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


class QualityScorer:
    """Scores job quality."""

    @staticmethod
    def calculate_score(job_data):
        """Calculate quality score (0-7)."""
        score = 0

        company = job_data.get("company", "Unknown")
        if company and company != "Unknown" and len(company) > 2:
            if company.lower() not in ["unknown", "n/a", "multiple companies"]:
                score += 2

        location = job_data.get("location", "Unknown")
        if location and location != "Unknown":
            score += 2

        job_id = job_data.get("job_id", "N/A")
        if job_id and job_id != "N/A":
            score += 1

        title = job_data.get("title", "")
        if 15 < len(title) < 120:
            score += 1

        sponsorship = job_data.get("sponsorship", "Unknown")
        if sponsorship and sponsorship != "Unknown":
            score += 1

        return score

    @staticmethod
    def is_acceptable_quality(score):
        """Check if quality meets minimum."""
        return score >= MIN_QUALITY_SCORE
