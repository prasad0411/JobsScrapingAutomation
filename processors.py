#!/usr/bin/env python3
# cSpell:disable
"""
Processing module - ULTIMATE Production v4.0
ALL FIXES: Location cleaning (remove "US"), ByteDance check fix, comprehensive validation
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
        """✅ ULTIMATE: Season validation with all exceptions."""
        combined_text = (title + " " + page_text).lower()

        wrong_patterns = [
            (r"fall\s*20\d{2}", "Fall"),
            (r"fall\s*semester", "Fall semester"),
            (r"winter\s*20\d{2}", "Winter"),
            (r"winter\s*semester", "Winter semester"),
            (r"winter\s*internship\s*2025/2026", "Winter 2025/2026"),
            (r"spring\s*20\d{2}", "Spring"),
            (r"spring\s*semester", "Spring semester"),
        ]

        for pattern, season_name in wrong_patterns:
            match = re.search(pattern, combined_text, re.I)
            if match:
                season = match.group(0)

                # ✅ EXCEPTION 1: Multiple seasons including Summer → ACCEPT
                multi_season_patterns = [
                    r"spring.*summer",
                    r"summer.*spring",
                    r"spring.*summer.*fall",
                    r"fall.*summer.*spring",
                    r"spring/summer",
                    r"summer/spring",
                    r"spring,\s*summer",
                    r"summer,\s*spring",
                ]

                for multi_pattern in multi_season_patterns:
                    if re.search(multi_pattern, combined_text, re.I):
                        return True, ""

                # ✅ EXCEPTION 2: Winter 2025/2026 → ACCEPT (ends in Summer 2026)
                if "winter" in season.lower():
                    if re.search(r"winter.*2025/2026", combined_text, re.I):
                        return True, ""
                    if re.search(r"2025/2026", combined_text, re.I):
                        return True, ""

                # Reject if ONLY wrong season
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
    """Processes and validates job locations."""

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

        meta_location = soup.find("meta", {"property": "og:location"})
        if meta_location and meta_location.get("content"):
            loc = meta_location.get("content").strip()
            if loc and len(loc) < 100:
                return loc

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

        if "Remote" in page_text[:1500] or "remote" in page_text[:500].lower():
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

        match = re.search(r"/([A-Z][a-z]+(?:-[A-Z][a-z]+)?)[_-]([A-Z]{2})\b", url, re.I)
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

        if (
            "on-site" in page_lower[:500]
            or "onsite" in page_lower[:500]
            or "in-office" in page_lower[:500]
        ):
            return "On Site"

        if url:
            url_lower = url.lower()
            if "remote" in url_lower:
                return "Remote"
            if "hybrid" in url_lower:
                return "Hybrid"

        return "Unknown"

    @staticmethod
    def format_location_clean(location):
        """✅ BULLETPROOF: Extract ONLY City - ST from any format."""
        if not location or location == "Unknown":
            return "Unknown"

        if location in ["Remote", "Hybrid"]:
            return location

        original = location.strip()

        # ✅ STEP 1: Aggressive cleaning - remove ALL extra info
        # Remove zip codes
        location = re.sub(r"\s*\d{5}(-\d{4})?\s*", " ", original)

        # Remove "USA" or "United States"
        location = re.sub(
            r",?\s*(?:USA|U\.S\.A\.|United States)\s*", "", location, flags=re.I
        )

        # Remove building/address codes
        location = re.sub(r"^[A-Z]{2}\d+:\s*", "", location)
        location = re.sub(
            r"\s*(?:Bldg|Building|Office|Suite|Floor|Drive|Street|Road|Avenue)\s+.*$",
            "",
            location,
            flags=re.I,
        )

        # Remove descriptive suffixes
        location = re.sub(r"\s*-\s*[A-Z][a-z]+\s+\d+\s*$", "", location)

        # ✅ STEP 2: Extract City, State from various formats

        # Format 1: "City, ST" - standard format
        match = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b", location)
        if match:
            city = match.group(1).strip()
            state = match.group(2).upper()
            if state in US_STATES.values():
                return f"{city} - {state}"

        # Format 2: "ST - City" or "US, ST - City" or "US - ST - City"
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

        # Format 3: "City" only - infer state from CITY_TO_STATE
        location_clean = re.sub(r"\s+", " ", location).strip()
        location_lower = location_clean.lower()

        if location_lower in CITY_TO_STATE:
            state = CITY_TO_STATE[location_lower]
            city = location_clean.title()
            return f"{city} - {state}"

        # Format 4: Multiple commas - take first city and last state
        if "," in location_clean:
            parts = [p.strip() for p in location_clean.split(",") if p.strip()]
            if len(parts) >= 2:
                # Try last part as state
                potential_state = parts[-1].upper()

                # Map full state names to abbreviations
                for state_name, abbr in US_STATES.items():
                    if potential_state.lower() == state_name:
                        potential_state = abbr
                        break

                if potential_state in US_STATES.values():
                    city = parts[0].title()
                    return f"{city} - {potential_state}"

        # Fallback: return cleaned version
        return location_clean

    @staticmethod
    def check_if_international(location, soup=None):
        """✅ ULTIMATE: International check with City, State bypass."""
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

        # ✅ CRITICAL BYPASS: If location is "City, ST" format → Validated US
        # DON'T scan soup (prevents false positives from company origin mentions)
        if re.search(r"^[A-Z][a-z]+(?: [A-Z][a-z]+)?,\s*[A-Z]{2}$", location):
            state = location.split(",")[-1].strip()
            if state in US_STATES.values():
                return None  # Validated US location

        # Only scan soup if location is vague/unknown
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

        canada_city_patterns = [
            (r"toronto[,\s]+ontario", "Canada"),
            (r"montreal[,\s]+quebec", "Canada"),
            (r"vancouver[,\s]+(?:british columbia|bc)", "Canada"),
            (r"calgary[,\s]+alberta", "Canada"),
            (r"ottawa[,\s]+ontario", "Canada"),
        ]

        for pattern, country in canada_city_patterns:
            if re.search(pattern, page_text, re.I):
                return country

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
                return False, "Invalid Jobright URL (not specific job)"
            if "jobright.ai/?utm" in url_lower or "jobright.ai?utm" in url_lower:
                return False, "Invalid Jobright URL (not specific job)"
            if "/jobs/recommend" in url_lower:
                return False, "Jobright recommendation page (not specific job)"

        excluded = [
            "/unsubscribe",
            "/my-alerts",
            "/blog",
            "/terms",
            "/privacy",
            "chromewebstore.google.com",
            "chrome.google.com/webstore",
        ]

        for pattern in excluded:
            if pattern in url_lower:
                return False, f"{pattern.split('/')[-1].title()} link (not a job)"

        return True, None

    @staticmethod
    def check_url_for_international(url):
        """Check URL for international indicators."""
        if not url:
            return None

        url_lower = url.lower()

        canadian_url_patterns = [
            "/en-ca/",
            "/en-gb/",
            "/canada/",
            "/toronto/",
            "/vancouver/",
            "/montreal/",
            "/calgary/",
            "/ottawa/",
            "/edmonton/",
            "-canada-",
            "/ca/en/",
            "/Canada---",
            "-Ontario-Canada",
            "-Canada/job/",
        ]

        for pattern in canadian_url_patterns:
            if pattern.lower() in url_lower:
                return f"International: Canada (from URL)"

        uk_patterns = [
            "/en-gb/",
            "/uk/",
            "/united-kingdom/",
            "/london/",
            "/manchester/",
        ]
        if any(p in url_lower for p in uk_patterns):
            if "/en-gb/" in url_lower and not any(
                city in url_lower
                for city in ["toronto", "vancouver", "montreal", "canada"]
            ):
                return f"International: UK (from URL)"

        country_patterns = {
            "/india/": "India",
            "/in/en/": "India",
            "/china/": "China",
            "/singapore/": "Singapore",
            "/australia/": "Australia",
            "/germany/": "Germany",
        }

        for pattern, country in country_patterns.items():
            if pattern in url_lower:
                return f"International: {country} (from URL)"

        return None

    @staticmethod
    def check_page_restrictions(soup):
        """Check for job restrictions."""
        if not soup:
            return None

        page_text = soup.get_text()[:4000]
        page_lower = page_text.lower()

        bachelors_patterns = [
            r"bachelor'?s?\s+degree.*(?:required|pursuing|only)",
            r"currently enrolled in.*4-year.*undergraduate",
            r"undergraduate.*degree.*required",
            r"must be.*pursuing.*bachelor'?s?",
            r"expected graduation.*(?:december 2026|2027|2028)",
            r"graduation date.*(?:2027|2028)",
            r"undergraduate.*only",
        ]

        for pattern in bachelors_patterns:
            match = re.search(pattern, page_lower, re.I)
            if match:
                context_start = max(0, match.start() - 100)
                context_end = min(len(page_lower), match.end() + 100)
                context = page_lower[context_start:context_end]

                if not re.search(
                    r"(?:bachelor'?s?|undergraduate)\s+or\s+(?:master'?s?|graduate)",
                    context,
                    re.I,
                ):
                    return "Bachelor's degree requirement (undergrad only)"

        citizenship_patterns = [
            r"u\.?s\.?\s+citizen(?:ship)?\s+(?:required|only)",
            r"must be a u\.?s\.?\s+citizen",
            r"us citizenship is required",
            r"only u\.?s\.?\s+citizens",
            r"citizenship:\s*u\.?s\.?",
            r"must possess.*u\.?s\.?\s+citizenship",
        ]

        for pattern in citizenship_patterns:
            if re.search(pattern, page_lower, re.I):
                return "US citizenship required"

        if re.search(
            r"phd|doctoral.*(?:required|pursuing|candidates?|students?)",
            page_lower,
            re.I,
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

        if re.search(
            r"(?:security clearance|clearance required|must (?:have|obtain|possess).*clearance)",
            page_lower,
            re.I,
        ):
            return "Security clearance required"

        return None

    @staticmethod
    def validate_company_field(company, title, url):
        """✅ ENHANCED: Validate company with better error detection."""
        if not company or company == "Unknown":
            company_from_url = ValidationHelper.extract_company_from_domain(url)
            return True, company_from_url, None

        company = company.strip()

        # ✅ Check if company is abnormally long or contains job-specific keywords
        if len(company) > 100:
            return False, company, "Company name too long (likely contains title)"

        # ✅ Check if company contains job title keywords (means it's actually the title)
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

        # Invalid company names
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
        """Extract company from domain with special mappings."""
        if not url:
            return "Unknown"

        try:
            domain_match = re.search(r"https?://(?:www\.)?([^/]+)", url, re.I)
            if not domain_match:
                return "Unknown"

            domain = domain_match.group(1).lower()
            domain = re.sub(r"\.(com|org|net|io|ai|co|jobs|careers)$", "", domain)

            parts = domain.split(".")
            if len(parts) > 1:
                company = parts[-1]
            else:
                company = parts[0]

            if company in SPECIAL_COMPANY_NAMES:
                return SPECIAL_COMPANY_NAMES[company]

            company = company.replace("-", " ").replace("_", " ")
            company = company.title()

            return company

        except:
            return "Unknown"

    @staticmethod
    def check_sponsorship_status(soup):
        """Check H1B sponsorship."""
        if not soup:
            return "Unknown"

        page_text = soup.get_text()[:3000]

        if re.search(
            r"(?:will|does|provides?)\s+sponsor|h-?1b.*sponsor|sponsorship.*available",
            page_text,
            re.I,
        ):
            return "Yes"

        if re.search(
            r"(?:no|not|does not|doesn't)\s+sponsor|no h-?1b|sponsorship.*(?:not available|unavailable)",
            page_text,
            re.I,
        ):
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
