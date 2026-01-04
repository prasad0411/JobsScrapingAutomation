#!/usr/bin/env python3
# cSpell:disable
"""
Processing module for job data validation, cleaning, and quality scoring - ENHANCED VERSION
Handles title cleaning, location formatting, international filtering, and all validation checks.
FIXES: Relaxed Spring detection, Bachelor's degree, US citizenship, Canadian URL detection, Workday support
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
        """Aggressively clean title by removing years, seasons, brackets, etc."""
        if not title or len(title) < 5:
            return title

        original = title

        # Remove parentheses and brackets (complete pairs)
        title = re.sub(r"\s*[\(\[].+?[\)\]]", "", title)

        # ✅ Remove unclosed brackets at end
        title = re.sub(r"\s*\([^)]*$", "", title)
        title = re.sub(r"\s*\[[^\]]*$", "", title)

        # Remove seasons + years
        title = re.sub(
            r"\s*-?\s*(Summer|Fall|Spring|Winter)\s*20\d{2}", "", title, flags=re.I
        )
        title = re.sub(
            r"\s*-?\s*20\d{2}\s*(Summer|Fall|Spring|Winter)", "", title, flags=re.I
        )

        # Remove standalone years
        title = re.sub(r"\s*-?\s*20\d{2}\s*-?\s*", " ", title)

        # Remove degree requirements
        title = re.sub(
            r"\s*[\(\[]\s*(BS/MS|MS|PhD|Bachelor|Master).*?[\)\]]",
            "",
            title,
            flags=re.I,
        )

        # Remove extra whitespace
        title = re.sub(r"\s+", " ", title).strip()
        title = re.sub(r"\s*-\s*$", "", title)
        title = re.sub(r"^\s*-\s*", "", title)

        if len(title) < 5:
            return original

        return title

    @staticmethod
    def is_valid_job_title(title):
        """Check if title is a valid job posting."""
        if not title or len(title) < 5:
            return False, "Title too short"

        title_lower = title.lower()

        # Exclude non-job titles
        excluded_phrases = [
            "application",
            "click here",
            "apply now",
            "view job",
            "see more",
            "show all",
            "company",
        ]

        for phrase in excluded_phrases:
            if phrase in title_lower:
                return False, f"Invalid title pattern: {phrase}"

        return True, None

    @staticmethod
    def is_cs_engineering_role(title):
        """Check if role is CS/engineering related."""
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
        """✅ FIXED: Allow multiple seasons (Spring+Summer), reject Spring-only."""
        combined_text = (title + " " + page_text).lower()

        # Check for explicit wrong seasons
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

                # ✅ CRITICAL FIX: If mentions MULTIPLE seasons including Summer → ACCEPT
                # Look for "Spring, Summer, Fall" or "Spring/Summer/Fall" patterns
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
                        return True, ""  # Multiple seasons including Summer = OK

                # If only mentions Spring/Fall/Winter (no Summer) → REJECT
                if "fall" in season.lower():
                    return False, f"Wrong season: {season_name}"
                elif "winter" in season.lower():
                    return False, f"Wrong season: {season_name}"
                elif "spring" in season.lower():
                    # Double-check: if Summer NOT mentioned anywhere → REJECT
                    if not re.search(r"summer", combined_text, re.I):
                        return False, f"Wrong season: Spring only"

        # Additional check for 2025 or earlier years
        year_match = re.search(r"(fall|spring|winter)\s*202[0-5]", combined_text, re.I)
        if year_match:
            return False, f"Wrong season: {year_match.group(0).title()}"

        return True, ""


class LocationProcessor:
    """Processes and validates job locations."""

    @staticmethod
    def extract_location_enhanced(soup, url):
        """Enhanced location extraction - URL FIRST, then page parsing."""

        # ✅ LAYER 0: Extract from URL FIRST (most reliable for Workday)
        url_location = LocationProcessor._extract_from_url(url)
        if url_location and url_location != "Unknown":
            return url_location

        # Special handling for Simplify.jobs pages
        if "simplify.jobs" in url.lower():
            simplify_loc = LocationProcessor._extract_from_simplify_page(soup)
            if simplify_loc and simplify_loc != "Unknown":
                return simplify_loc

        # LAYER 1: JSON-LD Schema
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

        # LAYER 2: Meta tags
        meta_location = soup.find("meta", {"property": "og:location"})
        if meta_location and meta_location.get("content"):
            loc = meta_location.get("content").strip()
            if loc and len(loc) < 100:
                return loc

        # LAYER 3: Common label patterns
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

        # LAYER 4: City, State pattern
        location_pattern = r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b"
        matches = re.findall(location_pattern, page_text[:2000])

        if matches:
            city, state = matches[0]
            if state in US_STATES.values():
                return f"{city}, {state}"

        # LAYER 5: Aggressive scan
        if "Remote" in page_text[:1500] or "remote" in page_text[:500].lower():
            return "Remote"

        return "Unknown"

    @staticmethod
    def _extract_from_url(url):
        """✅ ENHANCED: Extract location from URL (Workday-specific patterns)."""
        if not url:
            return None

        url_lower = url.lower()

        # Workday location patterns (most reliable)
        if "workday" in url_lower or "myworkdayjobs" in url_lower:
            # Pattern 1: /City-State-Country/ format
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

            # Pattern 2: /City-State/ format
            match = re.search(r"/([A-Z][a-z]+(?:-[A-Z][a-z]+)*)-([A-Z]{2})/", url, re.I)
            if match:
                city = match.group(1).replace("-", " ")
                state = match.group(2).upper()
                if state in US_STATES.values():
                    return f"{city}, {state}"

            # Pattern 3: /location/City-State
            match = re.search(
                r"/location/([A-Z][a-z]+(?:-[A-Z][a-z]+)*)-([A-Z]{2})\b", url, re.I
            )
            if match:
                city = match.group(1).replace("-", " ")
                state = match.group(2).upper()
                if state in US_STATES.values():
                    return f"{city}, {state}"

        # General City-State pattern in URL
        match = re.search(r"/([A-Z][a-z]+(?:-[A-Z][a-z]+)?)[_-]([A-Z]{2})\b", url, re.I)
        if match:
            city = match.group(1).replace("-", " ")
            state = match.group(2).upper()
            if state in US_STATES.values():
                return f"{city}, {state}"

        # Remote in URL
        if "/remote" in url_lower or "remote-" in url_lower:
            return "Remote"

        return None

    @staticmethod
    def _extract_from_simplify_page(soup):
        """Extract location from Simplify.jobs redirector page."""
        try:
            # Look for location in the page content
            page_text = soup.get_text()

            # Pattern: "Location: City, State"
            match = re.search(r"Location:\s*([A-Za-z\s,]+,\s*[A-Z]{2})", page_text)
            if match:
                return match.group(1).strip()

            # City, State pattern
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
        """Extract remote work status with enhanced detection."""
        if not soup:
            return "Unknown"

        # Check location first
        if location:
            location_lower = location.lower()
            if "remote" in location_lower:
                return "Remote"
            if "hybrid" in location_lower:
                return "Hybrid"

        # Check page text
        page_text = soup.get_text()[:2000]
        page_lower = page_text.lower()

        # Remote indicators
        if "100% remote" in page_lower or "fully remote" in page_lower:
            return "Remote"
        if "remote" in page_lower[:500]:
            return "Remote"

        # Hybrid indicators
        if "hybrid" in page_lower[:500]:
            return "Hybrid"

        # On-site indicators
        if (
            "on-site" in page_lower[:500]
            or "onsite" in page_lower[:500]
            or "in-office" in page_lower[:500]
        ):
            return "On Site"

        # Check URL
        if url:
            url_lower = url.lower()
            if "remote" in url_lower:
                return "Remote"
            if "hybrid" in url_lower:
                return "Hybrid"

        return "Unknown"

    @staticmethod
    def format_location_clean(location):
        """Format location string consistently."""
        if not location or location == "Unknown":
            return "Unknown"

        location = location.strip()

        # Handle "City - State" format
        location = re.sub(r"\s*-\s*([A-Z]{2})\b", r", \1", location)

        # Capitalize properly
        if "," in location:
            parts = location.split(",")
            city = parts[0].strip().title()
            state = parts[1].strip().upper()

            # Map full state names to abbreviations
            for state_name, abbr in US_STATES.items():
                if state.lower() == state_name:
                    state = abbr
                    break

            if state in US_STATES.values():
                return f"{city} - {state}"

        return location

    @staticmethod
    def check_if_international(location, soup=None):
        """✅ ENHANCED: Check if location is outside US (comprehensive Canadian detection)."""
        if not location or location == "Unknown":
            return None

        location_lower = location.lower()

        # Canadian provinces check
        for province in CANADA_PROVINCES:
            if f", {province}" in location or f" {province}" in location.upper():
                # ✅ CRITICAL: Exclude "Ontario, CA" (California city)
                if province == "ON" and ", ca" in location_lower:
                    continue
                return f"Location: Canada (province: {province})"

        # Canadian cities check
        for city, province in CANADA_CITIES.items():
            if city in location_lower:
                return f"Location: Canada ({city.title()})"

        # Check for "Canada" explicitly
        if "canada" in location_lower:
            return "Location: Canada"

        # UK check
        uk_indicators = ["uk", "united kingdom", "london", "manchester", "edinburgh"]
        if any(indicator in location_lower for indicator in uk_indicators):
            return "Location: UK"

        # Other common countries
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

        # If soup provided, do aggressive scan
        if soup:
            country = LocationProcessor._aggressive_country_scan(soup)
            if country and country not in ["USA", "United States", "US"]:
                return f"Location: {country}"

        return None

    @staticmethod
    def _aggressive_country_scan(soup):
        """✅ ENHANCED: Scan page for country mentions (with Canadian city + province context)."""
        if not soup:
            return None

        page_text = soup.get_text()[:3000]

        # Canadian cities with province context
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

        # Explicit country mentions
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
    """Helper methods for validation tasks."""

    @staticmethod
    def is_valid_job_url(url):
        """Check if URL is a valid job posting URL."""
        if not url or not url.startswith("http"):
            return False, "Invalid URL format"

        url_lower = url.lower()

        # Invalid Jobright URLs
        if "jobright.ai" in url_lower:
            if "/jobs/info/" not in url_lower:
                return False, "Invalid Jobright URL (not specific job)"
            if "jobright.ai/?utm" in url_lower or "jobright.ai?utm" in url_lower:
                return False, "Invalid Jobright URL (not specific job)"
            if "/jobs/recommend" in url_lower:
                return False, "Jobright recommendation page (not specific job)"

        # Exclude non-job URLs
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
        """✅ ENHANCED: Check URL for international job indicators (comprehensive Canadian detection)."""
        if not url:
            return None

        url_lower = url.lower()

        # ✅ COMPREHENSIVE Canadian URL patterns
        canadian_url_patterns = [
            "/en-ca/",
            "/en-gb/",  # Workday uses en-GB for Canada
            "/canada/",
            "/toronto/",
            "/vancouver/",
            "/montreal/",
            "/calgary/",
            "/ottawa/",
            "/edmonton/",
            "-canada-",
            "/ca/en/",
            "/Canada---",  # Workday format: /Canada---City/
            "-Ontario-Canada",  # TORONTO-Ontario-Canada
            "-Canada/job/",
        ]

        for pattern in canadian_url_patterns:
            if pattern.lower() in url_lower:
                return f"International: Canada (from URL)"

        # UK patterns
        uk_patterns = [
            "/en-gb/",
            "/uk/",
            "/united-kingdom/",
            "/london/",
            "/manchester/",
        ]
        if any(p in url_lower for p in uk_patterns):
            # ✅ CRITICAL: /en-gb/ could be Canada OR UK - need more context
            if "/en-gb/" in url_lower and not any(
                city in url_lower
                for city in ["toronto", "vancouver", "montreal", "canada"]
            ):
                return f"International: UK (from URL)"

        # Other countries
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
        """✅ ENHANCED: Check for various job restrictions (Bachelor's, US citizenship, security clearance)."""
        if not soup:
            return None

        page_text = soup.get_text()[:4000]
        page_lower = page_text.lower()

        # ✅ NEW: Bachelor's degree requirement patterns
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
                # ✅ Context check: Ensure NOT "bachelor's or master's"
                context_start = max(0, match.start() - 100)
                context_end = min(len(page_lower), match.end() + 100)
                context = page_lower[context_start:context_end]

                if not re.search(
                    r"(?:bachelor'?s?|undergraduate)\s+or\s+(?:master'?s?|graduate)",
                    context,
                    re.I,
                ):
                    return "Bachelor's degree requirement (undergrad only)"

        # ✅ NEW: US Citizenship requirement patterns
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

        # Existing: PhD requirement
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

        # Security clearance
        if re.search(
            r"(?:security clearance|clearance required|must (?:have|obtain|possess).*clearance)",
            page_lower,
            re.I,
        ):
            return "Security clearance required"

        return None

    @staticmethod
    def validate_company_field(company, title, url):
        """Validate and clean company name."""
        if not company or company == "Unknown":
            company_from_url = ValidationHelper.extract_company_from_domain(url)
            return True, company_from_url, None

        company = company.strip()

        # Check if company name accidentally contains title
        if len(company) > 100 or any(
            kw in company.lower() for kw in ["intern", "software", "engineer"]
        ):
            return False, company, "Company field contains job title"

        # Check for generic/invalid companies
        invalid_companies = [
            "apply",
            "careers",
            "jobs",
            "job board",
            "simplify",
            "multiple companies",
        ]

        if company.lower() in invalid_companies:
            company_from_url = ValidationHelper.extract_company_from_domain(url)
            return True, company_from_url, None

        return True, company, None

    @staticmethod
    def extract_company_from_domain(url):
        """✅ ENHANCED: Extract company name from domain with special mappings."""
        if not url:
            return "Unknown"

        try:
            # Extract domain
            domain_match = re.search(r"https?://(?:www\.)?([^/]+)", url, re.I)
            if not domain_match:
                return "Unknown"

            domain = domain_match.group(1).lower()

            # Remove common TLDs
            domain = re.sub(r"\.(com|org|net|io|ai|co|jobs|careers)$", "", domain)

            # Extract base domain
            parts = domain.split(".")
            if len(parts) > 1:
                company = parts[-1]
            else:
                company = parts[0]

            # ✅ Check special company name mappings
            if company in SPECIAL_COMPANY_NAMES:
                return SPECIAL_COMPANY_NAMES[company]

            # Clean and capitalize
            company = company.replace("-", " ").replace("_", " ")
            company = company.title()

            return company

        except:
            return "Unknown"

    @staticmethod
    def check_sponsorship_status(soup):
        """Check H1B sponsorship status."""
        if not soup:
            return "Unknown"

        page_text = soup.get_text()[:3000]

        # Positive indicators
        if re.search(
            r"(?:will|does|provides?)\s+sponsor|h-?1b.*sponsor|sponsorship.*available",
            page_text,
            re.I,
        ):
            return "Yes"

        # Negative indicators
        if re.search(
            r"(?:no|not|does not|doesn't)\s+sponsor|no h-?1b|sponsorship.*(?:not available|unavailable)",
            page_text,
            re.I,
        ):
            return "No"

        # Work authorization required (usually means no sponsorship)
        if re.search(
            r"(?:must have|requires?)\s+(?:us|u\.s\.)\s+work authorization",
            page_text,
            re.I,
        ):
            return "No"

        return "Unknown"


class QualityScorer:
    """Scores job quality based on data completeness."""

    @staticmethod
    def calculate_score(job_data):
        """Calculate quality score (0-7 scale)."""
        score = 0

        # Company present and valid (2 points)
        company = job_data.get("company", "Unknown")
        if company and company != "Unknown" and len(company) > 2:
            if company.lower() not in ["unknown", "n/a", "multiple companies"]:
                score += 2

        # Location present (2 points)
        location = job_data.get("location", "Unknown")
        if location and location != "Unknown":
            score += 2

        # Job ID available (1 point)
        job_id = job_data.get("job_id", "N/A")
        if job_id and job_id != "N/A":
            score += 1

        # Title length reasonable (1 point)
        title = job_data.get("title", "")
        if 15 < len(title) < 120:
            score += 1

        # Sponsorship info known (1 point)
        sponsorship = job_data.get("sponsorship", "Unknown")
        if sponsorship and sponsorship != "Unknown":
            score += 1

        return score

    @staticmethod
    def is_acceptable_quality(score):
        """Check if quality score meets minimum threshold."""
        return score >= MIN_QUALITY_SCORE
