#!/usr/bin/env python3
# cSpell:disable
"""
Processing and validation module - COMPREHENSIVE FIX
Fixes: Spring semester, Bachelor's degree, US citizenship, Canadian detection, location extraction
"""

import re
from config import US_STATES, CANADA_PROVINCES, SPECIAL_COMPANY_NAMES


class TitleProcessor:
    """Handles job title processing and validation."""

    @staticmethod
    def clean_title_aggressive(title):
        """✅ ENHANCED: Remove unclosed brackets at end."""
        if not title or title == "Unknown":
            return title

        # Remove complete pairs
        title = re.sub(r"\s*[\(\[].+?[\)\]]", "", title)

        # ✅ Remove unclosed brackets at end
        title = re.sub(r"\s*\([^)]*$", "", title)
        title = re.sub(r"\s*\[[^\]]*$", "", title)

        # Remove years/seasons
        title = re.sub(
            r"\s*[-–—]\s*(?:Summer|Fall|Spring|Winter)?\s*(?:20\d{2}|'\d{2})",
            "",
            title,
            flags=re.I,
        )

        # Remove special symbols
        title = re.sub(r"[↳→]", "", title)

        # Normalize whitespace
        title = re.sub(r"\s+", " ", title).strip()

        return title

    @staticmethod
    def is_valid_job_title(title):
        """Check if title is a valid job posting."""
        if not title or title == "Unknown":
            return False, "Empty or unknown title"

        title_lower = title.lower()

        # Marketing/recruiting roles
        marketing_keywords = [
            "marketing",
            "recruiter",
            "recruiting",
            "sales",
            "account manager",
            "business development",
        ]
        if any(kw in title_lower for kw in marketing_keywords):
            return False, "Marketing/recruiting role"

        # Non-job keywords
        non_job = [
            "event",
            "workshop",
            "info session",
            "webinar",
            "conference",
            "hiring event",
        ]
        if any(kw in title_lower for kw in non_job):
            return False, "Not a job posting"

        return True, "Valid"

    @staticmethod
    def is_internship_role(title):
        """Check if title is internship/co-op (not full-time)."""
        if not title:
            return False, "Empty title"

        title_lower = title.lower()

        # Must contain internship/co-op keywords
        intern_keywords = ["intern", "co-op", "coop", "co op"]
        if not any(kw in title_lower for kw in intern_keywords):
            return False, "Not internship/co-op role"

        # Exclude senior/lead/staff roles
        senior_keywords = [
            "senior",
            "sr.",
            "lead",
            "staff",
            "principal",
            "architect",
            "manager",
            "director",
        ]
        if any(kw in title_lower for kw in senior_keywords):
            return False, "Senior role excluded"

        return True, "Valid internship"

    @staticmethod
    def is_cs_engineering_role(title):
        """Check if title is CS/Engineering related."""
        if not title:
            return False

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
            "computer science",
            "cs",
            "backend",
            "frontend",
            "full stack",
            "fullstack",
            "devops",
            "cloud",
            "web",
            "mobile",
            "systems",
            "embedded",
            "firmware",
            "hardware",
            "asic",
            "vlsi",
            "computer",
            "technology",
            "tech",
            "cyber",
            "security",
            "network",
            "database",
            "sre",
            "platform",
            "infrastructure",
            "automation",
            "robotics",
            "sdet",
            "qa engineer",
            "test engineer",
            "solutions engineer",
        ]

        if any(kw in title_lower for kw in cs_keywords):
            return True

        # Exclude non-CS engineering
        non_cs = [
            "mechanical",
            "civil",
            "chemical",
            "electrical engineer",
            "biomedical",
            "environmental",
            "industrial",
            "manufacturing",
            "product engineer",
            "process engineer",
            "quality engineer",
            "retail",
            "risk modeling",
            "financial engineer",
            "sales engineer",
            "sustainability",
            "climate",
        ]

        if any(kw in title_lower for kw in non_cs):
            return False

        return False

    @staticmethod
    def check_season_requirement(title, page_text_sample):
        """✅ ENHANCED: Reject Fall/Spring/Winter, accept Summer 2026+."""
        if not title and not page_text_sample:
            return True, "No content"

        combined = f"{title} {page_text_sample}".lower()

        # ✅ REJECT: Fall, Spring, Winter (any year)
        wrong_seasons = [
            r"fall\s*20\d{2}",
            r"spring\s*20\d{2}",  # ✅ NEW: Reject Spring 2026
            r"winter\s*20\d{2}",
            r"fall\s*semester",
            r"spring\s*semester",  # ✅ NEW: Reject Spring Semester
            r"winter\s*semester",
        ]

        for pattern in wrong_seasons:
            match = re.search(pattern, combined)
            if match:
                return False, f"Wrong season: {match.group(0)}"

        # ACCEPT: Summer 2026+, 2026+ (no specific season), or unspecified
        return True, "Valid"


class LocationProcessor:
    """Handles location extraction and validation."""

    @staticmethod
    def extract_location_enhanced(soup, url):
        """✅ ENHANCED: Multi-strategy location extraction with Workday support."""
        # Strategy 1: JSON-LD
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                import json

                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    job_loc = data.get("jobLocation", {})
                    if isinstance(job_loc, dict):
                        address = job_loc.get("address", {})
                        if isinstance(address, dict):
                            city = address.get("addressLocality", "")
                            state = address.get("addressRegion", "")
                            if city and state:
                                return f"{city}, {state}"
            except:
                pass

        # ✅ Strategy 2: Workday-specific patterns (Micron, Cadence, HP, etc.)
        page_text = soup.get_text()

        # Workday location patterns
        workday_patterns = [
            r"Location[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z][a-z]+),\s*United States",  # City, State, United States
            r"Workplace Location[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",  # Workplace Location header
            r"\b([A-Z][a-z]+),\s+([A-Z][a-z]+),\s+United States of America",  # Full country name
        ]

        for pattern in workday_patterns:
            match = re.search(pattern, page_text[:3000])
            if match:
                if len(match.groups()) >= 2:
                    city = match.group(1)
                    state_or_province = match.group(2)

                    # Check if it's a full US state name
                    if state_or_province in US_STATES.values():
                        state_abbr = [
                            k for k, v in US_STATES.items() if v == state_or_province
                        ][0]
                        return f"{city}, {state_abbr}"

                    # Check if it's a US state abbreviation
                    if (
                        len(state_or_province) == 2
                        and state_or_province.upper() in US_STATES
                    ):
                        return f"{city}, {state_or_province.upper()}"

        # Strategy 3: URL-based extraction
        url_location = LocationProcessor.extract_location_from_url(url)
        if url_location and url_location != "Unknown":
            return url_location

        # Strategy 4: Standard city, state patterns
        match = re.search(
            r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b", page_text[:2000]
        )
        if match:
            city, state = match.group(1), match.group(2)
            if state in US_STATES:
                return f"{city}, {state}"

        # Strategy 5: Remote
        if re.search(r"\bRemote\b", page_text[:1500], re.I):
            return "Remote"

        return "Unknown"

    @staticmethod
    def extract_location_from_url(url):
        """✅ ENHANCED: Extract location from URL with Canadian city detection."""
        if not url:
            return "Unknown"

        url_lower = url.lower()

        # ✅ Detect Canadian cities in URL
        canadian_cities = [
            "toronto",
            "vancouver",
            "montreal",
            "ottawa",
            "calgary",
            "edmonton",
            "winnipeg",
        ]
        for city in canadian_cities:
            if f"/{city}" in url_lower or f"-{city}" in url_lower:
                return f"Canada ({city.title()})"

        # US cities/states in URL
        for state_abbr, state_name in US_STATES.items():
            if (
                f"/{state_abbr.lower()}/" in url_lower
                or f"-{state_abbr.lower()}-" in url_lower
            ):
                return f"US ({state_abbr})"
            if state_name.lower().replace(" ", "-") in url_lower:
                return f"US ({state_abbr})"

        # Remote
        if "remote" in url_lower:
            return "Remote"

        return "Unknown"

    @staticmethod
    def extract_remote_status_enhanced(soup, location, url):
        """Extract remote work status."""
        if location and "remote" in location.lower():
            return "Remote"

        page_text = soup.get_text()[:2000] if soup else ""

        if "hybrid" in page_text.lower():
            return "Hybrid"
        if "on-site" in page_text.lower() or "onsite" in page_text.lower():
            return "On Site"
        if "remote" in page_text.lower():
            return "Remote"

        return "Unknown"

    @staticmethod
    def format_location_clean(location):
        """Format location consistently."""
        if not location or location == "Unknown":
            return "Unknown"

        location = re.sub(r",?\s*United States.*$", "", location, flags=re.I)
        location = re.sub(r"\s*\(.*?\)", "", location)
        location = re.sub(r"\s+", " ", location).strip()

        # Format: "City, ST" or "City - ST"
        if ", " in location or " - " in location:
            return location

        return location

    @staticmethod
    def check_if_international(location, soup):
        """✅ ENHANCED: Check if location is outside US with comprehensive Canadian detection."""
        if not location or location == "Unknown":
            return None

        location_lower = location.lower()

        # ✅ Enhanced Canadian detection
        if "canada" in location_lower:
            return "Location: Canada"

        # Check Canadian provinces
        for province in CANADA_PROVINCES:
            if f", {province}" in location or f" {province.lower()}" in location_lower:
                return "Location: Canada"

        # ✅ Check Canadian cities (comprehensive list)
        canadian_cities = [
            "toronto",
            "vancouver",
            "montreal",
            "ottawa",
            "calgary",
            "edmonton",
            "winnipeg",
            "quebec",
            "hamilton",
            "kitchener",
            "london",
            "victoria",
            "halifax",
            "oshawa",
            "windsor",
        ]
        for city in canadian_cities:
            if city in location_lower:
                return "Location: Canada"

        # Check for Ontario (common in Canadian addresses)
        if "ontario" in location_lower and "california" not in location_lower:
            return "Location: Canada"

        # UK
        if (
            "uk" in location_lower
            or "united kingdom" in location_lower
            or "london" in location_lower
        ):
            return "Location: UK"

        # Other countries
        countries = [
            "india",
            "china",
            "japan",
            "germany",
            "france",
            "singapore",
            "australia",
            "mexico",
        ]
        for country in countries:
            if country in location_lower:
                return f"Location: {country.title()}"

        return None

    @staticmethod
    def _aggressive_country_scan(soup):
        """✅ ENHANCED: Aggressive scan for international locations with Canadian city detection."""
        if not soup:
            return None

        page_text = soup.get_text()[:5000]

        # Direct country matches
        if "United Kingdom" in page_text:
            return "United Kingdom"
        if "Canada" in page_text:
            return "Canada"

        # ✅ Canadian cities with province context
        canadian_cities = [
            "Toronto",
            "Vancouver",
            "Montreal",
            "Ottawa",
            "Calgary",
            "Edmonton",
            "Winnipeg",
        ]
        canadian_provinces = [
            "Ontario",
            "Quebec",
            "British Columbia",
            "Alberta",
            "Manitoba",
        ]

        for city in canadian_cities:
            if re.search(rf"\b{city}\b", page_text):
                # Check if province is mentioned nearby
                city_pos = page_text.find(city)
                context = page_text[max(0, city_pos - 50) : city_pos + 100]
                if any(prov in context for prov in canadian_provinces):
                    return "Canada"
                # Toronto specifically is almost always Canada
                if city == "Toronto":
                    return "Canada"

        # UK (exclude "UK work authorization")
        uk_match = re.search(r"\bUK\b", page_text)
        if uk_match:
            context = page_text[max(0, uk_match.start() - 20) : uk_match.end() + 30]
            if "work authorization" not in context.lower():
                return "United Kingdom"

        return None


class ValidationHelper:
    """Validation and restriction checking."""

    @staticmethod
    def check_url_for_international(url):
        """✅ COMPREHENSIVE: Check URL for international indicators including Canadian locales."""
        if not url:
            return None

        url_lower = url.lower()

        # ✅ Workday international locales (EXPANDED)
        intl_locales = {
            "/en-gb/": "United Kingdom (from URL)",
            "/en-ca/": "Canada (from URL)",  # Canadian locale
            "/en-in/": "India (from URL)",
            "/en-au/": "Australia (from URL)",
            "/en-sg/": "Singapore (from URL)",
            "/zh-cn/": "China (from URL)",
            ".co.uk/": "United Kingdom (from URL)",
            ".ca/": "Canada (from URL)",
        }

        for locale, country in intl_locales.items():
            if locale in url_lower:
                return f"Location: {country}"

        # ✅ Canadian cities in URL
        canadian_cities = [
            "toronto",
            "vancouver",
            "montreal",
            "ottawa",
            "calgary",
            "edmonton",
            "winnipeg",
            "quebec",
            "hamilton",
        ]
        for city in canadian_cities:
            if f"/{city}" in url_lower or f"-{city}" in url_lower:
                return f"Location: Canada (from URL)"

        # Other international domains
        if any(domain in url_lower for domain in [".uk/", ".in/", ".cn/", ".au/"]):
            return "Location: International (from URL)"

        return None

    @staticmethod
    def check_page_restrictions(soup):
        """✅ COMPREHENSIVE: Check for restrictions including citizenship and bachelor's requirements."""
        if not soup:
            return None

        page_text = soup.get_text()
        page_text_lower = page_text.lower()

        # ✅ 1. Security clearance
        if re.search(r"security clearance required", page_text_lower):
            return "Security clearance required"
        if re.search(r"must be able to obtain.*clearance", page_text_lower):
            return "Security clearance required"

        # ✅ 2. US Citizenship requirement (ENHANCED)
        citizenship_patterns = [
            r"u\.?s\.?\s+citizen(?:ship)?\s+required",
            r"must be a u\.?s\.?\s+citizen",
            r"only u\.?s\.?\s+citizens",
            r"united states citizen(?:ship)?\s+required",
            r"us citizenship is required",
            r"citizenship:\s*u\.?s\.?\s+citizen",
        ]
        for pattern in citizenship_patterns:
            if re.search(pattern, page_text_lower):
                return "US citizenship required"

        # ✅ 3. Bachelor's degree requirement (undergrad only) - ENHANCED
        bachelor_patterns = [
            r"bachelor'?s?\s+degree.*(?:required|pursuing)",
            r"currently enrolled in.*4-year.*(?:undergraduate|bachelor)",
            r"currently enrolled in a 4-year",
            r"undergraduate.*(?:required|only|must)",
            r"pursuing.*bachelor",
            r"expected graduation.*(?:december 2026|2027|2028)",  # Undergrads only
            r"must graduate.*(?:december 2026|2027|2028)",
        ]
        for pattern in bachelor_patterns:
            match = re.search(pattern, page_text_lower)
            if match:
                # Exclude if it also mentions "or master's" or "graduate students"
                context = page_text_lower[
                    max(0, match.start() - 100) : match.end() + 100
                ]
                if (
                    "master" not in context
                    and "graduate student" not in context
                    and "grad student" not in context
                ):
                    return "Bachelor's degree requirement (undergrad only)"

        # 4. Work authorization
        if re.search(r"us work authorization required", page_text_lower):
            return "US work authorization required"

        # 5. Driver's license
        if re.search(r"valid driver'?s? license.*required", page_text_lower):
            return "Driver's license required"

        return None

    @staticmethod
    def is_valid_job_url(url):
        """Validate if URL is a legitimate job URL."""
        if not url or not url.startswith("http"):
            return False, "Invalid URL format"

        url_lower = url.lower()

        # Block non-job URLs
        blocked_patterns = [
            "/unsubscribe",
            "/settings",
            "/account",
            "/profile",
            "/talent-network",
            "linkedin.com/company",
            "indeed.com/cmp",
        ]

        if any(pattern in url_lower for pattern in blocked_patterns):
            return False, "Non-job URL"

        return True, "Valid"

    @staticmethod
    def validate_company_field(company, title, url):
        """✅ ENHANCED: Validate and fix company name."""
        if not company or company == "Unknown":
            company = ValidationHelper.extract_company_from_domain(url)

        company_lower = company.lower()

        # Check if company is actually part of title
        title_lower = title.lower() if title else ""
        if company_lower in title_lower and len(company) < len(title) / 2:
            return False, company, "Company name embedded in title"

        # Fix special company names
        for domain, correct_name in SPECIAL_COMPANY_NAMES.items():
            if domain in url.lower():
                company = correct_name
                break

        # Remove suffixes
        company = re.sub(
            r"\s*(?:Inc\.|LLC|Corp\.|Corporation|Ltd\.|Limited).*$",
            "",
            company,
            flags=re.I,
        ).strip()

        return True, company, "Valid"

    @staticmethod
    def extract_company_from_domain(url):
        """✅ ENHANCED: Extract company from domain with special mappings."""
        if not url:
            return "Unknown"

        # Check special mappings first
        for domain, company_name in SPECIAL_COMPANY_NAMES.items():
            if domain in url.lower():
                return company_name

        # Extract from domain
        try:
            from urllib.parse import urlparse

            domain = urlparse(url).netloc
            domain = (
                domain.replace("www.", "").replace("careers.", "").replace("jobs.", "")
            )

            # Remove common suffixes
            company = domain.split(".")[0]

            # Capitalize properly
            if company:
                company = company.replace("-", " ").replace("_", " ")
                company = " ".join(word.capitalize() for word in company.split())
                return company

            return "Unknown"
        except:
            return "Unknown"

    @staticmethod
    def check_sponsorship_status(soup):
        """Check H1B sponsorship status."""
        if not soup:
            return "Unknown"

        page_text = soup.get_text()[:3000].lower()

        if (
            "will sponsor" in page_text
            or "h-1b sponsor" in page_text
            or "h1b sponsor" in page_text
        ):
            return "Yes"
        if "no sponsorship" in page_text or "cannot sponsor" in page_text:
            return "No"
        if "sponsorship available" in page_text:
            return "Yes"

        return "Unknown"


class QualityScorer:
    """Quality assessment for job listings."""

    @staticmethod
    def calculate_score(job_data):
        """Calculate quality score (0-7)."""
        score = 0

        if job_data.get("company") and job_data["company"] != "Unknown":
            score += 1
        if job_data.get("title") and job_data["title"] != "Unknown":
            score += 1
        if job_data.get("location") and job_data["location"] not in ["Unknown", ""]:
            score += 2
        if job_data.get("job_id") and job_data["job_id"] != "N/A":
            score += 2
        if job_data.get("sponsorship") and job_data["sponsorship"] != "Unknown":
            score += 1

        return score

    @staticmethod
    def is_acceptable_quality(score):
        """Determine if quality score is acceptable."""
        return score >= 4
