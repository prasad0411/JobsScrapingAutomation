#!/usr/bin/env python3

import re
import logging
from dataclasses import dataclass
from functools import lru_cache
from operator import attrgetter

from config import (
    COMPANY_SLUG_MAPPING,
    COMPANY_NAME_PREFIXES,
    COMPANY_NAME_STOPWORDS,
    COMPANY_PLACEHOLDERS,
    PLATFORM_DETECTION_PATTERNS,
    ROLE_CATEGORIES,
    JUNK_SUBDOMAIN_PATTERNS,
    extract_domain_and_subdomain,
    fuzzy_match_company,
    parse_date_flexible,
    RAPIDFUZZ_AVAILABLE,
    DATEUTIL_AVAILABLE,
)

_COMPILED_PLATFORM_PATTERNS = {
    platform: re.compile(pattern, re.I)
    for platform, pattern in PLATFORM_DETECTION_PATTERNS.items()
}

_COMPILED_JUNK_PATTERNS = [
    re.compile(pattern, re.I) for pattern in JUNK_SUBDOMAIN_PATTERNS
]

# ============================================================================
# Extraction Result Data Structure
# ============================================================================


@dataclass
class ExtractionResult:
    value: str
    confidence: float
    method: str

    def is_valid(self):
        """
        CRITICAL FIX: "Unknown" is NOT a valid result
        Only actual extracted values are valid
        """
        return self.value not in [None, "", "N/A", "Unknown"]


# ============================================================================
# Extraction Voter - CRITICAL FIX
# ============================================================================


class ExtractionVoter:
    @staticmethod
    def vote(results, min_confidence=0.6):
        """
        CRITICAL FIX: Filter out None, "Unknown", and empty strings BEFORE voting
        This ensures URL fallback methods always execute
        """
        if not results:
            return None

        try:
            # CRITICAL: Filter out invalid results (None, "Unknown", "")
            valid_results = [
                r
                for r in results
                if r.value not in [None, "", "Unknown", "N/A"]
                and r.confidence >= min_confidence
            ]

            if not valid_results:
                return None

            if len(valid_results) == 1:
                return valid_results[0]

            # Group by value (case-insensitive)
            value_groups = {}
            for result in valid_results:
                key = result.value.lower().strip()
                if key not in value_groups:
                    value_groups[key] = []
                value_groups[key].append(result)

            # Pick best group (by count * confidence)
            best_group = max(
                value_groups.items(),
                key=lambda x: (len(x[1]) * 1.2)
                * (sum(r.confidence for r in x[1]) / len(x[1])),
            )

            # Return highest confidence result from best group
            return max(best_group[1], key=attrgetter("confidence"))

        except Exception as e:
            logging.debug(f"Extraction voting failed: {e}")
            return None


# ============================================================================
# Platform Detector
# ============================================================================


class PlatformDetector:
    @staticmethod
    @lru_cache(maxsize=1024)
    def detect(url):
        """Detect job platform from URL"""
        if not url:
            return "generic"

        try:
            url_lower = url.lower()
            for platform, pattern in _COMPILED_PLATFORM_PATTERNS.items():
                if pattern.search(url_lower):
                    return platform
        except Exception as e:
            logging.debug(f"Platform detection failed for {url}: {e}")

        return "generic"


# ============================================================================
# Company Normalizer
# ============================================================================


class CompanyNormalizer:
    _COMPOUND_WORDS = {
        "motorolasolutions": "Motorola Solutions",
        "goldmansachs": "Goldman Sachs",
        "morganstanley": "Morgan Stanley",
        "bankofamerica": "Bank of America",
        "wellsfargo": "Wells Fargo",
        "americanexpress": "American Express",
        "capitalone": "Capital One",
    }

    _ACRONYMS = {"ibm", "att", "sap", "hpe", "aws", "gcp", "abb", "asml"}

    _SPECIAL_CAPS = {
        "openai": "OpenAI",
        "youtube": "YouTube",
        "linkedin": "LinkedIn",
        "paypal": "PayPal",
        "ebay": "eBay",
        "tiktok": "TikTok",
    }

    @classmethod
    @lru_cache(maxsize=512)
    def normalize(cls, company_name, url=""):
        """Normalize company name"""
        if not company_name or not company_name.strip():
            return None

        try:
            name = company_name.strip()
            name_lower = name.lower()

            # Remove prefixes
            for prefix in COMPANY_NAME_PREFIXES:
                if name_lower.startswith(prefix):
                    name = name[len(prefix) :]
                    name_lower = name.lower()
                    break

            # Remove stopwords
            for stopword in COMPANY_NAME_STOPWORDS:
                name = name.replace(stopword, "")

            name = name.strip()
            name_lower = name.lower()

            # Check mappings
            if name_lower in COMPANY_SLUG_MAPPING:
                return COMPANY_SLUG_MAPPING[name_lower]

            if name_lower in cls._COMPOUND_WORDS:
                return cls._COMPOUND_WORDS[name_lower]

            # Remove legal entity suffixes
            name = re.sub(r",?\s+(Inc\.?|LLC\.?|Corp\.?|Ltd\.?)$", "", name, flags=re.I)
            name = name.strip()

            if not name or len(name) < 2:
                return None

            if name in COMPANY_PLACEHOLDERS:
                return None

            return cls._apply_smart_capitalization(name)

        except Exception as e:
            logging.debug(f"Company normalization failed: {e}")
            return None

    @classmethod
    def _apply_smart_capitalization(cls, name):
        """Apply smart capitalization rules"""
        try:
            name_lower = name.lower()

            if name_lower in cls._ACRONYMS:
                return name.upper()

            if name_lower in cls._SPECIAL_CAPS:
                return cls._SPECIAL_CAPS[name_lower]

            if name.isupper() or name.islower():
                words = name_lower.split()
                result = []
                for word in words:
                    if word in ["ai", "ml", "api", "aws", "gcp"]:
                        result.append(word.upper())
                    else:
                        result.append(word.title())
                return " ".join(result)

            return name
        except:
            return name

    @classmethod
    def extract_from_url_path(cls, url, platform):
        """Extract company name from URL path"""
        patterns = {
            "greenhouse": r"greenhouse\.io/([^/]+)/jobs",
            "ashby": r"ashbyhq\.com/([^/]+)/",
            "lever": r"lever\.co/([^/]+)/",
        }

        try:
            if platform in patterns:
                match = re.search(patterns[platform], url, re.I)
                if match:
                    slug = match.group(1)
                    return cls.normalize(slug, url)
        except:
            pass

        return None


# ============================================================================
# Company Validator
# ============================================================================


class CompanyValidator:
    _INVALID_KEYWORDS = {"careers", "jobs", "external", "applicant", "portal"}
    _TITLE_INDICATORS = {"intern", "engineer", "developer", "software"}

    @staticmethod
    @lru_cache(maxsize=512)
    def is_valid(name):
        """Validate company name"""
        if not name or not name.strip():
            return False

        try:
            if name in COMPANY_PLACEHOLDERS:
                return False

            if name.lower() in CompanyValidator._INVALID_KEYWORDS:
                return False

            if len(name) > 60 or len(name) < 2:
                return False

            # Check if it looks like a job title
            title_count = sum(
                1 for kw in CompanyValidator._TITLE_INDICATORS if kw in name.lower()
            )
            if title_count >= 3:
                return False

            return True
        except:
            return False

    @staticmethod
    @lru_cache(maxsize=256)
    def is_junk_subdomain(subdomain):
        """Check if subdomain is junk"""
        if not subdomain:
            return True

        try:
            for pattern in _COMPILED_JUNK_PATTERNS:
                if pattern.search(subdomain):
                    return True
        except:
            pass

        return False


# ============================================================================
# Role Categorizer
# ============================================================================


class RoleCategorizer:
    @staticmethod
    @lru_cache(maxsize=512)
    def categorize(title):
        """Categorize job role"""
        if not title:
            return "Unknown", "ACCEPT", ""

        try:
            title_lower = title.lower()

            for category_name, config in ROLE_CATEGORIES.items():
                keyword_match = any(kw in title_lower for kw in config["keywords"])
                exclude_match = any(ex in title_lower for ex in config["exclude"])

                if keyword_match and not exclude_match:
                    return category_name, config["action"], config["alert"]

            # Generic software role
            generic_sw = {"engineer", "developer", "programmer", "software"}
            if any(kw in title_lower for kw in generic_sw):
                return "Pure Software", "ACCEPT", "✅ SOFTWARE"

        except:
            pass

        return "Unknown", "ACCEPT", ""

    @staticmethod
    def get_terminal_alert(title):
        """Get terminal alert string for role"""
        _, _, alert = RoleCategorizer.categorize(title)
        return f"[{alert}]" if alert else ""


# ============================================================================
# URL Cleaner
# ============================================================================


class URLCleaner:
    _CLEAN_PATTERN = re.compile(r"[?#].*$")

    @classmethod
    @lru_cache(maxsize=2048)
    def clean_url(cls, url):
        """Clean URL for comparison"""
        if not url:
            return ""

        try:
            # Special handling for Jobright
            if "jobright.ai/jobs/info/" in url.lower():
                match = re.search(r"(jobright\.ai/jobs/info/[a-f0-9]+)", url, re.I)
                if match:
                    return match.group(1).lower()

            # Remove query params and fragments
            return cls._CLEAN_PATTERN.sub("", url).lower().rstrip("/")
        except:
            return url.lower()

    @staticmethod
    @lru_cache(maxsize=2048)
    def normalize_text(text):
        """Normalize text for comparison"""
        if not text:
            return ""
        try:
            return re.sub(r"[^a-z0-9]", "", text.lower())
        except:
            return text.lower()


# ============================================================================
# Date Parser
# ============================================================================


class DateParser:
    _RELATIVE_PATTERN = re.compile(r"(\d+)\s*d(?:ays?)?\s*ago", re.I)
    _HOURS_PATTERN = re.compile(r"(\d+)\s*h(?:ours?)?\s*ago", re.I)
    _TODAY_PATTERN = re.compile(r"\b(today|just\s+now)\b", re.I)
    _YESTERDAY_PATTERN = re.compile(r"\byesterday\b", re.I)

    @classmethod
    def extract_days_ago(cls, text):
        """Extract days ago from text"""
        if not text:
            return None

        try:
            text_lower = text.lower()

            # Today
            if cls._TODAY_PATTERN.search(text_lower):
                return 0

            # Yesterday
            if cls._YESTERDAY_PATTERN.search(text_lower):
                return 1

            # Hours ago (treat as today)
            match = cls._HOURS_PATTERN.search(text_lower)
            if match:
                return 0

            # Days ago
            match = cls._RELATIVE_PATTERN.search(text_lower)
            if match:
                return int(match.group(1))

            # Try flexible date parsing
            if DATEUTIL_AVAILABLE:
                try:
                    parsed_date = parse_date_flexible(text)
                    if parsed_date:
                        from datetime import datetime

                        now = datetime.now()
                        days_diff = (now - parsed_date).days

                        # Handle year wrap (if date seems too far in future)
                        if days_diff < -60:
                            try:
                                adjusted_date = parsed_date.replace(
                                    year=parsed_date.year - 1
                                )
                                days_diff = (now - adjusted_date).days
                                if days_diff < 0:
                                    return None
                            except:
                                return None

                        if days_diff < 0:
                            return None

                        return days_diff
                except:
                    pass

        except:
            pass

        return None


# ============================================================================
# Quality Scorer
# ============================================================================


class QualityScorer:
    @staticmethod
    def calculate_score(job_data):
        """Calculate quality score for job data"""
        score = 0

        try:
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

        except:
            pass

        return score

    @staticmethod
    def is_acceptable_quality(score, min_score=4):
        """Check if quality score meets minimum"""
        return score >= min_score
