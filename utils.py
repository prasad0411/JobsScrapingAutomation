#!/usr/bin/env python3

import re
import logging
from dataclasses import dataclass
from functools import lru_cache
from collections import Counter
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

_STOPWORD_TRANS = str.maketrans("", "", "".join(set("".join(COMPANY_NAME_STOPWORDS))))


@dataclass
class ExtractionResult:
    value: str
    confidence: float
    method: str

    def is_valid(self):
        return self.value not in [None, "", "N/A", "Unknown"]


class PlatformDetector:
    @staticmethod
    @lru_cache(maxsize=1024)
    def detect(url):
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

    _ACRONYMS = {
        "ibm",
        "att",
        "sap",
        "hpe",
        "aws",
        "gcp",
        "api",
        "ai",
        "ml",
        "abb",
        "asml",
    }

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
        if not company_name or not company_name.strip():
            return None

        try:
            name = company_name.strip()
            name_lower = name.lower()

            for prefix in COMPANY_NAME_PREFIXES:
                if name_lower.startswith(prefix):
                    name = name[len(prefix) :]
                    name_lower = name.lower()
                    break

            for stopword in COMPANY_NAME_STOPWORDS:
                name = name.replace(stopword, "")

            name = name.strip()
            name_lower = name.lower()

            if name_lower in COMPANY_SLUG_MAPPING:
                return COMPANY_SLUG_MAPPING[name_lower]

            if name_lower in cls._COMPOUND_WORDS:
                return cls._COMPOUND_WORDS[name_lower]

            if len(name_lower) > 8 and " " not in name_lower:
                for compound, expanded in cls._COMPOUND_WORDS.items():
                    if compound in name_lower:
                        return expanded

            name = re.sub(r"^[A-Z]{2,4}[-\s]", "", name)
            name = re.sub(
                r",?\s+(Inc\.?|LLC\.?|Corp\.?|Ltd\.?|Corporation|Corp\s+Svcs\.?)$",
                "",
                name,
                flags=re.I,
            )
            name = name.strip()

            if not name or len(name) < 2:
                return None

            if name in COMPANY_PLACEHOLDERS or name.lower() in [
                p.lower() for p in COMPANY_PLACEHOLDERS
            ]:
                return None

            return cls._apply_smart_capitalization(name)

        except Exception as e:
            logging.debug(f"Company normalization failed for '{company_name}': {e}")
            return None

    @classmethod
    def _apply_smart_capitalization(cls, name):
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
                    if word in ["ai", "ml", "api", "aws", "gcp", "iot"]:
                        result.append(word.upper())
                    else:
                        result.append(word.title())
                return " ".join(result)

            return name
        except Exception as e:
            logging.debug(f"Capitalization failed for '{name}': {e}")
            return name

    @classmethod
    def extract_from_url_path(cls, url, platform):
        patterns = {
            "greenhouse": r"greenhouse\.io/([^/]+)/jobs",
            "ashby": r"ashbyhq\.com/([^/]+)/",
            "lever": r"lever\.co/([^/]+)/",
            "workable": r"workable\.com/([^/]+)/",
        }

        try:
            if platform in patterns:
                match = re.search(patterns[platform], url, re.I)
                if match:
                    slug = match.group(1)
                    return cls.normalize(slug, url)
        except Exception as e:
            logging.debug(f"URL path extraction failed for {url}: {e}")

        return None


class CompanyValidator:
    _INVALID_KEYWORDS = {
        "careers",
        "jobs",
        "external",
        "applicant",
        "portal",
        "apply",
        "join",
    }
    _TITLE_INDICATORS = {"intern", "engineer", "developer", "software", "position"}

    @staticmethod
    @lru_cache(maxsize=512)
    def is_valid(name):
        if not name or not name.strip():
            return False

        try:
            if name in COMPANY_PLACEHOLDERS or name.lower() in {
                p.lower() for p in COMPANY_PLACEHOLDERS
            }:
                return False

            if name.lower() in CompanyValidator._INVALID_KEYWORDS:
                return False

            if re.match(r"^[A-Z]{2,4}[-\s]", name):
                return False

            if len(name) > 60 or len(name) < 2:
                return False

            if name.isupper() and len(name) < 10 and not any(c.isdigit() for c in name):
                return False

            title_count = sum(
                1 for kw in CompanyValidator._TITLE_INDICATORS if kw in name.lower()
            )
            if title_count >= 3:
                return False

            return True
        except Exception as e:
            logging.debug(f"Company validation failed for '{name}': {e}")
            return False

    @staticmethod
    @lru_cache(maxsize=256)
    def is_junk_subdomain(subdomain):
        if not subdomain:
            return True

        try:
            for pattern in _COMPILED_JUNK_PATTERNS:
                if pattern.search(subdomain):
                    return True
            if subdomain.islower() and len(subdomain) > 20 and " " not in subdomain:
                return True
        except Exception as e:
            logging.debug(f"Junk subdomain check failed for '{subdomain}': {e}")

        return False


class RoleCategorizer:
    @staticmethod
    @lru_cache(maxsize=512)
    def categorize(title):
        if not title:
            return "Unknown", "ACCEPT", ""

        try:
            title_lower = title.lower()

            for category_name, config in ROLE_CATEGORIES.items():
                keyword_match = any(kw in title_lower for kw in config["keywords"])
                exclude_match = any(ex in title_lower for ex in config["exclude"])

                if keyword_match and not exclude_match:
                    return category_name, config["action"], config["alert"]

            generic_sw = {"engineer", "developer", "programmer", "software"}
            if any(kw in title_lower for kw in generic_sw):
                return "Pure Software", "ACCEPT", "✅ SOFTWARE"

        except Exception as e:
            logging.debug(f"Role categorization failed for '{title}': {e}")

        return "Unknown", "ACCEPT", ""

    @staticmethod
    def get_terminal_alert(title):
        _, _, alert = RoleCategorizer.categorize(title)
        return f"[{alert}]" if alert else ""


class URLCleaner:
    _CLEAN_PATTERN = re.compile(r"[?#].*$")

    @classmethod
    @lru_cache(maxsize=2048)
    def clean_url(cls, url):
        if not url:
            return ""

        try:
            if "jobright.ai/jobs/info/" in url.lower():
                match = re.search(r"(jobright\.ai/jobs/info/[a-f0-9]+)", url, re.I)
                if match:
                    return match.group(1).lower()

            return cls._CLEAN_PATTERN.sub("", url).lower().rstrip("/")
        except Exception as e:
            logging.debug(f"URL cleaning failed for '{url}': {e}")
            return url.lower()

    @staticmethod
    @lru_cache(maxsize=2048)
    def normalize_text(text):
        if not text:
            return ""
        try:
            return re.sub(r"[^a-z0-9]", "", text.lower())
        except Exception as e:
            logging.debug(f"Text normalization failed: {e}")
            return text.lower()


class ExtractionVoter:
    @staticmethod
    def vote(results, min_confidence=0.6):
        if not results:
            return None

        try:
            valid_results = [
                r for r in results if r.is_valid() and r.confidence >= min_confidence
            ]

            if not valid_results:
                return None

            if len(valid_results) == 1:
                return valid_results[0]

            value_groups = {}
            for result in valid_results:
                key = result.value.lower().strip()
                if key not in value_groups:
                    value_groups[key] = []
                value_groups[key].append(result)

            best_group = max(
                value_groups.items(),
                key=lambda x: (len(x[1]) * 1.2)
                * (sum(r.confidence for r in x[1]) / len(x[1])),
            )

            return max(best_group[1], key=attrgetter("confidence"))

        except Exception as e:
            logging.debug(f"Extraction voting failed: {e}")
            return None


class DateParser:
    _RELATIVE_PATTERN = re.compile(r"(\d+)\s*d(?:ays?)?\s*ago", re.I)
    _HOURS_PATTERN = re.compile(r"(\d+)\s*h(?:ours?)?\s*ago", re.I)
    _TODAY_PATTERN = re.compile(r"\b(today|just\s+now)\b", re.I)
    _YESTERDAY_PATTERN = re.compile(r"\byesterday\b", re.I)

    @classmethod
    def extract_days_ago(cls, text):
        if not text:
            return None

        try:
            text_lower = text.lower()

            if cls._TODAY_PATTERN.search(text_lower):
                return 0

            if cls._YESTERDAY_PATTERN.search(text_lower):
                return 1

            match = cls._HOURS_PATTERN.search(text_lower)
            if match:
                return 0

            match = cls._RELATIVE_PATTERN.search(text_lower)
            if match:
                return int(match.group(1))

            if DATEUTIL_AVAILABLE:
                try:
                    parsed_date = parse_date_flexible(text)
                    if parsed_date:
                        from datetime import datetime

                        now = datetime.now()
                        days_diff = (now - parsed_date).days

                        if days_diff < -60:
                            try:
                                adjusted_date = parsed_date.replace(
                                    year=parsed_date.year - 1
                                )
                                days_diff = (now - adjusted_date).days

                                if days_diff < 0:
                                    return None

                            except (ValueError, OverflowError):
                                return None

                        if days_diff < 0:
                            return None

                        return days_diff

                except Exception as e:
                    logging.debug(f"Flexible date parsing failed for '{text}': {e}")
                    pass

        except Exception as e:
            logging.debug(f"Date parsing failed for '{text}': {e}")

        return None


class QualityScorer:
    @staticmethod
    def calculate_score(job_data):
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

        except Exception as e:
            logging.debug(f"Quality scoring failed: {e}")

        return score

    @staticmethod
    def is_acceptable_quality(score, min_score=4):
        return score >= min_score
