#!/usr/bin/env python3

import re
import logging
from dataclasses import dataclass
from functools import lru_cache
from collections import Counter
from operator import attrgetter

from aggregator.config import (
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
    MAX_REASONABLE_AGE_DAYS,
)

_COMPILED_PLATFORM_PATTERNS = {
    platform: re.compile(pattern, re.I)
    for platform, pattern in PLATFORM_DETECTION_PATTERNS.items()
}

_COMPILED_JUNK_PATTERNS = [
    re.compile(pattern, re.I) for pattern in JUNK_SUBDOMAIN_PATTERNS
]

_STOPWORD_TRANS = str.maketrans("", "", "".join(set("".join(COMPANY_NAME_STOPWORDS))))


# ============================================================================
# Extraction Result - ORIGINAL
# ============================================================================


@dataclass
class ExtractionResult:
    value: str
    confidence: float
    method: str

    def is_valid(self):
        """
        CRITICAL FIX: "Unknown" is NOT a valid result
        This enables URL fallback to work
        """
        return self.value not in [None, "", "N/A", "Unknown"]


# ============================================================================
# Platform Detector - ORIGINAL
# ============================================================================


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


# ============================================================================
# Company Normalizer - ORIGINAL (FULL)
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


# ============================================================================
# Company Validator - ORIGINAL (FULL)
# ============================================================================


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


# ============================================================================
# Role Categorizer - ORIGINAL (FULL)
# ============================================================================


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


# ============================================================================
# URL Cleaner - ORIGINAL
# ============================================================================


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


# ============================================================================
# Extraction Voter - ORIGINAL + CRITICAL FIX
# ============================================================================


class ExtractionVoter:
    @staticmethod
    def vote(results, min_confidence=0.6):
        """
        CRITICAL FIX: Filter out None/"Unknown" BEFORE voting
        This is what enables URL fallback to work!
        """
        if not results:
            return None

        try:
            # CRITICAL: Filter out invalid results (None, "Unknown", empty)
            valid_results = [
                r
                for r in results
                if r.value not in [None, "", "N/A", "Unknown"]  # THE FIX
                and r.confidence >= min_confidence
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


# ============================================================================
# Date Parser - ORIGINAL + ENHANCED with sanity capping
# ============================================================================


class DateParser:
    _RELATIVE_PATTERN = re.compile(r"(\d+)\+?\s*d(?:ays?)?\s*ago", re.I)
    _HOURS_PATTERN = re.compile(r"(\d+)\s*h(?:ours?)?\s*ago", re.I)
    _TODAY_PATTERN = re.compile(r"\b(today|just\s+now)\b", re.I)
    _YESTERDAY_PATTERN = re.compile(r"\byesterday\b", re.I)

    @classmethod
    def extract_days_ago(cls, text):
        """ENHANCED: Handles "+", "1mo", absolute dates, with detailed logging"""
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

            month_match = re.search(r"(\d+)\+?\s*mo(?:nth)?s?\s*ago", text_lower)
            if month_match:
                months = int(month_match.group(1))
                days = months * 30
                has_plus = "+" in month_match.group(0)

                context_start = max(0, month_match.start() - 40)
                context_end = min(len(text_lower), month_match.end() + 40)
                context = text_lower[context_start:context_end]

                logging.debug(
                    f"Age extraction: {months}mo ({days} days) | Plus: {has_plus} | Text: '...{context}...'"
                )

                if has_plus:
                    return days + 1

                if 0 <= days <= MAX_REASONABLE_AGE_DAYS:
                    return days
                else:
                    return None

            match = re.search(r"(\d+)(\+)?\s*d(?:ays?)?\s*ago", text_lower)
            if match:
                days = int(match.group(1))
                has_plus = match.group(2) is not None

                context_start = max(0, match.start() - 40)
                context_end = min(len(text_lower), match.end() + 40)
                context = text_lower[context_start:context_end]

                logging.debug(
                    f"Age extraction: {days} days | Plus: {has_plus} | Text: '...{context}...'"
                )

                if has_plus:
                    return days + 1

                if 0 <= days <= MAX_REASONABLE_AGE_DAYS:
                    return days
                else:
                    logging.debug(
                        f"Age {days} exceeds MAX_REASONABLE_AGE_DAYS ({MAX_REASONABLE_AGE_DAYS})"
                    )
                    return None

            if DATEUTIL_AVAILABLE:
                try:
                    parsed_date = parse_date_flexible(text)
                    if parsed_date:
                        from datetime import datetime

                        now = datetime.now()
                        days_diff = (now - parsed_date).days

                        if days_diff < 0:
                            if days_diff > -7:
                                logging.debug(
                                    f"Future date by {abs(days_diff)} days ('{text}') - treating as today"
                                )
                                return 0
                            else:
                                logging.warning(
                                    f"Future date: {parsed_date} from '{text}' - ignoring"
                                )
                                return None

                        if days_diff < -60:
                            try:
                                adjusted_date = parsed_date.replace(
                                    year=parsed_date.year - 1
                                )
                                days_diff = (now - adjusted_date).days

                                if days_diff < 0:
                                    logging.debug(
                                        f"Date still future after year adjustment - ignoring"
                                    )
                                    return None

                            except (ValueError, OverflowError):
                                return None

                        if days_diff > MAX_REASONABLE_AGE_DAYS:
                            logging.warning(
                                f"Unreasonably old: {days_diff} days from '{text}' - ignoring"
                            )
                            return None

                        if days_diff >= 0 and days_diff <= MAX_REASONABLE_AGE_DAYS:
                            logging.debug(
                                f"Absolute date extracted: {days_diff} days from '{text}'"
                            )
                            return days_diff

                        return None

                except Exception as e:
                    logging.debug(f"Flexible date parsing failed for '{text}': {e}")
                    pass

        except Exception as e:
            logging.debug(f"Date parsing failed for '{text}': {e}")

        return None


# ============================================================================
# Quality Scorer - ORIGINAL
# ============================================================================


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


class DataSanitizer:
    _EMOJI_PATTERN = re.compile(
        "["
        "\U0001f600-\U0001f64f"
        "\U0001f300-\U0001f5ff"
        "\U0001f680-\U0001f6ff"
        "\U0001f1e0-\U0001f1ff"
        "\U0001f900-\U0001f9ff"
        "\U0001fa00-\U0001fa6f"
        "\U00002600-\U000026ff"
        "\U00002700-\U000027bf"
        "\U00002702-\U000027b0"
        "\U000024c2-\U0001f251"
        "]+",
        flags=re.UNICODE,
    )

    _HTML_ENTITIES = {
        "&amp;": "&",
        "&nbsp;": " ",
        "&quot;": '"',
        "&apos;": "'",
        "&lt;": "<",
        "&gt;": ">",
        "&#39;": "'",
        "&#x27;": "'",
    }

    @classmethod
    def sanitize_all_fields(cls, job_data):
        sanitized = {}

        sanitized["company"] = cls.sanitize_company(job_data.get("company", "Unknown"))
        sanitized["title"] = cls.sanitize_title(job_data.get("title", "Unknown"))
        sanitized["location"] = cls.sanitize_location(
            job_data.get("location", "Unknown")
        )
        sanitized["remote"] = job_data.get("remote", "Unknown")
        sanitized["url"] = job_data.get("url", "")
        sanitized["job_id"] = cls.sanitize_job_id(job_data.get("job_id", "N/A"))
        sanitized["sponsorship"] = cls.sanitize_sponsorship(
            job_data.get("sponsorship", "Unknown")
        )
        sanitized["job_type"] = job_data.get("job_type", "Internship")
        sanitized["entry_date"] = job_data.get("entry_date", "")
        sanitized["source"] = job_data.get("source", "Unknown")

        if "reason" in job_data:
            sanitized["reason"] = job_data["reason"]

        return sanitized

    @classmethod
    def sanitize_title(cls, title):
        if not title or title == "Unknown":
            return title

        try:
            text = str(title)

            from aggregator.config import DATA_SANITIZATION_PREFERENCES, FIELD_PREFIXES_TO_REMOVE

            if DATA_SANITIZATION_PREFERENCES.get("remove_emojis", True):
                text = cls._remove_emojis(text)

            if DATA_SANITIZATION_PREFERENCES.get("normalize_unicode", True):
                text = cls._normalize_unicode(text)

            if DATA_SANITIZATION_PREFERENCES.get("decode_html_entities", True):
                text = cls._decode_html_entities(text)

            if DATA_SANITIZATION_PREFERENCES.get("strip_field_prefixes", True):
                for prefix in FIELD_PREFIXES_TO_REMOVE:
                    if text.startswith(prefix):
                        text = text[len(prefix) :].strip()
                        break

            if DATA_SANITIZATION_PREFERENCES.get("trim_whitespace", True):
                text = re.sub(r"\s+", " ", text).strip()

            return text if text else "Unknown"

        except Exception as e:
            logging.debug(f"Title sanitization failed: {e}")
            return title

    @classmethod
    def sanitize_company(cls, company):
        if not company or company == "Unknown":
            return company

        try:
            text = str(company)

            from aggregator.config import DATA_SANITIZATION_PREFERENCES, FIELD_PREFIXES_TO_REMOVE

            if DATA_SANITIZATION_PREFERENCES.get("remove_emojis", True):
                text = cls._remove_emojis(text)

            if DATA_SANITIZATION_PREFERENCES.get("normalize_unicode", True):
                text = cls._normalize_unicode(text)

            if DATA_SANITIZATION_PREFERENCES.get("decode_html_entities", True):
                text = cls._decode_html_entities(text)

            if DATA_SANITIZATION_PREFERENCES.get("strip_field_prefixes", True):
                for prefix in FIELD_PREFIXES_TO_REMOVE:
                    if text.startswith(prefix):
                        text = text[len(prefix) :].strip()
                        break

            if DATA_SANITIZATION_PREFERENCES.get("trim_whitespace", True):
                text = re.sub(r"\s+", " ", text).strip()

            if not text or len(text) < 2:
                return "Unknown"

            return text

        except Exception as e:
            logging.debug(f"Company sanitization failed: {e}")
            return company

    @classmethod
    def sanitize_location(cls, location):
        if not location or location == "Unknown":
            return "Unknown"

        try:
            text = str(location)

            from aggregator.config import (
                DATA_SANITIZATION_PREFERENCES,
                FIELD_PREFIXES_TO_REMOVE,
                FULL_STATE_NAMES,
                CITY_TO_STATE_FALLBACK,
                validate_us_state_code,
            )

            if DATA_SANITIZATION_PREFERENCES.get("remove_emojis", True):
                text = cls._remove_emojis(text)

            if DATA_SANITIZATION_PREFERENCES.get("normalize_unicode", True):
                text = cls._normalize_unicode(text)

            if DATA_SANITIZATION_PREFERENCES.get("decode_html_entities", True):
                text = cls._decode_html_entities(text)

            if DATA_SANITIZATION_PREFERENCES.get("strip_field_prefixes", True):
                for prefix in FIELD_PREFIXES_TO_REMOVE:
                    if text.startswith(prefix):
                        text = text[len(prefix) :].strip()
                        break

            if DATA_SANITIZATION_PREFERENCES.get("trim_whitespace", True):
                text = re.sub(r"\s+", " ", text).strip()

            if text and len(text) > 15 and not re.search(r"[,\s-]", text):
                return "Unknown"

            if DATA_SANITIZATION_PREFERENCES.get("validate_garbage_locations", True):
                if cls._is_garbage_location(text):
                    return "Unknown"

            if "," in text and text.count(",") > 1:
                text = cls._parse_multi_location(text)

            if DATA_SANITIZATION_PREFERENCES.get("standardize_location_format", True):
                text = cls._standardize_location_format(text)

            return text if text else "Unknown"

        except Exception as e:
            logging.debug(f"Location sanitization failed: {e}")
            return location

    @classmethod
    def sanitize_job_id(cls, job_id):
        if not job_id:
            return "N/A"

        try:
            text = str(job_id).strip()

            from aggregator.config import JOB_ID_PREFERENCES

            if text.startswith("HASH_"):
                if not JOB_ID_PREFERENCES.get("hash_fallback_enabled", False):
                    return JOB_ID_PREFERENCES.get("fallback_value", "N/A")

            return text if text else "N/A"

        except Exception as e:
            logging.debug(f"Job ID sanitization failed: {e}")
            return job_id

    @classmethod
    def sanitize_sponsorship(cls, sponsorship):
        if not sponsorship:
            return "Unknown"

        try:
            text = str(sponsorship).strip()

            from aggregator.config import DATA_SANITIZATION_PREFERENCES

            if DATA_SANITIZATION_PREFERENCES.get("normalize_sponsorship_values", True):
                if "unknown" in text.lower():
                    return "Unknown"
                if text.lower() in ["yes", "no"]:
                    return text.capitalize()

            return text if text else "Unknown"

        except Exception as e:
            logging.debug(f"Sponsorship sanitization failed: {e}")
            return sponsorship

    @classmethod
    def _remove_emojis(cls, text):
        if not text:
            return text
        return cls._EMOJI_PATTERN.sub("", text)

    @classmethod
    def _normalize_unicode(cls, text):
        if not text:
            return text

        try:
            import unicodedata

            nfd = unicodedata.normalize("NFD", text)
            ascii_text = "".join(
                char for char in nfd if unicodedata.category(char) != "Mn"
            )
            return ascii_text
        except Exception:
            return text

    @classmethod
    def _decode_html_entities(cls, text):
        if not text:
            return text

        try:
            import html

            text = html.unescape(text)

            for entity, replacement in cls._HTML_ENTITIES.items():
                text = text.replace(entity, replacement)

            return text
        except Exception:
            return text

    @classmethod
    def _is_garbage_location(cls, text):
        if not text or text == "Unknown":
            return False

        text_clean = text.strip()
        text_lower = text_clean.lower()

        if text_clean.startswith(","):
            return True

        try:
            from aggregator.config import US_STATES_FALLBACK

            if text_clean.upper() in US_STATES_FALLBACK and len(text_clean) == 2:
                return True
        except (ImportError, AttributeError):
            pass

        if re.match(r"^\d+", text):
            return True

        if any(
            word in text_lower
            for word in ["hospital", "patient", "office building", "headquarters only"]
        ):
            return True

        if len(text) > 100:
            return True

        try:
            from aggregator.config import GARBAGE_LOCATION_PATTERNS

            if any(phrase in text_lower for phrase in GARBAGE_LOCATION_PATTERNS):
                return True
        except (ImportError, AttributeError):
            pass

        garbage_phrases = [
            "as well as",
            "in accordance with",
            "equal opportunity",
            "without regard to",
            "more search options",
        ]
        if any(phrase in text_lower for phrase in garbage_phrases):
            return True

        return False

    @classmethod
    def _parse_multi_location(cls, location_text):
        if not location_text or "," not in location_text:
            return location_text

        try:
            from aggregator.config import (
                CITY_TO_STATE_FALLBACK,
                FULL_STATE_NAMES,
                validate_us_state_code,
            )

            segments = [s.strip() for s in location_text.split(",")]

            for i in range(len(segments) - 1):
                city = segments[i]
                state_candidate = segments[i + 1]

                if len(state_candidate) == 2 and validate_us_state_code(
                    state_candidate
                ):
                    return f"{city}, {state_candidate.upper()}"

            for segment in segments:
                segment_lower = segment.lower()

                if segment_lower in CITY_TO_STATE_FALLBACK:
                    state = CITY_TO_STATE_FALLBACK[segment_lower]
                    return f"{segment.title()}, {state}"

                if segment_lower in FULL_STATE_NAMES:
                    state_code = FULL_STATE_NAMES[segment_lower]
                    if i > 0:
                        potential_city = segments[i - 1]
                        return f"{potential_city}, {state_code}"

            return location_text

        except Exception as e:
            logging.debug(f"Multi-location parsing failed: {e}")
            return location_text

    @classmethod
    def _standardize_location_format(cls, location):
        if not location or location in ["Unknown", "Remote", "Hybrid"]:
            return location

        try:
            from aggregator.config import validate_us_state_code, FULL_STATE_NAMES

            text = location.strip()

            text = re.sub(r"^locations?\s*", "", text, flags=re.I)
            text = re.sub(r"^location\s+", "", text, flags=re.I)

            text = re.sub(r"\.{2,}", ".", text)
            text = re.sub(r"\s*\.\s*", ", ", text)

            text = re.sub(r"\s*-\s*", ", ", text)

            match = re.search(r"^([A-Z]{2})\s*[,-]\s*(.+)$", text)
            if match:
                state, city = match.groups()
                if validate_us_state_code(state):
                    text = f"{city.strip()}, {state}"

            match = re.search(r"(.+?)\s*,\s*([A-Z]{2})(?:\s|$)", text)
            if match:
                city, state = match.groups()
                if validate_us_state_code(state):
                    return f"{city.strip()}, {state.upper()}"

            for full_name, code in FULL_STATE_NAMES.items():
                pattern = rf"\b{full_name}\b"
                if re.search(pattern, text, re.I):
                    text = re.sub(pattern, code, text, flags=re.I)
                    break

            text = re.sub(r"\s+", " ", text).strip()

            return text

        except Exception as e:
            logging.debug(f"Location standardization failed: {e}")
            return location
