#!/usr/bin/env python3

import re
from config import (
    COMPANY_SLUG_MAPPING,
    COMPANY_NAME_PREFIXES,
    COMPANY_NAME_STOPWORDS,
    COMPANY_PLACEHOLDERS,
    PLATFORM_DETECTION_PATTERNS,
    ROLE_CATEGORIES,
)


class PlatformDetector:
    @staticmethod
    def detect(url):
        if not url:
            return "generic"
        url_lower = url.lower()
        for platform, pattern in PLATFORM_DETECTION_PATTERNS.items():
            if re.search(pattern, url_lower):
                return platform
        return "generic"


class CompanyNormalizer:
    @staticmethod
    def normalize(company_name, url=""):
        if not company_name or not company_name.strip():
            return None

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

        if len(name_lower) > 8 and " " not in name_lower:
            split_name = CompanyNormalizer._split_compound_word(name_lower)
            if split_name:
                check_key = split_name.lower().replace(" ", "")
                if check_key in COMPANY_SLUG_MAPPING:
                    return COMPANY_SLUG_MAPPING[check_key]
                return split_name

        name = re.sub(r"^[A-Z]{2,4}-", "", name)
        name = re.sub(r"^[A-Z]{2,4}\s+", "", name)
        name = re.sub(
            r",?\s+(Inc\.?|LLC\.?|Corp\.?|Ltd\.?|Corporation|Corp\s+Svcs\.?)$",
            "",
            name,
            flags=re.I,
        )
        name = name.strip()

        if name.isupper() or name.islower():
            name = CompanyNormalizer._apply_smart_capitalization(name)

        if not name or len(name) < 2:
            return None

        if name in COMPANY_PLACEHOLDERS or name.lower() in [
            p.lower() for p in COMPANY_PLACEHOLDERS
        ]:
            return None

        return name

    @staticmethod
    def _split_compound_word(word):
        compound_patterns = {
            "motorolasolutions": "Motorola Solutions",
            "goldmansachs": "Goldman Sachs",
            "morganstanley": "Morgan Stanley",
            "bankofamerica": "Bank of America",
            "wellsfargo": "Wells Fargo",
            "americanexpress": "American Express",
            "capitalone": "Capital One",
            "johndeere": "John Deere",
        }

        if word in compound_patterns:
            return compound_patterns[word]

        common_second_words = [
            "solutions",
            "technologies",
            "systems",
            "software",
            "services",
        ]
        for second_word in common_second_words:
            if word.endswith(second_word):
                first_part = word[: -len(second_word)]
                if len(first_part) >= 3:
                    return f"{first_part.title()} {second_word.title()}"

        return None

    @staticmethod
    def _apply_smart_capitalization(name):
        name_lower = name.lower()

        acronyms = ["ibm", "att", "sap", "hpe", "aws", "gcp", "api", "ai", "ml"]
        if name_lower in acronyms:
            return name.upper()

        special_cases = {
            "openai": "OpenAI",
            "youtube": "YouTube",
            "linkedin": "LinkedIn",
            "paypal": "PayPal",
            "ebay": "eBay",
            "tiktok": "TikTok",
        }
        if name_lower in special_cases:
            return special_cases[name_lower]

        words = name_lower.split()
        result = []
        for word in words:
            if word in ["ai", "ml", "api", "aws", "gcp", "iot"]:
                result.append(word.upper())
            else:
                result.append(word.title())

        return " ".join(result)

    @staticmethod
    def extract_from_url_path(url, platform):
        if platform == "greenhouse":
            match = re.search(r"greenhouse\.io/([^/]+)/jobs", url, re.I)
            if match:
                slug = match.group(1)
                return CompanyNormalizer.normalize(slug, url)

        elif platform == "ashby":
            match = re.search(r"ashbyhq\.com/([^/]+)/", url, re.I)
            if match:
                slug = match.group(1)
                return CompanyNormalizer.normalize(slug, url)

        elif platform == "lever":
            match = re.search(r"lever\.co/([^/]+)/", url, re.I)
            if match:
                slug = match.group(1)
                return CompanyNormalizer.normalize(slug, url)

        elif platform == "workable":
            match = re.search(r"workable\.com/([^/]+)/", url, re.I)
            if match:
                slug = match.group(1)
                return CompanyNormalizer.normalize(slug, url)

        return None


class RoleCategorizer:
    @staticmethod
    def categorize(title):
        if not title:
            return "Unknown", "ACCEPT", ""

        title_lower = title.lower()

        for category_name, config in ROLE_CATEGORIES.items():
            keyword_match = any(kw in title_lower for kw in config["keywords"])
            exclude_match = any(ex in title_lower for ex in config["exclude"])

            if keyword_match and not exclude_match:
                return category_name, config["action"], config["alert"]

        generic_sw_keywords = ["engineer", "developer", "programmer", "software"]
        if any(kw in title_lower for kw in generic_sw_keywords):
            return "Pure Software", "ACCEPT", "✅ SOFTWARE"

        return "Unknown", "ACCEPT", ""

    @staticmethod
    def get_terminal_alert(title):
        category, action, alert = RoleCategorizer.categorize(title)
        if alert:
            return f"[{alert}]"
        return ""


class CompanyValidator:
    @staticmethod
    def is_valid(name):
        if not name or not name.strip():
            return False

        if name in COMPANY_PLACEHOLDERS:
            return False

        if name.lower() in [p.lower() for p in COMPANY_PLACEHOLDERS]:
            return False

        invalid_keywords = [
            "careers",
            "jobs",
            "external",
            "applicant",
            "portal",
            "apply",
            "join",
        ]
        if any(kw == name.lower() for kw in invalid_keywords):
            return False

        if re.match(r"^[A-Z]{2,4}-", name):
            return False

        if re.match(r"^[A-Z]{2,4}\s+", name):
            return False

        if len(name) > 60 or len(name) < 2:
            return False

        if name.isupper() and len(name) < 10 and not any(c.isdigit() for c in name):
            return False

        title_indicators = ["intern", "engineer", "developer", "software", "position"]
        keyword_count = sum(1 for kw in title_indicators if kw in name.lower())
        if keyword_count >= 3:
            return False

        return True
