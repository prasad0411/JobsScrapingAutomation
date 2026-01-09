#!/usr/bin/env python3
import re
import datetime

try:
    import us as us_library

    US_LIBRARY_AVAILABLE = True
except ImportError:
    US_LIBRARY_AVAILABLE = False

try:
    import pgeocode

    PGEOCODE_AVAILABLE = True
    _pgeocode_nomi = pgeocode.Nominatim("us")
except ImportError:
    PGEOCODE_AVAILABLE = False

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
    @staticmethod
    def clean_title_aggressive(title):
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
                multi_season_patterns = [
                    r"spring.*summer",
                    r"summer.*spring",
                    r"spring/summer",
                    r"summer/spring",
                ]
                for multi_pattern in multi_season_patterns:
                    if re.search(multi_pattern, combined_text, re.I):
                        return True, ""
                if "winter" in season.lower():
                    if re.search(r"(winter.*2025/2026|2025/2026)", combined_text, re.I):
                        return True, ""
                if "fall" in season.lower():
                    return False, f"Wrong season: {season_name}"
                elif "winter" in season.lower():
                    return False, f"Wrong season: {season_name}"
                elif "spring" in season.lower():
                    if not re.search(r"summer", combined_text, re.I):
                        return False, f"Wrong season: Spring only"
        year_match = re.search(r"(fall|spring|winter)\s*202[0-5]", combined_text, re.I)
        if year_match:
            if not re.search(r"(winter.*2025/2026|2025/2026)", combined_text, re.I):
                return False, f"Wrong season: {year_match.group(0).title()}"
        return True, ""


class LocationProcessor:
    @staticmethod
    def extract_location_enhanced(soup, url, title=""):
        url_location = LocationProcessor._extract_from_url(url)
        if url_location and url_location != "Unknown":
            return url_location
        if "greenhouse" in url.lower():
            greenhouse_loc = LocationProcessor._extract_from_greenhouse(soup)
            if greenhouse_loc and greenhouse_loc != "Unknown":
                return greenhouse_loc
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
                    cleaned = LocationProcessor._clean_location_text(location)
                    if cleaned and cleaned != "Unknown":
                        return cleaned
        location_pattern = r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b"
        matches = re.findall(location_pattern, page_text[:2000])
        if matches:
            city, state = matches[0]
            if state in US_STATES.values():
                return f"{city}, {state}"
        if "Remote" in page_text[:1500] or "remote" in page_text[:1500].lower():
            return "Remote"
        if title:
            title_location = LocationProcessor._extract_location_from_title(title)
            if title_location and title_location != "Unknown":
                return title_location
        extracted = LocationProcessor._aggressive_page_scan(soup)
        if extracted and extracted != "Unknown":
            return extracted
        return "Unknown"

    @staticmethod
    def _aggressive_page_scan(soup):
        if not soup:
            return None
        page_text = soup.get_text()[:3000]
        location_patterns = [
            (
                r"Location:?\s*On-site in ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z][a-z]+)",
                "city_state",
            ),
            (r"Location:?\s*([A-Z][a-z]+),\s*([A-Z]{2})\b", "city_code"),
            (r"On-site in ([A-Z][a-z]+),\s*(Ontario|Quebec|BC|Alberta)", "canada"),
            (
                r"\b([A-Z][a-z]+),\s*(California|Texas|New York|Washington)",
                "city_full_state",
            ),
        ]
        for pattern, pattern_type in location_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                if pattern_type == "canada":
                    return "Unknown (Canada detected)"
                elif pattern_type == "city_code":
                    city, state = match.groups()
                    if state in US_STATES.values():
                        return f"{city}, {state}"
                elif pattern_type == "city_full_state":
                    city, state_name = match.groups()
                    state_code = {
                        "california": "CA",
                        "texas": "TX",
                        "new york": "NY",
                        "washington": "WA",
                    }.get(state_name.lower())
                    if state_code:
                        return f"{city}, {state_code}"
        return None

    @staticmethod
    def _extract_location_from_title(title):
        if not title:
            return None
        parts = re.split(r"[-–—/|]", title)
        for part in parts:
            part_clean = part.strip()
            part_lower = part_clean.lower()
            skip_keywords = [
                "intern",
                "co-op",
                "summer",
                "winter",
                "fall",
                "spring",
                "software",
                "engineer",
                "developer",
                "analyst",
                "shift",
                "1st",
                "2nd",
                "3rd",
                "may",
                "june",
                "july",
                "august",
                "bs",
                "ms",
                "phd",
                "bachelor",
                "master",
                "2025",
                "2026",
            ]
            if any(kw in part_lower for kw in skip_keywords):
                continue
            if part_lower in CITY_TO_STATE:
                state = CITY_TO_STATE[part_lower]
                return f"{part_clean.title()} - {state}"
            if len(part_clean.split()) >= 2:
                multi_word_lower = " ".join(part_clean.lower().split())
                if multi_word_lower in CITY_TO_STATE:
                    state = CITY_TO_STATE[multi_word_lower]
                    return f"{part_clean.title()} - {state}"
        return None

    @staticmethod
    def _clean_location_text(location_text):
        if not location_text:
            return "Unknown"
        original = location_text.strip()
        if "|||" in location_text:
            parts = location_text.split("|||")
            for part in parts:
                part = part.strip()
                if re.match(r"^[A-Z][a-z\s]+(?:,\s*[A-Z]{2})?$", part):
                    return part
            if parts:
                location_text = parts[0].strip()
        stop_keywords = [
            "Employment",
            "Type",
            "Details",
            "Program",
            "Internship",
            "Job",
            "Position",
        ]
        for keyword in stop_keywords:
            if keyword in location_text:
                location_text = location_text.split(keyword)[0].strip()
                break
        match = re.search(
            r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s*([A-Z]{2})?", location_text
        )
        if match:
            city = match.group(1).strip()
            state = match.group(2)
            if state and state in US_STATES.values():
                return f"{city} - {state}"
            elif city:
                city_lower = city.lower()
                if city_lower in CITY_TO_STATE:
                    return f"{city} - {CITY_TO_STATE[city_lower]}"
                return city
        if 3 <= len(location_text) <= 100:
            return location_text
        return original

    @staticmethod
    def _extract_from_greenhouse(soup):
        if not soup:
            return None
        page_text = soup.get_text()
        match = re.search(
            r"📍\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),?\s*([A-Z]{2})\b", page_text[:2000]
        )
        if match:
            return f"{match.group(1)}, {match.group(2)}"
        match = re.search(
            r"Location:?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b",
            page_text[:3000],
        )
        if match:
            return f"{match.group(1)}, {match.group(2)}"
        location_div = soup.find("div", class_=re.compile("location", re.I))
        if location_div:
            text = location_div.get_text(separator="|||").strip()
            parts = [p.strip() for p in text.split("|||")]
            for part in parts:
                match = re.search(
                    r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s*([A-Z]{2})\b", part
                )
                if match:
                    return f"{match.group(1)}, {match.group(2)}"
            if parts and len(parts[0]) < 50:
                first_part = parts[0]
                for keyword in ["Employment", "Type", "Details", "Program"]:
                    if keyword in first_part:
                        first_part = first_part.split(keyword)[0].strip()
                        break
                return first_part if len(first_part) > 2 else None
        meta = soup.find("meta", {"property": "og:street-address"})
        if meta and meta.get("content"):
            return meta.get("content")
        match = re.search(r"\b([A-Z][a-z]+),\s*([A-Z]{2})\b", page_text[:3000])
        if match:
            city, state = match.groups()
            us_states_list = [
                "CA",
                "NY",
                "WA",
                "TX",
                "IL",
                "MA",
                "AZ",
                "NC",
                "OH",
                "FL",
                "PA",
                "GA",
            ]
            if state in us_states_list:
                return f"{city}, {state}"
        return None

    @staticmethod
    def _extract_from_url(url):
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
        if not location or location == "Unknown":
            return "Unknown"
        if location in ["Remote", "Hybrid"]:
            return location

        WORKDAY_HQ_CODES = {
            "TX-HDQ": ("Dallas", "TX"),
            "CA-HDQ": ("San Francisco", "CA"),
            "NY-HDQ": ("New York", "NY"),
            "IL-HDQ": ("Chicago", "IL"),
        }

        if location in WORKDAY_HQ_CODES:
            city, state = WORKDAY_HQ_CODES[location]
            return f"{city} - {state}"

        original = location.strip()
        company_prefixes = ["Corporate", "Headquarters", "Office", "Campus", "ascena"]
        for prefix in company_prefixes:
            location = re.sub(f"^{prefix}\\s+", "", location, flags=re.I)
            location = re.sub(f"{prefix}\\s*[-–—]\\s*", "", location, flags=re.I)
        location = location.strip()

        location = re.sub(
            r",?\s*\d{3,5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?", "", location
        )
        location = re.sub(
            r",?\s*\d{1,5}\s+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd)",
            "",
            location,
            flags=re.I,
        )
        location = location.strip(", ")

        match = re.search(
            r"^([A-Z]{2})\s*-\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", location
        )
        if match:
            state = match.group(1).upper()
            city = match.group(2).strip()
            if state in US_STATES.values():
                return f"{city} - {state}"

        match = re.search(
            r"^US\s+([A-Z]{2})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", location
        )
        if match:
            state = match.group(1).upper()
            city = match.group(2).strip()
            if state in US_STATES.values():
                return f"{city} - {state}"

        location = re.sub(r"^[A-Z]{2}[A-Z]{2}\d{2,4}:?\s*", "", location)
        location = re.sub(
            r"\s+(Green\s+St|Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd)\b",
            "",
            location,
            flags=re.I,
        )
        if location.strip() in ["Headquarters", "Office", "Campus", "Building"]:
            return "Unknown"
        location = re.sub(
            r"(Team|Department|Division|Group|Office|Building|Campus).*$",
            "",
            location,
            flags=re.I,
        )
        location = location.strip()
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
        match = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b", location)
        if match:
            city = match.group(1).strip()
            state = match.group(2).upper()
            if state in US_STATES.values():
                return f"{city} - {state}"
        location_clean = re.sub(r"\s+", " ", location).strip()
        location_lower = location_clean.lower()
        if location_lower in CITY_TO_STATE:
            return f"{location_clean.title()} - {CITY_TO_STATE[location_lower]}"
        known_cities = {
            "san jose": "CA",
            "san francisco": "CA",
            "seattle": "WA",
            "newark": "CA",
            "milpitas": "CA",
            "chicago": "IL",
            "scottsdale": "AZ",
        }
        if location_lower in known_cities:
            return f"{location_clean.title()} - {known_cities[location_lower]}"
        if PGEOCODE_AVAILABLE and 2 < len(location_clean) < 50:
            try:
                result = _pgeocode_nomi.query_location(location_clean, top_k=1)
                if not result.empty and result.iloc[0]["state_code"]:
                    state_code = result.iloc[0]["state_code"]
                    if state_code in US_STATES.values():
                        return f"{location_clean.title()} - {state_code}"
            except:
                pass
        return location_clean

    @staticmethod
    def check_if_international(location, soup=None):
        if not location or location == "Unknown":
            if soup:
                page_result = LocationProcessor._check_page_for_canada(soup)
                if page_result:
                    return page_result
            return None

        location_to_check = location.lower()

        full_provinces = {
            "ontario": "ON",
            "quebec": "QC",
            "british columbia": "BC",
            "alberta": "AB",
        }
        for full_name, code in full_provinces.items():
            if re.search(rf"\b{full_name}\b", location, re.I):
                if full_name == "ontario" and ", ca" in location_to_check:
                    continue
                return f"Location: Canada ({full_name.title()})"

        canada_province_codes = [
            "ON",
            "QC",
            "BC",
            "AB",
            "MB",
            "SK",
            "NS",
            "NB",
            "PE",
            "NL",
            "NT",
            "YT",
            "NU",
        ]
        for province in canada_province_codes:
            patterns = [
                rf",\s*{province}\b",
                rf"\s+-\s+{province}\b",
                rf",\s*{province},",
            ]
            for pattern in patterns:
                if re.search(pattern, location):
                    if province == "ON":
                        canada_cities = [
                            "toronto",
                            "ottawa",
                            "london",
                            "kingston",
                            "hamilton",
                            "waterloo",
                        ]
                        if any(city in location_to_check for city in canada_cities):
                            return f"Location: Canada (province: {province})"
                        if ", ca" in location_to_check:
                            continue
                    return f"Location: Canada (province: {province})"

        if ", CA" in location or " - CA" in location:
            canada_indicators = [
                "ontario",
                "quebec",
                "toronto",
                "ottawa",
                "montreal",
                "canada",
            ]
            if any(indicator in location_to_check for indicator in canada_indicators):
                return "Location: Canada (CA in Canadian context)"

        try:
            from unidecode import unidecode

            normalized = unidecode(location_to_check)
        except ImportError:
            normalized = (
                location_to_check.replace("é", "e").replace("è", "e").replace("à", "a")
            )

        canadian_cities = {
            "montreal": "Canada (Montréal)",
            "toronto": "Canada (Toronto)",
            "ottawa": "Canada (Ottawa)",
            "vancouver": "Canada (Vancouver)",
            "calgary": "Canada (Calgary)",
            "mississauga": "Canada (Mississauga)",
            "edmonton": "Canada (Edmonton)",
            "quebec": "Canada (Québec)",
        }
        for city, label in canadian_cities.items():
            if city in normalized:
                return f"Location: {label}"

        if "canada" in location_to_check:
            return "Location: Canada"

        if any(kw in location_to_check for kw in ["uk", "london", "india", "china"]):
            return f"Location: International"

        if soup:
            page_result = LocationProcessor._check_page_for_canada(soup)
            if page_result:
                return page_result

        return None

    @staticmethod
    def _check_page_for_canada(soup):
        if not soup:
            return None
        page_text = soup.get_text()[:5000]
        page_lower = page_text.lower()

        meta_desc = soup.find("meta", {"property": "og:description"})
        if meta_desc:
            content = meta_desc.get("content", "").lower()
            if content == "canada" or "canada only" in content:
                return "Location: Canada (from meta tag)"

        canadian_location_patterns = [
            (r"Ottawa,?\s+Ontario", "Ottawa, Ontario"),
            (r"Toronto,?\s+Ontario", "Toronto, Ontario"),
            (r"Montreal,?\s+Quebec", "Montreal, Quebec"),
            (r"Vancouver,?\s+(?:BC|British Columbia)", "Vancouver, BC"),
            (r"Calgary,?\s+Alberta", "Calgary, Alberta"),
        ]
        for pattern, city_name in canadian_location_patterns:
            if re.search(pattern, page_text, re.I):
                return f"Location: Canada ({city_name})"

        if re.search(r"Canada\s*\(On-site\)", page_text, re.I):
            return "Location: Canada (from page)"

        work_auth_patterns = [
            r"eligible\s+to\s+work\s+in,?\s+and\s+located\s+in,?\s+canada",
            r"legally\s+eligible.*(?:work\s+in|to\s+work).*canada",
            r"canada\s+work\s+permit",
            r"canadian\s+(?:citizen|permanent\s+resident)",
            r"must\s+be\s+(?:located\s+in|based\s+in)\s+canada",
        ]
        for pattern in work_auth_patterns:
            if re.search(pattern, page_lower, re.I):
                return "Location: Canada (work authorization)"

        if re.search(r"remote.*in\s+canada", page_lower, re.I):
            return "Location: Canada (remote)"

        return None

    @staticmethod
    def _aggressive_country_scan(soup):
        if not soup:
            return None
        page_text = soup.get_text()[:3000]
        if re.search(r"\bCanada\b", page_text, re.I):
            return "Canada"
        if re.search(r"\bU\.?K\.?\b", page_text, re.I):
            return "UK"
        return None


class ValidationHelper:
    @staticmethod
    def is_valid_job_url(url):
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
        if not url:
            return None
        url_lower = url.lower()
        canada_patterns = [
            "/montral-quebec-can/",
            "/toronto-ontario/",
            "/ottawa-ontario/",
            "-quebec-can",
            "-ontario-can",
            "/can/",
            "canada/",
            ".ca/",
            ".ca2.",
            "/ottawa",
            "/toronto",
            "/montreal",
            "/vancouver",
            "/calgary",
        ]
        for pattern in canada_patterns:
            if pattern in url_lower:
                return f"International: Canada (from URL)"
        return None

    @staticmethod
    def check_page_restrictions(soup):
        if not soup:
            return None, None, []
        page_text = soup.get_text()
        page_lower = page_text.lower()
        review_flags = []

        citizenship_patterns = [
            r"u\.?s\.?\s+citizenship\s+is\s+required",
            r"u\.?s\.?\s+citizen(?:ship)?\s+required",
            r"must be a u\.?s\.?\s+citizen",
            r"only u\.?s\.?\s+citizens\s+(?:are\s+)?eligible",
        ]
        for pattern in citizenship_patterns:
            if re.search(pattern, page_lower, re.I):
                return "REJECT", "US citizenship required", []

        work_auth_patterns = [
            r"us work authorization required",
            r"must have us work authorization",
            r"requires us work authorization",
            r"valid us work authorization",
            r"authorized to work in the us",
        ]
        for pattern in work_auth_patterns:
            if re.search(pattern, page_lower, re.I):
                return "REJECT", "US work authorization required", []

        ssn_patterns = [
            r"(?:valid|possess|have).*social security number.*(?:to complete|required)",
            r"must have.*(?:valid\s+)?(?:u\.?s\.?\s+)?social security number",
            r"social security number.*(?:is\s+)?required",
            r"ssn.*(?:required|to complete)",
            r"valid.*ssn.*(?:for|to)",
        ]
        for pattern in ssn_patterns:
            match = re.search(pattern, page_lower, re.I)
            if match:
                context_start = max(0, match.start() - 100)
                context_end = min(len(page_lower), match.end() + 100)
                context = page_lower[context_start:context_end]
                if "preferred" in context or "optional" in context:
                    continue
                return "REJECT", "SSN required", []

        university_pattern = r"(?:from|attending|enrolled\s+at)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+University"
        matches = re.finditer(university_pattern, page_text, re.I)
        for match in matches:
            university_name = match.group(1)
            generic_words = [
                "an",
                "a",
                "any",
                "accredited",
                "top",
                "leading",
                "participating",
            ]
            if university_name.lower() in generic_words:
                continue
            specific_schools = [
                "Drexel",
                "MIT",
                "Stanford",
                "Cornell",
                "Carnegie Mellon",
            ]
            if university_name in specific_schools:
                return "REJECT", f"{university_name} University students only", []
            review_flags.append(f"⚠️ Check if {university_name} University only")

        if re.search(r"cooperative learning track", page_text, re.I):
            if re.search(r"Drexel", page_text, re.I):
                return "REJECT", "Drexel cooperative program only", []

        clearance_requirement_patterns = [
            r"must be able to obtain.*clearance",
            r"must be able to.*maintain.*clearance",
            r"shall.*obtain.*clearance",
            r"ability to obtain.*clearance.*required",
            r"obtain and maintain.*clearance",
            r"applicable security clearance",
            r"government clearance",
            r"clearance.*required",
        ]
        for pattern in clearance_requirement_patterns:
            if re.search(pattern, page_lower, re.I):
                return "REJECT", "Security clearance required", []

        clearance_mentions = [
            m.start() for m in re.finditer(r"\bclearance\b", page_lower, re.I)
        ]
        for mention_pos in clearance_mentions:
            context = page_lower[
                max(0, mention_pos - 250) : min(len(page_lower), mention_pos + 250)
            ]
            requirement_keywords = ["required", "must have", "must obtain", "necessary"]
            if any(kw in context for kw in requirement_keywords):
                if "preferred" not in context:
                    return "REJECT", "Security clearance required", []

        explicit_rejection_patterns = [
            r"master'?s?\s+students?\s+not\s+eligible",
            r"master'?s?\s+not\s+accepted",
            r"graduate\s+students?\s+not\s+eligible",
            r"not\s+open\s+to.*master'?s?",
            r"undergrad(?:uate)?\s+only",
            r"undergraduate\s+students?\s+only",
            r"bachelor'?s?\s+candidates?\s+only",
            r"4-year\s+degree\s+only",
        ]
        for pattern in explicit_rejection_patterns:
            if re.search(pattern, page_lower, re.I):
                return "REJECT", "Master's students not eligible", []

        explicit_acceptance_patterns = [
            r"bachelor'?s?\s+or\s+master'?s?",
            r"bachelor\s+or\s+master\s+(?:student|candidate)",
            r"undergraduate\s+or\s+graduate",
            r"bachelor'?s?\s+(?:or|and)\s+(?:above|higher)",
            r"all degree levels?",
            r"next program.*\(ms\)",
            r"proof of enrollment.*into next program.*\(ms\)",
            r"into next program.*master",
            r"proof of enrollment.*\(ms\)",
            r"junior,?\s+senior,?\s+(?:or\s+)?graduate",
            r"sophomore,?\s+junior,?\s+senior,?\s+(?:and\s+)?graduate",
        ]
        for pattern in explicit_acceptance_patterns:
            if re.search(pattern, page_lower, re.I):
                return None, None, []

        implicit_bs_patterns = [
            r"pursuing\s+a\s+BS\s+in",
            r"currently\s+pursuing.*bachelor",
            r"enrolled\s+in.*bachelor",
            r"current.*BS\s+student",
            r"bachelor\s+of\s+science\s+in",
            r"pursuing.*bachelor'?s?\s+degree",
            r"working\s+towards.*bachelor",
        ]
        bs_confidence_scores = []
        for pattern in implicit_bs_patterns:
            match = re.search(pattern, page_lower, re.I)
            if match:
                pos = match.start()
                context = page_lower[
                    max(0, pos - 200) : min(len(page_lower), pos + 200)
                ]
                master_keywords = [
                    r"master",
                    r"ms",
                    r"graduate student",
                    r"or ms",
                    r"or master",
                    r"m\.s\.",
                ]
                if not any(re.search(kw, context, re.I) for kw in master_keywords):
                    bs_confidence_scores.append(0.8)
                else:
                    bs_confidence_scores.append(0.0)
        if bs_confidence_scores:
            max_bs_confidence = max(bs_confidence_scores)
            if max_bs_confidence >= 0.8:
                review_flags.append("⚠️ Possible BS only")

        undergrad_year_patterns = [
            r"sophomore.*year",
            r"junior.*year",
            r"senior.*standing",
            r"sophomore/junior",
            r"rising\s+(?:sophomore|junior|senior)",
            r"1st\s+year|2nd\s+year|3rd\s+year|4th\s+year",
        ]
        year_found = False
        for pattern in undergrad_year_patterns:
            if re.search(pattern, page_lower, re.I):
                year_found = True
                break
        if year_found:
            if r"master" not in page_lower and r"graduate student" not in page_lower:
                review_flags.append("⚠️ Mentions undergrad year")

        range_match = re.search(
            r"(?:graduating|graduate)\s+(?:between\s+)?([A-Za-z]+)\s+(\d{4})\s+(?:thru|through|to|and)\s+([A-Za-z]+)?\s*(\d{4})",
            page_lower,
            re.I,
        )
        if range_match:
            start_month = range_match.group(1)
            start_year = int(range_match.group(2))
            if start_year < 2027:
                context_pos = range_match.start()
                context = page_lower[
                    max(0, context_pos - 100) : min(len(page_lower), context_pos + 200)
                ]
                if "preferred" in context or "ideal" in context:
                    review_flags.append(
                        f"⚠️ Grad {start_month.title()} {start_year} preferred"
                    )
                else:
                    return (
                        "REJECT",
                        f"Graduation {start_month.title()} {start_year} (before May 2027)",
                        [],
                    )
            if start_year == 2027:
                before_may = [
                    "december",
                    "january",
                    "february",
                    "march",
                    "april",
                    "dec",
                    "jan",
                    "feb",
                    "mar",
                    "apr",
                ]
                if start_month and start_month.lower() in before_may:
                    return (
                        "REJECT",
                        f"Graduation {start_month.title()} 2027 (before May)",
                        [],
                    )

        slash_match = re.search(
            r"graduation.*([A-Za-z]+)/([A-Za-z]+)\s+(\d{4})", page_lower, re.I
        )
        if slash_match:
            first_month = slash_match.group(1)
            year = int(slash_match.group(3))
            if year < 2027:
                return (
                    "REJECT",
                    f"Graduation {first_month.title()}/{slash_match.group(2).title()} {year} (before May 2027)",
                    [],
                )

        class_match = re.search(r"class\s+of\s+(\d{4})", page_lower, re.I)
        if class_match:
            year = int(class_match.group(1))
            if year < 2027:
                return "REJECT", f"Graduation Class of {year} (before 2027)", []

        grad_patterns = [
            r"graduation\s+date:?\s*([A-Za-z]+)?\s*(\d{4})",
            r"expected\s+graduation[:\s]+([A-Za-z]+)?\s*(\d{4})",
            r"graduating\s+([A-Za-z]+)?\s*(\d{4})",
        ]
        for pattern in grad_patterns:
            match = re.search(pattern, page_lower, re.I)
            if match:
                month = (
                    match.group(1)
                    if match.group(1) and not match.group(1).isdigit()
                    else None
                )
                year_str = match.group(2) if match.group(2) else match.group(1)
                if year_str and year_str.isdigit():
                    year = int(year_str)
                    if year < 2027:
                        month_str = month.title() if month else ""
                        return (
                            "REJECT",
                            f"Graduation {month_str} {year} (before May 2027)",
                            [],
                        )
                    if year == 2027 and month:
                        before_may = [
                            "december",
                            "january",
                            "february",
                            "march",
                            "april",
                        ]
                        if month.lower() in before_may:
                            return (
                                "REJECT",
                                f"Graduation {month.title()} 2027 (before May)",
                                [],
                            )

        if re.search(
            r"graduating within (\d+)(?:-(\d+))? semesters?", page_lower, re.I
        ):
            return "REJECT", "Graduation within 1-2 semesters (before May 2027)", []

        start_match = re.search(
            r"available.*start.*(?:full-time|employment).*([A-Za-z]+)\s+(\d{4})",
            page_lower,
            re.I,
        )
        if start_match:
            month = start_match.group(1)
            year = int(start_match.group(2))
            if year < 2027 or (
                year == 2027
                and month.lower() in ["january", "february", "march", "april"]
            ):
                return (
                    "REJECT",
                    f"Available to start {month.title()} {year} (graduates before May 2027)",
                    [],
                )

        phd_definite_patterns = [
            r"phd.*required",
            r"phd\s+student\s+only",
            r"doctoral\s+degree\s+required",
            r"phd\s+candidate\s+required",
        ]
        for pattern in phd_definite_patterns:
            if re.search(pattern, page_lower, re.I):
                return "REJECT", "PhD requirement", []

        phd_indicator_patterns = [
            r"pursuing\s+(?:a\s+)?phd",
            r"phd\s+student",
            r"current\s+student\s+in\s+a\s+phd",
            r"phd\s+in\s+(?:ML|AI|CS|machine|artificial)",
            r"enrolled\s+in.*phd\s+program",
            r"doctoral\s+(?:student|candidate)",
            r"currently.*phd",
            r"pursuing\s+doctoral",
            r"phd\s+or\s+advanced",
        ]
        phd_confidence_scores = []
        for pattern in phd_indicator_patterns:
            match = re.search(pattern, page_lower, re.I)
            if match:
                pos = match.start()
                context = page_lower[
                    max(0, pos - 200) : min(len(page_lower), pos + 200)
                ]
                master_keywords = [
                    r"or master",
                    r"or ms",
                    r"master'?s?\s+student",
                    r"graduate student",
                    r"m\.s\.",
                ]
                if not any(re.search(kw, context, re.I) for kw in master_keywords):
                    phd_confidence_scores.append(0.9)
                else:
                    phd_confidence_scores.append(0.0)
        if phd_confidence_scores:
            max_phd_confidence = max(phd_confidence_scores)
            if max_phd_confidence >= 0.9:
                review_flags.append("⚠️ Possible PhD requirement")

        if review_flags:
            return None, None, review_flags
        return None, None, []

    @staticmethod
    def validate_company_field(company, title, url):
        if not company or company == "Unknown":
            company_from_url = ValidationHelper.extract_company_from_domain(url)
            return True, company_from_url, None
        company = company.strip()
        if len(company) > 100:
            return False, company, "Company name too long"
        generic_ui_text = [
            "Cookie Consent",
            "Privacy Policy",
            "Terms of Service",
            "Sign In",
            "Log In",
            "Apply Now",
            "Get Started",
            "Accept Cookies",
            "Manage Cookies",
            "Privacy Settings",
        ]
        for generic in generic_ui_text:
            if generic.lower() in company.lower():
                company_from_url = ValidationHelper.extract_company_from_domain(url)
                return True, company_from_url, None
        job_keywords = ["intern", "software", "engineer", "developer"]
        keyword_count = sum(1 for kw in job_keywords if kw in company.lower())
        if keyword_count >= 2:
            return False, company, "Company field contains job title"
        return True, company, None

    @staticmethod
    def extract_company_from_domain(url):
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
            return company.replace("-", " ").replace("_", " ").title()
        except:
            return "Unknown"

    @staticmethod
    def check_sponsorship_status(soup):
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

    @staticmethod
    def check_posted_date(posted_date_str, page_text=None, max_days=5):
        if not posted_date_str or posted_date_str == "Unknown":
            if page_text:
                posted_date_str = page_text
            else:
                return None
        text_to_check = posted_date_str.lower()
        day_match = re.search(r"(\d+)\s*d(?:ay)?s?\s+ago", text_to_check, re.I)
        if day_match:
            days = int(day_match.group(1))
            if days > max_days:
                return f"Posted {days}d ago (>{max_days} days)"
            return None
        week_patterns = [(r"(\d+)\s*w(?:ee)?k", 7), (r"(\d+)\s*weeks?", 7)]
        for pattern, multiplier in week_patterns:
            match = re.search(pattern, text_to_check, re.I)
            if match:
                weeks = int(match.group(1))
                days = weeks * multiplier
                if days > max_days:
                    return f"Posted >{weeks} week(s) ago (>{max_days} days)"
        month_patterns = [(r"(\d+)\s*mo(?:nth)?", 30), (r"(\d+)\s*months?", 30)]
        for pattern, multiplier in month_patterns:
            match = re.search(pattern, text_to_check, re.I)
            if match:
                months = int(match.group(1))
                return f"Posted >{months} month(s) ago (>{max_days} days)"
        return None


class QualityScorer:
    @staticmethod
    def calculate_score(job_data):
        score = 0
        company = job_data.get("company", "Unknown")
        if company and company != "Unknown" and len(company) > 2:
            if company.lower() not in ["unknown", "n/a"]:
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
        return score >= MIN_QUALITY_SCORE
