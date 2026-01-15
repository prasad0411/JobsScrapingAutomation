#!/usr/bin/env python3
import re
import datetime
import json

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
    STATE_NAME_TO_CODE,
    COMPANY_PLACEHOLDERS,
    LOCATION_CODE_PATTERNS,
    WORKDAY_HQ_CODES,
)
from utils import RoleCategorizer, PlatformDetector


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
        platform = (
            PlatformDetector.detect(url) if "PlatformDetector" in dir() else "generic"
        )

        if platform == "linkedin":
            linkedin_loc = LocationProcessor._extract_from_linkedin(soup)
            if linkedin_loc and linkedin_loc != "Unknown":
                return linkedin_loc

        location = LocationProcessor._extract_from_visible_elements(soup)
        if location and location != "Unknown":
            return location

        json_ld_location = LocationProcessor._extract_from_json_ld(soup)
        if json_ld_location and json_ld_location != "Unknown":
            return json_ld_location

        url_location = LocationProcessor._extract_from_url(url)
        if url_location and url_location != "Unknown":
            return url_location

        if "greenhouse" in url.lower():
            greenhouse_loc = LocationProcessor._extract_from_greenhouse(soup)
            if greenhouse_loc and greenhouse_loc != "Unknown":
                return greenhouse_loc

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
    def _extract_from_linkedin(soup):
        if not soup:
            return None

        page_text = soup.get_text()

        criteria_match = re.search(
            r"Location.*?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?(?:,\s*[A-Z]{2})?)",
            page_text[:2000],
            re.I,
        )
        if criteria_match:
            location = criteria_match.group(1).strip()
            if "," in location:
                parts = location.split(",")
                if len(parts) == 2:
                    city, state = parts[0].strip(), parts[1].strip()
                    if len(state) == 2 and state.upper() in US_STATES.values():
                        return f"{city}, {state.upper()}"
                    elif state.lower() in US_STATES:
                        state_code = US_STATES[state.lower()]
                        return f"{city}, {state_code}"
            city_lower = location.lower().replace(",", "").strip()
            if city_lower in CITY_TO_STATE:
                return f"{location.title()}, {CITY_TO_STATE[city_lower]}"
            return location

        state_match = re.search(
            r"\b(California|New York|Texas|Washington|Massachusetts|Illinois)\b",
            page_text[:1500],
            re.I,
        )
        if state_match:
            state_name = state_match.group(1).lower()
            if state_name in US_STATES:
                return US_STATES[state_name]

        if re.search(r"\bRemote\b", page_text[:1000]):
            return "Remote"

        return None

    @staticmethod
    def _extract_from_visible_elements(soup):
        if not soup:
            return None
        location_dt = soup.find("dt", text=re.compile(r"Location", re.I))
        if location_dt:
            location_dd = location_dt.find_next_sibling("dd")
            if location_dd:
                location = location_dd.get_text().strip()
                match = re.search(r"US-([A-Z]{2})-([A-Z][a-z\s]+)", location)
                if match:
                    state, city = match.groups()
                    return f"{city}, {state}"
                return location

        job_location = soup.find(attrs={"itemprop": "jobLocation"})
        if job_location:
            locality = job_location.find(attrs={"itemprop": "addressLocality"})
            region = job_location.find(attrs={"itemprop": "addressRegion"})
            if locality and region:
                city = locality.get_text().strip()
                state = region.get_text().strip()
                if len(state) > 2:
                    state = STATE_NAME_TO_CODE.get(state.lower(), state[:2].upper())
                return f"{city}, {state}"

        spl_location = soup.find("spl-job-location")
        if spl_location:
            formatted = spl_location.get("formattedaddress", "") or spl_location.get(
                "formattedAddress", ""
            )
            if formatted:
                match = re.search(r"([A-Z][a-z\s]+),\s*([A-Z]{2})", formatted)
                if match:
                    return f"{match.group(1)}, {match.group(2)}"

        job_details = soup.find("ul", class_=re.compile(r"job-details", re.I))
        if job_details:
            for li in job_details.find_all("li"):
                text = li.get_text().strip()
                match = re.search(r"([A-Z][a-z\s]+),\s*([A-Z]{2})\b", text)
                if match:
                    city, state = match.groups()
                    if state in US_STATES.values():
                        return f"{city}, {state}"

        page_text = soup.get_text()[:15000]
        state_name_match = re.search(
            r"([A-Z][a-z\s]+),\s+(Illinois|California|Texas|Pennsylvania|Maryland|Georgia|Washington|Michigan|Ohio|Florida|Massachusetts|Virginia|New York)",
            page_text,
            re.I,
        )
        if state_name_match:
            city = state_name_match.group(1).strip()
            state_name = state_name_match.group(2).lower()
            state_code = STATE_NAME_TO_CODE.get(state_name, state_name[:2].upper())
            return f"{city}, {state_code}"

        return None

    @staticmethod
    def _extract_from_json_ld(soup):
        if not soup:
            return None
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    job_location = data.get("jobLocation", {})
                    if isinstance(job_location, dict):
                        address = job_location.get("address", {})
                        if isinstance(address, dict):
                            city = address.get("addressLocality", "")
                            state = address.get("addressRegion", "")
                            if city and state:
                                if len(state) == 2 and state in US_STATES.values():
                                    return f"{city}, {state}"
                                elif len(state) > 2:
                                    state_code = STATE_NAME_TO_CODE.get(
                                        state.lower(), state[:2].upper()
                                    )
                                    return f"{city}, {state_code}"
                            elif city:
                                return city
            except:
                pass
        return None

    @staticmethod
    def _aggressive_page_scan(soup):
        if not soup:
            return None
        page_text = soup.get_text()[:15000]
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
                    state_code = STATE_NAME_TO_CODE.get(state_name.lower())
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
                "bs",
                "ms",
                "phd",
                "2025",
                "2026",
            ]
            if any(kw in part_lower for kw in skip_keywords):
                continue
            if part_lower in CITY_TO_STATE:
                return f"{part_clean.title()} - {CITY_TO_STATE[part_lower]}"
            if len(part_clean.split()) >= 2:
                multi_word_lower = " ".join(part_clean.lower().split())
                if multi_word_lower in CITY_TO_STATE:
                    return f"{part_clean.title()} - {CITY_TO_STATE[multi_word_lower]}"
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
        if location in WORKDAY_HQ_CODES:
            city, state = WORKDAY_HQ_CODES[location]
            if state != "UNKNOWN":
                return f"{city} - {state}"

        original = location.strip()
        location = re.sub(r"\s*\([^)]*\)\s*", " ", location)
        location = re.sub(r"\s*\([^)]*$", "", location)
        location = location.strip()
        location = re.sub(r"\s*-\s*[A-Z]{2,4}$", "", location)

        company_prefixes = ["Corporate", "Headquarters", "Office", "Campus"]
        for prefix in company_prefixes:
            location = re.sub(f"^{prefix}\\s+", "", location, flags=re.I)

        location = re.sub(
            r",?\s*\d{3,5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?", "", location
        )
        location = re.sub(
            r",?\s*\d{1,5}\s+(?:Street|St|Avenue|Ave|Road|Rd)", "", location, flags=re.I
        )
        location = location.strip(", ")

        match = re.search(r"^([A-Z]{2})\s+(.+)$", location)
        if match:
            state, rest = match.groups()
            if state in US_STATES.values():
                return f"{rest.strip()} - {state}"

        match = re.search(
            r"^([A-Z]{2})\s*-\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", location
        )
        if match:
            state, city = match.group(1).upper(), match.group(2).strip()
            if state in US_STATES.values():
                return f"{city} - {state}"

        location = re.sub(r"^[A-Z]{2}[A-Z]{2}\d{2,4}:?\s*", "", location)
        location = re.sub(r"\s+(Street|Ave|Road|Rd)\b", "", location, flags=re.I)

        if location.strip() in ["Headquarters", "Office", "Campus"]:
            return "Unknown"

        location = re.sub(
            r"(Team|Department|Office|Building).*$", "", location, flags=re.I
        )
        location = location.strip()
        location = re.sub(r"\s*\d{5}(-\d{4})?\s*", " ", location)
        location = re.sub(r",?\s*(?:USA|United States)\s*", "", location, flags=re.I)

        match = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b", location)
        if match:
            city, state = match.group(1).strip(), match.group(2).upper()
            if state in US_STATES.values():
                return f"{city} - {state}"

        location_clean = re.sub(r"\s+", " ", location).strip()
        location_lower = location_clean.lower()
        if location_lower in CITY_TO_STATE:
            return f"{location_clean.title()} - {CITY_TO_STATE[location_lower]}"

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
                            "kingston",
                            "hamilton",
                            "waterloo",
                        ]
                        if any(city in location_to_check for city in canada_cities):
                            return f"Location: Canada (province: {province})"
                        if (
                            ", ca" in location_to_check
                            or "fremont" in location_to_check
                        ):
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
                if "ontario, ca" not in location_to_check:
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

        meta_desc = soup.find("meta", {"property": "og:description"})
        if meta_desc:
            content = meta_desc.get("content", "").lower()
            if content == "canada" or "canada only" in content:
                return "Location: Canada (from meta tag)"
            if re.search(r"ottawa.*ontario", content, re.I):
                return "Location: Canada (Ottawa, Ontario)"

        page_text = soup.get_text()[:10000]
        canadian_location_patterns = [
            (r"Ottawa\s*,?\s*Ontario", "Ottawa, Ontario"),
            (r"Toronto\s*,?\s*Ontario", "Toronto, Ontario"),
            (r"Montreal\s*,?\s*Quebec", "Montreal, Quebec"),
            (r"Vancouver\s*,?\s*(?:BC|British Columbia)", "Vancouver, BC"),
        ]
        for pattern, city_name in canadian_location_patterns:
            if re.search(pattern, page_text, re.I):
                return f"Location: Canada ({city_name})"

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
            "/montreal-quebec-can/",
            "/toronto-ontario/",
            "/ottawa-ontario/",
            "-quebec-can",
            "-ontario-can",
            "/can/",
            "canada/",
            ".ca/",
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

        degree_decision, degree_reason = (
            ValidationHelper._check_degree_requirements_strict(soup)
        )
        if degree_decision == "REJECT":
            return degree_decision, degree_reason, []

        json_ld_desc = ValidationHelper._get_json_ld_description(soup)
        meta_desc = ValidationHelper._get_meta_description(soup)
        combined_meta_text = (json_ld_desc + " " + meta_desc).lower()

        if "social security number" in combined_meta_text:
            if re.search(
                r"(?:must have|required).*social security number",
                combined_meta_text,
                re.I,
            ):
                return "REJECT", "SSN required", []

        page_text = soup.get_text()
        page_lower = page_text.lower()

        citizenship_patterns = [
            r"u\.?s\.?\s+citizenship\s+is\s+required",
            r"u\.?s\.?\s+citizen(?:ship)?\s+required",
            r"must be a u\.?s\.?\s+citizen",
            r"only u\.?s\.?\s+citizens\s+(?:are\s+)?eligible",
            r"us\s+citizens?\s+or\s+green\s+card\s+holders?",
            r"must\s+be\s+us\s+citizen\s+or\s+green\s+card",
            r"citizenship\s+required",
            r"(?:due\s+to\s+)?government\s+contracts?.*(?:citizen|green\s+card)",
        ]
        for pattern in citizenship_patterns:
            if re.search(pattern, page_lower, re.I):
                return "REJECT", "US citizenship required", []

        work_auth_patterns = [
            r"us work authorization required",
            r"must have us work authorization",
            r"requires us work authorization",
        ]
        for pattern in work_auth_patterns:
            if re.search(pattern, page_lower, re.I):
                return "REJECT", "US work authorization required", []

        clearance_requirement_patterns = [
            r"must be able to obtain.*clearance",
            r"clearance.*required",
        ]
        for pattern in clearance_requirement_patterns:
            if re.search(pattern, page_lower, re.I):
                return "REJECT", "Security clearance required", []

        return None, None, []

    @staticmethod
    def _check_degree_requirements_strict(soup):
        if not soup:
            return None, None

        json_ld_desc = ValidationHelper._get_json_ld_description(soup)
        meta_desc = ValidationHelper._get_meta_description(soup)
        page_text = soup.get_text()[:15000]
        qual_section_text = ValidationHelper._get_qualification_section_text(soup)

        sections = ValidationHelper._extract_requirement_sections(soup)
        required_text = sections.get("required", "")

        if not required_text:
            required_text = page_text

        combined = (
            json_ld_desc
            + " "
            + meta_desc
            + " "
            + required_text
            + " "
            + qual_section_text
        ).lower()

        bs_definite_patterns = [
            r"undergraduate\s+(?:junior|senior)\s+(?:and\s+senior\s+)?level\s+students?",
            r"undergraduate\s+(?:junior|senior)\s+(?:or|and)\s+senior\s+level\s+students?",
            r"current\s+(?:junior|senior)\s+pursuing",
            r"(?:junior|senior)\s+year\s+students?",
            r"(?:3rd|4th)\s+year\s+students?",
            r"undergraduate\s+students?\s+majoring\s+in",
            r"undergraduate\s+students?\s+in",
            r"for\s+undergraduate\s+students?",
            r"pursuing\s+an?\s+undergraduate\s+degree\s+in",
            r"enrolled\s+in\s+an?\s+undergraduate\s+degree",
            r"bachelor'?s?\s+degree\s+program\s+majoring\s+in",
            r"bachelor'?s?\s+degree\s+program\s+in",
            r"pursuing\s+(?:a|their)\s+bachelor'?s?\s+degree\s+in",
            r"pursuing\s+a\s+bachelor'?s?\s+degree\s+(?:program\s+)?in",
            r"enrolled\s+in\s+(?:a|an)\s+bachelor'?s?\s+degree\s+(?:program\s+)?in",
            r"candidate\s+for\s+(?:a|an)\s+bachelor'?s?\s+degree",
            r"currently\s+a\s+candidate\s+for\s+a\s+bachelor",
            r"pursuing\s+a\s+ba/bs\s+degree\s+in",
            r"college\s+student\s+pursuing\s+(?:a|an)\s+ba",
            r"must\s+be\s+(?:currently\s+)?enrolled\s+in\s+(?:a|an)\s+bachelor'?s?",
            r"must\s+be\s+(?:currently\s+)?pursuing\s+(?:a|an)\s+bachelor'?s?",
            r"only\s+(?:for\s+)?bachelor'?s?\s+(?:degree\s+)?students?",
        ]

        inclusive_modifiers = [
            r"bachelor'?s?\s+(?:degree\s+)?(?:program\s+)?or\s+higher",
            r"bachelor'?s?\s+or\s+master'?s?",
            r"bachelor'?s?\s+or\s+graduate\s+(?:degree|student)",
            r"bachelor'?s?\s+or\s+ms\b",
            r"undergraduate\s+or\s+graduate",
            r"bs/ms\b",
            r"bachelor'?s?/master'?s?",
        ]

        for pattern in bs_definite_patterns:
            match = re.search(pattern, combined, re.I)
            if match:
                start = max(0, match.start() - 300)
                end = min(len(combined), match.end() + 300)
                context = combined[start:end]

                has_inclusive = any(
                    re.search(inc, context, re.I) for inc in inclusive_modifiers
                )
                if has_inclusive:
                    continue

                is_preferred = any(
                    kw in context
                    for kw in ["preferred", "ideal", "nice to have", "bonus"]
                )
                if is_preferred:
                    continue

                has_masters = any(
                    kw in context
                    for kw in [
                        "master's",
                        "masters",
                        "graduate student",
                        " ms ",
                        "graduate degree",
                    ]
                )
                if not has_masters:
                    return "REJECT", "Bachelor's students only"

        phd_definite_patterns = [
            r"must\s+be\s+pursuing\s+a\s+phd",
            r"currently\s+pursuing\s+a\s+phd?\s+(?:degree\s+)?in",
            r"phd?\s+candidates?\s+to\s+intern",
            r"looking\s+for\s+phd?\s+candidates?",
            r"phd?\s+student(?:s)?\s+in",
            r"phd?.*having\s+completed\s+at\s+least",
            r"phd?\s+\(enrolled\s+student\)",
        ]

        for pattern in phd_definite_patterns:
            match = re.search(pattern, combined, re.I)
            if match:
                start = max(0, match.start() - 300)
                end = min(len(combined), match.end() + 300)
                context = combined[start:end]

                is_preferred = any(
                    kw in context for kw in ["preferred", "ideal", "open to"]
                )
                if is_preferred:
                    continue

                has_masters = any(
                    kw in context for kw in ["master", "graduate student", " ms "]
                )
                if not has_masters:
                    return "REJECT", "PhD students only"

        try:
            title_tag = soup.find("title")
            if title_tag:
                title_text = title_tag.get_text().lower()
                if re.search(r"phd.*intern", title_text[:200]):
                    if not re.search(
                        r"master|graduate\s+student|ms\b", required_text, re.I
                    ):
                        return "REJECT", "PhD students only"
        except:
            pass

        return None, None

    @staticmethod
    def _extract_requirement_sections(soup):
        sections = {"required": None, "preferred": None}

        try:
            headers = soup.find_all(["h2", "h3", "h4", "strong", "b"])

            for header in headers:
                header_text = header.get_text().strip().lower()

                if len(header_text) > 100:
                    continue

                section_type = None

                if any(
                    kw in header_text
                    for kw in [
                        "required qualifications",
                        "minimum requirements",
                        "must have",
                        "requirements:",
                        "basic qualifications",
                    ]
                ):
                    section_type = "required"
                elif any(
                    kw in header_text
                    for kw in [
                        "preferred qualifications",
                        "preferred skills",
                        "nice to have",
                        "ideal candidate",
                        "bonus",
                    ]
                ):
                    section_type = "preferred"

                if not section_type:
                    continue

                text_parts = []
                current = header.find_next_sibling()

                while current:
                    if current.name in ["h2", "h3", "h4"]:
                        break
                    if current.name:
                        text_parts.append(current.get_text())
                    current = current.find_next_sibling()
                    if len(text_parts) > 50:
                        break

                sections[section_type] = " ".join(text_parts)
        except:
            pass

        return sections

    @staticmethod
    def _get_qualification_section_text(soup):
        try:
            qual_headers = soup.find_all(
                ["h2", "h3"],
                text=re.compile(r"(Qualifications|Requirements)", re.I),
            )
            qual_text = []
            for header in qual_headers:
                section_text = []
                for sibling in header.find_next_siblings():
                    if sibling.name in ["h2", "h3"]:
                        break
                    section_text.append(sibling.get_text())
                qual_text.append(" ".join(section_text))
            return " ".join(qual_text)
        except:
            return ""

    @staticmethod
    def _get_json_ld_description(soup):
        try:
            json_ld = soup.find("script", type="application/ld+json")
            if json_ld:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    return data.get("description", "")
        except:
            pass
        return ""

    @staticmethod
    def _get_meta_description(soup):
        meta_desc = soup.find("meta", {"property": "og:description"})
        if meta_desc:
            return meta_desc.get("content", "")
        meta_desc2 = soup.find("meta", {"name": "description"})
        if meta_desc2:
            return meta_desc2.get("content", "")
        return ""

    @staticmethod
    def validate_company_field(company, title, url):
        if not company or company == "Unknown" or not company.strip():
            company_from_url = ValidationHelper.extract_company_from_domain(url)
            return True, company_from_url, None

        company = company.strip()

        if not ValidationHelper.is_valid_company_name(company):
            company_from_url = ValidationHelper.extract_company_from_domain(url)
            return True, company_from_url, None

        if len(company) > 100:
            return False, company, "Company name too long"

        job_keywords = ["intern", "software", "engineer", "developer"]
        keyword_count = sum(1 for kw in job_keywords if kw in company.lower())
        if keyword_count >= 2:
            return False, company, "Company field contains job title"

        return True, company, None

    @staticmethod
    def is_valid_company_name(name):
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
        ]
        if any(kw == name.lower() for kw in invalid_keywords):
            return False

        if re.match(r"^[A-Z]{2,4}-", name):
            return False

        if name.isupper() and len(name) < 10:
            return False

        title_keywords = ["intern", "engineer", "developer", "software"]
        keyword_count = sum(1 for kw in title_keywords if kw in name.lower())
        if keyword_count >= 2:
            return False

        return True

    @staticmethod
    def clean_legal_entity(company):
        if not company:
            return company

        company = re.sub(r"^\d+\s+", "", company)
        company = re.sub(r"^US\d+\s+", "", company)
        company = re.sub(r"^[A-Z]{2,4}-", "", company)
        company = re.sub(r"^[A-Z]{2,4}\s+", "", company)
        company = re.sub(
            r",?\s+(Inc\.?|LLC\.?|Corp\.?|Ltd\.?|Corporation|Corp\s+Svcs\.?)$",
            "",
            company,
            flags=re.I,
        )

        return company.strip()

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
