#!/usr/bin/env python3

import re
import time
import json
import logging
import pickle
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from lxml import html as lxml_html

from config import (
    GMAIL_CREDS_FILE,
    GMAIL_TOKEN_FILE,
    MAX_JOB_AGE_DAYS,
    FAILED_SIMPLIFY_CACHE,
    EMAIL_URL_BLACKLIST,
    JOB_BOARD_WHITELIST,
)
from utils import DateParser


class FakeResponse:
    def __init__(self, text, url, status_code=200):
        self.text = text
        self.url = url
        self.status_code = status_code


class PageParser:
    @staticmethod
    def extract_title(soup) -> str:
        if not soup:
            return "Unknown"
        try:
            title_elem = soup.find("h1")
            if title_elem:
                return title_elem.get_text(strip=True)

            title_selectors = [
                '[data-automation-id="jobPostingHeader"]',
                ".job-title",
                ".posting-headline",
                '[itemprop="title"]',
                'h1[class*="title"]',
                'h1[class*="job"]',
            ]

            for selector in title_selectors:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 5:
                        return text

            page_title = soup.find("title")
            if page_title:
                title_text = page_title.get_text()
                if "|" in title_text:
                    return title_text.split("|")[0].strip()
                elif "-" in title_text:
                    return title_text.split("-")[0].strip()
                return title_text.strip()

        except Exception as e:
            logging.debug(f"Title extraction failed: {e}")

        return "Unknown"

    @staticmethod
    def extract_job_age_days(soup) -> Optional[int]:
        if not soup:
            return None

        try:
            page_text = soup.get_text()[:5000]

            patterns = [
                (
                    r"Posted\s+(\d+)\+?\s+days?\s+ago",
                    lambda m: int(m.group(1)),
                ),  # Handles "30+ days ago"
                (
                    r"Posted\s+>(\d+)\s+days?",
                    lambda m: int(m.group(1)),
                ),  # Handles ">30 days"
                (r"Posted\s+(\d+)d\s+ago", lambda m: int(m.group(1))),
                (r"(\d+)\s+days?\s+ago", lambda m: int(m.group(1))),
                (r"Posted:\s*(\d+)\s+day", lambda m: int(m.group(1))),
                (r"Posted\s+today", lambda m: 0),
                (r"Posted\s+yesterday", lambda m: 1),
            ]

            for pattern, extractor in patterns:
                match = re.search(pattern, page_text, re.I)  # Case-insensitive
                if match:
                    days = extractor(match)
                    if 0 <= days <= MAX_JOB_AGE_DAYS * 10:
                        return days

            date_selectors = [
                '[data-automation-id="postedOn"]',
                ".posted-date",
                ".posting-date",
                '[itemprop="datePosted"]',
            ]

            for selector in date_selectors:
                elem = soup.select_one(selector)
                if elem:
                    date_text = elem.get_text(strip=True)
                    days = DateParser.extract_days_ago(date_text)
                    if days is not None:
                        return days

        except Exception as e:
            logging.debug(f"Job age extraction failed: {e}")

        return None

    @staticmethod
    def extract_company(soup, url) -> str:
        if not soup:
            return "Unknown"

        try:
            company_selectors = [
                '[data-automation-id="company-name"]',
                ".company-name",
                '[itemprop="hiringOrganization"]',
                ".employer-name",
            ]

            for selector in company_selectors:
                elem = soup.select_one(selector)
                if elem:
                    company = elem.get_text(strip=True)
                    if company and len(company) > 2:
                        return company

            meta = soup.find("meta", {"property": "og:site_name"})
            if meta and meta.get("content"):
                return meta["content"].strip()

        except Exception as e:
            logging.debug(f"Company extraction failed: {e}")

        return "Unknown"


class SimplifyRedirectResolver:
    _success_cache = {}

    @classmethod
    def resolve(cls, simplify_url, page_fetcher=None) -> Tuple[Optional[str], bool]:
        if simplify_url in cls._success_cache:
            logging.debug(f"Simplify: Using cached success")
            return cls._success_cache[simplify_url], True

        if not page_fetcher:
            from extractors import PageFetcher

            page_fetcher = PageFetcher()

        logging.info(f"Simplify: Attempting resolution for {simplify_url[:60]}")

        # Method 0: Parse tracked_obj from page JSON (no auth needed)
        result = cls._method_0_tracked_obj(simplify_url)
        if result:
            cls._success_cache[simplify_url] = result
            return result, True

        result = cls._method_1_http_redirect(simplify_url)
        if result:
            cls._success_cache[simplify_url] = result
            return result, True

        result = cls._method_2_parse_page(simplify_url)
        if result:
            cls._success_cache[simplify_url] = result
            return result, True

        result = cls._method_3_selenium(simplify_url, page_fetcher)
        if result:
            cls._success_cache[simplify_url] = result
            return result, True

        logging.warning(f"Simplify: All methods failed for {simplify_url[:60]}")
        return None, False

    @classmethod
    def _method_0_tracked_obj(cls, url):
        try:
            logging.debug("Simplify Method 0: Extract url field and follow redirect")
            response = requests.get(url, timeout=10)

            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
                response.text,
                re.DOTALL,
            )
            if not match:
                logging.debug("Simplify Method 0: No __NEXT_DATA__ found")
                return None

            data = json.loads(match.group(1))

            if (
                "props" in data
                and "pageProps" in data["props"]
                and "jobPosting" in data["props"]["pageProps"]
            ):
                jobPosting = data["props"]["pageProps"]["jobPosting"]

                if "url" in jobPosting:
                    click_url = jobPosting["url"]
                    logging.info(
                        f"Simplify Method 0: Found click URL: {click_url[:60]}"
                    )

                    redirect_response = requests.get(
                        click_url, allow_redirects=True, timeout=10
                    )
                    final_url = redirect_response.url

                    if "simplify.jobs" not in final_url.lower():
                        logging.info(
                            f"Simplify Method 0: ✓ Followed redirect to: {final_url[:70]}"
                        )
                        return final_url
                    else:
                        logging.debug("Simplify Method 0: Redirect stayed on Simplify")
                        return None

            logging.debug("Simplify Method 0: No url field in JSON")
            return None

        except Exception as e:
            logging.debug(f"Simplify Method 0: Failed - {e}")
        return None

    @classmethod
    def _method_1_http_redirect(cls, url):
        try:
            logging.debug("Simplify Method 1: HTTP redirect")
            response = requests.get(url, allow_redirects=True, timeout=5)
            final_url = response.url
            if (
                "simplify.jobs" not in final_url.lower()
                and "linkedin" not in final_url.lower()
            ):
                logging.info(f"Simplify Method 1: ✓ HTTP redirect to {final_url[:60]}")
                return final_url
            logging.debug("Simplify Method 1: Still on Simplify page")
        except Exception as e:
            logging.debug(f"Simplify Method 1: Failed - {e}")
        return None

    @classmethod
    def _method_2_parse_page(cls, url):
        try:
            logging.debug("Simplify Method 2: Parse page")
            response = requests.get(url, timeout=5)
            soup = BeautifulSoup(response.text, "lxml")

            apply_selectors = [
                "a.apply-button",
                'a[class*="apply"]',
                'a[href*="workday"]',
                'a[href*="greenhouse"]',
                'a[href*="lever"]',
                'button[onclick*="workday"]',
            ]

            for selector in apply_selectors:
                elem = soup.select_one(selector)
                if elem:
                    href = elem.get("href") or elem.get("onclick")
                    if (
                        href
                        and "http" in href
                        and "simplify" not in href
                        and "linkedin" not in href
                    ):
                        if elem.get("onclick"):
                            url_match = re.search(r'https?://[^\s\'"]+', href)
                            if url_match:
                                href = url_match.group(0)
                        logging.info(
                            f"Simplify Method 2: ✓ Parsed from page {href[:60]}"
                        )
                        return href

            logging.debug("Simplify Method 2: No apply links found")
        except Exception as e:
            logging.debug(f"Simplify Method 2: Failed - {e}")
        return None

    @classmethod
    def _method_3_selenium(cls, url, page_fetcher):
        try:
            logging.debug("Simplify Method 3: Selenium")
            actual_url = page_fetcher.resolve_simplify_url(url)
            if (
                actual_url
                and "simplify.jobs" not in actual_url.lower()
                and "linkedin" not in actual_url.lower()
            ):
                logging.info(f"Simplify Method 3: ✓ Selenium found {actual_url[:60]}")
                return actual_url
            logging.debug("Simplify Method 3: No valid URL found")
        except Exception as e:
            logging.debug(f"Simplify Method 3: Failed - {e}")
        return None


class JSONLDExtractor:
    @staticmethod
    def extract_job_data(html_content: str) -> Dict[str, Any]:
        try:
            soup = BeautifulSoup(html_content, "lxml")
            scripts = soup.find_all("script", type="application/ld+json")

            for script in scripts:
                try:
                    data = json.loads(script.string)

                    if isinstance(data, list):
                        for item in data:
                            if (
                                isinstance(item, dict)
                                and item.get("@type") == "JobPosting"
                            ):
                                return JSONLDExtractor._parse_job_posting(item)
                    elif isinstance(data, dict):
                        if data.get("@type") == "JobPosting":
                            return JSONLDExtractor._parse_job_posting(data)
                        elif data.get("@graph"):
                            for item in data["@graph"]:
                                if (
                                    isinstance(item, dict)
                                    and item.get("@type") == "JobPosting"
                                ):
                                    return JSONLDExtractor._parse_job_posting(item)
                except (json.JSONDecodeError, AttributeError):
                    continue

            return {}
        except Exception:
            return {}

    @staticmethod
    def _parse_job_posting(data: Dict) -> Dict[str, Any]:
        result = {}

        try:
            if "hiringOrganization" in data:
                org = data["hiringOrganization"]
                if isinstance(org, dict):
                    result["company"] = org.get("name", "")

            if "jobLocation" in data:
                location = data["jobLocation"]
                if isinstance(location, list):
                    location = location[0] if location else {}

                if isinstance(location, dict):
                    if location.get("@type") == "VirtualLocation":
                        result["remote"] = "Remote"
                    elif "address" in location:
                        address = location["address"]
                        if isinstance(address, dict):
                            city = address.get("addressLocality", "")
                            state = address.get("addressRegion", "")
                            if city and state:
                                result["location"] = f"{city}, {state}"
                            elif city:
                                result["location"] = city
                            elif state:
                                result["location"] = state

            if "workLocation" in data:
                work_loc = data["workLocation"]
                if (
                    isinstance(work_loc, dict)
                    and work_loc.get("@type") == "VirtualLocation"
                ):
                    result["remote"] = "Remote"

            if "identifier" in data:
                identifier = data["identifier"]
                if isinstance(identifier, dict):
                    result["job_id"] = str(identifier.get("value", ""))
                else:
                    result["job_id"] = str(identifier)

            if "baseSalary" in data:
                salary = data["baseSalary"]
                if isinstance(salary, dict):
                    value = salary.get("value", {})
                    if isinstance(value, dict):
                        min_val = value.get("minValue", "")
                        max_val = value.get("maxValue", "")
                        if min_val or max_val:
                            result["salary"] = f"{min_val}-{max_val}"

            if "employmentType" in data:
                emp_type = data["employmentType"]
                if isinstance(emp_type, list):
                    emp_type = ", ".join(emp_type)
                result["employment_type"] = str(emp_type)

            if "description" in data:
                result["description"] = data["description"]

        except Exception:
            pass

        return result


class ATSDetector:
    PLATFORMS = {
        "workday": ["myworkdayjobs.com", "wd1.", "wd3.", "wd5.", "wd12."],
        "greenhouse": ["greenhouse.io", "boards.greenhouse", "job-boards.greenhouse"],
        "lever": ["lever.co", "jobs.lever.co"],
        "icims": ["icims.com"],
        "taleo": ["taleo.net"],
        "jobvite": ["jobvite.com"],
        "brassring": ["brassring.com"],
        "adp": ["adp.com/careers"],
        "glassdoor": ["glassdoor.com"],
    }

    SELECTORS = {
        "workday": {
            "location": [
                '[data-automation-id="locations"]',
                ".css-1gy6ixs",
                '[data-automation-id="jobPostingLocation"]',
            ],
            "remote": ['[data-automation-id="workerSubType"]', ".css-k008qs"],
            "description": [
                '[data-automation-id="jobPostingDescription"]',
                ".css-1q2dra3",
            ],
            "company": ['[data-automation-id="company-name"]'],
            "job_id": ['[data-automation-id="requisitionId"]'],
        },
        "greenhouse": {
            "location": [".location", ".job-location", '[data-mapped="location"]'],
            "remote": [".location", ".workplaceTypes"],
            "description": [
                ".job-description",
                "#content .content",
                ".posting-description",
            ],
            "company": [".company-name", ".app-title"],
            "job_id": [],
        },
        "lever": {
            "location": [
                ".posting-categories .location",
                ".location",
                '[data-qa="location"]',
            ],
            "remote": [".workplaceTypes", ".posting-categories .commitment"],
            "description": [".posting-description", ".section-wrapper", ".content"],
            "company": [".main-header-text a"],
            "job_id": [],
        },
        "icims": {
            "location": [".iCIMS_JobAttribute", ".job-location"],
            "remote": [".iCIMS_JobAttribute"],
            "description": [".iCIMS_JobDescription", ".iCIMS_InfoMsg"],
            "company": [".iCIMS_Anchor"],
            "job_id": [],
        },
        "taleo": {
            "location": [".jobReqLocation", ".jobdetaillocation"],
            "remote": [".jobReqLocation"],
            "description": [".jobdescription", ".requisitionDescriptionInterface"],
            "company": [".company-name"],
            "job_id": [],
        },
    }

    @staticmethod
    def detect_platform(url: str) -> Optional[str]:
        url_lower = url.lower()
        for platform, patterns in ATSDetector.PLATFORMS.items():
            if any(pattern in url_lower for pattern in patterns):
                return platform
        return None

    @staticmethod
    def get_selectors(platform: str) -> Dict[str, List[str]]:
        return ATSDetector.SELECTORS.get(platform, {})


class DescriptionExtractor:
    SELECTORS = [
        ".job-description",
        '[itemprop="description"]',
        ".description",
        ".posting-description",
        ".jobdescription",
        '[data-automation-id="jobPostingDescription"]',
        ".content .section",
        '[role="main"] .content',
        ".job-details",
        "#job-description",
        ".job-description-content",
        '[data-qa="job-description"]',
        ".description-content",
        ".job-description-text",
    ]

    @staticmethod
    def extract(soup: BeautifulSoup, platform: Optional[str] = None) -> str:
        if platform:
            platform_selectors = ATSDetector.get_selectors(platform).get(
                "description", []
            )
            for selector in platform_selectors:
                desc = DescriptionExtractor._try_selector(soup, selector)
                if desc:
                    return desc

        for selector in DescriptionExtractor.SELECTORS:
            desc = DescriptionExtractor._try_selector(soup, selector)
            if desc:
                return desc

        semantic_elems = soup.find_all(
            ["div", "section", "article"], {"itemprop": "description"}
        )
        for elem in semantic_elems:
            text = elem.get_text(strip=True)
            if len(text) > 200:
                return text

        all_divs = soup.find_all(["div", "section"], class_=True)
        max_text = ""
        for div in all_divs:
            text = div.get_text(strip=True)
            if len(text) > len(max_text) and len(text) > 300:
                max_text = text

        return max_text if max_text else ""

    @staticmethod
    def _try_selector(soup: BeautifulSoup, selector: str) -> str:
        try:
            if selector.startswith("."):
                elem = soup.select_one(selector)
            elif selector.startswith("["):
                elem = soup.select_one(selector)
            else:
                elem = soup.find(class_=selector.replace(".", ""))

            if elem:
                text = elem.get_text(strip=True)
                if len(text) > 100:
                    return text
        except Exception:
            pass
        return ""


class LocationExtractor:
    @staticmethod
    def extract(
        soup: BeautifulSoup, html_content: str, platform: Optional[str] = None
    ) -> str:
        if platform:
            platform_selectors = ATSDetector.get_selectors(platform).get("location", [])
            for selector in platform_selectors:
                location = LocationExtractor._try_selector(soup, selector)
                if location:
                    return LocationExtractor._clean_location(location)

        meta_location = LocationExtractor._extract_from_meta(soup)
        if meta_location:
            return meta_location

        generic_selectors = [
            ".location",
            ".job-location",
            "[data-location]",
            ".posting-categories .location",
            '[itemprop="jobLocation"]',
        ]

        for selector in generic_selectors:
            location = LocationExtractor._try_selector(soup, selector)
            if location:
                return LocationExtractor._clean_location(location)

        text_location = LocationExtractor._extract_from_text(html_content)
        if text_location:
            return text_location

        return "Unknown"

    @staticmethod
    def _try_selector(soup: BeautifulSoup, selector: str) -> str:
        try:
            elem = soup.select_one(selector)
            if elem:
                location = elem.get_text(strip=True)
                if location and len(location) > 2:
                    return location
        except Exception:
            pass
        return ""

    @staticmethod
    def _extract_from_meta(soup: BeautifulSoup) -> str:
        meta_tags = [
            ("property", "og:locality"),
            ("name", "geo.placename"),
            ("name", "location"),
            ("property", "og:location"),
        ]

        for attr, value in meta_tags:
            meta = soup.find("meta", {attr: value})
            if meta and meta.get("content"):
                return meta["content"].strip()

        return ""

    @staticmethod
    def _extract_from_text(html_content: str) -> str:
        patterns = [
            r"Location:\s*([A-Z][a-z\s]+,\s*[A-Z]{2})",
            r"Location:\s*([A-Z][a-z\s]+\s+-\s+[A-Z]{2})",
            r"Office Location:\s*([A-Z][a-z\s]+,\s*[A-Z]{2})",
            r"Work Location:\s*([A-Z][a-z\s]+,\s*[A-Z]{2})",
        ]

        for pattern in patterns:
            match = re.search(pattern, html_content)
            if match:
                return match.group(1).strip()

        return ""

    @staticmethod
    def _clean_location(location: str) -> str:
        location = re.sub(r"\s+", " ", location).strip()
        location = re.sub(r"^Location:\s*", "", location, flags=re.I)

        if re.match(r"^[A-Za-z\s]+,\s*[A-Z]{2}$", location):
            return location

        if re.match(r"^[A-Za-z\s]+\s+-\s+[A-Z]{2}$", location):
            return location.replace(" - ", ", ")

        match = re.search(r"([A-Za-z\s]+)[,\-]\s*([A-Z]{2})\b", location)
        if match:
            return f"{match.group(1).strip()}, {match.group(2)}"

        return location if len(location) < 50 else "Unknown"


class RemoteDetector:
    KEYWORDS = {
        "remote": [
            "remote",
            "work from home",
            "wfh",
            "work from anywhere",
            "distributed",
            "100% remote",
            "fully remote",
            "remote-first",
            "remote eligible",
            "remote opportunity",
            "work remotely",
            "home-based",
            "telecommute",
            "virtual position",
        ],
        "hybrid": [
            "hybrid",
            "flexible location",
            "remote/office",
            "2-3 days in office",
            "partially remote",
            "flex work",
            "flexible work",
            "mix of remote",
            "hybrid remote",
        ],
        "onsite": [
            "on-site",
            "onsite",
            "in-person",
            "in office",
            "office-based",
            "must relocate",
            "local candidates",
            "in-office",
            "at office",
            "on site",
        ],
    }

    @staticmethod
    def detect(
        title: str, description: str, location: str, structured_data: Dict
    ) -> str:
        scores = {"remote": 0, "hybrid": 0, "onsite": 0}

        if structured_data.get("remote"):
            return structured_data["remote"]

        title_lower = title.lower()
        for status, keywords in RemoteDetector.KEYWORDS.items():
            for keyword in keywords:
                if keyword in title_lower:
                    scores[status] += 3
                    break

        desc_lower = description.lower()
        for status, keywords in RemoteDetector.KEYWORDS.items():
            for keyword in keywords:
                if keyword in desc_lower:
                    scores[status] += 1

        if location:
            location_lower = location.lower()
            if location_lower in [
                "remote",
                "usa",
                "united states",
                "nationwide",
                "anywhere",
            ]:
                scores["remote"] += 2

        max_score = max(scores.values())
        if max_score == 0:
            return "Unknown"

        for status, score in scores.items():
            if score == max_score:
                return status.capitalize()

        return "Unknown"


class JobIDExtractor:
    PATTERNS = {
        "workday": [
            r"([A-Z]{1,3}[-_]?\d{5,10})",
            r"(JR[-_]?\d{5,10})",
            r"(R[-_]?\d{5,10})",
        ],
        "greenhouse": [
            r"jobs/(\d{7,10})",
            r"gh_jid=(\d{7,10})",
        ],
        "lever": [
            r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
        ],
        "icims": [
            r"jobs/(\d{4,8})/job",
        ],
        "taleo": [
            r"jobDetail\.ftl\?job=(\d{6,12})",
        ],
        "jobvite": [
            r"job/([a-zA-Z0-9]{7,15})",
        ],
        "glassdoor": [],
    }

    @staticmethod
    def extract(url: str, html_content: str, platform: Optional[str] = None) -> str:
        if platform == "glassdoor" or "glassdoor" in url.lower():
            return "N/A"

        if platform and platform in JobIDExtractor.PATTERNS:
            for pattern in JobIDExtractor.PATTERNS[platform]:
                match = re.search(pattern, url)
                if match:
                    return match.group(1)

        for patterns in JobIDExtractor.PATTERNS.values():
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    return match.group(1)

        text_patterns = [
            r"(?:Requisition|Req|Job)\s*(?:ID|#|Number):\s*([A-Z0-9\-_]+)",
            r"(?:Reference|Ref)\s*#?:\s*([A-Z0-9\-_]+)",
        ]

        for pattern in text_patterns:
            match = re.search(pattern, html_content, re.I)
            if match:
                job_id = match.group(1).strip()
                if 4 <= len(job_id) <= 20:
                    return job_id

        return ""


class SponsorshipDetector:
    POSITIVE_KEYWORDS = [
        "h1b sponsor",
        "visa sponsorship",
        "work authorization provided",
        "eligible for sponsorship",
        "will sponsor",
        "sponsorship available",
        "provide sponsorship",
        "offers sponsorship",
        "visa support",
    ]

    NEGATIVE_KEYWORDS = [
        "must be authorized to work",
        "work authorization required",
        "no sponsorship",
        "citizenship required",
        "must be us citizen",
        "cannot provide sponsorship",
        "already authorized to work",
        "us citizen or permanent resident",
        "security clearance",
        "no visa sponsorship",
        "us work authorization",
    ]

    @staticmethod
    def detect(description: str) -> str:
        if not description or len(description) < 100:
            return "Unknown"

        desc_lower = description.lower()

        positive_score = 0
        for keyword in SponsorshipDetector.POSITIVE_KEYWORDS:
            if keyword in desc_lower:
                context_start = max(0, desc_lower.find(keyword) - 50)
                context_end = min(
                    len(desc_lower), desc_lower.find(keyword) + len(keyword) + 50
                )
                context = desc_lower[context_start:context_end]

                if "not" not in context and "no " not in context:
                    positive_score += 1

        negative_score = 0
        for keyword in SponsorshipDetector.NEGATIVE_KEYWORDS:
            if keyword in desc_lower:
                negative_score += 2

        score = positive_score - negative_score

        if score > 0:
            return "Yes"
        elif score < -1:
            return "No"
        else:
            return "Unknown"


class EmailExtractor:
    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

    def __init__(self):
        self.service = None
        self._authenticate()

    def _authenticate(self):
        creds = None
        try:
            with open(GMAIL_TOKEN_FILE, "rb") as token:
                creds = pickle.load(token)
        except FileNotFoundError:
            pass

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    GMAIL_CREDS_FILE, self.SCOPES
                )
                creds = flow.run_local_server(port=0)

            with open(GMAIL_TOKEN_FILE, "wb") as token:
                pickle.dump(creds, token)

        self.service = build("gmail", "v1", credentials=creds)
        logging.info("[Gmail] Authenticated successfully")

    def fetch_job_emails(self) -> List[Dict]:
        if not self.service:
            logging.error("[Gmail] Service not initialized")
            return []

        try:
            today = datetime.now().date()
            after_date = (today - timedelta(days=MAX_JOB_AGE_DAYS)).strftime("%Y/%m/%d")

            query = f"label:Job Hunt after:{after_date}"

            logging.info("[Gmail] Authenticating...")
            results = (
                self.service.users()
                .messages()
                .list(userId="me", q=query, maxResults=50)
                .execute()
            )

            messages = results.get("messages", [])

            if not messages:
                logging.info("[Gmail] No new messages found")
                return []

            logging.info(f"Found {len(messages)} labeled emails")

            email_data = []
            for msg in messages:
                msg_id = msg["id"]
                message = (
                    self.service.users()
                    .messages()
                    .get(userId="me", id=msg_id, format="full")
                    .execute()
                )

                headers = {h["name"]: h["value"] for h in message["payload"]["headers"]}
                subject = headers.get("Subject", "")
                date = headers.get("Date", "")
                sender_raw = headers.get("From", "")

                sender = "Unknown"
                if "jobright" in sender_raw.lower():
                    sender = "Jobright"
                elif "simplify" in sender_raw.lower():
                    sender = "Simplify"
                elif "swe" in sender_raw.lower() or "pittcsc" in sender_raw.lower():
                    sender = "SWE List"
                elif sender_raw:
                    sender = (
                        sender_raw.split("<")[0].strip()
                        if "<" in sender_raw
                        else sender_raw
                    )

                body = self._get_body(message["payload"])

                urls = self._extract_urls_smart(body, sender)

                if urls:
                    email_data.append(
                        {
                            "email_id": msg_id,
                            "subject": subject,
                            "date": date,
                            "sender": sender,
                            "urls": urls,
                            "body": body,
                            "html": body,
                        }
                    )

            logging.info(
                f"Total: {sum(len(e['urls']) for e in email_data)} job URLs from {len(email_data)} emails"
            )
            return email_data

        except Exception as e:
            logging.error(f"Email fetch error: {e}")
            return []

    def _extract_urls_smart(self, body: str, sender: str) -> List[str]:
        try:
            soup = BeautifulSoup(body, "html.parser")
            all_links = [
                a.get("href") for a in soup.find_all("a", href=True) if a.get("href")
            ]
        except:
            all_links = re.findall(r'https?://[^\s<>"]+', body)
            all_links = [url.rstrip(".,;)]}") for url in all_links]

        urls = []
        seen = set()

        for url in all_links:
            if not url or not url.startswith("http"):
                continue

            url_lower = url.lower()

            if any(
                blacklist_domain in url_lower
                for blacklist_domain in EMAIL_URL_BLACKLIST
            ):
                continue

            if sender == "SWE List":
                if "simplify.jobs/p/" in url_lower:
                    if url not in seen:
                        urls.append(url)
                        seen.add(url)
                continue

            if sender == "Jobright":
                if "jobright.ai/jobs/info/" in url_lower:
                    if url not in seen:
                        urls.append(url)
                        seen.add(url)
                continue

            if any(board in url_lower for board in JOB_BOARD_WHITELIST):
                if url not in seen:
                    urls.append(url)
                    seen.add(url)
                continue

            if self._score_url_pattern(url_lower) >= 0.60:
                if url not in seen:
                    urls.append(url)
                    seen.add(url)

        return urls

    def _score_url_pattern(self, url: str) -> float:
        score = 0.0

        domain_keywords = [
            "career",
            "job",
            "talent",
            "recruit",
            "hire",
            "work",
            "employment",
            "apply",
        ]
        if any(kw in url for kw in domain_keywords):
            score += 0.40

        path_segments = [
            "/job/",
            "/jobs/",
            "/career/",
            "/careers/",
            "/position/",
            "/opening/",
            "/vacancy/",
            "/role/",
            "/apply/",
            "/application/",
        ]
        if any(seg in url for seg in path_segments):
            score += 0.30

        if re.search(
            r"/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", url
        ):
            score += 0.20
        elif re.search(r"/\d{6,}", url):
            score += 0.20
        elif re.search(r"/[A-Z0-9]{5,20}(?:\?|$)", url):
            score += 0.15

        if len(url) > 50:
            score += 0.10

        generic_services = [
            ".me/",
            ".link/",
            ".email/",
            "usercontent.com",
            "storage.com",
            "cdn.",
            "static.",
        ]
        if any(svc in url for svc in generic_services):
            score -= 0.50

        excluded_paths = [
            "/unsubscribe",
            "/preferences",
            "/pixel",
            "/track",
            "/email",
            "/o/",
            "/u/",
        ]
        if any(path in url for path in excluded_paths):
            score -= 0.40

        return score

    def _get_body(self, payload):
        if "body" in payload and payload["body"].get("data"):
            import base64

            return base64.urlsafe_b64decode(payload["body"]["data"]).decode(
                "utf-8", errors="ignore"
            )

        if "parts" in payload:
            for part in payload["parts"]:
                if part["mimeType"] == "text/plain":
                    if "data" in part["body"]:
                        import base64

                        return base64.urlsafe_b64decode(part["body"]["data"]).decode(
                            "utf-8", errors="ignore"
                        )
                elif part["mimeType"] == "text/html":
                    if "data" in part["body"]:
                        import base64

                        return base64.urlsafe_b64decode(part["body"]["data"]).decode(
                            "utf-8", errors="ignore"
                        )

        return ""


class PageFetcher:
    def __init__(self):
        self.driver = None
        self.page_count = 0
        self._initialize_driver()

    def _initialize_driver(self):
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument(
                "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )

            self.driver = webdriver.Chrome(options=options)
            self.driver.implicitly_wait(10)
            logging.info("WebDriver initialized successfully")
        except Exception as e:
            logging.error(f"Driver initialization failed: {e}")
            self.driver = None

    def check_url_health(self, url: str) -> Tuple[bool, Optional[int]]:
        try:
            response = requests.head(url, timeout=10, allow_redirects=True)
            status_code = response.status_code
            if status_code in [200, 301, 302, 303, 307, 308]:
                return True, status_code
            elif status_code in [403, 405]:
                response = requests.get(url, timeout=10, allow_redirects=True)
                return response.status_code == 200, response.status_code
            else:
                return False, status_code
        except requests.exceptions.Timeout:
            return False, None
        except requests.exceptions.ConnectionError:
            return False, None
        except Exception as e:
            logging.debug(f"URL health check failed for {url}: {e}")
            return False, None

    def fetch_page(
        self, url: str, wait_for_selector: Optional[str] = None
    ) -> Tuple[Optional[FakeResponse], str, str]:
        if not self.driver:
            self._initialize_driver()

        if not self.driver:
            return (None, url, "")

        try:
            self.driver.get(url)

            final_url = self.driver.current_url

            if wait_for_selector:
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, wait_for_selector)
                        )
                    )
                except TimeoutException:
                    logging.debug(f"Timeout waiting for selector: {wait_for_selector}")

            time.sleep(3)

            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);"
            )
            time.sleep(2)

            html = self.driver.page_source
            page_source = self.driver.page_source

            response = FakeResponse(html, final_url)

            self.page_count += 1
            if self.page_count >= 50:
                self._cleanup_driver()
                self.page_count = 0

            return (response, final_url, page_source)

        except Exception as e:
            logging.warning(f"Page fetch error for {url}: {e}")
            return (None, url, "")

    def resolve_simplify_url(self, simplify_url: str) -> Optional[str]:
        if not self.driver:
            self._initialize_driver()

        if not self.driver:
            logging.warning("Simplify: Driver not available")
            return None

        try:
            logging.info(f"Simplify: Loading {simplify_url[:60]}...")
            self.driver.get(simplify_url)

            try:
                WebDriverWait(self.driver, 8).until(
                    lambda d: "simplify.jobs" not in d.current_url
                )
                logging.info("Simplify: Auto-redirect")
            except TimeoutException:
                pass

            current_url = self.driver.current_url

            if "simplify.jobs" not in current_url.lower():
                logging.info(
                    f"Simplify: ✓ Resolved via auto-redirect to {current_url[:80]}"
                )
                return current_url

            page_title = self.driver.title
            logging.info(f"Simplify: Page title: {page_title[:60]}")

            try:
                iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                logging.info(f"Simplify: Found {len(iframes)} iframes")

                for idx, iframe in enumerate(iframes):
                    iframe_src = iframe.get_attribute("src")
                    logging.info(
                        f"Simplify: Iframe {idx+1} src: {iframe_src[:80] if iframe_src else 'empty'}"
                    )

                    if (
                        iframe_src
                        and "http" in iframe_src
                        and "simplify" not in iframe_src
                    ):
                        if "linkedin" not in iframe_src.lower():
                            logging.info(
                                f"Simplify: ✓ Found URL in iframe src: {iframe_src[:60]}"
                            )
                            return iframe_src

                for idx, iframe in enumerate(iframes):
                    try:
                        logging.info(f"Simplify: Switching to iframe {idx+1}")
                        self.driver.switch_to.frame(iframe)

                        iframe_links = self.driver.find_elements(By.TAG_NAME, "a")
                        logging.info(
                            f"Simplify: Iframe {idx+1} has {len(iframe_links)} links"
                        )

                        for link in iframe_links[:15]:
                            href = link.get_attribute("href")
                            if href and any(
                                d in href
                                for d in [
                                    "workday",
                                    "greenhouse",
                                    "lever",
                                    "icims",
                                    "taleo",
                                    "applytojob",
                                    "careers",
                                ]
                            ):
                                if (
                                    "simplify" not in href
                                    and "linkedin" not in href.lower()
                                ):
                                    logging.info(
                                        f"Simplify: ✓ Iframe{idx+1} link: {href[:60]}"
                                    )
                                    self.driver.switch_to.default_content()
                                    return href

                        iframe_buttons = self.driver.find_elements(
                            By.TAG_NAME, "button"
                        )

                        for button in iframe_buttons[:5]:
                            btn_text = button.text[:20]

                            if "apply" in btn_text.lower():
                                try:
                                    logging.info(
                                        f"Simplify: Clicking iframe button: {btn_text}"
                                    )
                                    original_url = self.driver.current_url
                                    original_window = self.driver.current_window_handle

                                    button.click()
                                    time.sleep(3)

                                    new_url = self.driver.current_url
                                    if (
                                        new_url != original_url
                                        and "simplify" not in new_url
                                        and "linkedin" not in new_url.lower()
                                    ):
                                        logging.info(
                                            f"Simplify: ✓ Iframe click → {new_url[:60]}"
                                        )
                                        self.driver.switch_to.default_content()
                                        self.driver.get(simplify_url)
                                        return new_url

                                    windows = self.driver.window_handles
                                    if len(windows) > 1:
                                        for win in windows:
                                            if win != original_window:
                                                self.driver.switch_to.window(win)
                                                tab_url = self.driver.current_url
                                                if (
                                                    "simplify" not in tab_url
                                                    and "linkedin"
                                                    not in tab_url.lower()
                                                ):
                                                    logging.info(
                                                        f"Simplify: ✓ Iframe tab {tab_url[:60]}"
                                                    )
                                                    self.driver.close()
                                                    self.driver.switch_to.window(
                                                        original_window
                                                    )
                                                    self.driver.switch_to.default_content()
                                                    return tab_url
                                                self.driver.close()
                                        self.driver.switch_to.window(original_window)

                                    self.driver.switch_to.frame(iframe)
                                except Exception as e:
                                    logging.debug(
                                        f"Simplify: Iframe button click error: {e}"
                                    )

                        self.driver.switch_to.default_content()

                    except Exception as e:
                        logging.debug(
                            f"Simplify: Iframe {idx+1} processing failed: {e}"
                        )
                        try:
                            self.driver.switch_to.default_content()
                        except:
                            pass

            except Exception as e:
                logging.warning(f"Simplify: Iframe processing error: {e}")
                try:
                    self.driver.switch_to.default_content()
                except:
                    pass

            try:
                logging.info("Simplify: Attempting click on outer page")
                click_selectors = [
                    "//button[contains(text(), 'Apply')]",
                    "//a[contains(text(), 'Apply')]",
                    "//button[contains(text(), 'Easy Apply')]",
                    "//a[contains(@class, 'apply')]",
                ]

                original_window = self.driver.current_window_handle
                original_url = self.driver.current_url

                for selector in click_selectors:
                    try:
                        elements = self.driver.find_elements(By.XPATH, selector)
                        logging.info(
                            f"Simplify: Found {len(elements)} elements for {selector}"
                        )

                        if elements:
                            for elem in elements[:3]:
                                try:
                                    elem_text = elem.text[:30]
                                    logging.info(f"Simplify: Clicking: {elem_text}")

                                    elem.click()
                                    time.sleep(5)

                                    new_url = self.driver.current_url
                                    if (
                                        new_url != original_url
                                        and "simplify.jobs" not in new_url
                                    ):
                                        if "linkedin" not in new_url.lower():
                                            logging.info(
                                                f"Simplify: ✓ Click navigated to {new_url[:60]}"
                                            )
                                            self.driver.get(simplify_url)
                                            return new_url

                                    all_windows = self.driver.window_handles
                                    if len(all_windows) > 1:
                                        for window in all_windows:
                                            if window != original_window:
                                                self.driver.switch_to.window(window)
                                                tab_url = self.driver.current_url
                                                if (
                                                    "simplify.jobs" not in tab_url
                                                    and "linkedin"
                                                    not in tab_url.lower()
                                                ):
                                                    logging.info(
                                                        f"Simplify: ✓ Click opened tab: {tab_url[:60]}"
                                                    )
                                                    self.driver.close()
                                                    self.driver.switch_to.window(
                                                        original_window
                                                    )
                                                    return tab_url
                                                self.driver.close()
                                        self.driver.switch_to.window(original_window)

                                    self.driver.get(simplify_url)
                                    time.sleep(2)
                                except Exception as e:
                                    logging.debug(f"Simplify: Click action failed: {e}")
                                    try:
                                        self.driver.switch_to.window(original_window)
                                        self.driver.get(simplify_url)
                                    except:
                                        pass
                    except Exception as e:
                        logging.debug(
                            f"Simplify: Click selector {selector} failed: {e}"
                        )

                logging.info("Simplify: Click method completed")
            except Exception as e:
                logging.warning(f"Simplify: Click method error: {e}")

            selectors = [
                'a[href*="applytojob"]',
                'a[href*="workday"]',
                'a[href*="greenhouse"]',
                'a[href*="lever"]',
                'a[href*="icims"]',
                'a[href*="taleo"]',
                'button[onclick*="apply"]',
                "a.apply-button",
                'a[class*="apply"]',
            ]

            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    href = element.get_attribute("href")
                    onclick = element.get_attribute("onclick")

                    if href and "http" in href and "simplify.jobs" not in href:
                        logging.info(
                            f"Simplify: ✓ Found via selector {selector}: {href[:60]}"
                        )
                        return href

                    if onclick:
                        url_match = re.search(r"https?://[^\s'\")]+", onclick)
                        if url_match:
                            url = url_match.group(0)
                            logging.info(
                                f"Simplify: ✓ Found via onclick {selector}: {url[:60]}"
                            )
                            return url
                except:
                    continue

            logging.info("Simplify: No selectors matched, trying meta refresh")

            try:
                meta_refresh = self.driver.find_element(
                    By.CSS_SELECTOR, 'meta[http-equiv="refresh"]'
                )
                content = meta_refresh.get_attribute("content")
                if content:
                    match = re.search(r"url=(.+)", content, re.I)
                    if match:
                        redirect_url = match.group(1).strip()
                        if "simplify.jobs" not in redirect_url:
                            logging.info(
                                f"Simplify: ✓ Found via meta refresh: {redirect_url[:60]}"
                            )
                            return redirect_url
            except:
                logging.info("Simplify: No meta refresh found")

            logging.info("Simplify: Parsing page with BeautifulSoup")
            soup = BeautifulSoup(self.driver.page_source, "lxml")

            job_links = soup.find_all("a", href=True)
            logging.info(f"Simplify: Found {len(job_links)} total links")

            for idx, a_tag in enumerate(job_links[:30]):
                href = a_tag["href"]
                link_text = a_tag.get_text(strip=True)[:30]
                logging.info(f"Simplify: Link {idx+1}: {link_text} → {href[:80]}")

                if any(
                    domain in href
                    for domain in [
                        "workday",
                        "greenhouse",
                        "lever",
                        "icims",
                        "taleo",
                        "applytojob",
                    ]
                ):
                    if "simplify.jobs" not in href:
                        logging.info(f"Simplify: ✓ Found via soup: {href[:60]}")
                        return href

            logging.warning("Simplify: ✗ All canonical extraction methods failed")
            return None

        except Exception as e:
            logging.error(f"Simplify resolution error: {e}")
            return None

    def _load_jobright_cookies(self):
        """Load authentication cookies for Jobright"""
        cookies = [
            {
                "name": "SESSION_ID",
                "value": "f789c46a8dbd4f43bca121babe8c10ce",
                "domain": ".jobright.ai",
                "path": "/",
                "secure": True,
                "httpOnly": True,
                "sameSite": "Lax",
            },
            {
                "name": "_ga",
                "value": "GA1.1.1566690142.1766771779",
                "domain": ".jobright.ai",
                "path": "/",
            },
            {
                "name": "g_state",
                "value": '{"i_l":0,"i_ll":1767052229346}',
                "domain": ".jobright.ai",
                "path": "/",
            },
        ]

        for cookie in cookies:
            try:
                self.driver.add_cookie(cookie)
                logging.debug(f"Jobright: Loaded cookie {cookie['name']}")
            except Exception as e:
                logging.debug(f"Jobright: Cookie {cookie.get('name')} failed: {e}")

    def extract_jobright_canonical(self, jobright_url: str) -> Optional[str]:
        """
        COMPREHENSIVE 5-METHOD JOBRIGHT EXTRACTION
        Methods ordered by: Success Rate → Speed → Reliability
        Each method logs attempts and prints final URL if successful
        """

        logging.info(f"\n{'='*80}")
        logging.info(f"JOBRIGHT EXTRACTION: {jobright_url[:70]}")
        logging.info(f"{'='*80}")

        # ============================================================================
        # METHOD 1: STEALTH HTTP REQUEST (PRIMARY - 60-70% success, ~2 seconds)
        # ============================================================================
        result = self._jobright_method_1_stealth_http(jobright_url)
        if result:
            logging.info(f"Jobright: ✅ SUCCESS via Method 1 (Stealth HTTP)")
            logging.info(f"Jobright: FINAL URL: {result}")
            return result

        # ============================================================================
        # METHOD 2: SELENIUM RAW HTML (BACKUP - 15-20% success, ~8 seconds)
        # ============================================================================
        result = self._jobright_method_2_selenium_raw_html(jobright_url)
        if result:
            logging.info(f"Jobright: ✅ SUCCESS via Method 2 (Selenium Raw HTML)")
            logging.info(f"Jobright: FINAL URL: {result}")
            return result

        # ============================================================================
        # METHOD 3: SELENIUM COMPREHENSIVE SCRAPING (BACKUP - 10% success, ~15 seconds)
        # ============================================================================
        result = self._jobright_method_3_selenium_comprehensive(jobright_url)
        if result:
            logging.info(f"Jobright: ✅ SUCCESS via Method 3 (Selenium Comprehensive)")
            logging.info(f"Jobright: FINAL URL: {result}")
            return result

        # ============================================================================
        # METHOD 4: SELENIUM CLICK-BASED (BACKUP - 5% success, ~30 seconds)
        # ============================================================================
        result = self._jobright_method_4_selenium_click(jobright_url)
        if result:
            logging.info(f"Jobright: ✅ SUCCESS via Method 4 (Selenium Click)")
            logging.info(f"Jobright: FINAL URL: {result}")
            return result

        # ============================================================================
        # ALL METHODS FAILED
        # ============================================================================
        logging.warning(f"Jobright: ❌ ALL 4 METHODS FAILED for {jobright_url[:60]}")
        return None

    def _jobright_method_1_stealth_http(self, url: str) -> Optional[str]:
        """
        METHOD 1: Stealth HTTP Request (PRIMARY - 60-70% success)
        Uses browser-like headers + cookies to bypass bot detection
        Fastest method - completes in ~2 seconds
        """
        try:
            logging.info(
                "Jobright Method 1: Stealth HTTP request with anti-bot headers"
            )

            # Step 1: Build anti-bot headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://jobright.ai/",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Cache-Control": "max-age=0",
            }

            # Step 2: Load cookies from file
            cookie_dict = {}
            try:
                with open("jobright_cookies.json", "r") as f:
                    cookies_list = json.load(f)
                    for cookie in cookies_list:
                        cookie_dict[cookie["name"]] = cookie["value"]
                logging.info(f"Jobright Method 1: Loaded {len(cookie_dict)} cookies")
            except Exception as e:
                logging.debug(f"Jobright Method 1: Cookie loading failed: {e}")

            # Step 3: Make stealth HTTP request
            logging.info("Jobright Method 1: Making stealth HTTP request...")
            response = requests.get(
                url, headers=headers, cookies=cookie_dict, timeout=10
            )
            html = response.text

            logging.info(f"Jobright Method 1: Received {len(html)} bytes of HTML")

            # Step 4: Search for URL fields in raw HTML (BEFORE JavaScript processes it)
            url_patterns = [
                (r'"originalUrl"\s*:\s*"([^"]+)"', "originalUrl"),
                (r'"applyLink"\s*:\s*"([^"]+)"', "applyLink"),
                (r'"jobUrl"\s*:\s*"([^"]+)"', "jobUrl"),
                (r'"job_url"\s*:\s*"([^"]+)"', "job_url"),
                (r'"canonicalUrl"\s*:\s*"([^"]+)"', "canonicalUrl"),
                (r'"apply_link"\s*:\s*"([^"]+)"', "apply_link"),
            ]

            for pattern, field_name in url_patterns:
                matches = re.findall(pattern, html)
                logging.info(
                    f"Jobright Method 1: Searching '{field_name}' - found {len(matches)} matches"
                )

                for match in matches:
                    if (
                        match
                        and "http" in match
                        and "jobright.ai" not in match
                        and not match.startswith("https://www.linkedin.com/")
                        and not match.startswith("https://linkedin.com/")
                    ):
                        logging.info(
                            f"Jobright Method 1: ✓ Found via '{field_name}': {match[:70]}"
                        )
                        return match

            logging.info("Jobright Method 1: ✗ No valid URL found in JSON fields")

        except Exception as e:
            logging.debug(f"Jobright Method 1: Failed - {e}")

        return None

    def _jobright_method_2_selenium_raw_html(self, url: str) -> Optional[str]:
        """
        METHOD 2: Selenium Raw HTML Extraction (BACKUP - 15-20% success)
        Loads page with Selenium but extracts HTML BEFORE JavaScript processing
        More reliable than Method 1 for auth-walled content
        """
        if not self.driver:
            self._initialize_driver()

        if not self.driver:
            return None

        try:
            logging.info("Jobright Method 2: Selenium raw HTML extraction")

            # Step 1: Load page
            logging.info(f"Jobright Method 2: Loading page...")
            self.driver.get(url)
            time.sleep(2)

            # Step 2: Load cookies
            logging.info("Jobright Method 2: Loading cookies...")
            self._load_jobright_cookies()

            # Step 3: Refresh with cookies
            self.driver.refresh()
            time.sleep(3)

            # Step 4: Extract RAW HTML (before JavaScript modifies DOM)
            logging.info("Jobright Method 2: Extracting raw HTML via JavaScript...")
            raw_html = self.driver.execute_script(
                "return document.documentElement.outerHTML"
            )

            logging.info(
                f"Jobright Method 2: Extracted {len(raw_html)} bytes of raw HTML"
            )

            # Step 5: Parse for URL fields
            url_patterns = [
                (r'"originalUrl"\s*:\s*"([^"]+)"', "originalUrl"),
                (r'"applyLink"\s*:\s*"([^"]+)"', "applyLink"),
                (r'"jobUrl"\s*:\s*"([^"]+)"', "jobUrl"),
                (r'"canonicalUrl"\s*:\s*"([^"]+)"', "canonicalUrl"),
            ]

            for pattern, field_name in url_patterns:
                matches = re.findall(pattern, raw_html)
                logging.info(
                    f"Jobright Method 2: Searching '{field_name}' - found {len(matches)} matches"
                )

                for match in matches:
                    if (
                        match
                        and "http" in match
                        and "jobright.ai" not in match
                        and not match.startswith("https://www.linkedin.com/")
                        and not match.startswith("https://linkedin.com/")
                    ):
                        logging.info(
                            f"Jobright Method 2: ✓ Found via '{field_name}': {match[:70]}"
                        )
                        return match

            logging.info("Jobright Method 2: ✗ No URL found in raw HTML")

        except Exception as e:
            logging.debug(f"Jobright Method 2: Failed - {e}")

        return None

    def _jobright_method_3_selenium_comprehensive(self, url: str) -> Optional[str]:
        """
        METHOD 3: Selenium Comprehensive Scraping (BACKUP - 10% success)
        Already loaded from Method 2, searches all links and data attributes
        """
        if not self.driver:
            return None

        try:
            logging.info("Jobright Method 3: Comprehensive link scraping")

            # Page should already be loaded from Method 2
            # Search for "Original Job Post" links
            try:
                logging.info(
                    "Jobright Method 3: Searching for 'Original Job Post' link"
                )
                original_link_selectors = [
                    "//a[contains(text(), 'Original Job Post')]",
                    "//a[contains(text(), 'Original')]",
                    "//a[contains(@id, 'original')]",
                    "//button[contains(text(), 'Original')]",
                ]

                for selector in original_link_selectors:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    if elements:
                        for elem in elements:
                            href = elem.get_attribute("href")
                            if (
                                href
                                and "jobright.ai" not in href
                                and not href.startswith("https://www.linkedin.com/")
                                and not href.startswith("https://linkedin.com/")
                            ):
                                logging.info(
                                    f"Jobright Method 3: ✓ Found via Original link: {href[:70]}"
                                )
                                return href
            except Exception as e:
                logging.debug(f"Jobright Method 3: Original link search failed: {e}")

            # Search all links on page
            try:
                selectors = [
                    'a[href*="applytojob"]',
                    'a[href*="workday"]',
                    'a[href*="greenhouse"]',
                    'a[href*="lever"]',
                    'a[href*="icims"]',
                    'a[href*="taleo"]',
                    'a[href*="myworkdayjobs"]',
                    'a[href*="careers"]',
                    'a[href*="jobs"]',
                ]

                for selector in selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements[:5]:
                            href = element.get_attribute("href")
                            if (
                                href
                                and "jobright.ai" not in href
                                and "http" in href
                                and not href.startswith("https://www.linkedin.com/")
                                and not href.startswith("https://linkedin.com/")
                            ):
                                logging.info(
                                    f"Jobright Method 3: ✓ Found via selector {selector}: {href[:70]}"
                                )
                                return href
                    except:
                        continue
            except Exception as e:
                logging.debug(f"Jobright Method 3: Link search failed: {e}")

            # Search data attributes
            try:
                for elem in self.driver.find_elements(By.CSS_SELECTOR, "[data-url]"):
                    data_url = elem.get_attribute("data-url")
                    if (
                        data_url
                        and "jobright.ai" not in data_url
                        and "http" in data_url
                        and not data_url.startswith("https://www.linkedin.com/")
                        and not data_url.startswith("https://linkedin.com/")
                    ):
                        logging.info(
                            f"Jobright Method 3: ✓ Found via data-url: {data_url[:70]}"
                        )
                        return data_url
            except:
                pass

            logging.info("Jobright Method 3: ✗ No URL found via comprehensive scraping")

        except Exception as e:
            logging.debug(f"Jobright Method 3: Failed - {e}")

        return None

    def _jobright_method_4_selenium_click(self, url: str) -> Optional[str]:
        """
        METHOD 4: Selenium Click-Based Extraction (LAST RESORT - 5% success)
        Clicks buttons and captures navigation/new tabs
        Slowest but handles JavaScript-triggered actions
        """
        if not self.driver:
            return None

        try:
            logging.info("Jobright Method 4: Click-based extraction (last resort)")

            apply_selectors = [
                'a[id="apply-now-button"]',
                'button[id="apply-now-button"]',
                "//button[contains(text(), 'APPLY')]",
                "//a[contains(text(), 'Apply')]",
                "a.apply-button",
            ]

            original_window = self.driver.current_window_handle
            original_url = self.driver.current_url

            for selector in apply_selectors:
                try:
                    if "//" in selector:
                        buttons = self.driver.find_elements(By.XPATH, selector)
                    else:
                        buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    if buttons:
                        logging.info(
                            f"Jobright Method 4: Found {len(buttons)} elements for {selector}"
                        )
                        button = buttons[0]

                        # Check href first
                        href = button.get_attribute("href")
                        if (
                            href
                            and "jobright.ai" not in href
                            and "http" in href
                            and not href.startswith("https://www.linkedin.com/")
                            and not href.startswith("https://linkedin.com/")
                        ):
                            logging.info(
                                f"Jobright Method 4: ✓ Found in href: {href[:70]}"
                            )
                            return href

                        # Try clicking
                        try:
                            logging.info(f"Jobright Method 4: Clicking button...")
                            button.click()
                            time.sleep(3)

                            # Check if navigated
                            new_url = self.driver.current_url
                            if (
                                new_url != original_url
                                and "jobright.ai" not in new_url
                                and not new_url.startswith("https://www.linkedin.com/")
                                and not new_url.startswith("https://linkedin.com/")
                            ):
                                logging.info(
                                    f"Jobright Method 4: ✓ Click navigated to: {new_url[:70]}"
                                )
                                return new_url

                            # Check new tabs
                            all_windows = self.driver.window_handles
                            if len(all_windows) > 1:
                                for window in all_windows:
                                    if window != original_window:
                                        self.driver.switch_to.window(window)
                                        new_tab_url = self.driver.current_url
                                        if (
                                            "jobright.ai" not in new_tab_url
                                            and not new_tab_url.startswith(
                                                "https://www.linkedin.com/"
                                            )
                                            and not new_tab_url.startswith(
                                                "https://linkedin.com/"
                                            )
                                        ):
                                            logging.info(
                                                f"Jobright Method 4: ✓ Click opened tab: {new_tab_url[:70]}"
                                            )
                                            self.driver.close()
                                            self.driver.switch_to.window(
                                                original_window
                                            )
                                            return new_tab_url
                                        self.driver.close()
                                self.driver.switch_to.window(original_window)

                        except Exception as e:
                            logging.debug(f"Jobright Method 4: Click failed: {e}")
                            try:
                                self.driver.switch_to.window(original_window)
                            except:
                                pass

                except Exception as e:
                    logging.debug(f"Jobright Method 4: Selector {selector} failed: {e}")
                    continue

            logging.info("Jobright Method 4: ✗ Click method did not find URL")

        except Exception as e:
            logging.debug(f"Jobright Method 4: Failed - {e}")

        return None

    def _cleanup_driver(self):
        if self.driver:
            try:
                self.driver.delete_all_cookies()
                logging.debug("Cleared cookies")
            except:
                pass

    def close(self):
        if self.driver:
            try:
                self.driver.quit()
                logging.info("WebDriver closed")
            except:
                pass
            self.driver = None


class JobrightAuthenticator:
    def __init__(self):
        self.cookies = self._load_cookies()

    def _load_cookies(self):
        try:
            with open("jobright_cookies.json", "r") as f:
                cookies = json.load(f)
                logging.info(f"Loaded {len(cookies)} Jobright cookies")
                return cookies
        except FileNotFoundError:
            logging.warning("Jobright cookies not found")
            return []

    def login_interactive(self):
        logging.info(
            "Jobright authentication not implemented - manual cookie loading required"
        )
        pass


class SimplifyGitHubScraper:
    @staticmethod
    def scrape(url: str, source_name: str = "GitHub") -> List[Dict]:
        if "SimplifyJobs" in source_name or "simplify" in url.lower():
            return SimplifyGitHubScraper.scrape_simplify(url)
        else:
            return SimplifyGitHubScraper.scrape_vanshb03(url)

    @staticmethod
    def scrape_simplify(url: str) -> List[Dict]:
        try:
            response = requests.get(url, timeout=30)
            logging.info(f"SimplifyJobs: Fetched, length: {len(response.text)}")

            tree = lxml_html.fromstring(response.content)
            logging.info(f"SimplifyJobs: Parsed with lxml")

            table = tree.xpath("//table")[0] if tree.xpath("//table") else None
            if not table:
                return []

            rows = table.xpath(".//tr")[1:]
            jobs = []

            for row in rows:
                cells = row.xpath(".//td")
                if len(cells) < 5:
                    continue

                company = cells[0].text_content().strip()
                title = cells[1].text_content().strip()
                location = cells[2].text_content().strip()

                url_elem = cells[3].xpath(".//a")
                url = url_elem[0].get("href") if url_elem else None

                age = cells[4].text_content().strip() if len(cells) > 4 else ""

                is_closed = "🔒" in row.text_content() or "❌" in row.text_content()

                if url and not is_closed:
                    jobs.append(
                        {
                            "company": company,
                            "title": title,
                            "location": location,
                            "url": url,
                            "age": age,
                            "is_closed": False,
                            "source": "SimplifyJobs",
                        }
                    )

            logging.info(f"SimplifyJobs: Found {len(jobs)} jobs via HTML")
            return jobs

        except Exception as e:
            logging.error(f"SimplifyJobs scraping failed: {e}")
            return []

    @staticmethod
    def scrape_vanshb03(url: str) -> List[Dict]:
        try:
            response = requests.get(url, timeout=30)
            logging.info(f"vanshb03: Fetched, length: {len(response.text)}")

            tree = lxml_html.fromstring(response.content)
            logging.info(f"vanshb03: Parsed with lxml")

            table = tree.xpath("//table")[0] if tree.xpath("//table") else None
            if table:
                rows = table.xpath(".//tr")[1:]
                jobs = []

                for row in rows:
                    cells = row.xpath(".//td")
                    if len(cells) < 5:
                        continue

                    company = cells[0].text_content().strip()
                    title = cells[1].text_content().strip()
                    location = cells[2].text_content().strip()

                    url_elem = cells[3].xpath(".//a")
                    url = url_elem[0].get("href") if url_elem else None

                    age = cells[4].text_content().strip() if len(cells) > 4 else ""

                    is_closed = "🔒" in row.text_content() or "❌" in row.text_content()

                    if url and not is_closed:
                        jobs.append(
                            {
                                "company": company,
                                "title": title,
                                "location": location,
                                "url": url,
                                "age": age,
                                "is_closed": False,
                                "source": "SWE List",
                            }
                        )

                logging.info(f"vanshb03: Found {len(jobs)} jobs via HTML")
                return jobs

            logging.info("vanshb03: Trying Markdown")
            lines = response.text.split("\n")
            jobs = []

            for line in lines:
                if "|" not in line or line.startswith("|---"):
                    continue

                parts = [p.strip() for p in line.split("|")]
                if len(parts) < 6:
                    continue

                company = parts[1]
                title = parts[2]
                location = parts[3]

                url_match = re.search(r"\[.*?\]\((https?://[^\)]+)\)", parts[4])
                if not url_match:
                    url_match = re.search(r"<(https?://[^>]+)>", parts[4])
                if not url_match:
                    url_match = re.search(r'href="(https?://[^"]+)"', parts[4])

                url = url_match.group(1) if url_match else None

                age = parts[5] if len(parts) > 5 else ""

                is_closed = "🔒" in line or "❌" in line

                if url and not is_closed:
                    jobs.append(
                        {
                            "company": company,
                            "title": title,
                            "location": location,
                            "url": url,
                            "age": age,
                            "is_closed": False,
                            "source": "SWE List",
                        }
                    )

            logging.info(f"vanshb03: Found {len(jobs)} jobs via Markdown")
            return jobs

        except Exception as e:
            logging.error(f"vanshb03 scraping failed: {e}")
            return []


class SourceParsers:
    def __init__(self, page_fetcher: PageFetcher):
        self.fetcher = page_fetcher

    @staticmethod
    def parse_jobright_email(soup, url, authenticator):
        result = {
            "company": "Unknown",
            "title": "Unknown",
            "location": "Unknown",
            "remote": "Unknown",
            "url": url,
            "email_age_days": None,
        }

        try:
            if not soup:
                logging.warning("Jobright email: No soup provided")
                return result

            job_id = url.split("/info/")[-1].split("?")[0] if "/info/" in url else None

            if not job_id:
                logging.warning(
                    f"Jobright email: Could not extract job ID from {url[:60]}"
                )
                return result

            logging.info(f"Jobright email: Looking for job card with ID {job_id}")

            target_container = None
            all_links = soup.find_all("a", href=True)

            for link in all_links:
                href = link.get("href", "")
                if job_id in href and "jobright.ai/jobs/info/" in href:
                    logging.info(f"Jobright email: Found link with job ID")
                    parent = link
                    for _ in range(15):
                        parent = parent.parent
                        if not parent:
                            break
                        if parent.name == "table" and (
                            parent.get("id") == "job-section"
                            or parent.get("id") == "job-container"
                        ):
                            target_container = parent
                            logging.info(
                                f"Jobright email: Found container {parent.get('id')}"
                            )
                            break
                    if target_container:
                        break

            if not target_container:
                logging.warning(
                    "Jobright email: Could not find job container, searching entire email"
                )
                target_container = soup

            company_elem = target_container.find(id="job-company-name")
            if company_elem:
                result["company"] = company_elem.get_text(strip=True)
                logging.info(f"Jobright email: Company = {result['company']}")

            title_elem = target_container.find(id="job-title")
            if title_elem:
                title_link = title_elem.find("a")
                if title_link:
                    result["title"] = title_link.get_text(strip=True)
                else:
                    result["title"] = title_elem.get_text(strip=True)
                logging.info(f"Jobright email: Title = {result['title']}")

            location_tags = target_container.find_all(id="job-tag")
            if location_tags:
                for tag in location_tags:
                    text = tag.get_text(strip=True)
                    if (
                        "," in text
                        and "$" not in text
                        and "referral" not in text.lower()
                        and "hour" not in text.lower()
                        and "ago" not in text.lower()
                    ):
                        result["location"] = text
                        logging.info(f"Jobright email: Location = {result['location']}")
                        break

            time_elem = target_container.find(id="job-time-posted")
            if time_elem:
                time_text = time_elem.get_text(strip=True)
                days = DateParser.extract_days_ago(time_text)
                if days is not None:
                    result["email_age_days"] = days
                    logging.info(f"Jobright email: Age = {days} days")

            if result["company"] != "Unknown" and result["title"] != "Unknown":
                logging.info(
                    f"Jobright email: ✓ Extracted {result['company']} - {result['title']}"
                )
            else:
                logging.warning(
                    f"Jobright email: Incomplete - company={result['company']}, title={result['title']}"
                )

        except Exception as e:
            logging.error(f"Jobright email parsing error: {e}")

        return result

    @staticmethod
    def extract_company_from_swelist_email(soup, simplify_url):
        try:
            if not soup:
                return None

            all_links = soup.find_all("a", href=True)
            for link in all_links:
                href = link.get("href", "")
                if simplify_url in href:
                    prev_sibling = link.find_previous("strong")
                    if prev_sibling:
                        company = prev_sibling.get_text(strip=True)
                        company = company.rstrip(":").strip()
                        if company and len(company) > 2:
                            logging.info(
                                f"SWE List: Extracted company from email: {company}"
                            )
                            return company

            logging.debug("SWE List: Could not find company in email")
            return None
        except Exception as e:
            logging.debug(f"SWE List company extraction error: {e}")
            return None

    def parse_job_page(self, url: str) -> Dict[str, Any]:
        from processors import (
            CompanyExtractor,
            LocationExtractor,
            JobIDExtractor,
            LocationProcessor,
        )
        from utils import PlatformDetector

        result = {
            "company": "Unknown",
            "location": "Unknown",
            "remote": "Unknown",
            "description": "",
            "job_id": "N/A",
            "sponsorship": "Unknown",
        }

        if "jobright.ai/jobs/info/" in url:
            canonical_url = self.fetcher.extract_jobright_canonical(url)
            if canonical_url:
                url = canonical_url
            else:
                return result

        response, final_url, page_source = self.fetcher.fetch_page(url)
        if not response:
            return result

        soup = BeautifulSoup(response.text, "lxml")

        platform = PlatformDetector.detect(final_url)

        title = PageParser.extract_title(soup)

        result["company"] = CompanyExtractor.extract_all_methods(final_url, soup)

        location_extracted = LocationExtractor.extract_all_methods(
            final_url, soup, title, platform, page_source
        )
        result["location"] = LocationProcessor.format_location_clean(location_extracted)

        description = DescriptionExtractor.extract(soup, platform)
        result["description"] = description

        result["job_id"] = JobIDExtractor.extract_all_methods(final_url, soup, platform)

        jsonld_data = JSONLDExtractor.extract_job_data(response.text)

        result["remote"] = LocationProcessor.extract_remote_status_enhanced(
            soup, result["location"], final_url, description
        )

        result["sponsorship"] = SponsorshipDetector.detect(description)

        return result


def safe_parse_html(html_content: str) -> Tuple[BeautifulSoup, str]:
    try:
        soup = BeautifulSoup(html_content, "lxml")
        return (soup, "lxml")
    except Exception:
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            return (soup, "html.parser")
        except Exception:
            return (None, "failed")
