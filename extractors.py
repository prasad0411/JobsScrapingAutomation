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
                (r"Posted\s+(\d+)\s+days?\s+ago", lambda m: int(m.group(1))),
                (r"Posted\s+(\d+)d\s+ago", lambda m: int(m.group(1))),
                (r"(\d+)\s+days?\s+ago", lambda m: int(m.group(1))),
                (r"Posted:\s*(\d+)\s+day", lambda m: int(m.group(1))),
                (r"Posted\s+today", lambda m: 0),
                (r"Posted\s+yesterday", lambda m: 1),
            ]

            for pattern, extractor in patterns:
                match = re.search(pattern, page_text, re.I)
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
    _failed_urls = set()
    _cache_loaded = False

    @classmethod
    def _load_failed_cache(cls):
        if cls._cache_loaded:
            return
        try:
            import os

            if os.path.exists(FAILED_SIMPLIFY_CACHE):
                with open(FAILED_SIMPLIFY_CACHE, "r") as f:
                    data = json.load(f)
                    cls._failed_urls = set(data.get("failed_urls", []))
                    logging.info(
                        f"Loaded {len(cls._failed_urls)} failed Simplify URLs from cache"
                    )
        except Exception as e:
            logging.debug(f"Failed to load Simplify cache: {e}")
        cls._cache_loaded = True

    @classmethod
    def _save_failed_cache(cls):
        try:
            with open(FAILED_SIMPLIFY_CACHE, "w") as f:
                json.dump({"failed_urls": list(cls._failed_urls)}, f)
        except Exception as e:
            logging.debug(f"Failed to save Simplify cache: {e}")

    @classmethod
    def resolve(cls, simplify_url, page_fetcher=None) -> Tuple[Optional[str], bool]:
        cls._load_failed_cache()

        if simplify_url in cls._failed_urls:
            logging.debug(f"Simplify URL in failed cache: {simplify_url}")
            return None, False

        if not page_fetcher:
            from extractors import PageFetcher

            page_fetcher = PageFetcher()

        try:
            actual_url = page_fetcher.resolve_simplify_url(simplify_url)
            if actual_url and "simplify.jobs" not in actual_url.lower():
                logging.info(f"Simplify HTTP: {actual_url[:80]}")
                return actual_url, True
            else:
                cls._failed_urls.add(simplify_url)
                cls._save_failed_cache()
                logging.warning(f"All methods failed: {simplify_url}")
                return None, False
        except Exception as e:
            cls._failed_urls.add(simplify_url)
            cls._save_failed_cache()
            logging.warning(f"Simplify resolution error for {simplify_url}: {e}")
            return None, False


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
            after_date = (today - timedelta(days=1)).strftime("%Y/%m/%d")

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
            return None

        try:
            self.driver.get(simplify_url)

            try:
                WebDriverWait(self.driver, 15).until(
                    lambda d: "simplify.jobs" not in d.current_url
                )
            except TimeoutException:
                logging.debug("Simplify redirect timeout")

            current_url = self.driver.current_url
            if "simplify.jobs" not in current_url.lower():
                return current_url

            selectors = [
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
                        return href

                    if onclick:
                        url_match = re.search(r"https?://[^\s'\")]+", onclick)
                        if url_match:
                            return url_match.group(0)
                except:
                    continue

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
                            return redirect_url
            except:
                pass

            soup = BeautifulSoup(self.driver.page_source, "lxml")
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if any(
                    domain in href
                    for domain in ["workday", "greenhouse", "lever", "icims", "taleo"]
                ):
                    if "simplify.jobs" not in href:
                        return href

            return None

        except Exception as e:
            logging.warning(f"Simplify resolution failed for {simplify_url}: {e}")
            return None

    def extract_jobright_canonical(self, jobright_url: str) -> Optional[str]:
        if not self.driver:
            self._initialize_driver()

        if not self.driver:
            return None

        try:
            self.driver.get(jobright_url)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                logging.debug("Timeout loading Jobright page")

            time.sleep(5)

            selectors = [
                'a[href*="workday"]',
                'a[href*="greenhouse"]',
                'a[href*="lever"]',
                'a[href*="icims"]',
                'a[href*="taleo"]',
                'a[href*="myworkdayjobs"]',
                "a.apply-button",
                'a[class*="apply"]',
                'a[href*="job"]',
            ]

            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        href = element.get_attribute("href")
                        onclick = element.get_attribute("onclick")

                        if href and "jobright.ai" not in href and "http" in href:
                            if "linkedin" not in href.lower():
                                return href

                        if onclick:
                            url_match = re.search(r"https?://[^\s'\")]+", onclick)
                            if url_match:
                                url = url_match.group(0)
                                if (
                                    "jobright.ai" not in url
                                    and "linkedin" not in url.lower()
                                ):
                                    return url
                except:
                    continue

            try:
                for elem in self.driver.find_elements(By.CSS_SELECTOR, "[data-url]"):
                    data_url = elem.get_attribute("data-url")
                    if (
                        data_url
                        and "jobright.ai" not in data_url
                        and "http" in data_url
                    ):
                        if "linkedin" not in data_url.lower():
                            return data_url

                for elem in self.driver.find_elements(
                    By.CSS_SELECTOR, "[data-job-url]"
                ):
                    data_url = elem.get_attribute("data-job-url")
                    if (
                        data_url
                        and "jobright.ai" not in data_url
                        and "http" in data_url
                    ):
                        if "linkedin" not in data_url.lower():
                            return data_url

                for elem in self.driver.find_elements(
                    By.CSS_SELECTOR, "[data-original-url]"
                ):
                    data_url = elem.get_attribute("data-original-url")
                    if (
                        data_url
                        and "jobright.ai" not in data_url
                        and "http" in data_url
                    ):
                        if "linkedin" not in data_url.lower():
                            return data_url
            except:
                pass

            try:
                script_tags = self.driver.find_elements(By.TAG_NAME, "script")
                for script in script_tags:
                    text = script.get_attribute("innerHTML")
                    if text:
                        if "original_url" in text:
                            match = re.search(r'"original_url"\s*:\s*"([^"]+)"', text)
                            if match:
                                url = match.group(1)
                                if "linkedin" not in url.lower():
                                    return url
                        if "jobUrl" in text:
                            match = re.search(r'"jobUrl"\s*:\s*"([^"]+)"', text)
                            if match:
                                url = match.group(1)
                                if (
                                    "jobright.ai" not in url
                                    and "linkedin" not in url.lower()
                                ):
                                    return url
            except:
                pass

            try:
                meta_refresh = self.driver.find_element(
                    By.CSS_SELECTOR, 'meta[http-equiv="refresh"]'
                )
                content = meta_refresh.get_attribute("content")
                if content:
                    match = re.search(r"url=(.+)", content, re.I)
                    if match:
                        redirect_url = match.group(1).strip()
                        if (
                            "jobright.ai" not in redirect_url
                            and "linkedin" not in redirect_url.lower()
                        ):
                            return redirect_url
            except:
                pass

            soup = BeautifulSoup(self.driver.page_source, "lxml")

            for elem in soup.find_all(attrs={"data-url": True}):
                url = elem.get("data-url")
                if url and "jobright.ai" not in url and "http" in url:
                    if "linkedin" not in url.lower():
                        return url

            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "jobright.ai" not in href and any(
                    d in href for d in ["workday", "greenhouse", "lever", "icims"]
                ):
                    if href.startswith("http") and "linkedin" not in href.lower():
                        return href

            return None

        except Exception as e:
            logging.warning(f"Jobright canonical extraction failed: {e}")
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
            if soup:
                title_elem = soup.find("h2")
                if not title_elem:
                    title_elem = soup.find("h3")
                if title_elem:
                    result["title"] = title_elem.get_text(strip=True)

                company_elem = soup.find(string=re.compile(r"Company:", re.I))
                if company_elem:
                    parent = company_elem.parent
                    if parent:
                        company_text = parent.get_text(strip=True)
                        company = company_text.replace("Company:", "").strip()
                        if company:
                            result["company"] = company

                location_elem = soup.find(string=re.compile(r"Location:", re.I))
                if location_elem:
                    parent = location_elem.parent
                    if parent:
                        location_text = parent.get_text(strip=True)
                        location = location_text.replace("Location:", "").strip()
                        if location:
                            result["location"] = location

                date_elem = soup.find(string=re.compile(r"Posted:", re.I))
                if date_elem:
                    parent = date_elem.parent
                    if parent:
                        date_text = parent.get_text(strip=True)
                        days = DateParser.extract_days_ago(date_text)
                        if days is not None:
                            result["email_age_days"] = days

        except Exception as e:
            logging.debug(f"Jobright email parsing error: {e}")

        return result

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
