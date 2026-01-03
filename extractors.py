#!/usr/bin/env python3
# cSpell:disable
"""
Extraction module for job data from various sources.
Handles web scraping, email parsing, and Jobright authentication.
ENHANCED: Markdown table parser for vanshb03, platform-specific job ID extraction
"""

import requests
import base64
import pickle
import os
import json
import time
import random
import re
from bs4 import BeautifulSoup
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    from selenium.webdriver.common.action_chains import ActionChains

    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

try:
    import undetected_chromedriver as uc

    UNDETECTED_CHROME_AVAILABLE = True
except ImportError:
    UNDETECTED_CHROME_AVAILABLE = False

import datetime

from config import (
    USER_AGENTS,
    GMAIL_CREDS_FILE,
    GMAIL_TOKEN_FILE,
    GMAIL_SCOPES,
    JOB_BOARD_DOMAINS,
    JOBRIGHT_COOKIES_FILE,
    HANDSHAKE_COOKIES_FILE,
    HANDSHAKE_CONFIG,
    US_STATES,
    CANADA_PROVINCES,
    SPECIAL_COMPANY_NAMES,
)


class JobrightAuthenticator:
    """Handles Jobright authentication and URL resolution."""

    def __init__(self):
        self.cookies = None
        self.load_cookies()

    def load_cookies(self):
        """Load saved cookies if available."""
        if os.path.exists(JOBRIGHT_COOKIES_FILE):
            try:
                with open(JOBRIGHT_COOKIES_FILE, "r") as f:
                    self.cookies = json.load(f)
                print(f"✓ Loaded {len(self.cookies)} Jobright cookies")
            except Exception as e:
                print(f"Cookie load error: {e}")

    def login_interactive(self):
        """Interactive login to Jobright using Selenium."""
        if not SELENIUM_AVAILABLE:
            print("Selenium not available - skipping Jobright authentication")
            return False

        print("\n" + "=" * 60)
        print("JOBRIGHT AUTHENTICATION")
        print("=" * 60)

        driver = None
        try:
            chrome_options = Options()
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option(
                "excludeSwitches", ["enable-logging"]
            )

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)

            driver.get("https://jobright.ai")
            time.sleep(3)

            print("[AUTH] Please log in through the browser window")
            print("       Press ENTER after completing login...")
            input()

            cookies = driver.get_cookies()
            if not cookies:
                print("✗ No cookies captured")
                driver.quit()
                return False

            self.cookies = cookies

            with open(JOBRIGHT_COOKIES_FILE, "w") as f:
                json.dump(cookies, f, indent=2)

            print(f"✓ Authentication successful ({len(cookies)} cookies saved)\n")
            driver.quit()
            return True

        except Exception as e:
            print(f"Authentication failed: {e}")
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            return False

    def resolve_jobright_url(self, jobright_url):
        """Resolve Jobright URL to actual company job page."""
        if "jobright.ai/jobs/info/" not in jobright_url.lower():
            return jobright_url, False

        if not self.cookies:
            return jobright_url, False

        try:
            session = requests.Session()
            for cookie in self.cookies:
                session.cookies.set(cookie["name"], cookie["value"])

            headers = {"User-Agent": USER_AGENTS[0]}
            response = session.get(jobright_url, headers=headers, timeout=15)

            if response.status_code != 200:
                return jobright_url, False

            soup = BeautifulSoup(response.content, "html.parser")
            script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

            if not script_tag:
                return jobright_url, False

            data = json.loads(script_tag.string)
            job_result = (
                data.get("props", {})
                .get("pageProps", {})
                .get("dataSource", {})
                .get("jobResult", {})
            )

            actual_url = job_result.get("applyLink") or job_result.get("originalUrl")
            is_company_site = job_result.get("isCompanySiteLink", False)

            if actual_url and "jobright.ai" not in actual_url:
                return actual_url, is_company_site

            return jobright_url, False
        except:
            return jobright_url, False


class EmailExtractor:
    """Extracts job postings from Gmail using Gmail API."""

    def __init__(self):
        self.service = None

    def authenticate(self):
        """Authenticate with Gmail API."""
        creds = None

        if os.path.exists(GMAIL_TOKEN_FILE):
            with open(GMAIL_TOKEN_FILE, "rb") as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    GMAIL_CREDS_FILE, GMAIL_SCOPES
                )
                creds = flow.run_local_server(port=0)

            with open(GMAIL_TOKEN_FILE, "wb") as token:
                pickle.dump(creds, token)

        self.service = build("gmail", "v1", credentials=creds)

    def fetch_job_emails(self, max_results=50):
        """Fetch job emails from last 24 hours with 'Job Hunt' label."""
        if not self.service:
            self.authenticate()

        try:
            query = 'label:"Job Hunt" newer_than:1d'
            results = (
                self.service.users()
                .messages()
                .list(userId="me", q=query, maxResults=max_results)
                .execute()
            )

            messages = results.get("messages", [])
            if not messages:
                print("No labeled emails found")
                return []

            print(f"Found {len(messages)} labeled emails")

            # Fetch all emails with timestamps
            emails_with_time = []
            for message in messages:
                msg = (
                    self.service.users()
                    .messages()
                    .get(userId="me", id=message["id"], format="full")
                    .execute()
                )

                # Get email timestamp (milliseconds since epoch)
                internal_date = int(msg.get("internalDate", 0))

                headers = msg["payload"].get("headers", [])
                sender = self._detect_sender(headers)
                html_content = self._extract_html(msg["payload"])

                if html_content:
                    urls = self._extract_job_urls(html_content)
                    emails_with_time.append(
                        {
                            "timestamp": internal_date,
                            "sender": sender,
                            "html": html_content,
                            "urls": urls,
                        }
                    )

            # Sort emails by timestamp (oldest first - chronological order)
            emails_with_time.sort(key=lambda x: x["timestamp"])

            # Build email_data list in chronological order, preserving URL order within each email
            email_data = []
            for email in emails_with_time:
                for url in email["urls"]:
                    email_data.append(
                        {
                            "url": url,
                            "email_html": email["html"],
                            "sender": email["sender"],
                        }
                    )

            print(f"Total: {len(email_data)} job URLs from all emails\n")
            return email_data

        except Exception as e:
            print(f"Gmail fetch error: {e}")
            return []

    def _detect_sender(self, headers):
        """Detect sender from email headers."""
        for header in headers:
            if header["name"] == "From":
                from_field = header["value"].lower()
                if "ziprecruiter" in from_field:
                    return "ZipRecruiter"
                elif "adzuna" in from_field:
                    return "Adzuna"
                elif "swelist" in from_field:
                    return "SWE List"
                elif "jobright" in from_field:
                    return "Jobright"
                elif "fursah" in from_field:
                    return "Fursah"
                return "Email"
        return "Email"

    def _extract_html(self, payload):
        """Extract HTML content from email payload."""
        if "parts" in payload:
            for part in payload["parts"]:
                if part["mimeType"] == "text/html":
                    html_data = part["body"].get("data", "")
                    return base64.urlsafe_b64decode(html_data).decode("utf-8")
        elif "body" in payload:
            html_data = payload["body"].get("data", "")
            if html_data:
                return base64.urlsafe_b64decode(html_data).decode("utf-8")
        return None

    def _extract_job_urls(self, email_html):
        """Extract job board URLs from email HTML."""
        soup = BeautifulSoup(email_html, "html.parser")
        job_urls = []

        for link in soup.find_all("a", href=True):
            url = link.get("href", "")
            if not url.startswith("http"):
                continue

            is_job_board = any(domain in url.lower() for domain in JOB_BOARD_DOMAINS)
            if is_job_board and not self._is_non_job_url(url):
                job_urls.append(url)

        return list(set(job_urls))

    def _is_non_job_url(self, url):
        """Check if URL is not a job posting."""
        url_lower = url.lower()
        non_job = [
            "/unsubscribe",
            "/my-alerts",
            "/blog",
            "/prepper",
            "twitter.com",
            "facebook.com",
            "/privacy",
            "/terms",
            "?retarget=",
        ]

        if any(p in url_lower for p in non_job):
            return True
        if "adzuna.com" in url_lower and "/land/ad/" not in url_lower:
            return True
        return False


class PageFetcher:
    """Fetches web pages using multiple fallback methods."""

    def __init__(self):
        self.outcomes = {
            "method_standard": 0,
            "method_rotating_agent": 0,
            "method_selenium": 0,
        }

    def fetch_page(self, url):
        """Fetch page using multiple fallback methods."""
        # Method 1: Standard request
        response = self._try_standard_request(url)
        if response and response.status_code == 200:
            self.outcomes["method_standard"] += 1
            return response, response.url

        # Method 2: Rotating user agents
        response = self._try_rotating_agents(url)
        if response and response.status_code == 200:
            self.outcomes["method_rotating_agent"] += 1
            return response, response.url

        # Method 3: Selenium (for blocked sites)
        if SELENIUM_AVAILABLE and "ziprecruiter" in url.lower():
            html, final_url = self._try_selenium(url)
            if html:
                self.outcomes["method_selenium"] += 1
                mock_response = type(
                    "obj",
                    (object,),
                    {"text": html, "status_code": 200, "url": final_url},
                )()
                return mock_response, final_url

        return None, None

    def _try_standard_request(self, url):
        """Standard HTTP request."""
        try:
            headers = {
                "User-Agent": USER_AGENTS[0],
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            }
            return requests.get(url, headers=headers, allow_redirects=True, timeout=20)
        except:
            return None

    def _try_rotating_agents(self, url):
        """Try multiple user agents."""
        for ua in USER_AGENTS:
            try:
                headers = {
                    "User-Agent": ua,
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "en-US,en;q=0.9",
                }
                response = requests.get(
                    url, headers=headers, allow_redirects=True, timeout=20
                )
                if response.status_code == 200:
                    return response
                time.sleep(1)
            except:
                continue
        return None

    def _try_selenium(self, url):
        """Selenium-based scraping (for anti-bot sites)."""
        if not SELENIUM_AVAILABLE:
            return None, None

        driver = None
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument(f"user-agent={USER_AGENTS[0]}")

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(30)
            driver.get(url)
            time.sleep(3)

            return driver.page_source, driver.current_url
        except:
            return None, None
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass


class PageParser:
    """Parses job data from web pages."""

    @staticmethod
    def extract_company(soup, url):
        """✅ ENHANCED: Extract company name with special domain mappings."""
        # Try JSON-LD first
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    org = data.get("hiringOrganization", {})
                    if isinstance(org, dict):
                        name = org.get("name", "")
                        if name and len(name) < 100:
                            return name
            except:
                pass

        # Try meta tags
        meta = soup.find("meta", {"property": "og:site_name"})
        if meta and meta.get("content"):
            company = meta.get("content").strip()
            company = re.sub(r"\s*[-|]\s*(careers|jobs).*$", "", company, flags=re.I)
            if company and len(company) < 50:
                return company

        # Extract from domain (with special mappings)
        from processors import ValidationHelper

        return ValidationHelper.extract_company_from_domain(url)

    @staticmethod
    def extract_title(soup):
        """Extract job title from page."""
        # Try JSON-LD
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                data = json.loads(json_ld.string)
                if isinstance(data, dict) and data.get("title"):
                    title = data["title"]
                    if 5 < len(title) < 200:
                        return title
            except:
                pass

        # Try H1
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text().strip()
            if 5 < len(title) < 200:
                return title

        # Try title tag
        title_tag = soup.find("title")
        if title_tag:
            full_title = title_tag.get_text().strip()
            parts = full_title.split("-")
            if parts:
                title = parts[0].strip()
                if 5 < len(title) < 200:
                    return title

        return "Unknown"

    @staticmethod
    def extract_job_id(soup, url):
        """✅ ENHANCED: Platform-specific job ID - prioritizes page text, uses word boundaries."""
        try:
            page_text = soup.get_text()

            # ✅ PRIORITY 1: Page text with word boundaries (fixes "A07000Apply" issue)

            # TikTok/General: Job Code with word boundary
            match = re.search(r"Job Code:\s*([A-Z0-9]{3,15})\b", page_text, re.I)
            if match:
                return match.group(1).strip()

            # Standard formats with word boundaries
            patterns = [
                r"Req ID:\s*([A-Z0-9\-]{3,20})\b",
                r"Job ID:\s*([A-Z0-9\-]{3,20})\b",
                r"Requisition ID:\s*([A-Z0-9\-]{3,20})\b",
                r"Requisition:\s*([A-Z0-9\-]{3,20})\b",
            ]

            for pattern in patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    return match.group(1).strip()

            # ✅ PRIORITY 2: URL-based extraction

            # Workday: REQ-XXXX-XXX format
            if "workday" in url.lower():
                match = re.search(r"REQ-\d{4}-\d{1,3}", url)
                if match:
                    return match.group(0)
                match = re.search(r"_([A-Z]*\d+)(?:\?|$)", url)
                if match:
                    return match.group(1)

            # GitHub/Jibe: /jobs/XXXX
            if "jibe" in url.lower() or "github" in url.lower():
                match = re.search(r"/jobs/(\d+)", url)
                if match:
                    return match.group(1)

            # TikTok: /search/XXXXX (only as fallback)
            if "tiktok.com" in url.lower() or "lifeattiktok.com" in url.lower():
                match = re.search(r"/search/(\d{10,})", url)
                if match:
                    return match.group(1)

            # Oracle: job/XXXXX
            if "oracle" in url.lower():
                match = re.search(r"/job/(\d{5,})", url)
                if match:
                    return match.group(1)

            return "N/A"
        except:
            return "N/A"

    @staticmethod
    def extract_job_age_days(soup):
        """✅ ENHANCED: Extract job age with comprehensive patterns."""
        try:
            page_text = soup.get_text()[:3000]

            # Pattern 1: "Posted X Days Ago"
            match = re.search(r"[Pp]osted\s+(\d+)\+?\s+[Dd]ays?\s+[Aa]go", page_text)
            if match:
                return int(match.group(1))

            # Pattern 2: Just "X Days Ago"
            match = re.search(r"(\d+)\+?\s+[Dd]ays?\s+[Aa]go", page_text)
            if match:
                return int(match.group(1))

            # Pattern 3: "today" or "yesterday"
            if re.search(r"[Pp]osted\s+today|Today", page_text):
                return 0
            if re.search(r"[Pp]osted\s+yesterday|Yesterday", page_text):
                return 1

            # Pattern 4: Hours ago
            if re.search(r"(\d+)\s+hours?\s+ago", page_text, re.I):
                return 0

            # Pattern 5: Date formats
            match = re.search(
                r"[Pp]osted:?\s*(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", page_text
            )
            if match:
                try:
                    import datetime

                    month, day, year = (
                        int(match.group(1)),
                        int(match.group(2)),
                        int(match.group(3)),
                    )
                    if year < 100:
                        year += 2000
                    posted_date = datetime.datetime(year, month, day)
                    today = datetime.datetime.now()
                    delta = today - posted_date
                    return delta.days
                except:
                    pass

            return None
        except:
            return None

    @staticmethod
    def extract_jobright_data(soup, url, jobright_auth):
        """Extract data from Jobright page."""
        page_text = soup.get_text()
        actual_url, is_company_site = jobright_auth.resolve_jobright_url(url)

        company = None
        title = None
        location = None
        sponsorship = "Unknown"
        remote = "Unknown"

        try:
            # Extract title from H1
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text().strip()

            # Extract company from H2/H3
            for tag in ["h2", "h3"]:
                elem = soup.find(tag)
                if elem:
                    text = elem.get_text().strip()
                    if 5 < len(text) < 60:
                        if not any(
                            w in text.lower()
                            for w in ["intern", "engineer", "summer", "2026"]
                        ):
                            company = text
                            break

            # Extract location
            loc_pattern = r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b"
            match = re.search(loc_pattern, page_text[:2000])
            if match:
                location = f"{match.group(1)}, {match.group(2)}"

            if not location and "Remote" in page_text[:1500]:
                location = "Remote"

            # Sponsorship
            if "H1B Sponsor Likely" in page_text or "H-1B Sponsor Likely" in page_text:
                sponsorship = "Yes"
            elif "No H1B" in page_text or "No H-1B" in page_text:
                sponsorship = "No"

            # Remote status
            if "Onsite" in page_text[:1500] or "On-site" in page_text[:1500]:
                remote = "On Site"
            elif "Hybrid" in page_text[:1500]:
                remote = "Hybrid"
            elif "Remote" in page_text[:1500]:
                remote = "Remote"

            if company and title:
                return {
                    "company": company,
                    "title": title,
                    "location": location if location else "Unknown",
                    "sponsorship": sponsorship,
                    "remote": remote,
                    "url": actual_url,
                    "is_company_site": is_company_site,
                }
        except:
            pass

        return None


class SourceParsers:
    """Parsers for specific job sources (ZipRecruiter, Adzuna, etc.)."""

    @staticmethod
    def parse_ziprecruiter_email(soup, url):
        """Enhanced ZipRecruiter email parsing with multiple strategies."""
        try:
            # Strategy 1: Find job cards/containers
            job_cards = soup.find_all(
                ["div", "table", "tr"], class_=re.compile(r"job|listing", re.I)
            )

            for card in job_cards:
                links = card.find_all("a", href=True)
                url_found = any(url[:50] in link["href"] for link in links)

                if url_found:
                    # Extract title
                    title = None
                    for link in links:
                        if url[:50] in link["href"]:
                            title = link.get_text().strip()
                            if len(title) > 10:
                                break

                    if not title:
                        heading = card.find(["h2", "h3", "strong", "b"])
                        if heading:
                            title = heading.get_text().strip()

                    # Extract company/location from bullet-separated format
                    card_text = card.get_text("\n", strip=True)
                    lines = [l.strip() for l in card_text.split("\n") if l.strip()]

                    company = "Unknown"
                    location = "Unknown"
                    remote = "Unknown"

                    for line in lines:
                        if "•" in line:
                            parts = [p.strip() for p in line.split("•")]
                            if len(parts) >= 2:
                                if company == "Unknown" and len(parts[0]) > 2:
                                    company = parts[0]
                                if location == "Unknown" and len(parts[1]) > 2:
                                    location = parts[1]
                                if len(parts) >= 3:
                                    work_type = parts[2].lower()
                                    if "remote" in work_type:
                                        remote = "Remote"
                                    elif "hybrid" in work_type:
                                        remote = "Hybrid"
                                    elif "onsite" in work_type:
                                        remote = "On Site"

                    # Fallback: city, state pattern
                    if location == "Unknown":
                        loc_match = re.search(
                            r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b",
                            card_text,
                        )
                        if loc_match:
                            location = f"{loc_match.group(1)}, {loc_match.group(2)}"

                    if title and title != "Unknown":
                        return {
                            "company": company,
                            "title": title,
                            "location": location,
                            "remote": (
                                remote
                                if remote != "Unknown"
                                else SourceParsers._infer_remote(location)
                            ),
                            "url": url,
                            "sponsorship": "Unknown (Email)",
                        }

            # Strategy 2: Line-by-line parsing fallback
            return SourceParsers._parse_ziprecruiter_fallback(soup, url)

        except:
            return None

    @staticmethod
    def _parse_ziprecruiter_fallback(soup, url):
        """Fallback parsing for ZipRecruiter."""
        try:
            lines = [l.strip() for l in soup.get_text().split("\n") if l.strip()]
            url_short = url[:60]

            url_index = -1
            for i, line in enumerate(lines):
                if url_short in line or ("/km/" in url and "/km/" in line):
                    url_index = i
                    break

            if url_index == -1:
                return None

            title = company = location = "Unknown"
            remote = work_type = None

            # Look backwards for title
            for i in range(max(0, url_index - 10), url_index):
                line = lines[i]
                if 10 < len(line) < 150:
                    if any(
                        kw in line.lower()
                        for kw in ["intern", "engineer", "developer", "software"]
                    ):
                        if "•" not in line and "View" not in line and "$" not in line:
                            title = line
                            break

            # Look forward for company/location
            for i in range(url_index, min(len(lines), url_index + 10)):
                line = lines[i]
                if "•" in line and "$" not in line:
                    parts = [p.strip() for p in line.split("•")]
                    if len(parts) >= 2:
                        company = parts[0]
                        location = re.sub(
                            r"\s*(Hybrid|Remote|In-person)$", "", parts[1], flags=re.I
                        ).strip()
                        if len(parts) >= 3:
                            work_type = parts[2]
                        break

            if title == "Unknown":
                return None

            if work_type:
                work_type_lower = work_type.lower()
                if "remote" in work_type_lower:
                    remote = "Remote"
                elif "hybrid" in work_type_lower:
                    remote = "Hybrid"
                elif "onsite" in work_type_lower:
                    remote = "On Site"

            return {
                "company": company,
                "title": title,
                "location": location,
                "remote": remote if remote else SourceParsers._infer_remote(location),
                "url": url,
                "sponsorship": "Unknown (Email)",
            }
        except:
            return None

    @staticmethod
    def parse_adzuna_email(soup, url):
        """Parse Adzuna email structure - ENHANCED."""
        try:
            h2_tags = soup.find_all("h2")

            for h2 in h2_tags:
                link = h2.find("a", href=re.compile(r"adzuna\.com/land/ad/"))
                if not link:
                    continue

                link_href = link.get("href")
                url_base = url.split("?")[0].split("&")[0]

                # More flexible URL matching
                if url_base not in link_href and not any(
                    x in link_href for x in url.split("?")[0].split("/")
                ):
                    continue

                title = link.get_text().strip()
                if not title or len(title) < 5:
                    continue

                company = location = "Unknown"

                # Strategy 1: Look for next <p> tag
                next_p = h2.find_next_sibling("p")
                if next_p:
                    text = next_p.get_text().strip()
                    # Format: "Company - Location" or "Company - Location - Salary"
                    if " - " in text:
                        parts = text.split(" - ")
                        if len(parts) >= 2:
                            company = parts[0].strip()
                            # Location might include zip code
                            location = re.sub(r",?\s*\d{5}$", "", parts[1]).strip()

                # Strategy 2: Look in parent table/div
                if company == "Unknown":
                    parent = h2.find_parent(["table", "tr", "td", "div"])
                    if parent:
                        # Find all text in parent, look for company pattern
                        parent_text = parent.get_text("\n")
                        lines = [
                            l.strip() for l in parent_text.split("\n") if l.strip()
                        ]

                        for line in lines:
                            # Skip the title line
                            if title in line:
                                continue
                            # Look for "Company - Location" format
                            if " - " in line and len(line) < 150:
                                parts = line.split(" - ")
                                if len(parts) >= 2:
                                    # Check if first part looks like a company (proper case, reasonable length)
                                    potential_company = parts[0].strip()
                                    if (
                                        len(potential_company) > 2
                                        and len(potential_company) < 80
                                    ):
                                        # Check if second part looks like location (has city/state)
                                        potential_location = parts[1].strip()
                                        if "," in potential_location or any(
                                            state in potential_location
                                            for state in US_STATES.values()
                                        ):
                                            company = potential_company
                                            location = re.sub(
                                                r",?\s*\d{5}$", "", potential_location
                                            ).strip()
                                            break

                # Validation: Don't return if we have no useful data
                if title == "Unknown" or (
                    company == "Unknown" and location == "Unknown"
                ):
                    continue

                return {
                    "company": company,
                    "title": title,
                    "location": location,
                    "remote": SourceParsers._infer_remote(location),
                    "url": url,
                    "sponsorship": "Unknown",
                }

            return None
        except:
            return None

    @staticmethod
    def parse_jobright_email(soup, url, jobright_auth):
        """Parse Jobright email structure."""
        try:
            url_base = url.split("?")[0]
            link = soup.find("a", href=re.compile(re.escape(url_base)))

            if not link:
                return None

            container = link.find_parent("table") or link.find_parent("div")
            if not container:
                return None

            company_elem = container.find("p", id="job-company-name")
            company = company_elem.get_text().strip() if company_elem else "Unknown"

            title_p = container.find("p", id="job-title")
            if title_p:
                title_link = title_p.find("a")
                title = (
                    title_link.get_text().strip()
                    if title_link
                    else title_p.get_text().strip()
                )
            else:
                title = link.get_text().strip()

            location = "Unknown"
            for tag in container.find_all("p", id="job-tag"):
                text = tag.get_text().strip()
                if "$" not in text and "referral" not in text.lower():
                    if "," in text or text == "Remote":
                        location = text
                        break

            actual_url, is_company_site = jobright_auth.resolve_jobright_url(url)

            return {
                "company": company,
                "title": title,
                "location": location,
                "remote": SourceParsers._infer_remote(location),
                "url": actual_url,
                "sponsorship": "Unknown (Email)",
                "is_company_site": is_company_site,
            }
        except:
            return None

    @staticmethod
    def _infer_remote(text):
        """Infer remote status from text."""
        if not text:
            return "Unknown"
        text_lower = text.lower()
        if "remote" in text_lower:
            return "Remote"
        if "hybrid" in text_lower:
            return "Hybrid"
        if "onsite" in text_lower or "on-site" in text_lower:
            return "On Site"
        return "Unknown"


class SimplifyGitHubScraper:
    """✅ FIXED: Handles both HTML and markdown/tab-delimited tables."""

    @staticmethod
    def scrape(url, source_name="GitHub"):
        """Scrape with markdown table support for vanshb03."""
        try:
            response = requests.get(url, timeout=10)

            if response.status_code != 200:
                print(f"     ✗ HTTP {response.status_code}")
                return []

            # Try HTML parsing first
            soup = BeautifulSoup(response.text, "html.parser")
            tables = soup.find_all("table")

            if tables:
                # HTML tables found
                return SimplifyGitHubScraper._parse_html_tables(soup, source_name)
            else:
                # No HTML tables - parse as markdown/tab-delimited
                return SimplifyGitHubScraper._parse_markdown_text(
                    response.text, source_name
                )

        except Exception as e:
            print(f"     ✗ Error: {e}")
            return []

    @staticmethod
    def _parse_markdown_text(text, source_name):
        """✅ Parse tab-delimited or pipe-delimited markdown tables - FIXED HTML anchor support."""
        lines = text.split("\n")
        jobs = []

        # Find header
        header_idx = -1
        for i, line in enumerate(lines):
            if re.search(
                r"Company.*Role.*Location.*(?:Application|Link).*Date", line, re.I
            ):
                header_idx = i
                print(f"  [DEBUG] Found header at line {i}: {line[:80]}")
                break

        if header_idx == -1:
            print(f"  [DEBUG] No header found in {len(lines)} lines")
            print(f"  [DEBUG] First 5 lines:")
            for i, line in enumerate(lines[:5]):
                print(f"    {i}: {line[:80]}")
            return []

        # Detect delimiter
        header = lines[header_idx]
        delimiter = None
        start = 0

        if "\t" in header:
            delimiter = "\t"
            start = header_idx + 1
            print(f"  [DEBUG] Using TAB delimiter, starting at line {start}")
        elif "|" in header:
            delimiter = "|"
            start = header_idx + 2  # Skip separator line
            print(f"  [DEBUG] Using PIPE delimiter, starting at line {start}")
        else:
            print(f"  [DEBUG] No delimiter found in header: {header[:80]}")
            return []

        # Parse rows
        total_lines = 0
        valid_rows = 0
        closed_jobs = 0
        skipped_short = 0
        skipped_no_url = 0

        for line_num, line in enumerate(lines[start:], start=start):
            if not line.strip():
                continue

            total_lines += 1

            # Split by delimiter
            parts = [p.strip() for p in line.split(delimiter) if p.strip()]

            # Debug first 3 lines
            if total_lines <= 3:
                print(f"  [DEBUG] Line {line_num}: {len(parts)} parts | {line[:80]}")

            if len(parts) < 5:
                skipped_short += 1
                if skipped_short <= 3:
                    print(f"  [DEBUG] Skipped (< 5 parts): {parts}")
                continue

            valid_rows += 1

            # Extract fields
            company = SimplifyGitHubScraper._remove_emojis(parts[0])
            title = SimplifyGitHubScraper._remove_emojis(parts[1])
            location = SimplifyGitHubScraper._remove_emojis(parts[2])
            link_cell = parts[3]
            age = parts[4] if len(parts) > 4 else "Unknown"

            # ✅ FIX: Extract URL from HTML <a> tag, markdown link, or direct URL
            url = None

            # Try HTML anchor tag first (vanshb03 format: <a href="url">text</a>)
            html_match = re.search(r'<a\s+href="(https?://[^"]+)"', link_cell)
            if html_match:
                url = html_match.group(1)
            else:
                # Try markdown link format (SimplifyJobs format: [text](url))
                md_match = re.search(r"\[.*?\]\((https?://[^\)]+)\)", link_cell)
                if md_match:
                    url = md_match.group(1)
                elif link_cell.startswith("http"):
                    url = link_cell

            if not url:
                skipped_no_url += 1
                if skipped_no_url <= 3:
                    print(f"  [DEBUG] No URL found in: {link_cell[:60]}")
                continue

            # Check if closed
            is_closed = "🔒" in line or "❌" in line or "closed" in line.lower()

            if is_closed:
                closed_jobs += 1
                if closed_jobs <= 3:
                    print(f"  [DEBUG] Closed: {company} - {title}")
                continue

            # Add to jobs
            jobs.append(
                {
                    "company": company,
                    "title": title,
                    "location": location,
                    "url": url,
                    "age": age,
                    "is_closed": is_closed,
                    "source": source_name,
                }
            )

        print(
            f"  [DEBUG] Summary: {total_lines} total, {valid_rows} valid rows, {skipped_short} skipped (short), {skipped_no_url} skipped (no URL), {closed_jobs} closed, {len(jobs)} ADDED"
        )

        # Show first 2 jobs added
        if jobs:
            print(f"  [DEBUG] Sample jobs added:")
            for i, job in enumerate(jobs[:2], 1):
                print(f"    {i}. {job['company']} - {job['title']}")

        return jobs

    @staticmethod
    def _parse_html_tables(soup, source_name):
        """Parse HTML tables (SimplifyJobs format)."""
        tables = soup.find_all("table")
        jobs = []

        for table in tables:
            for row in table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue

                company_link = cells[0].find("a")
                if not company_link:
                    continue

                company = SimplifyGitHubScraper._remove_emojis(
                    company_link.get_text(strip=True)
                )
                title = SimplifyGitHubScraper._remove_emojis(
                    cells[1].get_text(strip=True)
                )
                location = SimplifyGitHubScraper._remove_emojis(
                    cells[2].get_text(strip=True)
                )
                age = cells[4].get_text(strip=True)

                apply_link = cells[3].find("a", href=True)
                if not apply_link:
                    continue

                url = apply_link.get("href", "")
                is_closed = "🔒" in str(cells[3])

                jobs.append(
                    {
                        "company": company,
                        "title": title,
                        "location": location,
                        "url": url,
                        "age": age,
                        "is_closed": is_closed,
                        "source": source_name,
                    }
                )

        return jobs

    @staticmethod
    def _remove_emojis(text):
        """Remove emojis from text."""
        if not text:
            return text
        emoji_pattern = re.compile(
            "["
            "\U0001f600-\U0001f64f"
            "\U0001f300-\U0001f5ff"
            "\U0001f680-\U0001f6ff"
            "\U0001f1e0-\U0001f1ff"
            "\U00002500-\U00002bef"
            "]+",
            flags=re.UNICODE,
        )
        text = emoji_pattern.sub("", text)
        text = re.sub(r"[↳🇺🇸🛂\*🔒❌✅]+", "", text)
        return re.sub(r"\s+", " ", text).strip()


class HandshakeExtractor:
    def __init__(self):
        self.cookies, self.driver, self.jobs_scraped_today, self.last_scrape_date = (
            None,
            None,
            0,
            None,
        )
        self.config, self.cookies_file = HANDSHAKE_CONFIG, HANDSHAKE_COOKIES_FILE
        self._load_cookies()

    def _load_cookies(self):
        if os.path.exists(self.cookies_file):
            try:
                with open(self.cookies_file, "r") as f:
                    self.cookies = json.load(f)
            except:
                pass

    def is_safe_to_scrape(self):
        now = datetime.datetime.now()
        if not self.cookies:
            return False, "No cookies"
        if self.config["scrape_only_weekdays"] and now.weekday() >= 5:
            return False, "Weekend"
        start_hour, end_hour = self.config["scrape_hours"]
        if not (start_hour <= now.hour < end_hour):
            return False, "Outside hours"
        if (
            self.last_scrape_date
            and self.last_scrape_date.date() == now.date()
            and self.jobs_scraped_today >= self.config["max_jobs_per_session"]
        ):
            return False, "Limit reached"
        return True, "OK"

    def scrape_jobs(self):
        is_safe, reason = self.is_safe_to_scrape()
        if not is_safe:
            print(f"Handshake: {reason}")
            return []
        jobs = []
        try:
            if not self._init_browser() or not self._load_search():
                self._cleanup()
                return []
            self._human_browse()
            urls = self._get_urls()
            if not urls:
                self._cleanup()
                return []
            for idx, url in enumerate(urls[: self.config["max_jobs_per_session"]], 1):
                job = self._scrape_one(url, idx, len(urls))
                if job:
                    jobs.append(job)
                    self.jobs_scraped_today += 1
        except:
            pass
        finally:
            self._cleanup()
            self.last_scrape_date = datetime.datetime.now()
        return jobs

    def _init_browser(self):
        try:
            if UNDETECTED_CHROME_AVAILABLE:
                self.driver = uc.Chrome(options=uc.ChromeOptions(), use_subprocess=True)
            else:
                self.driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()), options=Options()
                )
            return True
        except:
            return False

    def _load_search(self):
        try:
            self.driver.get("https://app.joinhandshake.com")
            time.sleep(2)
            for c in self.cookies:
                try:
                    self.driver.add_cookie(
                        {
                            "name": c["name"],
                            "value": c["value"],
                            "domain": c.get("domain", ".joinhandshake.com"),
                        }
                    )
                except:
                    pass
            self.driver.get(self.config["search_url"])
            time.sleep(5)
            return "login" not in self.driver.current_url.lower()
        except:
            return False

    def _human_browse(self):
        try:
            self.driver.execute_script(
                f"window.scrollTo(0, {random.randint(300, 800)})"
            )
            time.sleep(random.uniform(3, 7))
        except:
            pass

    def _get_urls(self):
        try:
            cards = self.driver.find_elements(
                By.CSS_SELECTOR, 'a[href*="/job-search/"]'
            )
            urls = []
            for card in cards:
                href = card.get_attribute("href")
                if href and "/job-search/" in href:
                    match = re.search(r"/job-search/(\d+)", href)
                    if match:
                        urls.append(
                            f"https://app.joinhandshake.com/job-search/{match.group(1)}"
                        )
            return list(set(urls))
        except:
            return []

    def _scrape_one(self, url, idx, total):
        try:
            time.sleep(random.uniform(*self.config["delay_between_jobs"]))
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))
            self._deep_read()
            data = self._extract(url)
            if idx < total:
                self.driver.back()
                time.sleep(random.uniform(2, 4))
            return data
        except:
            return None

    def _deep_read(self):
        try:
            h = self.driver.execute_script("return document.body.scrollHeight")
            for i in range(random.randint(3, 6)):
                self.driver.execute_script(
                    f"window.scrollTo(0, {(h//(i+1))+random.randint(-50,50)})"
                )
                time.sleep(random.uniform(*self.config["scroll_delay"]))
            time.sleep(
                random.uniform(*self.config["read_time_per_job"]) / random.randint(3, 5)
            )
        except:
            pass

    def _extract(self, url):
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            text = soup.get_text()
            data = {
                "company": "Unknown",
                "title": "Unknown",
                "location": "Unknown",
                "remote": "Unknown",
                "job_type": "Internship",
                "url": url,
                "job_id": self._id(url),
                "work_authorization_required": "Unknown",
                "sponsorship": "Unknown",
            }
            h1 = soup.find("h1")
            if h1:
                data["title"] = h1.get_text().strip()
            for elem in [
                soup.find("a", href=re.compile(r"/employers/\d+")),
                soup.find(["h2", "h3"], string=re.compile(r"^[A-Z]")),
            ]:
                if elem:
                    c = elem.get_text().strip()
                    if 2 < len(c) < 100:
                        data["company"] = c
                        break
            loc = re.search(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b", text)
            if loc:
                data["location"] = f"{loc.group(1)}, {loc.group(2)}"
            elif "Remote" in text:
                data["location"] = "Remote"
            if "hybrid" in text.lower():
                data["remote"] = "Hybrid"
            elif "remote" in text.lower():
                data["remote"] = "Remote"
            elif "onsite" in text.lower():
                data["remote"] = "On Site"
            if re.search(r"US work authorization required", text, re.I):
                data["work_authorization_required"], data["sponsorship"] = "Yes", "No"
            elif re.search(r"Open to candidates with OPT/CPT", text, re.I):
                data["work_authorization_required"], data["sponsorship"] = "No", "Yes"
            elif re.search(r"will sponsor|H-?1B", text, re.I):
                data["work_authorization_required"], data["sponsorship"] = "No", "Yes"
            if "Co-op" in text or "Coop" in text:
                data["job_type"] = "Co-op"
            return data
        except:
            return None

    def _id(self, url):
        m = re.search(r"/job-search/(\d+)", url)
        return f"HS_{m.group(1)}" if m else "N/A"

    def _cleanup(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
