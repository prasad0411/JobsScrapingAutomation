#!/usr/bin/env python3
# cSpell:disable
"""
Extraction module for job data from various sources.
Handles web scraping, email parsing, and Jobright authentication.
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

    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

from config import (
    USER_AGENTS,
    GMAIL_CREDS_FILE,
    GMAIL_TOKEN_FILE,
    GMAIL_SCOPES,
    JOB_BOARD_DOMAINS,
    JOBRIGHT_COOKIES_FILE,
    US_STATES,
    CANADA_PROVINCES,
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

            email_data = []
            for message in messages:
                msg = (
                    self.service.users()
                    .messages()
                    .get(userId="me", id=message["id"], format="full")
                    .execute()
                )

                headers = msg["payload"].get("headers", [])
                sender = self._detect_sender(headers)
                html_content = self._extract_html(msg["payload"])

                if html_content:
                    urls = self._extract_job_urls(html_content)
                    for url in urls:
                        email_data.append(
                            {"url": url, "email_html": html_content, "sender": sender}
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
        """Extract company name from page."""
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

        # Extract from domain
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
        """Extract job ID from page."""
        page_text = soup.get_text()

        patterns = [r"Req ID:\s*([A-Z0-9\-]+)", r"Job ID:\s*([A-Z0-9\-]+)"]

        for pattern in patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                return match.group(1).strip()

        # Workday-specific
        if "workday" in url.lower():
            match = re.search(r"_([A-Z]*\d+)(?:\?|$)", url)
            if match:
                return match.group(1)

        return "N/A"

    @staticmethod
    def extract_job_age_days(soup):
        """Extract how many days ago job was posted."""
        try:
            page_text = soup.get_text()[:3000]

            # "Posted X days ago"
            match = re.search(r"[Pp]osted\s+(\d+)\s+days?\s+ago", page_text)
            if match:
                return int(match.group(1))

            # Just "X days ago"
            match = re.search(r"(\d+)\s+days?\s+ago", page_text)
            if match:
                return int(match.group(1))

            # "today" or "yesterday"
            if re.search(r"[Pp]osted\s+today|Today", page_text):
                return 0
            if re.search(r"[Pp]osted\s+yesterday|Yesterday", page_text):
                return 1

            # Hours ago
            if re.search(r"(\d+)\s+hours?\s+ago", page_text):
                return 0

            # Date formats
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
        """Parse Adzuna email structure."""
        try:
            h2_tags = soup.find_all("h2")

            for h2 in h2_tags:
                link = h2.find("a", href=re.compile(r"adzuna\.com/land/ad/"))
                if not link:
                    continue

                if url.split("?")[0] not in link.get("href"):
                    continue

                title = link.get_text().strip()
                company = location = "Unknown"

                next_elem = h2.find_next_sibling(["p", "td"])
                if not next_elem:
                    parent = h2.find_parent(["tr", "td", "div"])
                    if parent:
                        next_elem = parent.find_next(["p", "td"])

                if next_elem:
                    text = next_elem.get_text("\n", strip=True)
                    lines = [l.strip() for l in text.split("\n") if l.strip()]

                    for line in lines:
                        if " - " in line and len(line) < 100:
                            parts = line.split(" - ", 1)
                            company = parts[0].strip()
                            location = re.sub(r",?\s*\d{5}$", "", parts[1]).strip()
                            break
                        elif "•" in line or "·" in line:
                            parts = re.split("[•·]", line)
                            if len(parts) >= 2:
                                company = parts[0].strip()
                                location = re.sub(
                                    r"\s*(Hybrid|Remote)$", "", parts[1], flags=re.I
                                ).strip()
                                break

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
    """Scrapes SimplifyJobs GitHub repository."""

    @staticmethod
    def scrape():
        """Scrape and return job listings from GitHub."""
        from config import SIMPLIFY_URL

        try:
            response = requests.get(SIMPLIFY_URL, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            jobs = []
            for table in soup.find_all("table"):
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
                        }
                    )

            return jobs
        except Exception as e:
            print(f"GitHub scraping error: {e}")
            return []

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
