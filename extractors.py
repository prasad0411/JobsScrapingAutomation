#!/usr/bin/env python3

import requests
import base64
import pickle
import os
import json
import time
import random
import re
import logging
from functools import lru_cache
from contextlib import contextmanager

from bs4 import BeautifulSoup

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from config import (
    USER_AGENTS,
    GMAIL_CREDS_FILE,
    GMAIL_TOKEN_FILE,
    GMAIL_SCOPES,
    JOB_BOARD_DOMAINS,
    JOBRIGHT_COOKIES_FILE,
    COMPANY_SLUG_MAPPING,
    URL_TO_COMPANY_MAPPING,
    PLATFORM_CONFIGS,
    get_state_for_city,
    validate_us_state_code,
    parse_date_flexible,
    DATEUTIL_AVAILABLE,
    PARSER_CHAIN,
    DEFAULT_PARSER,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    BACKOFF_MULTIPLIER,
)

from utils import (
    PlatformDetector,
    CompanyNormalizer,
    CompanyValidator,
    DateParser,
)

from processors import (
    JobIDExtractor,
    LocationExtractor,
    CompanyExtractor,
    ValidationHelper,
)

# ============================================================================
# Session & Pattern Setup
# ============================================================================

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": USER_AGENTS[0]})

_URL_HEALTH_CACHE = {}
_EMOJI_PATTERN = re.compile(
    r"[\U0001f600-\U0001f64f\U0001f300-\U0001f5ff\U0001f680-\U0001f6ff\U0001f1e0-\U0001f1ff]+",
    re.UNICODE,
)
_HEADER_PATTERN = re.compile(
    r"Company.*Role.*Location.*(?:Application|Link).*Date", re.I
)
_HTML_LINK_PATTERN = re.compile(r'<a\s+href="(https?://[^"]+)"')
_MD_LINK_PATTERN = re.compile(r"\[.*?\]\((https?://[^\)]+)\)")

# ============================================================================
# Core Helper: Safe HTML Parsing with Fallback Chain
# ============================================================================


def safe_parse_html(html_content, preferred_parser=None):
    """Try to parse HTML with multiple parsers, falling back if one fails."""
    parsers_to_try = PARSER_CHAIN.copy()

    if preferred_parser and preferred_parser in parsers_to_try:
        parsers_to_try.remove(preferred_parser)
        parsers_to_try.insert(0, preferred_parser)

    for parser in parsers_to_try:
        try:
            soup = BeautifulSoup(html_content, parser)
            return soup, parser
        except Exception as e:
            logging.debug(f"Parser {parser} failed: {e}")
            continue

    logging.error(f"All parsers failed for HTML content")
    return None, None


# ============================================================================
# Core Helper: Network Request with Retry Logic
# ============================================================================


def retry_request(url, method="GET", max_retries=MAX_RETRIES, **kwargs):
    """Make HTTP request with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            if method.upper() == "GET":
                response = _SESSION.get(url, timeout=20, **kwargs)
            elif method.upper() == "HEAD":
                response = _SESSION.head(url, timeout=5, **kwargs)
            else:
                response = _SESSION.request(method, url, timeout=20, **kwargs)

            if response.status_code == 200:
                return response
            elif response.status_code in [403, 429]:
                wait_time = RETRY_DELAY_SECONDS * (BACKOFF_MULTIPLIER**attempt)
                logging.warning(f"Rate limited on {url}, waiting {wait_time}s")
                time.sleep(wait_time)
            else:
                logging.warning(f"HTTP {response.status_code} for {url}")
                return response

        except requests.exceptions.Timeout:
            wait_time = RETRY_DELAY_SECONDS * (BACKOFF_MULTIPLIER**attempt)
            logging.warning(f"Timeout on {url}, retrying in {wait_time}s")
            time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            wait_time = RETRY_DELAY_SECONDS * (BACKOFF_MULTIPLIER**attempt)
            logging.warning(f"Request error for {url}: {e}, retrying in {wait_time}s")
            time.sleep(wait_time)

    logging.error(f"All {max_retries} retries failed for {url}")
    return None


# ============================================================================
# Simplify Redirect Resolver - NEW
# ============================================================================


class SimplifyRedirectResolver:
    """Resolves Simplify.jobs redirect URLs to actual job posting URLs"""

    @staticmethod
    @lru_cache(maxsize=500)
    def resolve(simplify_url):
        """
        Resolve Simplify redirect URL to actual job URL.
        Returns: (actual_url, success)
        """
        if "simplify.jobs/p/" not in simplify_url.lower():
            return simplify_url, False

        try:
            # Follow redirects to get final destination
            response = retry_request(
                simplify_url,
                allow_redirects=True,
                headers={"User-Agent": USER_AGENTS[0]},
                max_retries=2,
            )

            if response and response.url and response.url != simplify_url:
                # Successfully resolved
                logging.info(f"Resolved Simplify URL: {simplify_url} -> {response.url}")
                return response.url, True
            else:
                logging.warning(f"Failed to resolve Simplify URL: {simplify_url}")
                return simplify_url, False

        except Exception as e:
            logging.error(f"Error resolving Simplify URL {simplify_url}: {e}")
            return simplify_url, False


# ============================================================================
# Jobright Authentication
# ============================================================================


class JobrightAuthenticator:
    def __init__(self):
        self.cookies = None
        self.session = requests.Session()
        self.load_cookies()

    def load_cookies(self):
        if os.path.exists(JOBRIGHT_COOKIES_FILE):
            try:
                with open(JOBRIGHT_COOKIES_FILE, "r") as f:
                    self.cookies = json.load(f)
                    for cookie in self.cookies:
                        self.session.cookies.set(cookie["name"], cookie["value"])
                logging.info(f"Loaded {len(self.cookies)} Jobright cookies")
            except Exception as e:
                logging.error(f"Failed to load Jobright cookies: {e}")

    def login_interactive(self):
        if not SELENIUM_AVAILABLE:
            logging.warning("Selenium not available - skipping Jobright authentication")
            return False

        print("\n" + "=" * 60)
        print("JOBRIGHT AUTHENTICATION")
        print("=" * 60)

        with self._get_driver() as driver:
            try:
                driver.get("https://jobright.ai")
                time.sleep(3)
                print("[AUTH] Please log in through the browser window")
                print("       Press ENTER after completing login...")
                input()

                cookies = driver.get_cookies()
                if not cookies:
                    print("✗ No cookies captured")
                    return False

                self.cookies = cookies
                with open(JOBRIGHT_COOKIES_FILE, "w") as f:
                    json.dump(cookies, f, indent=2)

                for cookie in cookies:
                    self.session.cookies.set(cookie["name"], cookie["value"])

                print(f"✓ Authentication successful ({len(cookies)} cookies saved)\n")
                return True
            except Exception as e:
                logging.error(f"Authentication failed: {e}")
                print(f"✗ Authentication failed: {e}")
                return False

    def resolve_jobright_url(self, jobright_url):
        if "jobright.ai/jobs/info/" not in jobright_url.lower():
            return jobright_url, False

        if not self.cookies:
            return jobright_url, False

        try:
            response = retry_request(
                jobright_url, headers={"User-Agent": USER_AGENTS[0]}
            )
            if not response or response.status_code != 200:
                return jobright_url, False

            soup, _ = safe_parse_html(response.content)
            if not soup:
                return jobright_url, False

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
        except Exception as e:
            logging.error(f"Failed to resolve Jobright URL {jobright_url}: {e}")
            return jobright_url, False

    @contextmanager
    def _get_driver(self):
        driver = None
        try:
            chrome_options = Options()
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option(
                "excludeSwitches", ["enable-logging"]
            )
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            yield driver
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass


# ============================================================================
# Email Extraction
# ============================================================================


class EmailExtractor:
    def __init__(self):
        self.service = None

    def authenticate(self):
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
        if not self.service:
            print("[Gmail] Authenticating...")
            self.authenticate()

        if not self.service:
            print("✗ Gmail authentication failed")
            logging.error("Gmail service not initialized after authentication")
            return []

        try:
            results = (
                self.service.users()
                .messages()
                .list(
                    userId="me",
                    q='label:"Job Hunt" newer_than:1d',
                    maxResults=max_results,
                )
                .execute()
            )

            messages = results.get("messages", [])
            if not messages:
                logging.info("No labeled emails found")
                print("No emails with 'Job Hunt' label found")
                return []

            print(f"Found {len(messages)} labeled emails")

            emails_with_time = []
            for message in messages:
                try:
                    msg = (
                        self.service.users()
                        .messages()
                        .get(userId="me", id=message["id"], format="full")
                        .execute()
                    )

                    internal_date = int(msg.get("internalDate", 0))
                    headers = {
                        h["name"]: h["value"] for h in msg["payload"].get("headers", [])
                    }
                    sender = self._detect_sender(headers.get("From", ""))
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
                except Exception as e:
                    logging.error(f"Failed to process email {message['id']}: {e}")
                    continue

            emails_with_time.sort(key=lambda x: x["timestamp"], reverse=True)

            email_data = []
            for email in emails_with_time:
                email_data.extend(
                    [
                        {
                            "url": url,
                            "email_html": email["html"],
                            "sender": email["sender"],
                        }
                        for url in email["urls"]
                    ]
                )

            print(f"Total: {len(email_data)} job URLs from all emails\n")
            return email_data
        except Exception as e:
            logging.error(f"Gmail fetch error: {e}", exc_info=True)
            print(f"✗ Gmail error: {e}")
            return []

    @staticmethod
    def _detect_sender(from_field):
        from_lower = from_field.lower()
        senders = {
            "ziprecruiter": "ZipRecruiter",
            "adzuna": "Adzuna",
            "swelist": "SWE List",
            "jobright": "Jobright",
            "fursah": "Fursah",
        }
        for key, value in senders.items():
            if key in from_lower:
                return value
        return "Email"

    @staticmethod
    def _extract_html(payload):
        if "parts" in payload:
            for part in payload["parts"]:
                if part["mimeType"] == "text/html":
                    try:
                        return base64.urlsafe_b64decode(
                            part["body"].get("data", "")
                        ).decode("utf-8")
                    except Exception as e:
                        logging.error(f"Failed to decode email part: {e}")
                        continue
        elif "body" in payload:
            html_data = payload["body"].get("data", "")
            if html_data:
                try:
                    return base64.urlsafe_b64decode(html_data).decode("utf-8")
                except Exception as e:
                    logging.error(f"Failed to decode email body: {e}")
        return None

    @staticmethod
    def _extract_job_urls(email_html):
        soup, _ = safe_parse_html(email_html)
        if not soup:
            return []

        seen = set()
        urls = []

        for link in soup.find_all("a", href=True):
            url = link.get("href", "")
            if url.startswith("http") and url not in seen:
                if any(domain in url.lower() for domain in JOB_BOARD_DOMAINS):
                    if not EmailExtractor._is_non_job_url(url):
                        urls.append(url)
                        seen.add(url)

        return urls

    @staticmethod
    def _is_non_job_url(url):
        non_job = [
            "/unsubscribe",
            "/my-alerts",
            "/blog",
            "/privacy",
            "/terms",
            "twitter.com",
            "facebook.com",
        ]
        return any(p in url.lower() for p in non_job)


# ============================================================================
# Page Fetcher with Smart Retry
# ============================================================================


class PageFetcher:
    def __init__(self):
        self.session = _SESSION

    def check_url_health(self, url):
        if url in _URL_HEALTH_CACHE:
            return _URL_HEALTH_CACHE[url]

        response = retry_request(url, method="HEAD", max_retries=2)
        if response:
            is_healthy = response.status_code == 200
            _URL_HEALTH_CACHE[url] = (is_healthy, response.status_code)
            return is_healthy, response.status_code

        _URL_HEALTH_CACHE[url] = (False, 0)
        return False, 0

    def fetch_page(self, url):
        is_healthy, status = self.check_url_health(url)
        if not is_healthy and status in [404, 403, 405]:
            logging.info(f"Skipping unhealthy URL: {url} (status {status})")
            return None, None

        # Try JS-heavy platforms with Selenium first
        if self._is_js_heavy_platform(url):
            html, final_url = self._try_selenium(url)
            if html:
                return self._create_mock_response(html, final_url), final_url

        # Standard request with retry
        response = retry_request(url)
        if response and response.status_code == 200:
            return response, response.url

        # Fallback to Selenium if standard request failed
        if SELENIUM_AVAILABLE:
            logging.info(f"Standard request failed, trying Selenium for {url}")
            html, final_url = self._try_selenium(url)
            if html:
                return self._create_mock_response(html, final_url), final_url

        return None, None

    @staticmethod
    def _is_js_heavy_platform(url):
        """ENHANCED: Added Ashby to JS-heavy platforms"""
        if not url:
            return False
        js_platforms = [
            "workday",
            "myworkdayjobs",
            "greenhouse.io",
            "oracle",
            "oraclecloud",
            "ashbyhq",  # NEW: Mark Ashby as JS-heavy
        ]
        return any(platform in url.lower() for platform in js_platforms)

    @staticmethod
    def _try_selenium(url):
        if not SELENIUM_AVAILABLE:
            return None, None

        driver = None
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument(f"user-agent={USER_AGENTS[0]}")

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(30)
            driver.get(url)

            url_lower = url.lower()
            if "oracle" in url_lower or "oraclecloud" in url_lower:
                time.sleep(15)
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "h1"))
                    )
                except:
                    pass
            elif "workday" in url_lower:
                time.sleep(12)  # Increased from 8
            elif "greenhouse" in url_lower:
                time.sleep(10)
            elif "ashby" in url_lower:
                time.sleep(5)
            else:
                time.sleep(3)

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            return driver.page_source, driver.current_url
        except Exception as e:
            logging.error(f"Selenium failed for {url}: {e}")
            return None, None
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    @staticmethod
    def _create_mock_response(html, url):
        return type("obj", (object,), {"text": html, "status_code": 200, "url": url})()


# ============================================================================
# Page Parser
# ============================================================================


class PageParser:
    @staticmethod
    def extract_company(soup, url):
        platform = PlatformDetector.detect(url)
        return CompanyExtractor.extract_all_methods(url, soup)

    @staticmethod
    def extract_title(soup):
        if not soup:
            return "Unknown"

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

        meta_title = soup.find("meta", {"property": "og:title"})
        if meta_title and meta_title.get("content"):
            title = meta_title.get("content").strip()
            if 5 < len(title) < 200 and "careers" not in title.lower():
                return title

        h1 = soup.find("h1")
        if h1:
            title = h1.get_text().strip()
            if 5 < len(title) < 200 and len(title.split()) > 1:
                return title

        return "Unknown"

    @staticmethod
    def extract_job_id(soup, url):
        return JobIDExtractor.extract_all_methods(url, soup)

    @staticmethod
    def extract_job_age_days(soup):
        if not soup:
            return None

        try:
            page_text = soup.get_text()[:3000]
            return DateParser.extract_days_ago(page_text)
        except:
            return None

    @staticmethod
    def extract_jobright_data(soup, url, jobright_auth):
        try:
            script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if not script_tag:
                return None

            data = json.loads(script_tag.string)
            job_result = (
                data.get("props", {})
                .get("pageProps", {})
                .get("dataSource", {})
                .get("jobResult", {})
            )

            if not job_result:
                return None

            company = job_result.get("companyResult", {}).get("companyName", "Unknown")
            title = job_result.get("jobTitle", "Unknown")
            location = job_result.get("jobLocation", "Unknown")

            is_remote = job_result.get("isRemote", False)
            work_model = job_result.get("workModel", "").lower()

            if is_remote or work_model == "remote":
                remote = "Remote"
            elif work_model == "hybrid":
                remote = "Hybrid"
            elif work_model == "onsite":
                remote = "On Site"
            else:
                remote = "Unknown"

            recommendation_tags = job_result.get("recommendationTags", [])
            sponsorship = (
                "Yes" if "H1B Sponsor Likely" in recommendation_tags else "Unknown"
            )

            actual_url = (
                job_result.get("applyLink") or job_result.get("originalUrl") or url
            )
            is_company_site = job_result.get("isCompanySiteLink", False)

            return {
                "company": company,
                "title": title,
                "location": location,
                "sponsorship": sponsorship,
                "remote": remote,
                "url": actual_url,
                "is_company_site": is_company_site,
            }
        except Exception as e:
            logging.error(f"Failed to extract Jobright data: {e}")
            return None


# ============================================================================
# Source Parsers
# ============================================================================


class SourceParsers:
    @staticmethod
    def parse_jobright_email(soup, url, jobright_auth):
        try:
            url_base = url.split("?")[0]
            all_links = soup.find_all("a", href=re.compile(re.escape(url_base)))

            title_link = None
            for link in all_links:
                link_text = link.get_text().strip()
                if len(link_text) > 15 and any(
                    kw in link_text.lower() for kw in ["intern", "engineer", "software"]
                ):
                    title_link = link
                    break

            if not title_link:
                title_link = next(
                    (link for link in all_links if len(link.get_text().strip()) > 15),
                    None,
                )

            if not title_link:
                return None

            job_section = title_link.find_parent("table", id="job-container")
            if not job_section:
                current = title_link
                for _ in range(5):
                    current = current.find_parent("table")
                    if current and len(current.get_text()) > 100:
                        job_section = current
                        break

            if not job_section:
                return None

            company_elem = job_section.find("p", id="job-company-name")
            company = company_elem.get_text().strip() if company_elem else "Unknown"

            title_text = title_link.get_text(separator="|||", strip=True)
            title_parts = title_text.split("|||")

            internship_kw = {
                "intern",
                "engineer",
                "developer",
                "software",
                "data",
                "ml",
                "ai",
            }
            title = next(
                (
                    re.sub(
                        r"\s*(APPLY NOW|Apply|View).*$", "", part, flags=re.I
                    ).strip()
                    for part in title_parts
                    if any(kw in part.lower() for kw in internship_kw) and len(part) > 5
                ),
                "Unknown",
            )

            location = "Unknown"
            remote = "Unknown"
            job_tags = job_section.find_all("p", id="job-tag")

            for tag in job_tags:
                text = tag.get_text(separator="|||", strip=True).split("|||")[0]
                if "$" in text or "referral" in text.lower():
                    continue

                text = re.sub(
                    r"(Team|Department|Division).*$", "", text, flags=re.I
                ).strip()

                if "," in text:
                    parts = text.split(",")
                    if len(parts) == 2:
                        city, state = parts[0].strip(), parts[1].strip()
                        if validate_us_state_code(state):
                            location = f"{city}, {state.upper()}"
                            remote = "On Site"
                            break

                if text.lower() == "remote":
                    location = "Remote"
                    remote = "Remote"
                    break
                elif text.lower() == "hybrid":
                    location = "Hybrid"
                    remote = "Hybrid"
                    break

            age_days = DateParser.extract_days_ago(job_section.get_text())
            actual_url, is_company_site = jobright_auth.resolve_jobright_url(url)

            return {
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": actual_url,
                "sponsorship": "Unknown (Email)",
                "is_company_site": is_company_site,
                "email_age_days": age_days,
            }
        except Exception as e:
            logging.error(f"Failed to parse Jobright email: {e}")
            return None

    @staticmethod
    def parse_ziprecruiter_email(soup, url):
        return None

    @staticmethod
    def parse_adzuna_email(soup, url):
        return None


# ============================================================================
# GitHub Scraper with Enhanced Error Handling
# ============================================================================


class SimplifyGitHubScraper:
    @staticmethod
    def scrape(url, source_name="GitHub"):
        try:
            logging.info(f"Fetching {source_name} from {url}")
            response = retry_request(url)

            if not response:
                logging.error(f"{source_name}: Failed to fetch URL after retries")
                return []

            if response.status_code != 200:
                logging.error(f"{source_name}: HTTP {response.status_code}")
                return []

            logging.info(
                f"{source_name}: Successfully fetched, response length: {len(response.text)}"
            )

            # Try HTML parsing first
            soup, parser = safe_parse_html(response.text)
            if soup:
                logging.info(
                    f"{source_name}: Parsed with {parser}, trying HTML table parsing"
                )
                tables = soup.find_all("table")
                if tables:
                    jobs = SimplifyGitHubScraper._parse_html_tables(soup, source_name)
                    if jobs:
                        logging.info(
                            f"{source_name}: Found {len(jobs)} jobs via HTML tables"
                        )
                        return jobs

            # Fallback to markdown text parsing
            logging.info(
                f"{source_name}: No HTML tables found, trying Markdown parsing"
            )
            jobs = SimplifyGitHubScraper._parse_markdown_text(
                response.text, source_name
            )

            if jobs:
                logging.info(f"{source_name}: Found {len(jobs)} jobs via Markdown")
            else:
                logging.warning(f"{source_name}: Markdown parsing returned 0 jobs")

            return jobs

        except Exception as e:
            logging.error(f"{source_name}: Unexpected error: {e}", exc_info=True)
            return []

    @staticmethod
    def _parse_markdown_text(text, source_name):
        lines = text.split("\n")
        jobs = []

        # Find header
        header_idx = next(
            (i for i, line in enumerate(lines) if _HEADER_PATTERN.search(line)), -1
        )

        if header_idx == -1:
            logging.warning(f"{source_name}: Could not find header pattern in Markdown")
            return []

        logging.info(f"{source_name}: Found header at line {header_idx}")
        header = lines[header_idx]
        delimiter = "\t" if "\t" in header else "|"
        start = header_idx + 1 if delimiter == "\t" else header_idx + 2

        logging.info(
            f"{source_name}: Using delimiter '{delimiter}', starting at line {start}"
        )

        parsed_count = 0
        for line_num, line in enumerate(lines[start:], start=start):
            if not line.strip():
                continue

            parts = [p.strip() for p in line.split(delimiter) if p.strip()]
            if len(parts) < 5:
                continue

            company = _EMOJI_PATTERN.sub("", parts[0]).strip()
            title = _EMOJI_PATTERN.sub("", parts[1]).strip()
            location = _EMOJI_PATTERN.sub("", parts[2]).strip()
            link_cell = parts[3]
            age = parts[4]

            match = _HTML_LINK_PATTERN.search(link_cell) or _MD_LINK_PATTERN.search(
                link_cell
            )
            url = (
                match.group(1)
                if match
                else (link_cell if link_cell.startswith("http") else None)
            )

            if not url or any(marker in line for marker in ["🔒", "❌", "closed"]):
                continue

            jobs.append(
                {
                    "company": company,
                    "title": title,
                    "location": location,
                    "url": url,
                    "age": age,
                    "is_closed": False,
                    "source": source_name,
                }
            )
            parsed_count += 1

        logging.info(f"{source_name}: Parsed {parsed_count} job entries from Markdown")
        return jobs

    @staticmethod
    def _parse_html_tables(soup, source_name):
        jobs = []

        for table in soup.find_all("table"):
            for row in table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue

                company_link = cells[0].find("a")
                if not company_link:
                    continue

                company = _EMOJI_PATTERN.sub("", company_link.get_text(strip=True))
                title = _EMOJI_PATTERN.sub("", cells[1].get_text(strip=True))
                location = _EMOJI_PATTERN.sub("", cells[2].get_text(strip=True))
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
