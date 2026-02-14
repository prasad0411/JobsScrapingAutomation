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
import atexit
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
    MAX_REASONABLE_AGE_DAYS,
    FAILED_SIMPLIFY_CACHE,
)

from utils import PlatformDetector, CompanyNormalizer, CompanyValidator, DateParser
from processors import (
    JobIDExtractor,
    LocationExtractor,
    CompanyExtractor,
    ValidationHelper,
)

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": USER_AGENTS[0]})
_URL_HEALTH_CACHE = {}
_HTTP_RESPONSE_CACHE = {}
_SELENIUM_DRIVER = None
_SELENIUM_LAST_USED = None


def _cleanup_selenium_driver():
    global _SELENIUM_DRIVER
    if _SELENIUM_DRIVER:
        try:
            _SELENIUM_DRIVER.quit()
            logging.info("Selenium driver cleaned up")
        except:
            pass
        _SELENIUM_DRIVER = None


atexit.register(_cleanup_selenium_driver)

_EMOJI_PATTERN = re.compile(
    r"[\U0001f600-\U0001f64f\U0001f300-\U0001f5ff\U0001f680-\U0001f6ff\U0001f1e0-\U0001f1ff]+",
    re.UNICODE,
)
_HEADER_PATTERN = re.compile(
    r"Company.*Role.*Location.*(?:Application|Link).*Date", re.I
)
_HTML_LINK_PATTERN = re.compile(r'<a\s+href="(https?://[^"]+)"')
_MD_LINK_PATTERN = re.compile(r"\[.*?\]\((https?://[^\)]+)\)")

STRICT_JOB_BOARDS = [
    "myworkdayjobs.com",
    "wd1.myworkdayjobs",
    "wd3.myworkdayjobs",
    "wd5.myworkdayjobs",
    "wd10.myworkdayjobs",
    "wd12.myworkdayjobs",
    "greenhouse.io",
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "lever.co",
    "jobs.lever.co",
    "smartrecruiters.com",
    "jobs.smartrecruiters.com",
    "ashbyhq.com",
    "jobs.ashbyhq.com",
    "icims.com",
    "workable.com",
    "apply.workable.com",
    "amazon.jobs",
    "jobs.ea.com",
    "breezy.hr",
    "applytojob.com",
]


def safe_parse_html(html_content, preferred_parser=None):
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
    logging.error(f"All parsers failed")
    return None, None


def retry_request(url, method="GET", max_retries=MAX_RETRIES, **kwargs):
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
                time.sleep(RETRY_DELAY_SECONDS * (BACKOFF_MULTIPLIER**attempt))
            else:
                logging.warning(f"HTTP {response.status_code} for {url}")
                return response
        except:
            time.sleep(RETRY_DELAY_SECONDS * (BACKOFF_MULTIPLIER**attempt))
    return None


class SimplifyRedirectResolver:
    _github_readme_cache = None
    _github_readme_fetch_time = None

    @staticmethod
    def load_failed_cache():
        if os.path.exists(FAILED_SIMPLIFY_CACHE):
            try:
                with open(FAILED_SIMPLIFY_CACHE, "r") as f:
                    return json.load(f)
            except:
                return {}
        return {}

    @staticmethod
    def save_failed_cache(cache):
        try:
            with open(FAILED_SIMPLIFY_CACHE, "w") as f:
                json.dump(cache, f, indent=2)
        except:
            pass

    @staticmethod
    @lru_cache(maxsize=500)
    def resolve(simplify_url):
        if "simplify.jobs/p/" not in simplify_url.lower():
            return simplify_url, False
        job_id_match = re.search(r"/p/([a-f0-9-]+)", simplify_url)
        if not job_id_match:
            return simplify_url, False
        job_id = job_id_match.group(1)
        failed_cache = SimplifyRedirectResolver.load_failed_cache()
        today = time.strftime("%Y-%m-%d")
        if job_id in failed_cache and failed_cache[job_id] == today:
            return simplify_url, False
        click_url = f"https://simplify.jobs/jobs/click/{job_id}"

        actual_url = SimplifyRedirectResolver._method_1_http_redirect(click_url)
        if actual_url:
            logging.info(f"Simplify HTTP: {actual_url[:70]}")
            return actual_url, True

        actual_url = SimplifyRedirectResolver._method_2_selenium_click(click_url)
        if actual_url:
            logging.info(f"Simplify Selenium: {actual_url[:70]}")
            return actual_url, True

        actual_url = SimplifyRedirectResolver._method_3_api_fetch(job_id)
        if actual_url:
            logging.info(f"Simplify API: {actual_url[:70]}")
            return actual_url, True

        actual_url = SimplifyRedirectResolver._method_4_github_lookup(job_id)
        if actual_url:
            logging.info(f"Simplify GitHub: {actual_url[:70]}")
            return actual_url, True

        failed_cache[job_id] = today
        SimplifyRedirectResolver.save_failed_cache(failed_cache)

        try:
            with open("simplify_manual_review.txt", "a") as f:
                f.write(f"{job_id}\t{simplify_url}\t{today}\n")
        except Exception:
            pass

        logging.warning(f"All 4 methods failed: {simplify_url[:60]}")
        return simplify_url, False

    @staticmethod
    def _method_1_http_redirect(click_url):
        try:
            response = requests.get(
                click_url,
                allow_redirects=True,
                timeout=15,
                headers={"User-Agent": USER_AGENTS[0]},
            )

            if response and response.url != click_url:
                if SimplifyRedirectResolver._is_valid_job_url(response.url):
                    return response.url

            if response and response.status_code == 200:
                from extractors import safe_parse_html

                soup, _ = safe_parse_html(response.text)
                if soup:
                    meta_refresh = soup.find("meta", {"http-equiv": "refresh"})
                    if meta_refresh:
                        content = meta_refresh.get("content", "")
                        match = re.search(r"url=(.+)", content, re.I)
                        if match:
                            redirect_url = match.group(1).strip().strip('"').strip("'")
                            if SimplifyRedirectResolver._is_valid_job_url(redirect_url):
                                return redirect_url

            if response and response.status_code == 200:
                js_match = re.search(
                    r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+)', response.text
                )
                if js_match:
                    redirect_url = js_match.group(1)
                    if SimplifyRedirectResolver._is_valid_job_url(redirect_url):
                        return redirect_url

        except:
            pass
        return None

    @staticmethod
    def _method_2_selenium_click(click_url):
        global _SELENIUM_DRIVER

        if not SELENIUM_AVAILABLE:
            return None

        try:
            if _SELENIUM_DRIVER is None:
                chrome_options = Options()
                chrome_options.add_argument("--headless")
                chrome_options.add_argument(
                    "--disable-blink-features=AutomationControlled"
                )
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument(f"user-agent={USER_AGENTS[0]}")
                chrome_options.add_experimental_option(
                    "excludeSwitches", ["enable-logging"]
                )
                service = Service(ChromeDriverManager().install())
                _SELENIUM_DRIVER = webdriver.Chrome(
                    service=service, options=chrome_options
                )
                _SELENIUM_DRIVER.set_page_load_timeout(20)
                logging.info(
                    "Selenium driver initialized (SimplifyRedirectResolver will reuse)"
                )

            _SELENIUM_DRIVER.get(click_url)

            for wait_time in [3, 2, 2]:
                time.sleep(wait_time)
                current_url = _SELENIUM_DRIVER.current_url

                if current_url != click_url:
                    if SimplifyRedirectResolver._is_valid_job_url(current_url):
                        return current_url

                if "simplify.jobs" not in current_url:
                    if SimplifyRedirectResolver._is_valid_job_url(current_url):
                        return current_url

        except Exception as e:
            logging.debug(f"SimplifyRedirectResolver Selenium failed: {e}")
            if _SELENIUM_DRIVER:
                try:
                    _SELENIUM_DRIVER.quit()
                except:
                    pass
                _SELENIUM_DRIVER = None

        return None

    @staticmethod
    def _is_valid_job_url(url):
        if not url or not url.startswith("http"):
            return False
        url_lower = url.lower()
        if "simplify.jobs" in url_lower:
            return False
        for board in STRICT_JOB_BOARDS:
            if board in url_lower:
                must_have = (
                    "/job/" in url_lower
                    or "/jobs/" in url_lower
                    or "/external/" in url_lower
                    or "/embed/" in url_lower
                    or "token=" in url_lower
                )
                if must_have:
                    reject = [
                        "/news/",
                        "/blog/",
                        "/press/",
                        "/article/",
                        "/accessibility",
                        "/privacy",
                        "/canada",
                        "/introduceyourself",
                        "/rewards",
                        "/wellness",
                        "/diversity",
                        "/inclusion",
                        "/about",
                        "/contact",
                    ]
                    if not any(pattern in url_lower for pattern in reject):
                        return True
        return False

    @staticmethod
    def _method_3_api_fetch(job_id):
        try:
            api_url = f"https://simplify.jobs/api/jobs/{job_id}"
            response = requests.get(
                api_url,
                timeout=10,
                headers={"User-Agent": USER_AGENTS[0]},
            )

            if response and response.status_code == 200:
                try:
                    data = response.json()
                    if "url" in data or "jobUrl" in data or "link" in data:
                        actual_url = (
                            data.get("url") or data.get("jobUrl") or data.get("link")
                        )
                        if SimplifyRedirectResolver._is_valid_job_url(actual_url):
                            return actual_url
                except:
                    pass

        except:
            pass
        return None

    @staticmethod
    def _method_4_github_lookup(job_id):
        try:
            import time

            current_time = time.time()

            if (
                SimplifyRedirectResolver._github_readme_cache is None
                or SimplifyRedirectResolver._github_readme_fetch_time is None
                or current_time - SimplifyRedirectResolver._github_readme_fetch_time
                > 600
            ):

                readme_url = "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/master/README.md"
                response = requests.get(readme_url, timeout=15)

                if response and response.status_code == 200:
                    SimplifyRedirectResolver._github_readme_cache = response.text
                    SimplifyRedirectResolver._github_readme_fetch_time = current_time
                else:
                    return None

            readme_text = SimplifyRedirectResolver._github_readme_cache
            if readme_text and job_id in readme_text:
                lines = readme_text.split("\n")
                for line in lines:
                    if job_id in line:
                        match = re.search(r"https?://[^\s\)]+", line)
                        if match:
                            url = match.group(0)
                            if "simplify.jobs" not in url and job_id not in url:
                                if SimplifyRedirectResolver._is_valid_job_url(url):
                                    return url
        except:
            pass
        return None


class JobrightRedirectResolver:
    """NEW: Resolves Jobright tracking URLs to actual job URLs"""

    _email_html_cache = {}

    @staticmethod
    def cache_email_html(email_id, html_content):
        """Store email HTML for URL extraction"""
        JobrightRedirectResolver._email_html_cache[email_id] = html_content

    @staticmethod
    def resolve(jobright_url, email_html=None):
        """
        Resolve Jobright tracking URL to actual job URL
        Returns: (actual_url, success_boolean)
        """
        if "jobright.ai" not in jobright_url.lower():
            return jobright_url, False

        job_id = JobrightRedirectResolver._extract_job_id(jobright_url)
        if not job_id:
            logging.debug("Jobright: No job ID found in URL")
            return jobright_url, False

        actual_url = JobrightRedirectResolver._method_1_email_html(job_id, email_html)
        if actual_url:
            logging.info(f"Jobright HTTP: {actual_url[:80]}")
            return actual_url, True

        actual_url = JobrightRedirectResolver._method_2_http_fetch(jobright_url)
        if actual_url:
            logging.info(f"Jobright HTTP: {actual_url[:80]}")
            return actual_url, True

        actual_url = JobrightRedirectResolver._method_3_selenium(jobright_url)
        if actual_url:
            logging.info(f"Jobright Selenium: {actual_url[:80]}")
            return actual_url, True

        actual_url = JobrightRedirectResolver._method_4_authenticated(
            jobright_url, job_id
        )
        if actual_url:
            logging.info(f"Jobright Auth API: {actual_url[:80]}")
            return actual_url, True

        logging.warning(f"Jobright resolution failed: {jobright_url}")
        return jobright_url, False

    @staticmethod
    def _extract_job_id(url):
        """Extract job ID from Jobright URL"""
        match = re.search(r"jobright\.ai/jobs/info/([a-f0-9]+)", url, re.I)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def _method_1_email_html(job_id, email_html):
        """ENHANCED: Extract actual URL from email HTML with multiple strategies"""
        if not email_html:
            return None

        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(email_html, "html.parser")

            all_links = []
            for link in soup.find_all("a", href=True):
                href = link["href"]
                all_links.append(href)

                if job_id in href:
                    continue

                valid_domains = [
                    ".myworkdayjobs.com",
                    "greenhouse.io",
                    "lever.co",
                    "ashbyhq.com",
                    "smartrecruiters.com",
                    "icims.com",
                    "taleo.net",
                    "ultipro.com",
                    "workable.com",
                    "breezy.hr",
                    "bamboohr.com",
                    "jobvite.com",
                ]

                if any(domain in href for domain in valid_domains):
                    if "/job/" in href or "/jobs/" in href or "/career" in href:
                        logging.debug(f"Jobright email HTML: Found {href[:80]}")
                        return href

            for link_href in all_links:
                if (
                    link_href.startswith("http")
                    and "jobright" not in link_href
                    and "linkedin" not in link_href
                ):
                    if any(
                        x in link_href
                        for x in ["apply", "career", "position", "job", "requisition"]
                    ):
                        logging.debug(f"Jobright email HTML fallback: {link_href[:80]}")
                        return link_href

        except Exception as e:
            logging.debug(f"Jobright email HTML extraction failed: {e}")

        return None

    @staticmethod
    def _method_2_http_fetch(jobright_url):
        """ENHANCED: Fetch Jobright page and extract actual URL with multiple strategies"""
        try:
            response = retry_request(jobright_url, max_retries=2)
            if not response:
                return None

            from bs4 import BeautifulSoup

            soup = BeautifulSoup(response.content, "html.parser")

            script_tags = soup.find_all("script")
            for script in script_tags:
                if script.string:
                    url_matches = re.findall(
                        r'https?://[^\s"\'<>]+(?:myworkdayjobs|greenhouse|lever|ashby|icims|smartrecruiters)[^\s"\'<>]+',
                        script.string,
                    )
                    for url_match in url_matches:
                        if "job" in url_match.lower():
                            logging.debug(
                                f"Jobright script extraction: {url_match[:80]}"
                            )
                            return url_match

            apply_links = soup.find_all(
                "a", {"class": lambda x: x and "apply" in str(x).lower()}
            )
            for link in apply_links:
                href = link.get("href", "")
                if href and href.startswith("http") and "jobright.ai" not in href:
                    logging.debug(f"Jobright apply button: {href[:80]}")
                    return href

            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "jobright.ai" in href or "linkedin.com" in href:
                    continue

                if any(
                    domain in href
                    for domain in [
                        ".myworkdayjobs.com",
                        "greenhouse.io",
                        "ashbyhq.com",
                        "icims.com",
                    ]
                ):
                    logging.debug(f"Jobright link scan: {href[:80]}")
                    return href

        except Exception as e:
            logging.debug(f"Jobright HTTP fetch failed: {e}")

        return None

    @staticmethod
    def _method_3_selenium(jobright_url):
        """ENHANCED: Use Selenium to click through and get final URL"""
        global _SELENIUM_DRIVER

        if not SELENIUM_AVAILABLE:
            return None

        try:
            if _SELENIUM_DRIVER is None:
                from selenium import webdriver
                from selenium.webdriver.chrome.service import Service
                from selenium.webdriver.chrome.options import Options
                from webdriver_manager.chrome import ChromeDriverManager

                chrome_options = Options()
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                service = Service(ChromeDriverManager().install())
                _SELENIUM_DRIVER = webdriver.Chrome(
                    service=service, options=chrome_options
                )
                _SELENIUM_DRIVER.set_page_load_timeout(25)

            _SELENIUM_DRIVER.get(jobright_url)
            time.sleep(5)

            try:
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC

                apply_button = WebDriverWait(_SELENIUM_DRIVER, 10).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "//a[contains(text(), 'Apply') or contains(@class, 'apply')]",
                        )
                    )
                )

                apply_url = apply_button.get_attribute("href")
                if apply_url and "jobright.ai" not in apply_url:
                    logging.debug(f"Jobright Selenium button click: {apply_url[:80]}")
                    return apply_url

            except:
                pass

            current_url = _SELENIUM_DRIVER.current_url
            if current_url != jobright_url and "jobright.ai" not in current_url:
                logging.debug(f"Jobright Selenium redirect: {current_url[:80]}")
                return current_url

        except Exception as e:
            logging.debug(f"Jobright Selenium failed: {e}")

        return None

    @staticmethod
    def _method_4_authenticated(jobright_url, job_id):
        """NEW: Try authenticated request to Jobright API"""
        try:
            import json
            import os

            cookies_file = "jobright_cookies.json"
            if not os.path.exists(cookies_file):
                return None

            with open(cookies_file, "r") as f:
                cookies = json.load(f)

            session = requests.Session()
            for cookie in cookies:
                session.cookies.set(
                    cookie["name"],
                    cookie["value"],
                    domain=cookie.get("domain", "jobright.ai"),
                )

            api_url = f"https://jobright.ai/api/jobs/{job_id}"
            response = session.get(api_url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if "jobUrl" in data:
                    logging.debug(f"Jobright auth API: {data['jobUrl'][:80]}")
                    return data["jobUrl"]
                elif "url" in data:
                    return data["url"]

        except Exception as e:
            logging.debug(f"Jobright authenticated API failed: {e}")

        return None


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
            logging.warning("Selenium not available")
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

            if not actual_url or "jobright.ai" in actual_url:
                try:
                    origin_link = soup.find("a", class_=re.compile(r"index_origin"))

                    if not origin_link:
                        origin_link = soup.find(
                            "a", string=re.compile(r"original\s+job\s+post", re.I)
                        )

                    if not origin_link:
                        for link in soup.find_all("a", href=True):
                            link_text = link.get_text().strip().lower()
                            if link_text and (
                                "original" in link_text or "job post" in link_text
                            ):
                                href = link.get("href")
                                if href and "jobright.ai" not in href:
                                    origin_link = link
                                    break

                    if origin_link:
                        html_url = origin_link.get("href")
                        if (
                            html_url
                            and html_url.startswith("http")
                            and "jobright.ai" not in html_url
                        ):
                            actual_url = html_url
                            is_company_site = True
                            logging.info(
                                f"Resolved Jobright URL via HTML to {actual_url[:70]}"
                            )

                except Exception as html_error:
                    logging.debug(f"HTML URL extraction failed: {html_error}")

            if actual_url and "jobright.ai" not in actual_url:
                logging.info(f"Resolved Jobright URL to {actual_url[:70]}")
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


class EmailExtractor:
    def __init__(self):
        self.service = None

    def authenticate(self):
        creds = None
        if os.path.exists(GMAIL_TOKEN_FILE):
            try:
                with open(GMAIL_TOKEN_FILE, "rb") as token:
                    creds = pickle.load(token)
            except Exception as e:
                logging.warning(f"Corrupted token file: {e}")
                try:
                    os.remove(GMAIL_TOKEN_FILE)
                except:
                    pass
                creds = None
        if creds and not creds.valid:
            if creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logging.warning(f"Token refresh failed: {e}")
                    print("⚠️  Gmail token expired - re-authenticating...")
                    try:
                        os.remove(GMAIL_TOKEN_FILE)
                    except:
                        pass
                    creds = None
        if not creds:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    GMAIL_CREDS_FILE, GMAIL_SCOPES
                )
                creds = flow.run_local_server(port=0)
                with open(GMAIL_TOKEN_FILE, "wb") as token:
                    pickle.dump(creds, token)
                print("✓ Gmail authenticated successfully")
            except Exception as e:
                logging.error(f"Gmail authentication failed: {e}")
                print(f"✗ Gmail authentication failed: {e}")
                return False
        self.service = build("gmail", "v1", credentials=creds)
        return True

    def fetch_job_emails(self, max_results=100):
        if not self.service:
            print("[Gmail] Authenticating...")
            if not self.authenticate():
                return []
        if not self.service:
            print("✗ Gmail authentication failed")
            logging.error("Gmail service not initialized")
            return []
        try:
            results = (
                self.service.users()
                .messages()
                .list(
                    userId="me",
                    q='label:"Job Hunt" newer_than:3d',
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
            emails_with_data = []
            for message in messages:
                try:
                    msg = (
                        self.service.users()
                        .messages()
                        .get(userId="me", id=message["id"], format="full")
                        .execute()
                    )
                    internal_date = int(msg.get("internalDate", 0))
                    email_id = message["id"]
                    headers = {
                        h["name"]: h["value"] for h in msg["payload"].get("headers", [])
                    }
                    sender = self._detect_sender(headers.get("From", ""))
                    subject = headers.get("Subject", "Unknown Subject")
                    html_content = self._extract_html(msg["payload"])
                    if html_content:
                        urls = self._extract_job_urls(html_content)
                        if urls:
                            emails_with_data.append(
                                {
                                    "email_id": email_id,
                                    "timestamp": internal_date,
                                    "sender": sender,
                                    "subject": subject,
                                    "html": html_content,
                                    "urls": urls,
                                }
                            )
                except Exception as e:
                    logging.error(f"Failed to process email: {e}")
                    continue
            emails_with_data.sort(key=lambda x: x["timestamp"], reverse=True)
            total_urls = sum(len(email["urls"]) for email in emails_with_data)
            print(f"Total: {total_urls} job URLs from {len(emails_with_data)} emails\n")
            return emails_with_data
        except Exception as e:
            logging.error(f"Gmail fetch error: {e}")
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
            "/preferences",
            "/settings",
            "/explore",
            "view-more",
            "install-autofill",
        ]
        return any(p in url.lower() for p in non_job)


class PageFetcher:
    def __init__(self):
        self.session = _SESSION

    def check_url_health(self, url):
        if url in _URL_HEALTH_CACHE:
            return _URL_HEALTH_CACHE[url]
        response = retry_request(url, method="HEAD", max_retries=2)
        if response:
            is_healthy = 200 <= response.status_code < 400 or response.status_code in [
                301,
                302,
                303,
                307,
                308,
            ]
            _URL_HEALTH_CACHE[url] = (is_healthy, response.status_code)
            return is_healthy, response.status_code
        _URL_HEALTH_CACHE[url] = (False, 0)
        return False, 0

    def fetch_page(self, url):
        if url in _HTTP_RESPONSE_CACHE:
            cached = _HTTP_RESPONSE_CACHE[url]
            return cached["response"], cached["final_url"], cached["page_source"]

        is_healthy, status = self.check_url_health(url)
        if not is_healthy and status in [404, 403, 405]:
            logging.info(f"Skipping unhealthy URL: {url} (status {status})")
            _HTTP_RESPONSE_CACHE[url] = {
                "response": None,
                "final_url": None,
                "page_source": None,
            }
            return None, None, None

        if self._is_js_heavy_platform(url):
            html, final_url, page_source = self._try_selenium(url)
            if html:
                response = self._create_mock_response(html, final_url)
                _HTTP_RESPONSE_CACHE[url] = {
                    "response": response,
                    "final_url": final_url,
                    "page_source": page_source,
                }
                return response, final_url, page_source

        response = retry_request(url)
        if response and response.status_code == 200:
            _HTTP_RESPONSE_CACHE[url] = {
                "response": response,
                "final_url": response.url,
                "page_source": response.text,
            }
            return response, response.url, response.text

        if SELENIUM_AVAILABLE:
            logging.info(f"Standard request failed, trying Selenium for {url}")
            html, final_url, page_source = self._try_selenium(url)
            if html:
                response = self._create_mock_response(html, final_url)
                _HTTP_RESPONSE_CACHE[url] = {
                    "response": response,
                    "final_url": final_url,
                    "page_source": page_source,
                }
                return response, final_url, page_source

        _HTTP_RESPONSE_CACHE[url] = {
            "response": None,
            "final_url": None,
            "page_source": None,
        }
        return None, None, None

    @staticmethod
    def _is_js_heavy_platform(url):
        if not url:
            return False
        js_platforms = [
            "workday",
            "myworkdayjobs",
            "greenhouse.io",
            "oracle",
            "oraclecloud",
            "ashbyhq",
        ]
        return any(platform in url.lower() for platform in js_platforms)

    @staticmethod
    def _try_selenium(url):
        global _SELENIUM_DRIVER, _SELENIUM_LAST_USED

        if not SELENIUM_AVAILABLE:
            return None, None, None

        try:
            if _SELENIUM_DRIVER is None:
                chrome_options = Options()
                chrome_options.add_argument("--headless")
                chrome_options.add_argument(
                    "--disable-blink-features=AutomationControlled"
                )
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument(f"user-agent={USER_AGENTS[0]}")
                chrome_options.add_experimental_option(
                    "excludeSwitches", ["enable-logging"]
                )
                service = Service(ChromeDriverManager().install())
                _SELENIUM_DRIVER = webdriver.Chrome(
                    service=service, options=chrome_options
                )
                _SELENIUM_DRIVER.set_page_load_timeout(30)
                logging.info("Selenium driver initialized (will be reused)")

            _SELENIUM_DRIVER.get(url)
            _SELENIUM_LAST_USED = time.time()

            url_lower = url.lower()
            if "oracle" in url_lower or "oraclecloud" in url_lower:
                time.sleep(15)
                try:
                    WebDriverWait(_SELENIUM_DRIVER, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "h1"))
                    )
                except:
                    pass
            elif "workday" in url_lower:
                time.sleep(15)
            elif "greenhouse" in url_lower:
                time.sleep(10)
            elif "ashby" in url_lower:
                time.sleep(6)
            else:
                time.sleep(3)

            _SELENIUM_DRIVER.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);"
            )
            time.sleep(2)
            page_source = _SELENIUM_DRIVER.page_source
            current_url = _SELENIUM_DRIVER.current_url

            return page_source, current_url, page_source

        except Exception as e:
            logging.error(f"Selenium failed for {url}: {e}")

            if _SELENIUM_DRIVER:
                try:
                    _SELENIUM_DRIVER.quit()
                except:
                    pass
                _SELENIUM_DRIVER = None

            return None, None, None

    @staticmethod
    def _create_mock_response(html, url):
        return type("obj", (object,), {"text": html, "status_code": 200, "url": url})()


class PageParser:
    @staticmethod
    def extract_company(soup, url):
        platform = PlatformDetector.detect(url)
        return CompanyExtractor.extract_all_methods(url, soup)

    @staticmethod
    def extract_title(soup):
        if not soup:
            return "Unknown"

        candidates = []

        try:
            json_ld = soup.find("script", type="application/ld+json")
            if json_ld:
                try:
                    data = json.loads(json_ld.string)
                    if isinstance(data, dict) and data.get("title"):
                        title = data["title"]
                        if 5 < len(title) < 200:
                            candidates.append((title, 100))
                except:
                    pass

            meta_title = soup.find("meta", {"property": "og:title"})
            if meta_title and meta_title.get("content"):
                title = meta_title.get("content").strip()
                if 5 < len(title) < 200 and "careers" not in title.lower():
                    candidates.append((title, 95))

            meta_title_name = soup.find("meta", {"name": "title"})
            if meta_title_name and meta_title_name.get("content"):
                title = meta_title_name.get("content").strip()
                if 5 < len(title) < 200:
                    candidates.append((title, 90))

            title_selectors = [
                ("h1.job-title", 95),
                ("h1[class*='job']", 90),
                ("h1[class*='title']", 85),
                (".job-title", 80),
                (".job-details-title", 80),
                ("[class*='job-title']", 75),
                ("[data-automation='job-title']", 90),
                ("[data-test='job-title']", 90),
                ("h1[itemprop='title']", 85),
                ("span[itemprop='title']", 75),
                ("div.job-title", 80),
                ("h2.job-title", 75),
            ]

            for selector, priority in title_selectors:
                try:
                    elem = soup.select_one(selector)
                    if elem:
                        title = elem.get_text().strip()
                        if 5 < len(title) < 200:
                            candidates.append((title, priority))
                except:
                    pass

            h1_tags = soup.find_all("h1", limit=3)
            for h1 in h1_tags:
                title = h1.get_text().strip()
                if 5 < len(title) < 200 and len(title.split()) > 1:
                    candidates.append((title, 70))

            if candidates:
                candidates.sort(key=lambda x: x[1], reverse=True)

                for title, priority in candidates:
                    title_lower = title.lower()

                    if any(
                        bad in title_lower
                        for bad in [
                            "careers",
                            "job board",
                            "opportunities",
                            "join our team",
                            "working at",
                            "about us",
                            "company",
                            "apply now",
                            "search jobs",
                            "current openings",
                            "work with us",
                        ]
                    ):
                        continue

                    job_keywords = [
                        "intern",
                        "co-op",
                        "software",
                        "engineer",
                        "developer",
                        "analyst",
                        "data",
                        "scientist",
                        "architect",
                        "designer",
                        "programmer",
                        "manager",
                        "specialist",
                        "coordinator",
                        "associate",
                        "technical",
                        "technology",
                        "ai",
                        "ml",
                    ]
                    if any(kw in title_lower for kw in job_keywords):
                        if len(title.split()) >= 2:
                            return title

                for title, priority in candidates:
                    if len(title.split()) >= 2:
                        return title

        except Exception as e:
            logging.debug(f"Title extraction failed: {e}")

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
            days = DateParser.extract_days_ago(page_text)
            if days is not None:
                if days > MAX_REASONABLE_AGE_DAYS or days < 0:
                    return None
            return days
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


class JobTypeExtractor:
    @staticmethod
    def extract_all_methods(soup, url, title):
        if not soup:
            return "Unknown"

        results = []

        results.append(JobTypeExtractor.extract_from_json_ld(soup))
        results.append(JobTypeExtractor.extract_from_meta(soup))
        results.append(JobTypeExtractor.extract_from_selectors(soup))
        results.append(JobTypeExtractor.extract_from_page_text(soup))
        results.append(JobTypeExtractor.extract_from_url(url))

        valid_results = [r for r in results if r and r != "Unknown"]

        if not valid_results:
            return "Unknown"

        from collections import Counter

        counts = Counter(valid_results)
        most_common = counts.most_common(1)[0][0]

        return most_common

    @staticmethod
    def extract_from_json_ld(soup):
        try:
            script = soup.find("script", type="application/ld+json")
            if script:
                data = json.loads(script.string)
                emp_type = data.get("employmentType", "")
                return JobTypeExtractor._normalize_type(emp_type)
        except:
            pass
        return "Unknown"

    @staticmethod
    def extract_from_meta(soup):
        try:
            for prop in ["og:job:type", "job:type", "employmentType"]:
                meta = soup.find("meta", {"property": prop})
                if meta and meta.get("content"):
                    return JobTypeExtractor._normalize_type(meta.get("content"))

            for name in ["job-type", "employment-type", "jobType"]:
                meta = soup.find("meta", {"name": name})
                if meta and meta.get("content"):
                    return JobTypeExtractor._normalize_type(meta.get("content"))
        except:
            pass
        return "Unknown"

    @staticmethod
    def extract_from_selectors(soup):
        try:
            selectors = [
                ("span", {"class": "job-type"}),
                ("div", {"class": "employment-type"}),
                ("dd", {"class": "job-classification"}),
                ("span", {"class": re.compile(r"job.*type", re.I)}),
                ("div", {"class": re.compile(r"employment.*type", re.I)}),
            ]

            for tag, attrs in selectors:
                elem = soup.find(tag, attrs)
                if elem:
                    text = elem.get_text().strip()
                    normalized = JobTypeExtractor._normalize_type(text)
                    if normalized != "Unknown":
                        return normalized
        except:
            pass
        return "Unknown"

    @staticmethod
    def extract_from_page_text(soup):
        try:
            text = soup.get_text()[:2000]

            patterns = [
                (r"(?:job|employment)\s+type:?\s*(intern(?:ship)?|co-?op)", 1),
                (r"time\s+type:?\s*((?:full|part)[\s-]time)", 1),
            ]

            for pattern, group in patterns:
                match = re.search(pattern, text, re.I)
                if match:
                    return JobTypeExtractor._normalize_type(match.group(group))
        except:
            pass
        return "Unknown"

    @staticmethod
    def extract_from_url(url):
        try:
            url_lower = url.lower()
            if "/internship/" in url_lower or "/intern/" in url_lower:
                return "Internship"
            if "/co-op/" in url_lower or "/coop/" in url_lower:
                return "Co-op"
            if "/fellowship/" in url_lower:
                return "Fellowship"
        except:
            pass
        return "Unknown"

    @staticmethod
    def _normalize_type(text):
        if not text:
            return "Unknown"

        text_lower = text.lower().strip()

        if text_lower in ["intern", "internship", "intern/co-op", "summer intern"]:
            return "Internship"
        if text_lower in ["co-op", "coop", "cooperative", "co-operative"]:
            return "Co-op"
        if text_lower in ["fellowship", "fellow"]:
            return "Fellowship"
        if text_lower in ["apprentice", "apprenticeship"]:
            return "Apprenticeship"
        if text_lower in ["trainee", "training program"]:
            return "Trainee"
        if text_lower in ["full time", "full-time", "fulltime"]:
            return "Full Time"
        if text_lower in ["part time", "part-time", "parttime"]:
            return "Part Time"

        return "Unknown"


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
                    kw in link_text.lower()
                    for kw in ["intern", "engineer", "software", "data", "analyst"]
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
            company = SourceParsers._extract_company_multi_method(job_section, soup)
            title = SourceParsers._extract_title_multi_method(title_link, job_section)
            location, remote = SourceParsers._extract_location_multi_method(
                job_section, soup
            )
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
    def _extract_company_multi_method(job_section, soup):
        company_elem = job_section.find("p", id="job-company-name")
        if company_elem:
            company = company_elem.get_text().strip()
            if company and company != "Unknown":
                return company
        company_elem = job_section.find("div", class_="company-name")
        if company_elem:
            company = company_elem.get_text().strip()
            if company:
                return company
        for header in job_section.find_all(["h2", "h3", "h4"]):
            text = header.get_text().strip()
            if (
                len(text) > 3
                and len(text) < 50
                and not any(
                    kw in text.lower()
                    for kw in ["intern", "engineer", "software", "match", "referral"]
                )
            ):
                return text
        all_text = job_section.get_text()
        lines = [line.strip() for line in all_text.split("\n") if line.strip()]
        for line in lines[:10]:
            if len(line) > 3 and len(line) < 50:
                if not any(
                    kw in line.lower()
                    for kw in [
                        "match",
                        "apply",
                        "referral",
                        "ago",
                        "hour",
                        "minute",
                        "/hr",
                    ]
                ):
                    if line[0].isupper() and not line.isupper():
                        return line
        return "Unknown"

    @staticmethod
    def _extract_title_multi_method(title_link, job_section):
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
            "analyst",
            "co-op",
            "coop",
        }
        title = next(
            (
                re.sub(r"\s*(APPLY NOW|Apply|View).*$", "", part, flags=re.I).strip()
                for part in title_parts
                if any(kw in part.lower() for kw in internship_kw) and len(part) > 5
            ),
            None,
        )
        if title:
            return title
        for elem in job_section.find_all(["h1", "h2", "h3"]):
            text = elem.get_text().strip()
            if any(kw in text.lower() for kw in internship_kw) and len(text) > 10:
                return re.sub(
                    r"\s*(APPLY NOW|Apply|View).*$", "", text, flags=re.I
                ).strip()
        return "Unknown"

    @staticmethod
    def _extract_location_multi_method(job_section, soup):
        location = "Unknown"
        remote = "Unknown"
        job_tags = job_section.find_all("p", id="job-tag")
        for tag in job_tags:
            text = tag.get_text(separator="|||", strip=True).split("|||")[0]
            if "$" in text or "referral" in text.lower() or "/hr" in text.lower():
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
                        return location, remote
            if "remote" in text.lower():
                location = "Remote"
                remote = "Remote"
                return location, remote
            elif "hybrid" in text.lower():
                location = "Hybrid"
                remote = "Hybrid"
                return location, remote
        all_text = job_section.get_text()
        city_state_pattern = r"\b([A-Z][a-z]+(?: [A-Z][a-z]+)*),\s*([A-Z]{2})\b"
        matches = re.findall(city_state_pattern, all_text)
        for city, state in matches:
            if validate_us_state_code(state):
                location = f"{city}, {state}"
                remote = "On Site"
                return location, remote
        if re.search(r"\b(remote|100%\s*remote|fully\s*remote)\b", all_text, re.I):
            location = "Remote"
            remote = "Remote"
            return location, remote
        if re.search(r"\bhybrid\b", all_text, re.I):
            location = "Hybrid"
            remote = "Hybrid"
            return location, remote
        all_soup_text = soup.get_text()
        matches = re.findall(city_state_pattern, all_soup_text)
        for city, state in matches[:5]:
            if validate_us_state_code(state):
                location = f"{city}, {state}"
                remote = "On Site"
                return location, remote
        return location, remote

    @staticmethod
    def parse_ziprecruiter_email(soup, url):
        return None

    @staticmethod
    def parse_adzuna_email(soup, url):
        return None


class SimplifyGitHubScraper:
    @staticmethod
    def scrape(url, source_name="GitHub"):
        try:
            logging.info(f"Fetching {source_name} from {url}")
            response = retry_request(url)
            if not response:
                logging.error(f"{source_name}: Failed to fetch URL")
                return []
            if response.status_code != 200:
                logging.error(f"{source_name}: HTTP {response.status_code}")
                return []
            logging.info(f"{source_name}: Fetched, length: {len(response.text)}")
            soup, parser = safe_parse_html(response.text)
            if soup:
                logging.info(f"{source_name}: Parsed with {parser}")
                tables = soup.find_all("table")
                if tables:
                    jobs = SimplifyGitHubScraper._parse_html_tables(soup, source_name)
                    if jobs:
                        logging.info(f"{source_name}: Found {len(jobs)} jobs via HTML")
                        return jobs
            logging.info(f"{source_name}: Trying Markdown")
            jobs = SimplifyGitHubScraper._parse_markdown_text(
                response.text, source_name
            )
            if jobs:
                logging.info(f"{source_name}: Found {len(jobs)} jobs via Markdown")
            return jobs
        except Exception as e:
            logging.error(f"{source_name}: Error: {e}")
            return []

    @staticmethod
    def _parse_markdown_text(text, source_name):
        lines = text.split("\n")
        jobs = []
        header_idx = next(
            (i for i, line in enumerate(lines) if _HEADER_PATTERN.search(line)), -1
        )
        if header_idx == -1:
            return []
        header = lines[header_idx]
        delimiter = "\t" if "\t" in header else "|"
        start = header_idx + 1 if delimiter == "\t" else header_idx + 2
        for line in lines[start:]:
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
