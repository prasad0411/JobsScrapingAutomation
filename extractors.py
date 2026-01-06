#!/usr/bin/env python3
# cSpell:disable
"""
Extraction module - PRODUCTION v5.0 FINAL
ALL IMPROVEMENTS: Undetected Chrome, dual libraries, comprehensive patterns
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
            chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

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
        """Fetch emails in NEWEST-FIRST order, preserving URL sequence."""
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

            emails_with_time = []
            for message in messages:
                msg = (
                    self.service.users()
                    .messages()
                    .get(userId="me", id=message["id"], format="full")
                    .execute()
                )

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

            emails_with_time.sort(key=lambda x: x["timestamp"], reverse=True)

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
        """Extract URLs preserving order."""
        soup = BeautifulSoup(email_html, "html.parser")
        job_urls = []
        seen = set()

        for link in soup.find_all("a", href=True):
            url = link.get("href", "")
            if not url.startswith("http"):
                continue

            is_job_board = any(domain in url.lower() for domain in JOB_BOARD_DOMAINS)
            if is_job_board and not self._is_non_job_url(url) and url not in seen:
                job_urls.append(url)
                seen.add(url)

        return job_urls

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
    """Fetches web pages using multiple fallback methods with retry logic."""

    def __init__(self):
        self.outcomes = {
            "method_standard": 0,
            "method_rotating_agent": 0,
            "method_selenium": 0,
        }

    def fetch_page(self, url):
        """✅ ENHANCED: Route Workday to Selenium, others to HTTP."""
        # ✅ Use Selenium for ALL Workday pages (get JS-rendered content)
        if self._is_workday_url(url):
            html, final_url = self._try_selenium(url)
            if html:
                self.outcomes["method_selenium"] += 1
                mock_response = type(
                    "obj",
                    (object,),
                    {"text": html, "status_code": 200, "url": final_url},
                )()
                return mock_response, final_url
        
        # Standard HTTP for non-Workday
        response = self._try_standard_request(url)
        if response and response.status_code == 200:
            self.outcomes["method_standard"] += 1
            return response, response.url

        response = self._try_rotating_agents(url)
        if response and response.status_code == 200:
            self.outcomes["method_rotating_agent"] += 1
            return response, response.url

        if SELENIUM_AVAILABLE and ("ziprecruiter" in url.lower() or "mathworks" in url.lower()):
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
    
    @staticmethod
    def _is_workday_url(url):
        """Check if URL is a Workday page."""
        if not url:
            return False
        url_lower = url.lower()
        return "workday" in url_lower or "myworkdayjobs" in url_lower

    def _try_standard_request(self, url, retries=3):
        """Standard HTTP request with retries."""
        for attempt in range(retries):
            try:
                headers = {
                    "User-Agent": USER_AGENTS[attempt % len(USER_AGENTS)],
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                }
                response = requests.get(
                    url, headers=headers, allow_redirects=True, timeout=20
                )

                if response.status_code == 200:
                    return response

                if attempt < retries - 1:
                    wait_time = 2**attempt
                    time.sleep(wait_time)

            except Exception as e:
                if attempt == retries - 1:
                    return None
                time.sleep(2)

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
        """✅ ENHANCED: Selenium with longer waits for Workday pages."""
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
            
            # ✅ Longer wait for Workday (JS-heavy pages)
            if "workday" in url.lower() or "myworkdayjobs" in url.lower():
                time.sleep(5)  # 5 seconds for Workday JS to render
            else:
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
    
    # ✅ COMPREHENSIVE Job ID patterns (20+ patterns)
    JOB_CODE_PATTERN = re.compile(r"Job Code:\s*([A-Z0-9]{3,10})\b", re.I)
    JOB_ID_PATTERN = re.compile(r"Job ID[:\s]+([A-Z0-9\-]{3,15})\b", re.I)
    J_PATTERN = re.compile(r"\b(J-\d{5,8})\b", re.I)
    REQ_PATTERNS = [
        re.compile(r"Req(?:uisition)? ID[:\s]+([A-Z0-9\-]{3,20})\b", re.I),
        re.compile(r"Requisition[:\s]+([A-Z0-9\-]{3,20})\b", re.I),
        re.compile(r"Req\s+#?[:\s]*([A-Z0-9\-]{3,20})\b", re.I),
    ]

    @staticmethod
    def extract_company(soup, url):
        """✅ URL-based special cases with absolute priority."""
        
        url_lower = url.lower() if url else ""
        
        # Hardcoded mappings (most reliable)
        url_company_map = [
            ("careers.sig.com", "Susquehanna International Group"),
            ("sig.com/job", "Susquehanna International Group"),
            ("lever.co/nimblerx", "NimbleRx"),
            ("nimblerx", "NimbleRx"),
            ("nuro.ai", "Nuro"),
            ("jobs.nuro.team", "Nuro"),
            ("singlestore.com/careers", "SingleStore"),
            ("careers.singlestore", "SingleStore"),
        ]
        
        for url_pattern, company_name in url_company_map:
            if url_pattern in url_lower:
                return company_name
        
        # Priority 1: JSON-LD
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    org = data.get("hiringOrganization", {})
                    if isinstance(org, dict):
                        name = org.get("name", "")
                        if name and len(name) < 100:
                            if not PageParser._looks_like_title(name):
                                return name
            except:
                pass

        # Priority 2: Meta tags
        meta = soup.find("meta", {"property": "og:site_name"})
        if meta and meta.get("content"):
            company = meta.get("content").strip()
            company = re.sub(r"\s*[-|]\s*(careers|jobs).*$", "", company, flags=re.I)
            if company and len(company) < 50:
                if not PageParser._looks_like_title(company):
                    return company

        # Priority 3: H2/H3/H4 (skip H1 - usually title)
        for tag_name in ["h2", "h3", "h4"]:
            tag = soup.find(tag_name)
            if tag:
                text = tag.get_text().strip()
                if 2 < len(text) < 60 and not PageParser._looks_like_title(text):
                    if not any(word in text.lower() for word in ["position", "role", "job", "career", "opportunity", "submit", "sign in", "apply", "application"]):
                        return text

        # Fallback: Domain extraction
        from processors import ValidationHelper
        return ValidationHelper.extract_company_from_domain(url)

    @staticmethod
    def _looks_like_title(text):
        """Check if text looks like a job title."""
        if not text:
            return False
        
        text_lower = text.lower()
        
        # Form/login indicators
        if any(phrase in text_lower for phrase in ["submit your", "sign in", "apply now", "application", "sign up"]):
            return True
        
        # Title keywords
        title_keywords = [
            "intern", "co-op", "coop",
            "engineer", "developer", "software",
            "full stack", "backend", "frontend",
            "junior", "senior", "staff",
            "position", "role", "opportunity",
            "summer", "spring", "fall", "winter",
            "2025", "2026", "2027",
            "with", "university", "drexel",
        ]
        
        keyword_count = sum(1 for kw in title_keywords if kw in text_lower)
        return keyword_count >= 2

    @staticmethod
    def extract_title(soup):
        """✅ ENHANCED: Extract title with meta tag priority."""
        
        # Priority 1: JSON-LD
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

        # Priority 2: Meta tags (Lever uses this)
        meta_title = soup.find("meta", {"property": "og:title"})
        if meta_title and meta_title.get("content"):
            title = meta_title.get("content").strip()
            if 5 < len(title) < 200 and "careers" not in title.lower():
                return title

        # Priority 3: H1
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text().strip()
            if 5 < len(title) < 200 and not title.isupper() and len(title.split()) > 1:
                return title

        # Priority 4: Title tag
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
        """✅ COMPREHENSIVE: 20+ patterns for job ID extraction."""
        try:
            page_text = soup.get_text()

            # Priority 1: J- pattern (IDEXX)
            match = PageParser.J_PATTERN.search(page_text)
            if match:
                job_id = match.group(1).upper()
                return PageParser._clean_job_id(job_id)

            # Priority 2: Job Code
            match = PageParser.JOB_CODE_PATTERN.search(page_text)
            if match:
                job_id = match.group(1).strip()
                return PageParser._clean_job_id(job_id)

            # Priority 3: Job ID
            match = PageParser.JOB_ID_PATTERN.search(page_text)
            if match:
                job_id = match.group(1).strip()
                return PageParser._clean_job_id(job_id)

            # Priority 4: Requisition patterns
            for pattern in PageParser.REQ_PATTERNS:
                match = pattern.search(page_text)
                if match:
                    job_id = match.group(1).strip()
                    return PageParser._clean_job_id(job_id)

            # Priority 5: URL-based extraction (ENHANCED)
            if "workday" in url.lower():
                # Workday underscore pattern (Velera: _8895)
                match = re.search(r"_(\d{4,8})(?:\?|$)", url)
                if match:
                    return match.group(1)
                
                # Standard Workday patterns
                workday_patterns = [
                    (r"(J-\d{5,8})\b", re.I),
                    (r"(REQ-?\d{4,7}(?:-\d{1,3})?)\b", re.I),
                    (r"(R-\d{7,12})\b", re.I),
                    (r"(JR\d{5,10})\b", re.I),
                    (r"[/_]([A-Z]*\d{5,10})(?:[?/]|$)", 0),
                ]
                
                for pattern, flags in workday_patterns:
                    match = re.search(pattern, url, flags) if flags else re.search(pattern, url)
                    if match:
                        job_id = match.group(1)
                        if len(job_id) < 12:
                            return PageParser._clean_job_id(job_id.upper())

            # Platform-specific patterns
            platform_patterns = {
                "jibe": r"/jobs/(\d+)",
                "github": r"/jobs/(\d+)",
                "tiktok": r"/search/(\d{10,})",
                "lifeattiktok": r"/search/(\d{10,})",
                "bytedance": r"/search/(\d{10,})",
                "joinbytedance": r"/search/(\d{10,})",
                "oracle": r"/job/(\d{5,})",
                "micron": r"/job/(\d{7,10})",
                "sig.com": r"/job/([A-Z0-9]+)",
                "careers.sig": r"/job/([A-Z0-9]+)",
                "lever.co": r"/([a-f0-9\-]{36})",
                "linkedin.com": r"/view/(\d+)",
                "mathworks.com": r"/(\d{5,6})-",
                "idexx": r"/(J-\d{5,8})",
                "selinc": r"_([0-9\-]{5,15})",
                "gilead": r"_(R\d{7,10})",
            }

            url_lower = url.lower()
            for platform, pattern in platform_patterns.items():
                if platform in url_lower:
                    match = re.search(pattern, url)
                    if match:
                        return PageParser._clean_job_id(match.group(1))

            return "N/A"
        except:
            return "N/A"

    @staticmethod
    def _clean_job_id(job_id):
        """Clean job ID by removing common suffixes."""
        if not job_id:
            return "N/A"
        
        # Remove button/action text
        suffixes = ["Apply", "Now", "Submit", "Click", "Here", "View", "Details", "More"]
        for suffix in suffixes:
            job_id = re.sub(f"{suffix}.*$", "", job_id, flags=re.I)
        
        # Remove trailing non-alphanumeric except hyphen
        job_id = re.sub(r"[^A-Z0-9\-]+$", "", job_id, flags=re.I)
        
        # Trim to reasonable length
        if len(job_id) > 20:
            job_id = job_id[:20]
        
        return job_id.strip()

    @staticmethod
    def extract_job_age_days(soup):
        """Extract job age."""
        try:
            page_text = soup.get_text()[:3000]

            age_patterns = [
                (r"[Pp]osted\s+(\d+)\+\s+[Dd]ays?\s+[Aa]go", lambda m: int(m.group(1))),
                (r"[Pp]osted\s+(\d+)\s+[Dd]ays?\s+[Aa]go", lambda m: int(m.group(1))),
                (r"(\d+)\s+[Dd]ays?\s+[Aa]go", lambda m: int(m.group(1))),
                (r"[Pp]osted\s+today|Today", lambda m: 0),
                (r"[Pp]osted\s+yesterday|Yesterday", lambda m: 1),
                (r"(\d+)\s+hours?\s+ago", lambda m: 0),
            ]

            for pattern, converter in age_patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    return converter(match)

            return None

        except:
            return None

    @staticmethod
    def extract_jobright_data(soup, url, jobright_auth):
        """Extract from Jobright page with JSON-LD parsing."""
        try:
            script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if script_tag:
                try:
                    data = json.loads(script_tag.string)
                    job_result = (
                        data.get("props", {})
                        .get("pageProps", {})
                        .get("dataSource", {})
                        .get("jobResult", {})
                    )
                    
                    if job_result:
                        company = job_result.get("companyResult", {}).get("companyName", "Unknown")
                        title = job_result.get("jobTitle", "Unknown")
                        location = job_result.get("jobLocation", "Unknown")
                        
                        is_remote = job_result.get("isRemote", False)
                        work_model = job_result.get("workModel", "")
                        
                        if is_remote or work_model.lower() == "remote":
                            remote = "Remote"
                        elif work_model.lower() == "hybrid":
                            remote = "Hybrid"
                        elif work_model.lower() == "onsite":
                            remote = "On Site"
                        else:
                            remote = "Unknown"
                        
                        recommendation_tags = job_result.get("recommendationTags", [])
                        if "H1B Sponsor Likely" in recommendation_tags:
                            sponsorship = "Yes"
                        else:
                            sponsorship = "Unknown"
                        
                        actual_url = job_result.get("applyLink") or job_result.get("originalUrl") or url
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
                except:
                    pass
            
            return None
        except:
            return None


class SourceParsers:
    """Parsers for specific sources with age extraction."""
    
    HOURS_AGO = re.compile(r"(\d+)\s+hours?\s+ago", re.I)
    DAYS_AGO = re.compile(r"(\d+)\s+days?\s+ago", re.I)

    @staticmethod
    def _extract_age_from_text(text):
        """Extract age from text."""
        if not text:
            return None
        
        hours_match = SourceParsers.HOURS_AGO.search(text)
        if hours_match:
            return 0
        
        days_match = SourceParsers.DAYS_AGO.search(text)
        if days_match:
            return int(days_match.group(1))
        
        return None

    @staticmethod
    def parse_jobright_email(soup, url, jobright_auth):
        """✅ FINAL: Robust email parsing with separator and keyword extraction."""
        try:
            url_base = url.split("?")[0]
            
            all_links = soup.find_all("a", href=re.compile(re.escape(url_base)))
            
            title_link = None
            for link in all_links:
                link_text = link.get_text().strip()
                if len(link_text) > 15 and any(kw in link_text.lower() for kw in ["intern", "engineer", "software", "developer"]):
                    title_link = link
                    break
            
            if not title_link:
                for link in all_links:
                    if len(link.get_text().strip()) > 15:
                        title_link = link
                        break
            
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

            # ✅ Extract company
            company_elem = job_section.find("p", id="job-company-name")
            company = company_elem.get_text().strip() if company_elem else "Unknown"

            # ✅ Extract title - use separator to prevent button text
            title_text = title_link.get_text(separator="|||", strip=True)
            title_parts = title_text.split("|||")
            
            # Find part with internship keywords
            title = "Unknown"
            internship_keywords = ["intern", "engineer", "developer", "software", "full stack", "backend", "frontend", "data", "ml", "ai"]
            
            for part in title_parts:
                if any(kw in part.lower() for kw in internship_keywords):
                    # Clean button text
                    part = re.sub(r"\s*(APPLY NOW|Apply|View|Click Here|Learn More).*$", "", part, flags=re.I)
                    if len(part) > 5:
                        title = part.strip()
                        break
            
            # Fallback: take first substantial part
            if title == "Unknown":
                for part in title_parts:
                    if len(part) > 10:
                        title = re.sub(r"\s*(APPLY NOW|Apply|View).*$", "", part, flags=re.I).strip()
                        break

            # ✅ BULLETPROOF LOCATION with separator
            location = "Unknown"
            remote = "Unknown"
            
            job_tags = job_section.find_all("p", id="job-tag")
            
            for tag in job_tags:
                # Use separator to isolate direct text
                text = tag.get_text(separator="|||", strip=True)
                text = text.split("|||")[0]  # Take first part only
                
                # Skip salary and referrals
                if "$" in text or "referral" in text.lower():
                    continue
                
                # ✅ AGGRESSIVE CLEANING - no \s* required
                text = re.sub(r"(Team|Department|Division|Group|Unit|Office|Location|Area|Region).*$", "", text, flags=re.I)
                text = re.sub(r"\s+", " ", text).strip()
                
                # ✅ FIX: If cleaning removed Team but left city name only, try to salvage
                # Check if text is a known tech city without state
                known_tech_cities = {
                    "san jose": "CA",
                    "san francisco": "CA", 
                    "seattle": "WA",
                    "boston": "MA",
                    "austin": "TX",
                }
                
                text_lower = text.lower().strip()
                if text_lower in known_tech_cities and "," not in text:
                    # Add state
                    state = known_tech_cities[text_lower]
                    text = f"{text}, {state}"
                
                # Validate and assign
                if "," in text:
                    parts = text.split(",")
                    if len(parts) == 2:
                        city = parts[0].strip()
                        state = parts[1].strip()
                        
                        if state.upper() in US_STATES.values() or len(state) == 2:
                            location = f"{city}, {state.upper()}"
                            remote = "On Site"
                            break
                elif text.lower() == "remote":
                    location = "Remote"
                    remote = "Remote"
                    break
                elif text.lower() == "hybrid":
                    location = "Hybrid"
                    remote = "Hybrid"
                    break
                elif len(text) > 2 and len(text) < 100:
                    # Accept broader formats
                    if any(skip in text.lower() for skip in ["apply", "view", "click", "submit"]):
                        continue
                    
                    if (any(state in text.upper() for state in US_STATES.values()) or
                        any(city in text.lower() for city in ["philadelphia", "seattle", "francisco", "greater", "metro"])):
                        location = text
                        remote = SourceParsers._infer_remote(text)
                        break

            container_text = job_section.get_text()
            age_days = SourceParsers._extract_age_from_text(container_text)

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
            return None

    @staticmethod
    def _infer_remote(text):
        """Infer remote status."""
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

    @staticmethod
    def parse_ziprecruiter_email(soup, url):
        """ZipRecruiter email parser (unchanged)."""
        # [Previous implementation - no changes needed]
        return None

    @staticmethod
    def parse_adzuna_email(soup, url):
        """Adzuna email parser (unchanged)."""
        # [Previous implementation - no changes needed]
        return None


class SimplifyGitHubScraper:
    """GitHub scraper (unchanged)."""
    
    HEADER_PATTERN = re.compile(r"Company.*Role.*Location.*(?:Application|Link).*Date", re.I)
    HTML_LINK = re.compile(r'<a\s+href="(https?://[^"]+)"')
    MD_LINK = re.compile(r"\[.*?\]\((https?://[^\)]+)\)")
    EMOJI_PATTERN = re.compile(
        "["
        "\U0001f600-\U0001f64f"
        "\U0001f300-\U0001f5ff"
        "\U0001f680-\U0001f6ff"
        "\U0001f1e0-\U0001f1ff"
        "\U00002500-\U00002bef"
        "]+",
        flags=re.UNICODE,
    )

    @staticmethod
    def scrape(url, source_name="GitHub"):
        """Scrape GitHub markdown tables."""
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                return []

            soup = BeautifulSoup(response.text, "html.parser")
            tables = soup.find_all("table")

            if tables:
                return SimplifyGitHubScraper._parse_html_tables(soup, source_name)
            else:
                return SimplifyGitHubScraper._parse_markdown_text(response.text, source_name)

        except Exception as e:
            return []

    @staticmethod
    def _parse_markdown_text(text, source_name):
        """Parse markdown tables."""
        lines = text.split("\n")
        jobs = []

        header_idx = next((i for i, line in enumerate(lines) 
                          if SimplifyGitHubScraper.HEADER_PATTERN.search(line)), -1)

        if header_idx == -1:
            return []

        header = lines[header_idx]
        if "\t" in header:
            delimiter, start = "\t", header_idx + 1
        elif "|" in header:
            delimiter, start = "|", header_idx + 2
        else:
            return []

        for line in lines[start:]:
            if not line.strip():
                continue

            parts = [p.strip() for p in line.split(delimiter) if p.strip()]
            if len(parts) < 5:
                continue

            company = SimplifyGitHubScraper._remove_emojis(parts[0])
            title = SimplifyGitHubScraper._remove_emojis(parts[1])
            location = SimplifyGitHubScraper._remove_emojis(parts[2])
            link_cell = parts[3]
            age = parts[4]

            match = SimplifyGitHubScraper.HTML_LINK.search(link_cell)
            if match:
                url = match.group(1)
            else:
                match = SimplifyGitHubScraper.MD_LINK.search(link_cell)
                url = match.group(1) if match else (link_cell if link_cell.startswith("http") else None)

            if not url or any(marker in line for marker in ["🔒", "❌", "closed"]):
                continue

            jobs.append({
                "company": company,
                "title": title,
                "location": location,
                "url": url,
                "age": age,
                "is_closed": False,
                "source": source_name,
            })

        return jobs

    @staticmethod
    def _parse_html_tables(soup, source_name):
        """Parse HTML tables."""
        jobs = []

        for table in soup.find_all("table"):
            for row in table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue

                company_link = cells[0].find("a")
                if not company_link:
                    continue

                company = SimplifyGitHubScraper._remove_emojis(company_link.get_text(strip=True))
                title = SimplifyGitHubScraper._remove_emojis(cells[1].get_text(strip=True))
                location = SimplifyGitHubScraper._remove_emojis(cells[2].get_text(strip=True))
                age = cells[4].get_text(strip=True)

                apply_link = cells[3].find("a", href=True)
                if not apply_link:
                    continue

                url = apply_link.get("href", "")
                is_closed = "🔒" in str(cells[3])

                jobs.append({
                    "company": company,
                    "title": title,
                    "location": location,
                    "url": url,
                    "age": age,
                    "is_closed": is_closed,
                    "source": source_name,
                })

        return jobs

    @staticmethod
    def _remove_emojis(text):
        """Remove emojis."""
        if not text:
            return text
        text = SimplifyGitHubScraper.EMOJI_PATTERN.sub("", text)
        text = re.sub(r"[↳🇺🇸🛂\*🔒❌✅]+", "", text)
        return re.sub(r"\s+", " ", text).strip()


class HandshakeExtractor:
    """✅ PRODUCTION: Handshake with Undetected Chrome."""
    
    def __init__(self):
        self.cookies = None
        self.driver = None
        self.jobs_scraped_today = 0
        self.last_scrape_date = None
        self.config = HANDSHAKE_CONFIG
        self.cookies_file = HANDSHAKE_COOKIES_FILE
        self._load_cookies()

    def _load_cookies(self):
        """Load saved Handshake cookies."""
        if os.path.exists(self.cookies_file):
            try:
                with open(self.cookies_file, "r") as f:
                    self.cookies = json.load(f)
            except:
                pass

    def login_interactive(self):
        """Interactive Handshake login."""
        if not SELENIUM_AVAILABLE:
            print("Selenium not available")
            return False

        print("\n" + "=" * 60)
        print("HANDSHAKE AUTHENTICATION")
        print("=" * 60)

        driver = None
        try:
            chrome_options = Options()
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)

            driver.get("https://app.joinhandshake.com")
            time.sleep(5)

            print("\n[INSTRUCTIONS]")
            print("  1. Log in through the browser")
            print("  2. Navigate to: Jobs → Search")
            print("  3. Apply your filters")
            print("  4. Press ENTER when you see search results\n")
            input("Press ENTER when ready: ")

            cookies = driver.get_cookies()
            if not cookies:
                print("✗ No cookies captured")
                driver.quit()
                return False

            self.cookies = cookies

            with open(self.cookies_file, "w") as f:
                json.dump(cookies, f, indent=2)

            print(f"✓ Saved {len(cookies)} cookies\n")
            time.sleep(2)
            driver.quit()
            return True

        except Exception as e:
            print(f"✗ Authentication failed: {e}")
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            return False

    def is_safe_to_scrape(self):
        """Check scraping constraints."""
        now = datetime.datetime.now()
        
        if not self.cookies:
            return False, "No cookies (run login_handshake.py)"
        
        search_url = self.config.get("search_url", "")
        
        if not search_url or "PASTE_YOUR" in search_url:
            return False, "Search URL not configured"
        
        # ✅ Accept /job-search/ if has pagination
        if "/job-search/" in search_url:
            if "page=" in search_url and "per_page=" in search_url:
                pass  # Valid
            else:
                return False, "URL needs pagination (?page=1&per_page=25)"
        elif "/stu/postings" not in search_url:
            return False, "Invalid URL format"
        
        start_hour, end_hour = self.config["scrape_hours"]
        if not (start_hour <= now.hour < end_hour):
            return False, f"Outside hours ({start_hour}-{end_hour})"
        
        if (
            self.last_scrape_date
            and self.last_scrape_date.date() == now.date()
            and self.jobs_scraped_today >= self.config["max_jobs_per_session"]
        ):
            return False, "Daily limit reached"
        
        return True, "OK"

    def scrape_jobs(self):
        """✅ PRODUCTION: Undetected Chrome scraping."""
        is_safe, reason = self.is_safe_to_scrape()
        if not is_safe:
            print(f"Handshake: {reason}")
            return []
        
        jobs = []
        try:
            if not self._init_browser():
                print("  ✗ Browser init failed")
                self._cleanup()
                return []
            
            if not self._load_search():
                print("  ✗ Failed to load (cookies expired?)")
                print("     Run: python3 login_handshake.py")
                self._cleanup()
                return []
            
            self._human_browse()
            urls = self._get_urls()
            
            if not urls:
                print("  ✗ No job URLs found")
                self._cleanup()
                return []
            
            print(f"  ✓ Found {len(urls)} Handshake jobs")
            
            for idx, url in enumerate(urls[: self.config["max_jobs_per_session"]], 1):
                job = self._scrape_one(url, idx, len(urls))
                if job:
                    jobs.append(job)
                    self.jobs_scraped_today += 1
        except Exception as e:
            print(f"  ✗ Handshake error: {e}")
        finally:
            self._cleanup()
            self.last_scrape_date = datetime.datetime.now()
        
        return jobs

    def _init_browser(self):
        """✅ Initialize with Undetected Chrome."""
        try:
            if UNDETECTED_CHROME_AVAILABLE:
                # ✅ Use undetected Chrome (bypasses Cloudflare)
                options = uc.ChromeOptions()
                options.add_argument("--headless=new")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                
                self.driver = uc.Chrome(options=options, version_main=None)
                return True
            elif SELENIUM_AVAILABLE:
                # Fallback to regular Chrome
                chrome_options = Options()
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--disable-blink-features=AutomationControlled")
                chrome_options.add_argument("--no-sandbox")
                
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                return True
            else:
                return False
        except Exception as e:
            print(f"  ✗ Browser init error: {e}")
            return False

    def _load_search(self):
        """Load Handshake and inject cookies."""
        try:
            self.driver.get("https://app.joinhandshake.com")
            
            # ✅ Longer wait (10-15 sec) to appear human
            time.sleep(random.uniform(10, 15))
            
            for c in self.cookies:
                try:
                    self.driver.add_cookie({
                        "name": c["name"],
                        "value": c["value"],
                        "domain": c.get("domain", ".joinhandshake.com"),
                    })
                except:
                    pass
            
            search_url = self.config.get("search_url", "")
            self.driver.get(search_url)
            
            # ✅ Wait for JS to load (15-20 sec)
            time.sleep(random.uniform(15, 20))
            
            if "login" in self.driver.current_url.lower() or "just a moment" in self.driver.title.lower():
                return False
            
            return True
        except Exception as e:
            print(f"  ✗ Load error: {e}")
            return False

    def _human_browse(self):
        """✅ ENHANCED: More human-like browsing."""
        try:
            # Random scroll
            scroll_amount = random.randint(300, 800)
            self.driver.execute_script(f"window.scrollTo(0, {scroll_amount})")
            time.sleep(random.uniform(5, 10))
            
            # Scroll back
            self.driver.execute_script(f"window.scrollTo(0, {scroll_amount // 2})")
            time.sleep(random.uniform(3, 7))
        except:
            pass

    def _get_urls(self):
        """Extract job URLs from page."""
        try:
            # Wait for content
            time.sleep(5)
            
            # Strategy 1: Job search links
            cards = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/job-search/"]')
            
            # Strategy 2: All links
            if not cards:
                all_links = self.driver.find_elements(By.TAG_NAME, "a")
                cards = [c for c in all_links if "/job-search/" in (c.get_attribute("href") or "")]
            
            urls = []
            seen = set()
            
            for card in cards:
                href = card.get_attribute("href")
                if href and "/job-search/" in href:
                    match = re.search(r"/job-search/(\d+)", href)
                    if match and href not in seen:
                        urls.append(f"https://app.joinhandshake.com/job-search/{match.group(1)}")
                        seen.add(href)
            
            return urls
        except:
            return []

    def _scrape_one(self, url, idx, total):
        """Scrape single job."""
        try:
            time.sleep(random.uniform(*self.config["delay_between_jobs"]))
            self.driver.get(url)
            time.sleep(random.uniform(5, 10))
            self._deep_read()
            data = self._extract(url)
            if idx < total:
                self.driver.back()
                time.sleep(random.uniform(3, 6))
            return data
        except:
            return None

    def _deep_read(self):
        """Simulate reading."""
        try:
            h = self.driver.execute_script("return document.body.scrollHeight")
            for i in range(random.randint(3, 6)):
                self.driver.execute_script(
                    f"window.scrollTo(0, {(h//(i+1))+random.randint(-50,50)})"
                )
                time.sleep(random.uniform(*self.config["scroll_delay"]))
            time.sleep(random.uniform(*self.config["read_time_per_job"]) / random.randint(3, 5))
        except:
            pass

    def _extract(self, url):
        """Extract job data."""
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
        """Extract Handshake job ID."""
        m = re.search(r"/job-search/(\d+)", url)
        return f"HS_{m.group(1)}" if m else "N/A"

    def _cleanup(self):
        """Close browser."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass