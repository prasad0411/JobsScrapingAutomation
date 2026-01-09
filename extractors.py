#!/usr/bin/env python3
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

import datetime

from config import (
    USER_AGENTS,
    GMAIL_CREDS_FILE,
    GMAIL_TOKEN_FILE,
    GMAIL_SCOPES,
    JOB_BOARD_DOMAINS,
    JOBRIGHT_COOKIES_FILE,
    US_STATES,
)


class JobrightAuthenticator:
    def __init__(self):
        self.cookies = None
        self.load_cookies()

    def load_cookies(self):
        if os.path.exists(JOBRIGHT_COOKIES_FILE):
            try:
                with open(JOBRIGHT_COOKIES_FILE, "r") as f:
                    self.cookies = json.load(f)
            except Exception as e:
                print(f"Cookie load error: {e}")

    def login_interactive(self):
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
    def __init__(self):
        self.outcomes = {
            "method_standard": 0,
            "method_rotating_agent": 0,
            "method_selenium": 0,
        }

    def fetch_page(self, url):
        if self._is_js_heavy_platform(url):
            html, final_url = self._try_selenium(url)
            if html:
                self.outcomes["method_selenium"] += 1
                mock_response = type(
                    "obj",
                    (object,),
                    {"text": html, "status_code": 200, "url": final_url},
                )()
                return mock_response, final_url
        response = self._try_standard_request(url)
        if response and response.status_code == 200:
            self.outcomes["method_standard"] += 1
            return response, response.url
        response = self._try_rotating_agents(url)
        if response and response.status_code == 200:
            self.outcomes["method_rotating_agent"] += 1
            return response, response.url
        if SELENIUM_AVAILABLE and (
            "ziprecruiter" in url.lower() or "mathworks" in url.lower()
        ):
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
    def _is_js_heavy_platform(url):
        if not url:
            return False
        url_lower = url.lower()
        js_platforms = [
            "workday",
            "myworkdayjobs",
            "greenhouse.io",
            "boards.greenhouse",
        ]
        return any(platform in url_lower for platform in js_platforms)

    def _try_standard_request(self, url, retries=3):
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
            if "workday" in url_lower or "myworkdayjobs" in url_lower:
                time.sleep(5)
            elif "greenhouse" in url_lower:
                time.sleep(8)
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
    @staticmethod
    def extract_company(soup, url):
        url_lower = url.lower() if url else ""
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
        json_ld = soup.find("script", type="application/ld+json")
        if json_ld:
            try:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    org = data.get("hiringOrganization", {})
                    if isinstance(org, dict):
                        name = org.get("name", "")
                        if name and len(name) < 100:
                            if not PageParser._is_generic_ui_text(name):
                                if not PageParser._looks_like_title(name):
                                    return name
            except:
                pass
        meta = soup.find("meta", {"property": "og:site_name"})
        if meta and meta.get("content"):
            company = meta.get("content").strip()
            company = re.sub(r"\s*[-|]\s*(careers|jobs).*$", "", company, flags=re.I)
            if company and len(company) < 50:
                if not PageParser._is_generic_ui_text(company):
                    if not PageParser._looks_like_title(company):
                        return company
        for tag_name in ["h2", "h3", "h4"]:
            tag = soup.find(tag_name)
            if tag:
                text = tag.get_text().strip()
                if 2 < len(text) < 60:
                    if not PageParser._is_generic_ui_text(text):
                        if not PageParser._looks_like_title(text):
                            if not any(
                                word in text.lower()
                                for word in [
                                    "position",
                                    "role",
                                    "job",
                                    "career",
                                    "opportunity",
                                    "submit",
                                    "sign in",
                                    "apply",
                                    "application",
                                ]
                            ):
                                return text
        from processors import ValidationHelper

        return ValidationHelper.extract_company_from_domain(url)

    @staticmethod
    def _is_generic_ui_text(text):
        if not text:
            return False
        text_lower = text.lower()
        generic_terms = [
            "cookie consent",
            "privacy policy",
            "terms of service",
            "sign in",
            "log in",
            "apply now",
            "get started",
            "accept cookies",
            "manage cookies",
            "privacy settings",
            "cookie manager",
            "cookie settings",
            "manage preferences",
        ]
        return any(term in text_lower for term in generic_terms)

    @staticmethod
    def _looks_like_title(text):
        if not text:
            return False
        text_lower = text.lower()
        if any(
            phrase in text_lower
            for phrase in [
                "submit your",
                "sign in",
                "apply now",
                "application",
                "sign up",
            ]
        ):
            return True
        title_keywords = [
            "intern",
            "co-op",
            "coop",
            "engineer",
            "developer",
            "software",
            "full stack",
            "backend",
            "frontend",
            "junior",
            "senior",
            "staff",
            "position",
            "role",
            "opportunity",
            "summer",
            "spring",
            "fall",
            "winter",
            "2025",
            "2026",
            "2027",
            "with",
            "university",
            "drexel",
        ]
        keyword_count = sum(1 for kw in title_keywords if kw in text_lower)
        return keyword_count >= 2

    @staticmethod
    def extract_title(soup):
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
            if 5 < len(title) < 200 and not title.isupper() and len(title.split()) > 1:
                return title
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
        try:
            page_text = soup.get_text()

            if "bytedance" in url.lower() or "joinbytedance" in url.lower():
                match = re.search(r"Job Code:\s*([A-Z0-9]{5,15})\b", page_text, re.I)
                if match:
                    return PageParser._clean_job_id(match.group(1))

            labeled_patterns = [
                r"Job Code:\s*([A-Z0-9]{4,15})\b",
                r"Job ID[:\s]+([A-Z0-9\-]{4,15})\b",
                r"Req(?:uisition)? ID[:\s]+([A-Z0-9\-]{4,20})\b",
                r"Requisition[:\s]+([A-Z0-9\-]{4,20})\b",
                r"ID:\s*(\d{7,10})\b",
            ]
            for pattern in labeled_patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    job_id = match.group(1).strip()
                    return PageParser._clean_job_id(job_id)

            id_patterns = [
                r"\b(J-\d{5,8})\b",
                r"\b(JR\d{4,7})\b",
                r"\b(R-\d{4,5}-\d{4,6})\b",
                r"\b(R-\d{6,8}-\d{1,2})\b",
                r"\b(R\d{5,7})\b",
                r"\b(REQ-?\d{4,7})\b",
            ]
            for pattern in id_patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    job_id = match.group(1)
                    return PageParser._clean_job_id(job_id)

            if "workday" in url.lower():
                match = re.search(r"_(\d{4,8})(?:\?|$)", url)
                if match:
                    return match.group(1)
                workday_patterns = [
                    r"\b(R-\d{4,5}-\d{4,6})\b",
                    r"\b(J-\d{5,8})\b",
                    r"\b(REQ-?\d{4,7}(?:-\d{1,3})?)\b",
                    r"\b(JR\d{5,10})\b",
                ]
                for pattern in workday_patterns:
                    match = re.search(pattern, url, re.I)
                    if match:
                        return PageParser._clean_job_id(match.group(1))

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
                "eightfold.ai": r"/job/(\d{8,10})",
                "eightfold": r"/job/(\d{8,10})",
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
        if not job_id:
            return "N/A"

        all_caps_suffixes = [
            "START",
            "JOIN",
            "MAKE",
            "AS",
            "APPLY",
            "NOW",
            "CLICK",
            "SOFTWARE",
            "ENGINEER",
            "INTERN",
            "HERE",
            "VIEW",
            "DETAILS",
            "MORE",
            "ABOUT",
            "OUR",
            "EXTERNAL",
            "PAY",
            "CURRENT",
            "COMPANY",
            "INC",
            "CORP",
            "LLC",
            "LTD",
            "POSITION",
            "ARE",
            "JOB",
            "STACK",
        ]
        for suffix in all_caps_suffixes:
            job_id = re.sub(rf"{suffix}\b.*$", "", job_id, flags=re.I)

        job_id = re.sub(r"[a-z].*$", "", job_id)

        if job_id.lower().startswith("id"):
            job_id = job_id[2:]

        if not re.match(r"^[A-Z0-9\-]+$", job_id, re.I):
            return "N/A"

        if len(job_id) < 4:
            return "N/A"
        if len(job_id) > 20:
            job_id = job_id[:20]
        return job_id.upper().strip()

    @staticmethod
    def extract_job_age_days(soup):
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
                        company = job_result.get("companyResult", {}).get(
                            "companyName", "Unknown"
                        )
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
                        actual_url = (
                            job_result.get("applyLink")
                            or job_result.get("originalUrl")
                            or url
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
                except:
                    pass
            return None
        except:
            return None


class SourceParsers:
    HOURS_AGO = re.compile(r"(\d+)\s+hours?\s+ago", re.I)
    DAYS_AGO = re.compile(r"(\d+)\s+days?\s+ago", re.I)

    @staticmethod
    def _extract_age_from_text(text):
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
        try:
            url_base = url.split("?")[0]
            all_links = soup.find_all("a", href=re.compile(re.escape(url_base)))
            title_link = None
            for link in all_links:
                link_text = link.get_text().strip()
                if len(link_text) > 15 and any(
                    kw in link_text.lower()
                    for kw in ["intern", "engineer", "software", "developer"]
                ):
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
            company_elem = job_section.find("p", id="job-company-name")
            company = company_elem.get_text().strip() if company_elem else "Unknown"
            title_text = title_link.get_text(separator="|||", strip=True)
            title_parts = title_text.split("|||")
            title = "Unknown"
            internship_keywords = [
                "intern",
                "engineer",
                "developer",
                "software",
                "full stack",
                "backend",
                "frontend",
                "data",
                "ml",
                "ai",
            ]
            for part in title_parts:
                if any(kw in part.lower() for kw in internship_keywords):
                    part = re.sub(
                        r"\s*(APPLY NOW|Apply|View|Click Here|Learn More).*$",
                        "",
                        part,
                        flags=re.I,
                    )
                    if len(part) > 5:
                        title = part.strip()
                        break
            if title == "Unknown":
                for part in title_parts:
                    if len(part) > 10:
                        title = re.sub(
                            r"\s*(APPLY NOW|Apply|View).*$", "", part, flags=re.I
                        ).strip()
                        break
            location = "Unknown"
            remote = "Unknown"
            job_tags = job_section.find_all("p", id="job-tag")
            for tag in job_tags:
                text = tag.get_text(separator="|||", strip=True)
                text = text.split("|||")[0]
                if "$" in text or "referral" in text.lower():
                    continue
                text = re.sub(
                    r"(Team|Department|Division|Group|Unit|Office|Location|Area|Region).*$",
                    "",
                    text,
                    flags=re.I,
                )
                text = re.sub(r"\s+", " ", text).strip()
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
                    if any(
                        skip in text.lower()
                        for skip in ["apply", "view", "click", "submit"]
                    ):
                        continue
                    if any(
                        state in text.upper() for state in US_STATES.values()
                    ) or any(
                        city in text.lower()
                        for city in [
                            "philadelphia",
                            "seattle",
                            "francisco",
                            "greater",
                            "metro",
                        ]
                    ):
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
        return None

    @staticmethod
    def parse_adzuna_email(soup, url):
        return None


class SimplifyGitHubScraper:
    HEADER_PATTERN = re.compile(
        r"Company.*Role.*Location.*(?:Application|Link).*Date", re.I
    )
    HTML_LINK = re.compile(r'<a\s+href="(https?://[^"]+)"')
    MD_LINK = re.compile(r"\[.*?\]\((https?://[^\)]+)\)")
    EMOJI_PATTERN = re.compile(
        "[\U0001f600-\U0001f64f\U0001f300-\U0001f5ff\U0001f680-\U0001f6ff\U0001f1e0-\U0001f1ff\U00002500-\U00002bef]+",
        flags=re.UNICODE,
    )

    @staticmethod
    def scrape(url, source_name="GitHub"):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                return []
            soup = BeautifulSoup(response.text, "html.parser")
            tables = soup.find_all("table")
            if tables:
                return SimplifyGitHubScraper._parse_html_tables(soup, source_name)
            else:
                return SimplifyGitHubScraper._parse_markdown_text(
                    response.text, source_name
                )
        except Exception as e:
            return []

    @staticmethod
    def _parse_markdown_text(text, source_name):
        lines = text.split("\n")
        jobs = []
        header_idx = next(
            (
                i
                for i, line in enumerate(lines)
                if SimplifyGitHubScraper.HEADER_PATTERN.search(line)
            ),
            -1,
        )
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
        if not text:
            return text
        text = SimplifyGitHubScraper.EMOJI_PATTERN.sub("", text)
        text = re.sub(r"[↳🇺🇸🛂\*🔒❌✅]+", "", text)
        return re.sub(r"\s+", " ", text).strip()
