#!/usr/bin/env python3

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import datetime
import time
import re
import random
import base64
import pickle
import os
import json
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
    print(
        "Selenium not installed. Install with: pip install selenium webdriver-manager"
    )

SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Valid Entries"
DISCARDED_WORKSHEET = "Discarded Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"
SHEETS_CREDS_FILE = "credentials.json"
GMAIL_CREDS_FILE = "gmail_credentials.json"
GMAIL_TOKEN_FILE = "gmail_token.pickle"
SIMPLIFY_URL = "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/master/README.md"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

US_STATES = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}

CANADA_PROVINCES = {
    "ON",
    "QC",
    "BC",
    "AB",
    "MB",
    "SK",
    "NS",
    "NB",
    "NL",
    "PE",
    "YT",
    "NT",
    "NU",
}

CITY_TO_STATE = {
    "new york": "NY",
    "brooklyn": "NY",
    "manhattan": "NY",
    "queens": "NY",
    "bronx": "NY",
    "los angeles": "CA",
    "san francisco": "CA",
    "san diego": "CA",
    "san jose": "CA",
    "palo alto": "CA",
    "mountain view": "CA",
    "sunnyvale": "CA",
    "santa clara": "CA",
    "cupertino": "CA",
    "menlo park": "CA",
    "redwood city": "CA",
    "irvine": "CA",
    "santa monica": "CA",
    "pasadena": "CA",
    "berkeley": "CA",
    "oakland": "CA",
    "sacramento": "CA",
    "fresno": "CA",
    "long beach": "CA",
    "anaheim": "CA",
    "cerritos": "CA",
    "san mateo": "CA",
    "fremont": "CA",
    "san carlos": "CA",
    "hanover": "MD",
    "des plaines": "IL",
    "la grange park": "IL",
    "west valley city": "UT",
    "salt lake city": "UT",
    "provo": "UT",
    "seattle": "WA",
    "bellevue": "WA",
    "redmond": "WA",
    "tacoma": "WA",
    "spokane": "WA",
    "boston": "MA",
    "cambridge": "MA",
    "somerville": "MA",
    "worcester": "MA",
    "chicago": "IL",
    "naperville": "IL",
    "aurora": "IL",
    "rockford": "IL",
    "houston": "TX",
    "dallas": "TX",
    "austin": "TX",
    "san antonio": "TX",
    "fort worth": "TX",
    "el paso": "TX",
    "arlington": "TX",
    "plano": "TX",
    "phoenix": "AZ",
    "tucson": "AZ",
    "mesa": "AZ",
    "chandler": "AZ",
    "scottsdale": "AZ",
    "philadelphia": "PA",
    "pittsburgh": "PA",
    "allentown": "PA",
    "denver": "CO",
    "colorado springs": "CO",
    "boulder": "CO",
    "atlanta": "GA",
    "augusta": "GA",
    "columbus": "GA",
    "savannah": "GA",
    "miami": "FL",
    "orlando": "FL",
    "tampa": "FL",
    "jacksonville": "FL",
    "fort lauderdale": "FL",
    "tallahassee": "FL",
    "st petersburg": "FL",
    "detroit": "MI",
    "grand rapids": "MI",
    "warren": "MI",
    "ann arbor": "MI",
    "minneapolis": "MN",
    "st paul": "MN",
    "rochester": "MN",
    "bloomington": "MN",
    "shakopee": "MN",
    "portland": "OR",
    "salem": "OR",
    "eugene": "OR",
    "hillsboro": "OR",
    "las vegas": "NV",
    "reno": "NV",
    "henderson": "NV",
    "baltimore": "MD",
    "frederick": "MD",
    "rockville": "MD",
    "gaithersburg": "MD",
    "germantown": "MD",
    "annapolis": "MD",
    "silver spring": "MD",
    "milwaukee": "WI",
    "madison": "WI",
    "green bay": "WI",
    "nashville": "TN",
    "memphis": "TN",
    "knoxville": "TN",
    "indianapolis": "IN",
    "fort wayne": "IN",
    "evansville": "IN",
    "columbus": "OH",
    "cleveland": "OH",
    "cincinnati": "OH",
    "toledo": "OH",
    "charlotte": "NC",
    "raleigh": "NC",
    "durham": "NC",
    "greensboro": "NC",
    "chapel hill": "NC",
    "wilmington": "NC",
    "oklahoma city": "OK",
    "tulsa": "OK",
    "norman": "OK",
    "louisville": "KY",
    "lexington": "KY",
    "kansas city": "MO",
    "st louis": "MO",
    "springfield": "MO",
    "omaha": "NE",
    "lincoln": "NE",
    "albuquerque": "NM",
    "santa fe": "NM",
    "boise": "ID",
    "meridian": "ID",
    "des moines": "IA",
    "cedar rapids": "IA",
    "little rock": "AR",
    "fayetteville": "AR",
    "providence": "RI",
    "warwick": "RI",
    "bridgeport": "CT",
    "new haven": "CT",
    "stamford": "CT",
    "hartford": "CT",
    "newark": "NJ",
    "jersey city": "NJ",
    "princeton": "NJ",
    "hoboken": "NJ",
    "richmond": "VA",
    "virginia beach": "VA",
    "norfolk": "VA",
    "chesapeake": "VA",
    "arlington": "VA",
    "alexandria": "VA",
    "mclean": "VA",
    "reston": "VA",
    "charleston": "SC",
    "columbia": "SC",
    "greenville": "SC",
    "birmingham": "AL",
    "montgomery": "AL",
    "huntsville": "AL",
    "new orleans": "LA",
    "baton rouge": "LA",
    "shreveport": "LA",
    "jackson": "MS",
    "gulfport": "MS",
    "honolulu": "HI",
    "pearl city": "HI",
    "anchorage": "AK",
    "fairbanks": "AK",
    "portland": "ME",
    "lewiston": "ME",
    "manchester": "NH",
    "nashua": "NH",
    "concord": "NH",
    "burlington": "VT",
    "essex": "VT",
    "sioux falls": "SD",
    "rapid city": "SD",
    "fargo": "ND",
    "bismarck": "ND",
    "billings": "MT",
    "missoula": "MT",
    "bozeman": "MT",
    "helena": "MT",
    "cheyenne": "WY",
    "casper": "WY",
    "newark": "DE",
    "wilmington": "DE",
    "dover": "DE",
    "chaska": "MN",
    "irving": "TX",
    "sarasota": "FL",
}

CANADA_CITIES = {
    "toronto": "ON",
    "markham": "ON",
    "ottawa": "ON",
    "mississauga": "ON",
    "montreal": "QC",
    "quebec city": "QC",
    "quebec": "QC",
    "vancouver": "BC",
    "victoria": "BC",
    "burnaby": "BC",
    "calgary": "AB",
    "edmonton": "AB",
    "winnipeg": "MB",
    "regina": "SK",
    "halifax": "NS",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


class UnifiedJobAggregator:
    def __init__(self):
        print("=" * 80)
        print("INITIALIZING JOB AGGREGATOR")
        print("=" * 80)

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            SHEETS_CREDS_FILE, scope
        )
        client = gspread.authorize(creds)

        spreadsheet = client.open(SHEET_NAME)
        self.sheet = spreadsheet.worksheet(WORKSHEET_NAME)
        self.spreadsheet = spreadsheet

        headers = self.sheet.row_values(1)
        if len(headers) < 13:
            self.sheet.resize(rows=1000, cols=13)
            time.sleep(1)

        if "Sponsorship" not in headers:
            self.sheet.update_cell(1, 13, "Sponsorship")
            time.sleep(1)
            self.format_sheet_headers(self.sheet, num_cols=13)

        try:
            self.discarded_sheet = spreadsheet.worksheet(DISCARDED_WORKSHEET)
            disc_headers = self.discarded_sheet.row_values(1)
            if len(disc_headers) < 13:
                self.discarded_sheet.resize(rows=1000, cols=13)
                time.sleep(1)
            if "Sponsorship" not in disc_headers:
                self.discarded_sheet.update_cell(1, 13, "Sponsorship")
                time.sleep(1)
                self.format_sheet_headers(self.discarded_sheet, num_cols=13)
        except:
            self.discarded_sheet = spreadsheet.add_worksheet(
                title=DISCARDED_WORKSHEET, rows=1000, cols=13
            )
            headers = [
                "Sr. No.",
                "Discard Reason",
                "Company",
                "Title",
                "Date Applied",
                "Job URL",
                "Job ID",
                "Job Type",
                "Location",
                "Remote?",
                "Entry Date",
                "Source",
                "Sponsorship",
            ]
            self.discarded_sheet.append_row(headers)
            self.format_sheet_headers(self.discarded_sheet, num_cols=13)

        try:
            self.reviewed_sheet = spreadsheet.worksheet(REVIEWED_WORKSHEET)
            rev_headers = self.reviewed_sheet.row_values(1)
            if len(rev_headers) < 12:
                self.reviewed_sheet.resize(rows=1000, cols=12)
                time.sleep(1)
            if "Sponsorship" not in rev_headers:
                self.reviewed_sheet.update_cell(1, 12, "Sponsorship")
                time.sleep(1)
                self.format_sheet_headers(self.reviewed_sheet, num_cols=12)
        except:
            self.reviewed_sheet = spreadsheet.add_worksheet(
                title=REVIEWED_WORKSHEET, rows=1000, cols=12
            )
            headers = [
                "Sr. No.",
                "Reason",
                "Company",
                "Title",
                "Job URL",
                "Job ID",
                "Job Type",
                "Location",
                "Remote?",
                "Moved Date",
                "Source",
                "Sponsorship",
            ]
            self.reviewed_sheet.append_row(headers)
            self.format_sheet_headers(self.reviewed_sheet, num_cols=12)

        self.remove_duplicates_from_all_sheets()

        self.existing_jobs = set()
        self.existing_urls = set()
        self.existing_job_ids = set()
        self.processing_lock = set()
        self.processed_jobs_cache = {}

        self.next_row = 2
        self.next_sr_no = 1
        self.next_discarded_row = 2
        self.next_discarded_sr_no = 1

        main_data = self.sheet.get_all_values()
        for idx, row in enumerate(main_data[1:], start=2):
            if len(row) > 2:
                company = row[2].strip()
                title = row[3].strip()
                url = row[5].strip() if len(row) > 5 else ""
                job_id = row[6].strip() if len(row) > 6 else ""

                if company or title:
                    self.next_row = idx + 1
                    self.next_sr_no = idx
                    if company and title:
                        normalized_key = self.normalize_for_dedup(f"{company}_{title}")
                        self.existing_jobs.add(normalized_key)
                        self.processed_jobs_cache[normalized_key] = {
                            "company": company,
                            "title": title,
                            "job_id": job_id,
                            "url": url,
                        }

                if url and "http" in url:
                    self.existing_urls.add(self.clean_url(url))
                if job_id and job_id != "N/A":
                    self.existing_job_ids.add(job_id.lower())

        discarded_data = self.discarded_sheet.get_all_values()
        for idx, row in enumerate(discarded_data[1:], start=2):
            if len(row) > 2:
                company = row[2].strip()
                title = row[3].strip()
                url = row[5].strip() if len(row) > 5 else ""
                job_id = row[6].strip() if len(row) > 6 else ""

                if company or title:
                    self.next_discarded_row = idx + 1
                    self.next_discarded_sr_no = idx
                    if company and title:
                        normalized_key = self.normalize_for_dedup(f"{company}_{title}")
                        self.existing_jobs.add(normalized_key)
                        self.processed_jobs_cache[normalized_key] = {
                            "company": company,
                            "title": title,
                            "job_id": job_id,
                            "url": url,
                        }

                if url and "http" in url:
                    self.existing_urls.add(self.clean_url(url))
                if job_id and job_id != "N/A":
                    self.existing_job_ids.add(job_id.lower())

        reviewed_data = self.reviewed_sheet.get_all_values()
        for row in reviewed_data[1:]:
            if len(row) > 2:
                company = row[2].strip()
                title = row[3].strip()
                url = row[4].strip() if len(row) > 4 else ""
                job_id = row[5].strip() if len(row) > 5 else ""

                if company and title:
                    normalized_key = self.normalize_for_dedup(f"{company}_{title}")
                    self.existing_jobs.add(normalized_key)
                    self.processed_jobs_cache[normalized_key] = {
                        "company": company,
                        "title": title,
                        "job_id": job_id,
                        "url": url,
                    }

                if url and "http" in url:
                    self.existing_urls.add(self.clean_url(url))
                if job_id and job_id != "N/A":
                    self.existing_job_ids.add(job_id.lower())

        print(
            f"Loaded: {len(self.existing_jobs)} jobs, {len(self.existing_urls)} URLs, {len(self.existing_job_ids)} IDs"
        )

        self.added = 0
        self.discarded = 0
        self.valid_jobs = []
        self.discarded_jobs = []
        self.gmail_service = None
        self.selenium_driver = None
        self.ziprecruiter_blocks = False
        self.jobright_cookies = None

        self.outcomes = {
            "valid": 0,
            "discarded": 0,
            "skipped_duplicate_url": 0,
            "skipped_duplicate_company_title": 0,
            "skipped_non_job": 0,
            "skipped_marketing": 0,
            "skipped_too_old": 0,
            "failed_http": 0,
            "failed_extraction": 0,
            "low_quality": 0,
            "kept_both_variants": 0,
            "method_standard": 0,
            "method_rotating_agent": 0,
            "method_selenium": 0,
            "method_email_parsed": 0,
            "url_resolved": 0,
        }

    def login_to_jobright_once(self):
        if self.jobright_cookies:
            print("Already authenticated with Jobright")
            return True

        print("\n" + "=" * 60)
        print("JOBRIGHT AUTHENTICATION")
        print("=" * 60)

        driver = None
        try:
            print("[1/6] Installing ChromeDriver...")
            chrome_options = Options()
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option(
                "excludeSwitches", ["enable-logging"]
            )
            chrome_options.add_experimental_option("useAutomationExtension", False)

            service = Service(ChromeDriverManager().install())

            print("[2/6] Opening Chrome browser...")
            driver = webdriver.Chrome(service=service, options=chrome_options)

            print("[3/6] Navigating to Jobright.ai...")
            driver.get("https://jobright.ai")
            time.sleep(3)

            print("[4/6] PLEASE LOG IN NOW:")
            print("        → Click 'Sign in with Google' in browser")
            print("        → Complete Google authentication")
            print("        → Wait until you see your Jobright dashboard")
            print("        → Then press ENTER in this terminal...\n")

            input()

            print("[5/6] Extracting authentication cookies...")
            cookies = driver.get_cookies()

            if not cookies:
                print("✗ ERROR: No cookies found")
                print("  Login may have failed. Please try again.\n")
                driver.quit()
                return False

            self.jobright_cookies = cookies
            print(f"        ✓ Captured {len(cookies)} cookies")

            print("[6/6] Saving cookies to jobright_cookies.json...")
            with open("jobright_cookies.json", "w") as f:
                json.dump(cookies, f, indent=2)

            print("\n✓ AUTHENTICATION SUCCESSFUL!")
            print("  Cookies saved for future runs")
            print("=" * 60 + "\n")

            driver.quit()
            return True

        except Exception as e:
            print(f"\n✗ AUTHENTICATION FAILED")
            print(f"  Error: {e}")
            print("  Jobright URLs will not be resolved")
            print("=" * 60 + "\n")

            if driver:
                try:
                    driver.quit()
                except:
                    pass
            return False

    def resolve_jobright_url(self, jobright_url):
        if "jobright.ai/jobs/info/" not in jobright_url.lower():
            return jobright_url, False

        try:
            if not self.jobright_cookies:
                if os.path.exists("jobright_cookies.json"):
                    with open("jobright_cookies.json", "r") as f:
                        self.jobright_cookies = json.load(f)
                else:
                    return jobright_url, False

            session = requests.Session()
            for cookie in self.jobright_cookies:
                session.cookies.set(cookie["name"], cookie["value"])

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
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
                self.outcomes["url_resolved"] += 1
                return actual_url, is_company_site
            else:
                return jobright_url, False
        except:
            return jobright_url, False

    def normalize_for_dedup(self, text):
        if not text:
            return ""
        text = text.lower()
        text = re.sub(r"[^a-z0-9]", "", text)
        return text

    def should_keep_both_jobs(self, new_job, existing_job):
        new_id = new_job.get("job_id", "N/A")
        existing_id = existing_job.get("job_id", "N/A")

        if new_id != "N/A" and existing_id != "N/A":
            if new_id.lower() != existing_id.lower():
                return True
            else:
                return False

        new_company_norm = self.normalize_for_dedup(new_job.get("company", ""))
        existing_company_norm = self.normalize_for_dedup(
            existing_job.get("company", "")
        )

        if new_company_norm != existing_company_norm:
            return True

        new_title_norm = self.normalize_for_dedup(new_job.get("title", ""))
        existing_title_norm = self.normalize_for_dedup(existing_job.get("title", ""))

        if new_title_norm != existing_title_norm:
            return True

        return False

    def remove_duplicates_from_all_sheets(self):
        total_removed = 0
        total_removed += self.remove_duplicates_from_sheet(
            self.sheet, "Valid", 2, 3, 5, 6
        )
        total_removed += self.remove_duplicates_from_sheet(
            self.discarded_sheet, "Discarded", 2, 3, 5, 6
        )
        total_removed += self.remove_duplicates_from_sheet(
            self.reviewed_sheet, "Reviewed", 2, 3, 4, 5
        )

        if total_removed > 0:
            print(f"Removed {total_removed} duplicates from all sheets")

    def remove_duplicates_from_sheet(self, sheet, name, c_idx, t_idx, u_idx, j_idx):
        try:
            all_data = sheet.get_all_values()
            if len(all_data) <= 1:
                return 0

            seen_jobs = set()
            seen_urls = set()
            seen_job_ids = set()
            rows_to_delete = []

            for idx, row in enumerate(all_data[1:], start=2):
                if len(row) <= max(c_idx, t_idx, u_idx, j_idx):
                    continue

                company = row[c_idx].strip()
                title = row[t_idx].strip()
                url = row[u_idx].strip() if len(row) > u_idx else ""
                job_id = row[j_idx].strip() if len(row) > j_idx else ""

                if not company and not title:
                    continue

                job_key = self.normalize_for_dedup(f"{company}_{title}")
                url_key = self.clean_url(url) if url and "http" in url else None
                job_id_key = job_id.lower() if job_id and job_id != "N/A" else None

                is_dup = (
                    job_key in seen_jobs
                    or (url_key and url_key in seen_urls)
                    or (job_id_key and job_id_key in seen_job_ids)
                )

                if is_dup:
                    rows_to_delete.append(idx)
                else:
                    seen_jobs.add(job_key)
                    if url_key:
                        seen_urls.add(url_key)
                    if job_id_key:
                        seen_job_ids.add(job_id_key)

            if not rows_to_delete:
                return 0

            for row_num in reversed(rows_to_delete):
                sheet.delete_rows(row_num)
                time.sleep(0.3)

            remaining = sheet.get_all_values()
            for idx in range(1, len(remaining)):
                sheet.update_cell(idx + 1, 1, idx)
                time.sleep(0.3)

            return len(rows_to_delete)
        except:
            return 0

    def authenticate_gmail(self):
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

        self.gmail_service = build("gmail", "v1", credentials=creds)

    # NEW: Extract job posting age from page
    def extract_job_age_days(self, soup):
        """Extract how many days ago the job was posted. Returns None if can't determine."""
        try:
            page_text = soup.get_text()[:3000]  # First 3000 chars

            # Pattern 1: "Posted X days ago"
            match = re.search(r"[Pp]osted\s+(\d+)\s+days?\s+ago", page_text)
            if match:
                return int(match.group(1))

            # Pattern 2: "X days ago"
            match = re.search(r"(\d+)\s+days?\s+ago", page_text)
            if match:
                return int(match.group(1))

            # Pattern 3: "Posted today" or "today"
            if re.search(r"[Pp]osted\s+today|Today", page_text):
                return 0

            # Pattern 4: "Posted yesterday"
            if re.search(r"[Pp]osted\s+yesterday|Yesterday", page_text):
                return 1

            # Pattern 5: Hours ago
            match = re.search(r"(\d+)\s+hours?\s+ago", page_text)
            if match:
                return 0  # Less than a day

            # Pattern 6: Specific date formats
            match = re.search(
                r"[Pp]osted:?\s*(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", page_text
            )
            if match:
                # Calculate days difference
                try:
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

            return None  # Can't determine
        except:
            return None

    def fetch_page_comprehensive(self, url, email_html=None, sender=None):
        response = self.try_standard_request(url)
        if response and response.status_code == 200:
            self.outcomes["method_standard"] += 1
            return response, response.url, "standard"

        response = self.try_rotating_user_agents(url)
        if response and response.status_code == 200:
            self.outcomes["method_rotating_agent"] += 1
            return response, response.url, "rotating_agent"

        if SELENIUM_AVAILABLE and (
            "ziprecruiter" in url.lower() or self.ziprecruiter_blocks
        ):
            html, final_url = self.try_selenium(url)
            if html:
                self.outcomes["method_selenium"] += 1
                soup = BeautifulSoup(html, "html.parser")
                mock_response = type(
                    "obj",
                    (object,),
                    {"text": html, "status_code": 200, "url": final_url},
                )()
                return mock_response, final_url, "selenium"

        if email_html:
            job_data = self.extract_job_from_email_content(email_html, url, sender)
            if job_data:
                self.outcomes["method_email_parsed"] += 1
                return None, url, "email_parsed", job_data

        return None, None, "all_failed"

    def try_standard_request(self, url):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            }
            response = requests.get(
                url, headers=headers, allow_redirects=True, timeout=20
            )
            return response
        except:
            return None

    def try_rotating_user_agents(self, url):
        for ua in USER_AGENTS[:3]:
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

    def try_selenium(self, url):
        driver = None
        try:
            if not SELENIUM_AVAILABLE:
                return None, None

            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument(f"user-agent={USER_AGENTS[0]}")

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(30)
            driver.get(url)
            time.sleep(3)

            final_url = driver.current_url
            html = driver.page_source
            return html, final_url
        except:
            return None, None
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    # IMPROVED: Better ZipRecruiter email parsing
    def extract_job_from_email_content(self, email_html, url, sender):
        try:
            soup = BeautifulSoup(email_html, "html.parser")
            sender_lower = sender.lower() if sender else ""

            if "ziprecruiter" in sender_lower or "ziprecruiter" in url.lower():
                return self.parse_ziprecruiter_email_enhanced(soup, url)
            elif "adzuna" in sender_lower or "adzuna" in url.lower():
                return self.parse_adzuna_email(soup, url)
            elif "swelist" in sender_lower:
                return self.parse_swelist_email(soup, url)
            elif "jobright" in sender_lower or "jobright" in url.lower():
                return self.parse_jobright_email_content(soup, url)
            else:
                return self.parse_generic_email(soup, url)
        except:
            return None

    # NEW: Enhanced ZipRecruiter parsing with multiple fallback strategies
    def parse_ziprecruiter_email_enhanced(self, soup, url):
        """Enhanced ZipRecruiter email parsing with multiple strategies."""
        try:
            # Strategy 1: Look for job cards/containers
            job_cards = soup.find_all(
                ["div", "table", "tr"], class_=re.compile(r"job|listing", re.I)
            )

            for card in job_cards:
                # Check if this card contains our URL
                links = card.find_all("a", href=True)
                url_found = any(url[:50] in link["href"] for link in links)

                if url_found:
                    # Extract title from link text or nearby heading
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

                    # Extract company and location from text
                    card_text = card.get_text("\n", strip=True)
                    lines = [l.strip() for l in card_text.split("\n") if l.strip()]

                    company = "Unknown"
                    location = "Unknown"
                    remote = "Unknown"

                    # Look for company and location in bullet-separated format
                    for line in lines:
                        if "•" in line:
                            parts = [p.strip() for p in line.split("•")]
                            if len(parts) >= 2:
                                # First part is usually company
                                if company == "Unknown" and len(parts[0]) > 2:
                                    company = parts[0]
                                # Second part is usually location
                                if location == "Unknown" and len(parts[1]) > 2:
                                    location = parts[1]
                                # Third part might be work type
                                if len(parts) >= 3:
                                    work_type = parts[2].lower()
                                    if "remote" in work_type:
                                        remote = "Remote"
                                    elif "hybrid" in work_type:
                                        remote = "Hybrid"
                                    elif (
                                        "onsite" in work_type
                                        or "in-person" in work_type
                                    ):
                                        remote = "On Site"

                    # Fallback: look for city, state pattern
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
                            "url": url,
                            "job_id": "N/A",
                            "remote": (
                                remote
                                if remote != "Unknown"
                                else self.infer_remote_from_text(location)
                            ),
                            "sponsorship": "Unknown (Email)",
                            "source_method": "email_parsed",
                        }

            # Strategy 2: Original line-by-line parsing (fallback)
            all_text = soup.get_text()
            lines = [l.strip() for l in all_text.split("\n") if l.strip()]
            url_short = url[:60]

            url_index = -1
            for i, line in enumerate(lines):
                if (
                    url_short in line
                    or ("/km/" in url and "/km/" in line)
                    or ("/ekm/" in url and "/ekm/" in line)
                ):
                    url_index = i
                    break

            if url_index == -1:
                return None

            title = "Unknown"
            company = "Unknown"
            location = "Unknown"
            work_type = None

            # Look backwards for title
            for i in range(max(0, url_index - 10), url_index):
                line = lines[i]
                if 10 < len(line) < 150:
                    if any(
                        kw in line.lower()
                        for kw in ["intern", "engineer", "developer", "software", "swe"]
                    ):
                        if (
                            "•" not in line
                            and "View" not in line
                            and "Apply" not in line
                            and "$" not in line
                        ):
                            title = line
                            break

            # Look forward for company/location
            for i in range(url_index, min(len(lines), url_index + 10)):
                line = lines[i]
                if "•" in line and "$" not in line:
                    parts = [p.strip() for p in line.split("•")]
                    if len(parts) >= 2:
                        company = parts[0]
                        location_raw = parts[1]
                        if len(parts) >= 3:
                            work_type = parts[2]
                        location = re.sub(
                            r"\s*(Hybrid|Remote|In-person|On-site)$",
                            "",
                            location_raw,
                            flags=re.I,
                        ).strip()
                        break

            if title == "Unknown":
                return None

            remote = "Unknown"
            if work_type:
                if "remote" in work_type.lower():
                    remote = "Remote"
                elif "hybrid" in work_type.lower():
                    remote = "Hybrid"
                elif "onsite" in work_type.lower() or "in-person" in work_type.lower():
                    remote = "On Site"
            else:
                remote = self.infer_remote_from_text(location)

            return {
                "company": company,
                "title": title,
                "location": location,
                "url": url,
                "job_id": "N/A",
                "remote": remote,
                "sponsorship": "Unknown (Email)",
                "source_method": "email_parsed",
            }
        except Exception as e:
            print(f"  ZipRecruiter email parsing error: {e}")
            return None

    def parse_adzuna_email(self, soup, url):
        try:
            h2_tags = soup.find_all("h2")

            for h2 in h2_tags:
                link = h2.find("a", href=re.compile(r"adzuna\.com/land/ad/"))
                if not link:
                    continue

                job_url = link.get("href")
                job_url_clean = job_url.split("?")[0]
                url_clean = url.split("?")[0]

                if url_clean not in job_url_clean:
                    continue

                title = link.get_text().strip()
                company = "Unknown"
                location = "Unknown"

                next_elem = h2.find_next_sibling(["p", "td"])
                if not next_elem:
                    parent = h2.find_parent(["tr", "td", "div"])
                    if parent:
                        next_elem = parent.find_next(["p", "td"])

                if next_elem:
                    text = next_elem.get_text("\n", strip=True)
                    lines = [l.strip() for l in text.split("\n") if l.strip()]

                    for line in lines:
                        if any(
                            skip in line.lower()
                            for skip in [
                                "more details",
                                "view details",
                                "new",
                                "top match",
                            ]
                        ):
                            continue

                        if " - " in line and len(line) < 100:
                            parts = line.split(" - ", 1)
                            company = parts[0].strip()
                            location = parts[1].strip()
                            location = re.sub(r",?\s*\d{5}$", "", location).strip()
                            break
                        elif "•" in line or "·" in line:
                            parts = re.split("[•·]", line)
                            if len(parts) >= 2:
                                company = parts[0].strip()
                                location_raw = parts[1].strip()
                                location = re.sub(
                                    r"\s*(Hybrid|Remote|In-person)$",
                                    "",
                                    location_raw,
                                    flags=re.I,
                                ).strip()
                                break
                        elif "," in line and len(line) < 60:
                            if re.search(r"[A-Z][a-z]+,\s*[A-Z]{2}", line):
                                location = line
                        elif company == "Unknown" and len(line) > 3 and len(line) < 50:
                            if not re.search(r"\d{5}", line):
                                company = line

                if company == "Unknown":
                    img = h2.find_next("img", alt=True)
                    if img:
                        alt_text = img.get("alt", "").strip()
                        if alt_text and alt_text not in ["NEW", "Top Match", "Adzuna"]:
                            company = alt_text

                return {
                    "company": company,
                    "title": title,
                    "location": location,
                    "url": url,
                    "job_id": "N/A",
                    "remote": self.infer_remote_from_text(location),
                    "sponsorship": "Unknown",
                    "source_method": "email_parsed",
                }

            return None
        except:
            return None

    def parse_swelist_email(self, soup, url):
        try:
            link = soup.find("a", href=url)
            if not link:
                return None

            title = link.get_text().strip()
            tr = link.find_parent("tr")

            if tr:
                cells = tr.find_all("td")
                if len(cells) >= 2:
                    company = cells[0].get_text().strip()
                    if not title:
                        title = cells[1].get_text().strip()
                    location = (
                        cells[2].get_text().strip() if len(cells) > 2 else "Unknown"
                    )

                    return {
                        "company": company,
                        "title": title,
                        "location": location,
                        "url": url,
                        "job_id": "N/A",
                        "remote": self.infer_remote_from_text(location),
                        "sponsorship": "Unknown",
                        "source_method": "email_parsed",
                    }
            return None
        except:
            return None

    def parse_jobright_email_content(self, soup, url):
        try:
            url_base = url.split("?")[0]
            link = soup.find("a", href=re.compile(re.escape(url_base)))

            if not link:
                return None

            container = link.find_parent("table")
            if not container:
                container = link.find_parent("div")

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
            job_tags = container.find_all("p", id="job-tag")

            for tag in job_tags:
                text = tag.get_text().strip()
                if "$" in text or "/hr" in text or "/wk" in text:
                    continue
                if "referral" in text.lower():
                    continue
                if "," in text:
                    location = text
                    break
                elif text == "Remote":
                    location = "Remote"
                    break

            actual_url, is_company_site = self.resolve_jobright_url(url)

            return {
                "company": company,
                "title": title,
                "location": location,
                "url": actual_url,
                "job_id": "N/A",
                "remote": self.infer_remote_from_text(location),
                "sponsorship": "Unknown (Email)",
                "source_method": "email_parsed",
                "is_company_site": is_company_site,
            }
        except:
            return None

    def parse_generic_email(self, soup, url):
        try:
            link = soup.find("a", href=url)
            if not link:
                return None

            title = link.get_text().strip()
            parent = link.find_parent(["div", "td", "tr"])

            if parent:
                text = parent.get_text()
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                company = "Unknown"
                location = "Unknown"

                for line in lines:
                    if "," in line and len(line) < 80:
                        location = line
                    elif line != title and len(line) < 50:
                        company = line

                if company == "Unknown":
                    company = self.extract_company_from_domain(url)

                return {
                    "company": company,
                    "title": title,
                    "location": location,
                    "url": url,
                    "job_id": "N/A",
                    "remote": self.infer_remote_from_text(location + " " + title),
                    "sponsorship": "Unknown",
                    "source_method": "email_parsed",
                }
            return None
        except:
            return None

    def infer_remote_from_text(self, text):
        if not text:
            return "Unknown"
        text_lower = text.lower()
        if "remote" in text_lower:
            return "Remote"
        if "hybrid" in text_lower:
            return "Hybrid"
        if "on-site" in text_lower or "onsite" in text_lower:
            return "On Site"
        return "Unknown"

    def is_recent_posting(self, time_text):
        if not time_text:
            return True
        time_lower = time_text.lower()
        if "minute" in time_lower or "hour" in time_lower:
            return True
        days_match = re.search(r"(\d+)\s*days?\s+ago", time_lower)
        if days_match:
            return int(days_match.group(1)) <= 3
        return True

    def is_external_job_board(self, url):
        if not url:
            return False
        url_lower = url.lower()
        if "ziprecruiter.com" in url_lower or "jobright.ai" in url_lower:
            return False
        external = [
            "greenhouse",
            "lever.co",
            "workday",
            "paylocity",
            "icims",
            "ashbyhq",
            "smartrecruiters",
            "bamboohr",
            "buildsubmarines",
            "recruiting.",
            "careers.",
            "jobs.",
            "apply.",
        ]
        return any(board in url_lower for board in external)

    def detect_sender_name(self, msg_headers):
        for header in msg_headers:
            if header["name"] == "From":
                from_field = header["value"]
                if "ziprecruiter" in from_field.lower():
                    return "ZipRecruiter"
                elif "adzuna" in from_field.lower():
                    return "Adzuna"
                elif "swelist" in from_field.lower():
                    return "SWE List"
                elif "jobright" in from_field.lower():
                    return "Jobright"
                elif "fursah" in from_field.lower():
                    return "Fursah"
                else:
                    match = re.search(r"(?:from|at)\s+([A-Za-z\s]+)", from_field, re.I)
                    if match:
                        return match.group(1).strip()
                    return "Email"
        return "Email"

    def extract_from_jobright_page(self, soup, url):
        company = None
        title = None
        location = None
        sponsorship = "Unknown"
        remote = "Unknown"

        page_text = soup.get_text()
        actual_url, is_company_site = self.resolve_jobright_url(url)

        try:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text().strip()

            for tag in ["h2", "h3"]:
                elem = soup.find(tag)
                if elem:
                    text = elem.get_text().strip()
                    if 5 < len(text) < 60:
                        if not any(
                            w in text.lower()
                            for w in [
                                "intern",
                                "engineer",
                                "summer",
                                "2026",
                                "developer",
                            ]
                        ):
                            company = text
                            break

            loc_patterns = [r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b"]
            for pattern in loc_patterns:
                match = re.search(pattern, page_text[:2000])
                if match:
                    location = f"{match.group(1)}, {match.group(2)}"
                    break

            if not location and "Remote" in page_text[:1500]:
                location = "Remote"

            if "H1B Sponsor Likely" in page_text or "H-1B Sponsor Likely" in page_text:
                sponsorship = "Yes"
            elif "No H1B" in page_text or "No H-1B" in page_text:
                sponsorship = "No"

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

        try:
            company_elem = soup.find(
                ["div", "span"], class_=re.compile("company|employer", re.I)
            )
            if company_elem:
                company = company_elem.get_text().strip()

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

        try:
            lines = [l.strip() for l in page_text.split("\n") if l.strip()]
            for line in lines[:50]:
                if re.match(r"^[A-Z][A-Za-z\s&,\.]+$", line) and len(line) < 60:
                    if not any(
                        w in line.lower() for w in ["intern", "engineer", "summer"]
                    ):
                        company = line
                        break

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

        try:
            json_ld = soup.find("script", type="application/ld+json")
            if json_ld:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    if data.get("hiringOrganization"):
                        company = data["hiringOrganization"].get("name")
                    if data.get("title"):
                        title = data.get("title")

                    if data.get("jobLocation"):
                        job_loc = data["jobLocation"]
                        if isinstance(job_loc, dict):
                            if job_loc.get("address"):
                                addr = job_loc["address"]
                                city = addr.get("addressLocality", "")
                                state = addr.get("addressRegion", "")
                                if city and state:
                                    location = f"{city}, {state}"
                                elif city:
                                    location = city
                            elif isinstance(job_loc.get("addressRegion"), str):
                                location = job_loc.get("addressRegion")

                    if data.get("workType"):
                        work_type = data.get("workType")
                        if "remote" in work_type.lower():
                            remote = "Remote"
                        elif "hybrid" in work_type.lower():
                            remote = "Hybrid"
                        elif "onsite" in work_type.lower():
                            remote = "On Site"

                    if not location and data.get("jobLocationType"):
                        loc_type = data.get("jobLocationType")
                        if "TELECOMMUTE" in loc_type.upper():
                            location = "Remote"
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

    def fetch_swelist_emails(self):
        try:
            if not self.gmail_service:
                self.authenticate_gmail()

            query = 'label:"Job Hunt" newer_than:1d'

            print("Fetching emails with 'Job Hunt' label from last 24 hours")

            results = (
                self.gmail_service.users()
                .messages()
                .list(userId="me", q=query, maxResults=50)
                .execute()
            )

            messages = results.get("messages", [])

            if not messages:
                print("No labeled emails found")
                return []

            print(f"Found {len(messages)} labeled emails")

            all_email_data = []

            for idx, message in enumerate(messages, 1):
                msg = (
                    self.gmail_service.users()
                    .messages()
                    .get(userId="me", id=message["id"], format="full")
                    .execute()
                )

                headers = msg["payload"].get("headers", [])

                subject = ""
                for header in headers:
                    if header["name"] == "Subject":
                        subject = header["value"]
                        break

                sender = self.detect_sender_name(headers)

                html_content = None

                if "parts" in msg["payload"]:
                    for part in msg["payload"]["parts"]:
                        if part["mimeType"] == "text/html":
                            html_data = part["body"].get("data", "")
                            html_content = base64.urlsafe_b64decode(html_data).decode(
                                "utf-8"
                            )
                            break
                elif "body" in msg["payload"]:
                    html_data = msg["payload"]["body"].get("data", "")
                    if html_data:
                        html_content = base64.urlsafe_b64decode(html_data).decode(
                            "utf-8"
                        )

                if html_content:
                    urls = self.extract_job_urls_from_email(html_content)

                    for url in urls:
                        all_email_data.append(
                            {"url": url, "email_html": html_content, "sender": sender}
                        )

            print(f"Total: {len(all_email_data)} job URLs from all emails\n")
            return all_email_data

        except FileNotFoundError:
            print("Gmail credentials file not found")
            return []
        except Exception as e:
            print(f"Gmail error: {e}")
            return []

    def extract_job_urls_from_email(self, email_html):
        soup = BeautifulSoup(email_html, "html.parser")

        job_urls = []
        all_links = soup.find_all("a", href=True)

        job_board_domains = [
            "greenhouse",
            "lever.co",
            "workday",
            "ashbyhq",
            "smartrecruiters",
            "icims.com",
            "myworkdayjobs",
            "jobs.lever.co",
            "boards.greenhouse.io",
            "simplify.jobs",
            "linkedin.com/jobs",
            "indeed.com",
            "glassdoor.com",
            "angellist.com",
            "wellfound.com",
            "monster.com",
            "dice.com",
            "builtin.com",
            "ycombinator.com/jobs",
            "stackoverflow.com/jobs",
            "jobs.github.com",
            "careers.",
            "apply.workable.com",
            "breezy.hr",
            "recruiting.",
            "talentify",
            "workable",
            "jobvite",
            "ultipro",
            "paylocity",
            "paycomonline",
            "bamboohr",
            "fountain.com",
            "ziprecruiter",
            "ziprecruiter.com",
            "adzuna",
            "adzuna.com",
            "jobright",
            "jobright.ai",
            "fursah",
            "fursah.com",
        ]

        for link in all_links:
            url = link.get("href", "")

            if not url.startswith("http"):
                continue

            is_job_board = any(domain in url.lower() for domain in job_board_domains)

            if is_job_board:
                if not self.is_non_job_url(url):
                    job_urls.append(url)

        return list(set(job_urls))

    def is_valid_job_title(self, title):
        if not title or title == "Unknown":
            return False, "No title"

        title_lower = title.lower()

        marketing_phrases = [
            "meet your",
            "join our team",
            "learn more",
            "discover how",
            "explore our",
            "about our",
            "contact us",
            "get started",
            "find out",
            "see how",
            "welcome to",
            "introducing",
        ]

        for phrase in marketing_phrases:
            if phrase in title_lower:
                return False, f"Marketing: '{phrase}'"

        job_role_words = [
            "intern",
            "engineer",
            "developer",
            "analyst",
            "scientist",
            "designer",
            "manager",
            "specialist",
            "coordinator",
            "associate",
            "consultant",
            "architect",
            "researcher",
            "technician",
            "administrator",
        ]

        has_job_word = any(word in title_lower for word in job_role_words)

        if not has_job_word:
            return False, "No job keywords"

        if len(title) < 10:
            return False, "Too short"

        generic_service = ["copilot", "platform", "service", "tool", "portal"]
        for word in generic_service:
            if word in title_lower and "engineer" not in title_lower:
                return False, f"Service page: '{word}'"

        return True, None

    def is_non_job_url(self, url):
        if not url:
            return True

        url_lower = url.lower()

        non_job = [
            "/unsubscribe",
            "/my-alerts",
            "/blog",
            "/prepper",
            "twitter.com",
            "facebook.com",
            "play.google.com",
            "chromewebstore",
            "/privacy",
            "/terms",
            "/opt_out",
            "?retarget=",
        ]

        if any(p in url_lower for p in non_job):
            return True

        if "adzuna.com" in url_lower and "/land/ad/" not in url_lower:
            return True

        return False

    def calculate_quality_score(self, job_data):
        score = 0

        if job_data.get("company") and job_data["company"] != "Unknown":
            score += 2

        if job_data.get("location") and job_data["location"] != "Unknown":
            score += 2

        if job_data.get("job_id") and job_data["job_id"] != "N/A":
            score += 1

        if job_data.get("title") and 15 < len(job_data["title"]) < 120:
            score += 1

        if job_data.get("sponsorship") and job_data["sponsorship"] not in [
            "Unknown",
            "Unknown (Email)",
        ]:
            score += 1

        return score

    def process_email_jobs(self, email_data_list):
        if not email_data_list:
            return

        print("=" * 80)
        print(f"PROCESSING {len(email_data_list)} EMAIL URLS")
        print("=" * 80 + "\n")

        for idx, email_data in enumerate(email_data_list, 1):
            url = email_data["url"]
            email_html = email_data["email_html"]
            sender = email_data["sender"]

            print(f"[{idx}/{len(email_data_list)}] {url[:100]}")
            print(f"  Sender: {sender}")

            if "jobright.ai/jobs/info/" in url.lower():
                print(f"  Resolving Jobright URL...")
                url, is_company_site = self.resolve_jobright_url(url)

            clean_url_original = self.clean_url(url)

            if clean_url_original in self.processing_lock:
                print(f"  → SKIP: Already in processing lock")
                self.outcomes["skipped_duplicate_url"] += 1
                continue

            if clean_url_original in self.existing_urls:
                print(f"  → SKIP: URL already exists")
                self.outcomes["skipped_duplicate_url"] += 1
                continue

            self.processing_lock.add(clean_url_original)

            result = self.process_single_job_comprehensive(
                url, email_html, sender, idx, len(email_data_list)
            )

            if not result:
                continue

            decision = result["decision"]

            if decision == "skip":
                reason_type = result.get("reason_type", "non_job")
                self.outcomes[f"skipped_{reason_type}"] += 1
                print(f"  → SKIP: {result.get('reason')}")
                continue

            elif decision == "discard":
                self.discarded_jobs.append(
                    {
                        "company": result["company"],
                        "title": result["title"],
                        "location": result["location"],
                        "job_type": self.determine_job_type(result["title"]),
                        "remote": result["remote"],
                        "url": result["url"],
                        "job_id": result["job_id"],
                        "reason": result["reason"],
                        "source": result["source"],
                        "sponsorship": result["sponsorship"],
                    }
                )

                normalized_key = self.normalize_for_dedup(
                    f"{result['company']}_{result['title']}"
                )
                self.existing_jobs.add(normalized_key)
                self.existing_urls.add(self.clean_url(result["url"]))

                if result["job_id"] != "N/A":
                    self.existing_job_ids.add(result["job_id"].lower())

                self.outcomes["discarded"] += 1
                print(f"  ✗ DISCARDED: {result['reason']}")

            elif decision == "valid":
                self.valid_jobs.append(
                    {
                        "company": result["company"],
                        "job_id": result["job_id"],
                        "title": result["title"],
                        "job_type": self.determine_job_type(result["title"]),
                        "location": result["location"],
                        "remote": result["remote"],
                        "entry_date": self.format_date(),
                        "url": result["url"],
                        "source": result["source"],
                        "sponsorship": result["sponsorship"],
                    }
                )

                normalized_key = self.normalize_for_dedup(
                    f"{result['company']}_{result['title']}"
                )
                self.existing_jobs.add(normalized_key)
                self.processed_jobs_cache[normalized_key] = {
                    "company": result["company"],
                    "title": result["title"],
                    "job_id": result["job_id"],
                    "url": result["url"],
                }
                self.existing_urls.add(self.clean_url(result["url"]))

                if result["job_id"] != "N/A":
                    self.existing_job_ids.add(result["job_id"].lower())

                self.outcomes["valid"] += 1
                print(f"  ✓ VALID: Added to valid_jobs")

        print("\n" + "=" * 80)
        print("EMAIL PROCESSING COMPLETE")
        print("=" * 80 + "\n")

    def process_single_job_comprehensive(
        self, url, email_html, sender, current_idx, total
    ):
        try:
            time.sleep(random.uniform(1.5, 2.5))

            fetch_result = self.fetch_page_comprehensive(url, email_html, sender)

            if len(fetch_result) == 4:
                _, final_url, method, email_parsed_data = fetch_result

                if method == "email_parsed":
                    return self.process_email_parsed_job(email_parsed_data, sender)

            elif len(fetch_result) == 3:
                response, final_url, method = fetch_result

                if not response:
                    self.outcomes["failed_http"] += 1
                    print(f"  → FAIL: All mechanisms failed")
                    return None

                clean_final = self.clean_url(final_url)
                clean_original = self.clean_url(url)

                if (
                    clean_final in self.processing_lock
                    and clean_final != clean_original
                ):
                    self.outcomes["skipped_duplicate_url"] += 1
                    print(f"  → SKIP: Final URL in processing lock")
                    return None

                if clean_final in self.existing_urls:
                    self.outcomes["skipped_duplicate_url"] += 1
                    print(f"  → SKIP: Final URL duplicate")
                    self.existing_urls.add(self.clean_url(url))
                    return None

                self.processing_lock.add(clean_final)

                print(f"  Final URL: {final_url[:100]}")

                soup = BeautifulSoup(response.text, "html.parser")

                # NEW: Check job age
                job_age_days = self.extract_job_age_days(soup)
                if job_age_days is not None and job_age_days > 3:
                    print(f"  → DISCARD: Job too old ({job_age_days} days)")
                    company = self.extract_company_from_page(soup, final_url)
                    title = self.extract_title_from_page(soup)
                    return {
                        "decision": "discard",
                        "company": company if company else "Unknown",
                        "title": title if title else "Unknown",
                        "location": "Unknown",
                        "remote": "Unknown",
                        "url": final_url,
                        "job_id": "N/A",
                        "reason": f"Posted {job_age_days} days ago (>3 days)",
                        "source": sender,
                        "sponsorship": "Unknown",
                    }

                return self.process_scraped_job(
                    soup, final_url, url, email_html, sender, method
                )

            else:
                self.outcomes["failed_http"] += 1
                return None

        except Exception as e:
            self.outcomes["failed_extraction"] += 1
            print(f"  → EXCEPTION: {str(e)[:100]}")
            return None

    def process_email_parsed_job(self, job_data, sender):
        company = job_data["company"]
        title_raw = job_data["title"]
        location_raw = job_data.get("location", "Unknown")
        url = job_data["url"]

        print(f"  Processing email-parsed data")
        print(f"  Company: {company}")
        print(f"  Title: {title_raw[:80]}")

        # IMPROVED: More aggressive title cleaning
        title_no_location = self.remove_location_from_title(title_raw)
        title_final = self.clean_title_aggressive(title_no_location)

        is_valid_title, title_reason = self.is_valid_job_title(title_final)
        if not is_valid_title:
            print(f"  → SKIP: {title_reason}")
            return {
                "decision": "skip",
                "reason": title_reason,
                "reason_type": (
                    "marketing" if "Marketing" in title_reason else "non_job"
                ),
            }

        if not self.is_cs_engineering_role(title_final):
            print(f"  → DISCARD: Non-CS role")
            return {
                "decision": "discard",
                "company": company,
                "title": title_final,
                "location": "Unknown",
                "remote": "Unknown",
                "url": url,
                "job_id": "N/A",
                "reason": "Non-CS role",
                "source": sender,
                "sponsorship": "Unknown (Email)",
            }

        is_valid_company, fixed_company, company_reason = self.validate_company_field(
            company, title_final, url
        )

        if not is_valid_company:
            print(f"  → DISCARD: {company_reason}")
            return {
                "decision": "discard",
                "company": company,
                "title": title_final,
                "location": "Unknown",
                "remote": "Unknown",
                "url": url,
                "job_id": "N/A",
                "reason": company_reason,
                "source": sender,
                "sponsorship": "Unknown (Email)",
            }

        company = fixed_company

        normalized_key = self.normalize_for_dedup(f"{company}_{title_final}")

        if normalized_key in self.existing_jobs:
            print(f"  Company+Title already exists - checking if different job")

            existing_job = self.processed_jobs_cache.get(normalized_key)

            new_job_data = {
                "company": company,
                "title": title_final,
                "job_id": "N/A",
                "url": url,
            }

            if existing_job and self.should_keep_both_jobs(new_job_data, existing_job):
                self.outcomes["kept_both_variants"] += 1
            else:
                self.outcomes["skipped_duplicate_company_title"] += 1
                return None

        # IMPROVED: Better location formatting
        location_formatted = self.format_location_clean(location_raw)

        quality_score = self.calculate_quality_score(
            {
                "company": company,
                "title": title_final,
                "location": location_formatted,
                "job_id": "N/A",
                "sponsorship": "Unknown (Email)",
            }
        )

        print(f"  Quality score: {quality_score}/7")

        if quality_score < 3:
            print(f"  → DISCARD: Low quality")
            return {
                "decision": "discard",
                "company": company,
                "title": title_final,
                "location": location_formatted,
                "remote": job_data.get("remote", "Unknown"),
                "url": url,
                "job_id": "N/A",
                "reason": f"Low quality: {quality_score}/7",
                "source": sender,
                "sponsorship": "Unknown (Email)",
            }

        print(f"  ✓ VALID (from email)")

        self.processed_jobs_cache[normalized_key] = {
            "company": company,
            "title": title_final,
            "job_id": "N/A",
            "url": url,
        }

        return {
            "decision": "valid",
            "company": company,
            "title": title_final,
            "location": location_formatted,
            "remote": job_data.get("remote", "Unknown"),
            "url": url,
            "job_id": "N/A",
            "source": sender,
            "sponsorship": "Unknown (Email)",
        }

    def validate_and_decide_on_job(
        self,
        company,
        title_raw,
        location,
        remote,
        sponsorship,
        url,
        job_id,
        sender,
        soup,
    ):
        title_no_loc = self.remove_location_from_title(title_raw)
        title = self.clean_title_aggressive(title_no_loc)  # NEW: Aggressive cleaning

        is_valid, reason = self.is_valid_job_title(title)
        if not is_valid:
            return {"decision": "skip", "reason": reason, "reason_type": "non_job"}

        if not self.is_cs_engineering_role(title):
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": "Unknown",
                "remote": "Unknown",
                "url": url,
                "job_id": job_id,
                "reason": "Non-CS",
                "source": sender,
                "sponsorship": sponsorship,
            }

        is_valid_co, fixed_co, co_reason = self.validate_company_field(
            company, title, url
        )
        if not is_valid_co:
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": "Unknown",
                "remote": "Unknown",
                "url": url,
                "job_id": job_id,
                "reason": co_reason,
                "source": sender,
                "sponsorship": sponsorship,
            }

        company = fixed_co

        norm_key = self.normalize_for_dedup(f"{company}_{title}")
        if norm_key in self.existing_jobs:
            existing = self.processed_jobs_cache.get(norm_key)
            if existing and self.should_keep_both_jobs(
                {"company": company, "title": title, "job_id": job_id, "url": url},
                existing,
            ):
                self.outcomes["kept_both_variants"] += 1
            else:
                self.outcomes["skipped_duplicate_company_title"] += 1
                return None

        if soup:
            restriction = self.check_page_for_restrictions(soup)
            if restriction:
                country = self.detect_country_simple(location)
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": country,
                    "remote": remote,
                    "url": url,
                    "job_id": job_id,
                    "reason": restriction,
                    "source": sender,
                    "sponsorship": sponsorship,
                }

        # IMPROVED: Better international location check
        intl_check = self.check_if_international_location_strict(location, soup)
        if intl_check:
            country = self.detect_country_simple(location)
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": country,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": intl_check,
                "source": sender,
                "sponsorship": sponsorship,
            }

        # IMPROVED: Clean location formatting
        location_fmt = self.format_location_clean(location)

        quality = self.calculate_quality_score(
            {
                "company": company,
                "title": title,
                "location": location_fmt,
                "job_id": job_id,
                "sponsorship": sponsorship,
            }
        )

        if quality < 3:
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_fmt,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": f"Low quality: {quality}/7",
                "source": sender,
                "sponsorship": sponsorship,
            }

        self.processed_jobs_cache[norm_key] = {
            "company": company,
            "title": title,
            "job_id": job_id,
            "url": url,
        }

        return {
            "decision": "valid",
            "company": company,
            "title": title,
            "location": location_fmt,
            "remote": remote,
            "url": url,
            "job_id": job_id,
            "source": sender,
            "sponsorship": sponsorship,
        }

    def process_scraped_job(
        self, soup, final_url, original_url, email_html, sender, method
    ):
        print(f"  Extracting from page (method: {method})")

        if "jobright.ai/jobs/info/" in final_url.lower():
            print(f"  Platform: JOBRIGHT")

            jobright_data = self.extract_from_jobright_page(soup, final_url)

            if jobright_data:
                company = jobright_data["company"]
                title_raw = jobright_data["title"]
                location = jobright_data["location"]
                sponsorship = jobright_data["sponsorship"]
                remote = jobright_data["remote"]
                actual_url = jobright_data["url"]

                print(f"  Jobright: {company} - {title_raw[:60]}")
                print(f"  Actual URL: {actual_url[:100]}")

                title_no_loc = self.remove_location_from_title(title_raw)
                title = self.clean_title_aggressive(
                    title_no_loc
                )  # NEW: Aggressive cleaning

                return self.validate_and_decide_on_job(
                    company,
                    title,
                    location,
                    remote,
                    sponsorship,
                    actual_url,
                    "N/A",
                    sender,
                    soup,
                )
            else:
                print(f"  Jobright extraction failed - email fallback")
                email_data = self.extract_job_from_email_content(
                    email_html, original_url, sender
                )
                if email_data:
                    return self.process_email_parsed_job(email_data, sender)
                return None

        elif "ziprecruiter.com" in original_url.lower():
            print(f"  Platform: ZIPRECRUITER")

            if self.is_external_job_board(final_url):
                print(f"  ✓ External board: {final_url[:80]}")

                company = self.extract_company_from_page(soup, final_url)
                title_raw = self.extract_title_from_page(soup)

                if company and title_raw:
                    location = self.extract_location_enhanced(
                        soup, final_url
                    )  # NEW: Enhanced extraction
                    job_id = self.extract_job_id_from_page(soup, final_url)
                    remote_status = self.extract_remote_status_enhanced(
                        soup, location, final_url
                    )  # NEW
                    sponsorship = self.check_sponsorship_status(soup)

                    title_no_loc = self.remove_location_from_title(title_raw)
                    title = self.clean_title_aggressive(
                        title_no_loc
                    )  # NEW: Aggressive cleaning

                    return self.validate_and_decide_on_job(
                        company,
                        title,
                        location,
                        remote_status,
                        sponsorship,
                        final_url,
                        job_id,
                        sender,
                        soup,
                    )
                return None
            else:
                print(f"  ✗ ZipRecruiter page - email fallback")
                email_data = self.extract_job_from_email_content(
                    email_html, original_url, sender
                )
                if email_data:
                    return self.process_email_parsed_job(email_data, sender)
                return None

        else:
            company = self.extract_company_from_page(soup, final_url)
            title_raw = self.extract_title_from_page(soup)

            if not company or not title_raw:
                self.outcomes["failed_extraction"] += 1
                print(f"  → FAIL: No company or title")
                return None

            title_no_location = self.remove_location_from_title(title_raw)
            title_final = self.clean_title_aggressive(
                title_no_location
            )  # NEW: Aggressive cleaning

            print(f"  Company: {company}")
            print(f"  Title: {title_final[:80]}")

            is_valid_title, title_reason = self.is_valid_job_title(title_final)
            if not is_valid_title:
                print(f"  → SKIP: {title_reason}")
                return {
                    "decision": "skip",
                    "reason": title_reason,
                    "reason_type": (
                        "marketing" if "Marketing" in title_reason else "non_job"
                    ),
                }

            if not self.is_cs_engineering_role(title_final):
                print(f"  → DISCARD: Non-CS role")
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title_final,
                    "location": "Unknown",
                    "remote": "Unknown",
                    "url": final_url,
                    "job_id": "N/A",
                    "reason": "Non-CS role",
                    "source": sender,
                    "sponsorship": "Unknown",
                }

            is_valid_company, fixed_company, company_reason = (
                self.validate_company_field(company, title_final, final_url)
            )

            if not is_valid_company:
                print(f"  → DISCARD: {company_reason}")
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title_final,
                    "location": "Unknown",
                    "remote": "Unknown",
                    "url": final_url,
                    "job_id": "N/A",
                    "reason": company_reason,
                    "source": sender,
                    "sponsorship": "Unknown",
                }

            company = fixed_company

            normalized_key = self.normalize_for_dedup(f"{company}_{title_final}")

            if normalized_key in self.existing_jobs:
                print(f"  Company+Title exists - comparing")

                existing_job = self.processed_jobs_cache.get(normalized_key)

                new_job_data = {
                    "company": company,
                    "title": title_final,
                    "job_id": self.extract_job_id_from_page(soup, final_url),
                    "url": final_url,
                }

                if existing_job and self.should_keep_both_jobs(
                    new_job_data, existing_job
                ):
                    self.outcomes["kept_both_variants"] += 1
                else:
                    self.outcomes["skipped_duplicate_company_title"] += 1
                    return None

            print(f"  Checking page restrictions...")
            restriction = self.check_page_for_restrictions(soup)

            if restriction:
                print(f"  → DISCARD: {restriction}")
                job_id = self.extract_job_id_from_page(soup, final_url)
                location = self.extract_location_enhanced(
                    soup, final_url
                )  # NEW: Enhanced extraction
                sponsorship = self.check_sponsorship_status(soup)

                country_only = self.detect_country_simple(location)

                return {
                    "decision": "discard",
                    "company": company,
                    "title": title_final,
                    "location": country_only,
                    "remote": "Unknown",
                    "url": final_url,
                    "job_id": job_id,
                    "reason": restriction,
                    "source": sender,
                    "sponsorship": sponsorship,
                }

            print(f"  Extracting location...")
            job_id = self.extract_job_id_from_page(soup, final_url)
            location_extracted = self.extract_location_enhanced(
                soup, final_url
            )  # NEW: Enhanced extraction
            remote = self.extract_remote_status_enhanced(
                soup, location_extracted, final_url
            )  # NEW
            sponsorship = self.check_sponsorship_status(soup)

            print(f"  Location: {location_extracted}")
            print(f"  Job ID: {job_id}")

            # IMPROVED: Stricter international check
            location_intl_check = self.check_if_international_location_strict(
                location_extracted, soup
            )

            if location_intl_check:
                print(f"  → DISCARD: {location_intl_check}")

                country_only = self.detect_country_simple(location_extracted)

                return {
                    "decision": "discard",
                    "company": company,
                    "title": title_final,
                    "location": country_only,
                    "remote": remote,
                    "url": final_url,
                    "job_id": job_id,
                    "reason": location_intl_check,
                    "source": sender,
                    "sponsorship": sponsorship,
                }

            # IMPROVED: Clean location formatting
            location_formatted = self.format_location_clean(location_extracted)

            print(f"  Formatted location: {location_formatted}")

            quality_score = self.calculate_quality_score(
                {
                    "company": company,
                    "title": title_final,
                    "location": location_formatted,
                    "job_id": job_id,
                    "sponsorship": sponsorship,
                }
            )

            print(f"  Quality score: {quality_score}/7")

            if quality_score < 3:
                print(f"  → DISCARD: Low quality")
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title_final,
                    "location": location_formatted,
                    "remote": remote,
                    "url": final_url,
                    "job_id": job_id,
                    "reason": f"Low quality: {quality_score}/7",
                    "source": sender,
                    "sponsorship": sponsorship,
                }

            print(f"  ✓ VALID JOB - All checks passed")

            self.processed_jobs_cache[normalized_key] = {
                "company": company,
                "title": title_final,
                "job_id": job_id,
                "url": final_url,
            }

            return {
                "decision": "valid",
                "company": company,
                "title": title_final,
                "location": location_formatted,
                "remote": remote,
                "url": final_url,
                "job_id": job_id,
                "source": sender,
                "sponsorship": sponsorship,
            }

    def detect_country_simple(self, location):
        if not location or location == "Unknown":
            return "Unknown"

        location_lower = location.lower()

        if "canada" in location_lower or any(
            f", {p}" in location for p in CANADA_PROVINCES
        ):
            return "Canada"
        if "uk" in location_lower or "united kingdom" in location_lower:
            return "UK"
        if "india" in location_lower:
            return "India"
        if "china" in location_lower:
            return "China"
        if "australia" in location_lower:
            return "Australia"
        if "singapore" in location_lower:
            return "Singapore"

        return location

    # IMPROVED: Stricter international location checking
    def check_if_international_location_strict(self, location, soup):
        """More strict international location checking that only flags definite international locations."""
        if not location or location == "Unknown":
            # If location is unknown, check page content more carefully
            if soup:
                country = self.detect_country_from_page_content_strict(soup)
                if country and country not in ["USA", "United States", "US"]:
                    return f"Location: {country}"
            return None

        location_lower = location.lower()

        # Check for definite Canadian locations
        canadian_cities_definite = [
            "toronto",
            "montreal",
            "vancouver",
            "ottawa",
            "calgary",
            "edmonton",
            "winnipeg",
            "quebec",
            "markham",
            "mississauga",
            "hamilton",
            "kitchener",
            "waterloo",
            "halifax",
            "victoria",
        ]

        for city in canadian_cities_definite:
            if city in location_lower:
                # Double-check this isn't a US city with similar name
                if not any(us_state in location for us_state in US_STATES.values()):
                    return "Location: Canada"

        # Check for Canadian province codes (more strict - must be at end or after comma)
        for prov in CANADA_PROVINCES:
            if re.search(r",\s*" + prov + r"\b", location) or location.endswith(prov):
                return "Location: Canada"

        # Explicit "Canada" mention
        if re.search(r"\bcanada\b", location_lower):
            # But check if it's in a list with US locations
            us_city_count = sum(
                1 for city in CITY_TO_STATE.keys() if city in location_lower
            )
            if us_city_count == 0:  # No US cities found
                return "Location: Canada"

        # Other international locations (more strict)
        if re.search(r"\b(uk|united kingdom)\b", location_lower):
            return "Location: UK"

        intl_countries = {
            "australia": "Australia",
            "india": "India",
            "singapore": "Singapore",
            "china": "China",
            "japan": "Japan",
            "germany": "Germany",
            "france": "France",
            "netherlands": "Netherlands",
        }

        for country_key, country_name in intl_countries.items():
            if re.search(r"\b" + country_key + r"\b", location_lower):
                return f"Location: {country_name}"

        return None

    def detect_country_from_page_content_strict(self, soup):
        """More strict country detection from page content."""
        try:
            page_text = soup.get_text()[:5000]  # First 5000 chars

            # Look for definite patterns that indicate Canada
            canada_definite_patterns = [
                r"\b(?:Toronto|Montreal|Vancouver|Ottawa|Calgary),\s*(?:ON|QC|BC|AB),?\s*Canada\b",
                r"\bCanada\s+(?:only|exclusively|office|location)\b",
                r"\b(?:authorized|eligible)\s+to\s+work\s+in\s+Canada\b",
            ]

            for pattern in canada_definite_patterns:
                if re.search(pattern, page_text, re.I):
                    return "Canada"

            # Check for UK
            if re.search(r"\bLondon,\s*(?:UK|United Kingdom)\b", page_text, re.I):
                return "UK"

            # Don't flag as international if we see US cities
            us_city_mentions = sum(
                1
                for city in list(CITY_TO_STATE.keys())[:20]
                if city in page_text.lower()
            )
            if us_city_mentions >= 2:
                return None  # Likely US-based

            return None
        except:
            return None

    # NEW: More aggressive title cleaning
    def clean_title_aggressive(self, title):
        """Aggressively clean title by removing years, seasons, parenthetical info, and extra fluff."""
        if not title or len(title) < 5:
            return title

        original = title

        # Remove anything in parentheses or brackets
        title = re.sub(r"\s*[\(\[].+?[\)\]]", "", title)

        # Remove season + year patterns (anywhere in title)
        title = re.sub(
            r"\s*(Summer|Fall|Winter|Spring)\s*20\d{2}\s*", " ", title, flags=re.I
        )

        # Remove just years
        title = re.sub(r"\s*20\d{2}\s*", " ", title)

        # Remove "or X months" patterns
        title = re.sub(r",?\s*[Oo]r\s+\d+\s+months", "", title)

        # Remove trailing dashes and extra spaces
        title = re.sub(r"\s*[-–—]\s*$", "", title)
        title = re.sub(r"\s+", " ", title).strip()

        # Remove common suffixes that are not part of the core role
        title = re.sub(r"\s*[-–]\s*(Remote|Hybrid|On-?site)\s*$", "", title, flags=re.I)
        title = re.sub(
            r"\s*[-–]\s*(Full[- ]time|Part[- ]time)\s*$", "", title, flags=re.I
        )

        # If we removed too much, return original
        if len(title) < 10 and len(original) > 15:
            return original

        return title.strip()

    # NEW: Enhanced location extraction
    def extract_location_enhanced(self, soup, url):
        """Enhanced location extraction with better validation."""
        # Try multiple extraction methods in order of reliability

        # Method 1: JSON-LD structured data (most reliable)
        json_loc = self.extract_location_from_json_ld_enhanced(soup)
        if json_loc and self.is_valid_us_location(json_loc):
            return json_loc

        # Method 2: Specific labeled fields
        labeled_loc = self.extract_location_from_labels_enhanced(soup)
        if labeled_loc and self.is_valid_us_location(labeled_loc):
            return labeled_loc

        # Method 3: Simplify-specific extraction
        if "simplify.jobs" in url.lower():
            simplify_loc = self.extract_location_from_simplify(soup)
            if simplify_loc and simplify_loc != "Unknown":
                return simplify_loc

        # Method 4: Workday URL parsing
        if "workday" in url.lower():
            match = re.search(r"/job/([^/]+)/", url)
            if match:
                location_raw = match.group(1)
                if not location_raw.lower().startswith("remote"):
                    workday_loc = self.extract_city_from_workday_backwards(location_raw)
                    if workday_loc != "Unknown":
                        return workday_loc

        # Method 5: Enhanced page scanning
        scanned_loc = self.scan_page_for_location_enhanced_v2(soup)
        if scanned_loc and self.is_valid_us_location(scanned_loc):
            return scanned_loc

        # Method 6: Meta tags
        meta_loc = self.extract_location_from_meta(soup)
        if meta_loc and self.is_valid_us_location(meta_loc):
            return meta_loc

        return "Unknown"

    def is_valid_us_location(self, location):
        """Check if location looks like a valid US location."""
        if not location or location == "Unknown":
            return False

        location_lower = location.lower()

        # Check for US state codes or city names
        has_us_state = any(state in location for state in US_STATES.values())
        has_us_city = any(
            city in location_lower for city in list(CITY_TO_STATE.keys())[:50]
        )

        # Check for Canadian indicators
        has_canada = "canada" in location_lower or any(
            prov in location for prov in CANADA_PROVINCES
        )

        return (has_us_state or has_us_city) and not has_canada

    def extract_location_from_json_ld_enhanced(self, soup):
        """Enhanced JSON-LD extraction."""
        try:
            json_ld_tags = soup.find_all("script", type="application/ld+json")
            for json_ld in json_ld_tags:
                try:
                    data = json.loads(json_ld.string)
                    if isinstance(data, dict):
                        job_loc = data.get("jobLocation", {})
                        if isinstance(job_loc, dict):
                            addr = job_loc.get("address", {})
                            if isinstance(addr, dict):
                                city = addr.get("addressLocality", "")
                                state = addr.get("addressRegion", "")
                                if city and state:
                                    # Validate it's a US state
                                    if state.upper() in US_STATES.values():
                                        return f"{city}, {state.upper()}"
                except:
                    continue
            return None
        except:
            return None

    def extract_location_from_labels_enhanced(self, soup):
        """Enhanced labeled field extraction."""
        try:
            # Look for common label patterns
            labels = [
                "location:",
                "office location:",
                "work location:",
                "job location:",
            ]

            for label in labels:
                # Method 1: Look in dt/dd pairs
                dt = soup.find("dt", string=re.compile(label, re.I))
                if dt:
                    dd = dt.find_next_sibling("dd")
                    if dd:
                        location = dd.get_text().strip()
                        if len(location) < 100 and "," in location:
                            return location

                # Method 2: Look in spans/divs with the label
                for tag in soup.find_all(
                    ["span", "div", "p"], string=re.compile(label, re.I)
                ):
                    parent = tag.parent
                    if parent:
                        siblings = parent.find_next_siblings()
                        if siblings:
                            location = siblings[0].get_text().strip()
                            if len(location) < 100 and "," in location:
                                return location

            return None
        except:
            return None

    def scan_page_for_location_enhanced_v2(self, soup):
        """Enhanced version of page scanning for location."""
        try:
            page_text = soup.get_text()[:6000]

            # Pattern 1: City, State (US format)
            pattern1 = r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b"
            matches = re.findall(pattern1, page_text)

            for city, state in matches:
                if state.upper() in US_STATES.values():
                    # Validate this isn't part of a date or weird context
                    full_match = f"{city}, {state}"
                    context = page_text[
                        max(0, page_text.find(full_match) - 50) : page_text.find(
                            full_match
                        )
                        + 100
                    ]

                    # Skip if it looks like it's in a weird context
                    if any(
                        skip in context.lower()
                        for skip in ["copyright", "reserved", "patent", "trademark"]
                    ):
                        continue

                    return f"{city}, {state.upper()}"

            # Pattern 2: Just city name (validate with known cities)
            for city, state in list(CITY_TO_STATE.items())[:100]:
                if city in page_text.lower():
                    # Make sure it's a word boundary
                    if re.search(r"\b" + city + r"\b", page_text, re.I):
                        return f"{city.title()}, {state}"

            return None
        except:
            return None

    def extract_location_from_meta(self, soup):
        """Extract location from meta tags."""
        try:
            # Check various meta tags
            meta_tags = [
                ("property", "og:locality"),
                ("property", "og:region"),
                ("name", "location"),
                ("name", "geo.placename"),
            ]

            for attr, value in meta_tags:
                meta = soup.find("meta", {attr: value})
                if meta and meta.get("content"):
                    return meta.get("content").strip()

            return None
        except:
            return None

    # NEW: Enhanced remote status extraction
    def extract_remote_status_enhanced(self, soup, location, url):
        """More accurate remote status extraction."""
        try:
            # Check URL first
            if "remote" in url.lower():
                return "Remote"

            # Check location string
            if location:
                location_lower = location.lower()
                if location_lower == "remote" or "remote" in location_lower:
                    return "Remote"
                if "hybrid" in location_lower:
                    return "Hybrid"

            # Check page content (first 3000 chars for speed)
            page_text = soup.get_text()[:3000].lower()

            # Look for explicit remote indicators
            remote_patterns = [
                r"\b100%\s*remote\b",
                r"\bfully\s*remote\b",
                r"\bremote\s*work\b",
                r"\bwork\s*from\s*home\b",
            ]

            for pattern in remote_patterns:
                if re.search(pattern, page_text):
                    return "Remote"

            # Look for hybrid
            if re.search(r"\bhybrid\b", page_text):
                return "Hybrid"

            # Look for on-site indicators
            if re.search(r"\bon[-\s]?site\b|\bin[-\s]?person\b", page_text):
                return "On Site"

            # If we have a valid location, assume on-site
            if location and location != "Unknown":
                return "On Site"

            return "Unknown"
        except:
            return "Unknown"

    # NEW: Clean location formatting
    def format_location_clean(self, location):
        """Clean and format location to just City - State or City, State."""
        if not location or location == "Unknown":
            return "Unknown"

        location = location.strip()

        # Handle multiple locations
        if "|" in location or (", " in location and location.count(",") > 2):
            # Check if all US locations
            us_mentions = sum(1 for state in US_STATES.values() if state in location)
            if us_mentions >= 2:
                return "Multiple US Locations"
            return location  # Keep as is if mixed

        # Remove "USA", "United States", etc.
        location = re.sub(r",?\s*(USA?|United States)$", "", location, flags=re.I)

        # Extract city and state from "City, State" or "City, State, Country" format
        match = re.search(r"([^,]+),\s*([A-Z]{2})(?:,|\s|$)", location)
        if match:
            city = match.group(1).strip()
            state = match.group(2).upper()

            # Validate it's a US state
            if state in US_STATES.values():
                return f"{city} - {state}"
            # Check if it's a Canadian province
            elif state in CANADA_PROVINCES:
                return f"{city}, {state}"

        # If location is just a state code, expand it
        if location.upper() in US_STATES.values():
            return location.upper()

        # Remove extra whitespace
        location = re.sub(r"\s+", " ", location).strip()

        return location

    def extract_location_comprehensive(self, soup, url):
        # Deprecated - use extract_location_enhanced
        return self.extract_location_enhanced(soup, url)

    def extract_location_from_simplify(self, soup):
        try:
            page_text = soup.get_text()

            patterns = [
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2}),\s*(USA|Canada|UK)",
                r"\b([A-Z][a-z]+),\s*(Canada|India|UK)",
                r"(?:Location|Office|Based):\s*([A-Za-z\s,]+)",
            ]

            for pattern in patterns:
                match = re.search(pattern, page_text[:2500], re.I)
                if match:
                    groups = match.groups()
                    if len(groups) == 3:
                        return f"{groups[0]}, {groups[1]}, {groups[2]}"
                    elif len(groups) == 2:
                        return f"{groups[0]}, {groups[1]}"
                    else:
                        extracted = groups[0].strip()
                        if 10 < len(extracted) < 100:
                            return extracted

            return "Unknown"
        except:
            return "Unknown"

    def scan_page_for_location_enhanced(self, soup):
        try:
            page_text = soup.get_text()[:5000]

            pattern1 = (
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2}),\s*(USA|Canada)"
            )
            matches = re.findall(pattern1, page_text)
            if matches:
                city, state, country = matches[0]
                return f"{city}, {state}, {country}"

            pattern2 = r"\b([A-Z][a-z]+),\s*(Canada|UK|India)"
            matches = re.findall(pattern2, page_text)
            if matches:
                return f"{matches[0][0]}, {matches[0][1]}"

            pattern3 = r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b"
            matches = re.findall(pattern3, page_text)
            if matches:
                for city, state in matches:
                    if state.upper() in US_STATES.values():
                        return f"{city}, {state.upper()}"
                    if state.upper() in CANADA_PROVINCES:
                        return f"{city}, {state.upper()}, Canada"

            return "Unknown"
        except:
            return "Unknown"

    def validate_company_field(self, company, title, url):
        if not company or company == "Unknown":
            fixed = self.extract_company_from_domain(url)
            if fixed != "Unknown":
                return True, fixed, None
            return True, company, None

        company_lower = company.lower().strip()
        title_lower = title.lower().strip()

        if company_lower == title_lower:
            fixed = self.extract_company_from_domain(url)
            if fixed != "Unknown" and fixed.lower() != title_lower:
                print(f"    Fixed Company=Title: '{company}' → '{fixed}'")
                return True, fixed, None
            return False, company, "Bad data: Company=Title"

        if re.search(r"20\d{2}", company):
            return False, company, "Bad data: Year in company"

        if re.search(r"\bintern(ship)?\b", company, re.I):
            return False, company, "Bad data: Intern in company"

        return True, company, None

    def remove_location_from_title(self, title):
        if not title:
            return title

        title = re.sub(
            r"\s*[-,]\s*(Canada|UK|India|Remote|Hybrid).*$", "", title, flags=re.I
        )
        title = re.sub(r"\s*[-,]\s*[A-Z][a-z]+,\s*[A-Z]{2}.*$", "", title)
        title = re.sub(r",?\s*Or\s+\d+\s+months.*$", "", title, flags=re.I)

        return title.strip()

    def clean_title(self, title):
        # Deprecated - use clean_title_aggressive
        return self.clean_title_aggressive(title)

    def extract_company_from_domain(self, url):
        try:
            if "workday" in url.lower():
                match = re.search(r"https?://([^.]+)\.(?:wd\d+\.)?myworkdayjobs", url)
                if match:
                    return self.format_company_name(match.group(1))

            match = re.search(r"https?://(?:www\.)?([^./]+)", url)
            if match:
                return self.format_company_name(match.group(1))

            return "Unknown"
        except:
            return "Unknown"

    def format_company_name(self, slug):
        slug = slug.replace("-", " ").replace("_", " ")

        special = {
            "stanfordhealthcare": "Stanford Health Care",
            "bmo": "BMO",
            "jpmorgan": "JPMorgan",
            "figma": "Figma",
            "ibm": "IBM",
            "simplify": "Simplify Jobs",
        }

        slug_clean = slug.lower().replace(" ", "")
        if slug_clean in special:
            return special[slug_clean]

        return slug.title()

    def extract_city_from_workday_backwards(self, location_str):
        try:
            location_str = re.sub(r"^[0-9]+[A-Z]+\s*[-–]?\s*", "", location_str)
            location_str = location_str.replace("-", " ").replace("_", " ")
            parts = [p.strip() for p in location_str.split() if p.strip()]

            if not parts:
                return "Unknown"

            state = None
            city_words = []

            for i in range(len(parts) - 1, -1, -1):
                word_upper = parts[i].upper()

                if not state and word_upper in US_STATES.values():
                    state = word_upper
                    continue

                if state:
                    city_words.insert(0, parts[i])
                    potential_city = " ".join(city_words).lower()

                    if potential_city in CITY_TO_STATE:
                        if CITY_TO_STATE[potential_city] == state:
                            return f"{' '.join(city_words).title()} - {state}"

            if state and city_words:
                facility = ["hospital", "building", "pkwy", "patient", "meadows"]
                cleaned = [
                    w
                    for w in city_words
                    if w.lower() not in facility and not w.isdigit()
                ]

                if cleaned:
                    return f"{' '.join(cleaned).title()} - {state}"

            return "Unknown"
        except:
            return "Unknown"

    def extract_location_from_page_labels(self, soup):
        # Deprecated - use extract_location_from_labels_enhanced
        return self.extract_location_from_labels_enhanced(soup)

    def extract_location_from_json_ld(self, soup):
        # Deprecated - use extract_location_from_json_ld_enhanced
        return self.extract_location_from_json_ld_enhanced(soup)

    def format_location_for_us(self, location):
        # Deprecated - use format_location_clean
        return self.format_location_clean(location)

    def clean_url(self, url):
        if not url:
            return ""

        if "jobright.ai/jobs/info/" in url.lower():
            match = re.search(r"(jobright\.ai/jobs/info/[a-f0-9]+)", url, re.I)
            if match:
                return match.group(1).lower()

        url = re.sub(r"\?.*$", "", url)
        url = re.sub(r"#.*$", "", url)
        return url.lower().rstrip("/")

    def remove_emojis(self, text):
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

        text = emoji_pattern.sub(r"", text)
        text = re.sub(r"[↳🇺🇸🛂\*🔒❌✅]+", "", text)
        text = re.sub(r"\s+", " ", text).strip()

        return text

    def safe_get_cell(self, row, index, default=""):
        try:
            if len(row) > index:
                return row[index].strip() if row[index] else default
            return default
        except:
            return default

    def format_sheet_headers(self, sheet, num_cols=13):
        try:
            col_letter = chr(ord("A") + num_cols - 1)
            sheet.format(
                f"A1:{col_letter}1",
                {
                    "horizontalAlignment": "CENTER",
                    "textFormat": {
                        "fontFamily": "Times New Roman",
                        "fontSize": 14,
                        "bold": True,
                    },
                    "backgroundColor": {"red": 0.7, "green": 0.9, "blue": 0.7},
                },
            )
        except:
            pass

    # NEW: Dynamic column width for status column
    def auto_resize_columns_with_status_dynamic(
        self, sheet, url_column_index, total_columns
    ):
        """Auto-resize columns with special handling for status column (column B)."""
        try:
            # First, auto-resize all columns
            self.spreadsheet.batch_update(
                {
                    "requests": [
                        {
                            "autoResizeDimensions": {
                                "dimensions": {
                                    "sheetId": sheet.id,
                                    "dimension": "COLUMNS",
                                    "startIndex": 0,
                                    "endIndex": total_columns,
                                }
                            }
                        }
                    ]
                }
            )
            time.sleep(1)

            is_discarded = total_columns == 13 and url_column_index == 5

            # Get all data to determine status column width
            all_data = sheet.get_all_values()
            max_status_length = 10  # Default minimum

            for row in all_data[1:]:  # Skip header
                if len(row) > 1:
                    status_text = row[1].strip()
                    if status_text:
                        # Approximate pixel width (8 pixels per character)
                        text_width = len(status_text) * 8 + 20  # +20 for padding
                        max_status_length = max(max_status_length, text_width)

            # Cap at reasonable maximum
            max_status_length = min(max_status_length, 250)

            fixed_widths = []

            # Status column (B) - dynamic width
            fixed_widths.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": 1,
                            "endIndex": 2,
                        },
                        "properties": {
                            "pixelSize": max(max_status_length, 100)
                        },  # Minimum 100 pixels
                        "fields": "pixelSize",
                    }
                }
            )

            # Date Applied column (E)
            fixed_widths.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": 4,
                            "endIndex": 5,
                        },
                        "properties": {"pixelSize": 150},
                        "fields": "pixelSize",
                    }
                }
            )

            # URL column - compact
            fixed_widths.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": url_column_index,
                            "endIndex": url_column_index + 1,
                        },
                        "properties": {"pixelSize": 100},
                        "fields": "pixelSize",
                    }
                }
            )

            # Sponsorship column
            fixed_widths.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": 12,
                            "endIndex": 13,
                        },
                        "properties": {"pixelSize": 110},
                        "fields": "pixelSize",
                    }
                }
            )

            self.spreadsheet.batch_update({"requests": fixed_widths})
        except Exception as e:
            print(f"Column resize error: {e}")

    def auto_resize_all_columns_except_url(
        self, sheet, url_column_index, total_columns
    ):
        # Deprecated - use auto_resize_columns_with_status_dynamic
        self.auto_resize_columns_with_status_dynamic(
            sheet, url_column_index, total_columns
        )

    def get_status_color(self, status):
        colors = {
            "Not Applied": {"red": 0.6, "green": 0.76, "blue": 1.0},
            "Applied": {"red": 0.58, "green": 0.93, "blue": 0.31},
            "Rejected": {"red": 0.97, "green": 0.42, "blue": 0.42},
            "OA Round 1": {"red": 1.0, "green": 0.95, "blue": 0.4},
            "OA Round 2": {"red": 1.0, "green": 0.95, "blue": 0.4},
            "Interview 1": {"red": 0.82, "green": 0.93, "blue": 0.94},
            "Offer accepted": {"red": 0.16, "green": 0.65, "blue": 0.27},
            "Assessment": {"red": 0.89, "green": 0.89, "blue": 0.89},
        }
        return colors.get(status, None)

    def apply_status_colors_to_range(self, start_row, end_row):
        try:
            all_data = self.sheet.get_all_values()
            color_requests = []

            for row_idx in range(start_row - 1, min(end_row, len(all_data))):
                if row_idx < 1:
                    continue

                row = all_data[row_idx]
                status = self.safe_get_cell(row, 1, "")
                color = self.get_status_color(status)

                if color:
                    text_color = (
                        {"red": 1.0, "green": 1.0, "blue": 1.0}
                        if status == "Offer accepted"
                        else {"red": 0.0, "green": 0.0, "blue": 0.0}
                    )

                    color_requests.append(
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": self.sheet.id,
                                    "startRowIndex": row_idx,
                                    "endRowIndex": row_idx + 1,
                                    "startColumnIndex": 1,
                                    "endColumnIndex": 2,
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": color,
                                        "textFormat": {
                                            "foregroundColor": text_color,
                                            "fontFamily": "Times New Roman",
                                            "fontSize": 13,
                                        },
                                        "horizontalAlignment": "CENTER",
                                    }
                                },
                                "fields": "userEnteredFormat",
                            }
                        }
                    )

            if color_requests:
                for i in range(0, len(color_requests), 20):
                    batch = color_requests[i : i + 20]
                    self.spreadsheet.batch_update({"requests": batch})
                    time.sleep(1)
        except:
            pass

    def check_sponsorship_status(self, soup):
        try:
            page_text = soup.get_text().lower()

            positive = [
                "visa sponsorship available",
                "h1b sponsorship",
                "will sponsor",
                "opt eligible",
            ]
            for indicator in positive:
                if indicator in page_text:
                    return "Yes"

            negative = ["no visa sponsorship", "does not sponsor", "cannot sponsor"]
            for indicator in negative:
                if indicator in page_text:
                    return "No"

            return "Unknown"
        except:
            return "Unknown"

    def get_detailed_discard_reason(self, title):
        title_lower = title.lower()

        if "phd" in title_lower and "or" not in title_lower:
            return "PhD required"

        excluded = ["product management", "marketing", "sales", "hr", "finance"]
        for kw in excluded:
            if kw in title_lower:
                return f"Non-CS: {kw.title()}"

        if "🔒" in title:
            return "Position closed"

        return "Filtered"

    def is_cs_engineering_role(self, title):
        title_lower = title.lower()

        excluded = ["product management", "marketing", "sales", "hr", "finance"]
        for kw in excluded:
            if kw in title_lower:
                return False

        required = [
            "software",
            "swe",
            "engineer",
            "developer",
            "data",
            "tech",
            "algorithm",
            "ml",
            "ai",
        ]
        return any(kw in title_lower for kw in required)

    def check_page_for_restrictions(self, soup):
        try:
            page_text = soup.get_text().lower()

            if "security clearance" in page_text:
                return "Security clearance required"

            if "us citizen only" in page_text or "must be a us citizen" in page_text:
                return "US citizenship required"

            if (
                "undergraduate students only" in page_text
                or "bachelor's degree in progress" in page_text
            ):
                return "Bachelor's requirement only"

            return None
        except:
            return None

    def extract_job_id_from_page(self, soup, url):
        try:
            page_text = soup.get_text()

            patterns = [
                (r"Req ID:\s*([A-Z0-9\-]+)", 1),
                (r"Job ID:\s*([A-Z0-9\-]+)", 1),
            ]

            for pattern, group in patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    return match.group(group).strip()

            if "workday" in url.lower():
                match = re.search(r"_([A-Z]*\d+)(?:\?|$)", url)
                if match:
                    return match.group(1)

            return "N/A"
        except:
            return "N/A"

    def extract_remote_status(self, soup, location, url):
        # Deprecated - use extract_remote_status_enhanced
        return self.extract_remote_status_enhanced(soup, location, url)

    def determine_job_type(self, title):
        if "co-op" in title.lower():
            return "Co-op"
        return "Internship"

    def format_date(self):
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")

    def parse_age(self, age_str):
        match = re.search(r"(\d+)d", age_str.lower()) if age_str else None
        return int(match.group(1)) if match else 999

    def is_duplicate(self, company, title, url, job_id="N/A"):
        normalized_key = self.normalize_for_dedup(f"{company}_{title}")
        if normalized_key in self.existing_jobs:
            return True

        if self.clean_url(url) in self.existing_urls:
            return True

        if job_id != "N/A" and job_id.lower() in self.existing_job_ids:
            return True

        return False

    def extract_company_from_page(self, soup, url):
        try:
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

            meta = soup.find("meta", {"property": "og:site_name"})
            if meta and meta.get("content"):
                company = meta.get("content").strip()
                company = re.sub(
                    r"\s*[-|]\s*(careers|jobs).*$", "", company, flags=re.I
                )
                if company and len(company) < 50:
                    return company

            return self.extract_company_from_domain(url)
        except:
            return "Unknown"

    def extract_title_from_page(self, soup):
        try:
            json_ld = soup.find("script", type="application/ld+json")
            if json_ld:
                try:
                    data = json.loads(json_ld.string)
                    if isinstance(data, dict):
                        title = data.get("title", "")
                        if title and 5 < len(title) < 200:
                            return title
                except:
                    pass

            h1 = soup.find("h1")
            if h1:
                title = h1.get_text().strip()
                if title and 5 < len(title) < 200:
                    return title

            title_tag = soup.find("title")
            if title_tag:
                full_title = title_tag.get_text().strip()
                parts = full_title.split("-")
                if parts:
                    title = parts[0].strip()
                    if title and 5 < len(title) < 200:
                        return title

            return "Unknown"
        except:
            return "Unknown"

    def process_job_url(self, url, company, title):
        try:
            fetch_result = self.fetch_page_comprehensive(
                url, email_html=None, sender="GitHub"
            )

            if len(fetch_result) != 3:
                self.outcomes["failed_http"] += 1
                return {"status": "rejected", "reason": "Failed", "url": url}

            response, final_url, method = fetch_result

            if not response:
                return {"status": "rejected", "reason": "HTTP failed", "url": url}

            clean_final = self.clean_url(final_url)

            if clean_final in self.processing_lock or clean_final in self.existing_urls:
                self.outcomes["skipped_duplicate_url"] += 1
                self.existing_urls.add(self.clean_url(url))
                self.existing_urls.add(clean_final)
                return None

            self.processing_lock.add(clean_final)

            soup = BeautifulSoup(response.text, "html.parser")

            # NEW: Check job age
            job_age_days = self.extract_job_age_days(soup)
            if job_age_days is not None and job_age_days > 3:
                return {
                    "status": "rejected",
                    "reason": f"Posted {job_age_days} days ago",
                    "url": final_url,
                    "job_id": "N/A",
                    "location": "Unknown",
                    "sponsorship": "Unknown",
                }

            restriction = self.check_page_for_restrictions(soup)

            if restriction:
                job_id = self.extract_job_id_from_page(soup, final_url)
                location = self.extract_location_enhanced(soup, final_url)  # NEW
                sponsorship = self.check_sponsorship_status(soup)

                country_only = self.detect_country_simple(location)

                self.existing_urls.add(clean_final)

                return {
                    "status": "rejected",
                    "reason": restriction,
                    "url": final_url,
                    "job_id": job_id,
                    "location": country_only,
                    "sponsorship": sponsorship,
                }

            job_id = self.extract_job_id_from_page(soup, final_url)
            location = self.extract_location_enhanced(soup, final_url)  # NEW
            remote = self.extract_remote_status_enhanced(
                soup, location, final_url
            )  # NEW
            sponsorship = self.check_sponsorship_status(soup)

            self.existing_urls.add(clean_final)

            return {
                "status": "accepted",
                "final_url": final_url,
                "job_id": job_id,
                "location": location,
                "remote": remote,
                "sponsorship": sponsorship,
            }

        except:
            self.outcomes["failed_extraction"] += 1
            return {"status": "rejected", "reason": "Error", "url": url}

    def scrape_simplify_github(self):
        print("Scraping SimplifyJobs GitHub")

        try:
            response = requests.get(SIMPLIFY_URL, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            tables = soup.find_all("table")

            for table in tables:
                rows = table.find_all("tr")

                for row in rows[1:]:
                    cells = row.find_all("td")
                    if len(cells) < 5:
                        continue

                    company_link = cells[0].find("a")
                    if not company_link:
                        continue

                    company = self.remove_emojis(company_link.get_text(strip=True))
                    title_raw = self.remove_emojis(cells[1].get_text(strip=True))
                    location = self.remove_emojis(cells[2].get_text(strip=True))
                    age = cells[4].get_text(strip=True)

                    apply_link = cells[3].find("a", href=True)
                    if not apply_link:
                        continue
                    apply_url = apply_link.get("href", "")

                    if not company or not title_raw or not apply_url:
                        continue

                    # CHANGED: Check age <= 1 day (was > 1)
                    if self.parse_age(age) > 1:
                        continue

                    title_no_loc = self.remove_location_from_title(title_raw)
                    title = self.clean_title_aggressive(
                        title_no_loc
                    )  # NEW: Aggressive cleaning

                    if self.is_duplicate(
                        company,
                        title,
                        apply_url,
                        self.extract_job_id_from_url(apply_url),
                    ):
                        self.outcomes["skipped_duplicate_url"] += 1
                        continue

                    is_valid, reason = self.is_valid_job_title(title)
                    if not is_valid:
                        self.outcomes["skipped_non_job"] += 1
                        continue

                    discard_reason = self.get_detailed_discard_reason(title)

                    if "🔒" in str(cells[3]):
                        discard_reason = "Position closed"

                    if discard_reason != "Filtered":
                        country = (
                            self.detect_country_simple(location)
                            if discard_reason.startswith("Location")
                            else self.format_location_clean(location)
                        )

                        self.discarded_jobs.append(
                            {
                                "company": company,
                                "title": title,
                                "location": country,
                                "job_type": self.determine_job_type(title),
                                "remote": (
                                    "Remote"
                                    if "remote" in location.lower()
                                    else "On Site"
                                ),
                                "url": apply_url,
                                "job_id": self.extract_job_id_from_url(apply_url),
                                "reason": discard_reason,
                                "source": "GitHub",
                                "sponsorship": "Unknown",
                            }
                        )

                        self.existing_jobs.add(
                            self.normalize_for_dedup(f"{company}_{title}")
                        )
                        self.existing_urls.add(self.clean_url(apply_url))
                        self.outcomes["discarded"] += 1
                        continue

                    result = self.process_job_url(apply_url, company, title)

                    if not result:
                        continue

                    if result["status"] == "rejected":
                        self.discarded_jobs.append(
                            {
                                "company": company,
                                "title": title,
                                "location": result.get("location", location),
                                "job_type": self.determine_job_type(title),
                                "remote": result.get("remote", "On Site"),
                                "url": result.get("url", apply_url),
                                "job_id": result.get("job_id", "N/A"),
                                "reason": result.get("reason", "Unknown"),
                                "source": "GitHub",
                                "sponsorship": result.get("sponsorship", "Unknown"),
                            }
                        )

                        self.existing_jobs.add(
                            self.normalize_for_dedup(f"{company}_{title}")
                        )
                        self.existing_urls.add(
                            self.clean_url(result.get("url", apply_url))
                        )
                        self.outcomes["discarded"] += 1
                    else:
                        quality = self.calculate_quality_score(
                            {
                                "company": company,
                                "title": title,
                                "location": result.get("location"),
                                "job_id": result.get("job_id"),
                                "sponsorship": result.get("sponsorship"),
                            }
                        )

                        if quality < 3:
                            self.outcomes["low_quality"] += 1
                            continue

                        # IMPROVED: Stricter location check
                        location_check = self.check_if_international_location_strict(
                            result.get("location"), None
                        )

                        if location_check:
                            country = self.detect_country_simple(result.get("location"))

                            self.discarded_jobs.append(
                                {
                                    "company": company,
                                    "title": title,
                                    "location": country,
                                    "job_type": self.determine_job_type(title),
                                    "remote": result.get("remote"),
                                    "url": result["final_url"],
                                    "job_id": result.get("job_id"),
                                    "reason": location_check,
                                    "source": "GitHub",
                                    "sponsorship": result.get("sponsorship"),
                                }
                            )
                            self.outcomes["discarded"] += 1
                        else:
                            self.valid_jobs.append(
                                {
                                    "company": company,
                                    "job_id": result["job_id"],
                                    "title": title,
                                    "job_type": self.determine_job_type(title),
                                    "location": self.format_location_clean(
                                        result.get("location")
                                    ),  # NEW
                                    "remote": result.get("remote"),
                                    "entry_date": self.format_date(),
                                    "url": result["final_url"],
                                    "source": "GitHub",
                                    "sponsorship": result.get("sponsorship"),
                                }
                            )

                            norm_key = self.normalize_for_dedup(f"{company}_{title}")
                            self.existing_jobs.add(norm_key)
                            self.processed_jobs_cache[norm_key] = {
                                "company": company,
                                "title": title,
                                "job_id": result["job_id"],
                                "url": result["final_url"],
                            }
                            self.existing_urls.add(self.clean_url(result["final_url"]))
                            self.outcomes["valid"] += 1

            print(
                f"GitHub: {len([j for j in self.valid_jobs if j['source'] == 'GitHub'])} valid\n"
            )

        except Exception as e:
            print(f"GitHub error: {e}")

    def extract_job_id_from_url(self, url):
        try:
            if "workday" in url.lower():
                match = re.search(r"_([A-Z]*\d+)(?:\?|$)", url)
                if match:
                    return match.group(1)
            return "N/A"
        except:
            return "N/A"

    def ensure_mutual_exclusion(self):
        if not self.valid_jobs or not self.discarded_jobs:
            print("Mutual exclusion: No overlap possible")
            return

        valid_keys = {
            (
                self.normalize_for_dedup(j["company"]),
                self.normalize_for_dedup(j["title"]),
                self.clean_url(j["url"]),
            )
            for j in self.valid_jobs
        }

        discarded_keys = {
            (
                self.normalize_for_dedup(j["company"]),
                self.normalize_for_dedup(j["title"]),
                self.clean_url(j["url"]),
            )
            for j in self.discarded_jobs
        }

        overlap = valid_keys & discarded_keys

        if overlap:
            print(f"⚠ MUTUAL EXCLUSION: {len(overlap)} jobs in BOTH lists")

            overlap_simple = {(c, t) for c, t, u in overlap}

            removed = [
                j
                for j in self.valid_jobs
                if (
                    self.normalize_for_dedup(j["company"]),
                    self.normalize_for_dedup(j["title"]),
                )
                in overlap_simple
            ]

            for job in removed:
                print(f"  Removed: {job['company']} - {job['title'][:60]}")

            self.valid_jobs = [
                j
                for j in self.valid_jobs
                if (
                    self.normalize_for_dedup(j["company"]),
                    self.normalize_for_dedup(j["title"]),
                )
                not in overlap_simple
            ]

            self.outcomes["valid"] = len(self.valid_jobs)
            print(f"  After exclusion: {len(self.valid_jobs)} valid remain")
        else:
            print("Mutual exclusion: No overlap - clean!")

    def batch_update_with_links_and_dropdowns(
        self, sheet, start_row, rows_data, is_valid_sheet=True
    ):
        try:
            if not rows_data:
                return

            range_name = f"A{start_row}:M{start_row + len(rows_data) - 1}"
            sheet.update(
                values=rows_data, range_name=range_name, value_input_option="RAW"
            )
            time.sleep(2)

            sheet.format(
                range_name,
                {
                    "horizontalAlignment": "CENTER",
                    "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
                },
            )
            time.sleep(2)

            url_requests = []
            for idx, row_data in enumerate(rows_data):
                url = row_data[5]
                if url and url.startswith("http"):
                    url_requests.append(
                        {
                            "updateCells": {
                                "range": {
                                    "sheetId": sheet.id,
                                    "startRowIndex": start_row + idx - 1,
                                    "endRowIndex": start_row + idx,
                                    "startColumnIndex": 5,
                                    "endColumnIndex": 6,
                                },
                                "rows": [
                                    {
                                        "values": [
                                            {
                                                "userEnteredValue": {
                                                    "stringValue": url
                                                },
                                                "textFormatRuns": [
                                                    {"format": {"link": {"uri": url}}}
                                                ],
                                            }
                                        ]
                                    }
                                ],
                                "fields": "userEnteredValue,textFormatRuns",
                            }
                        }
                    )

            if url_requests:
                self.spreadsheet.batch_update({"requests": url_requests})
                time.sleep(2)

            if is_valid_sheet:
                dropdown_requests = []
                for idx in range(len(rows_data)):
                    dropdown_requests.append(
                        {
                            "setDataValidation": {
                                "range": {
                                    "sheetId": sheet.id,
                                    "startRowIndex": start_row + idx - 1,
                                    "endRowIndex": start_row + idx,
                                    "startColumnIndex": 1,
                                    "endColumnIndex": 2,
                                },
                                "rule": {
                                    "condition": {
                                        "type": "ONE_OF_LIST",
                                        "values": [
                                            {"userEnteredValue": "Not Applied"},
                                            {"userEnteredValue": "Applied"},
                                            {"userEnteredValue": "Rejected"},
                                            {"userEnteredValue": "OA Round 1"},
                                            {"userEnteredValue": "OA Round 2"},
                                            {"userEnteredValue": "Interview 1"},
                                            {"userEnteredValue": "Offer accepted"},
                                            {"userEnteredValue": "Assessment"},
                                        ],
                                    },
                                    "showCustomUi": True,
                                    "strict": False,
                                },
                            }
                        }
                    )

                if dropdown_requests:
                    self.spreadsheet.batch_update({"requests": dropdown_requests})
                    time.sleep(2)

                self.apply_status_colors_to_range(start_row, start_row + len(rows_data))
        except Exception as e:
            print(f"Batch update error: {e}")

    def add_to_sheet(self):
        if not self.valid_jobs:
            print("No new valid jobs")
            return

        print(f"Adding {len(self.valid_jobs)} valid jobs")

        for i in range(0, len(self.valid_jobs), 10):
            batch = self.valid_jobs[i : i + 10]

            rows = []
            for idx, job in enumerate(batch):
                sr_no = self.next_sr_no + i + idx
                rows.append(
                    [
                        sr_no,
                        "Not Applied",
                        job["company"],
                        job["title"],
                        "N/A",
                        job["url"],
                        job["job_id"],
                        job["job_type"],
                        job["location"],
                        job["remote"],
                        job["entry_date"],
                        job["source"],
                        job.get("sponsorship", "Unknown"),
                    ]
                )

            self.batch_update_with_links_and_dropdowns(
                self.sheet, self.next_row + i, rows, True
            )
            self.added += len(rows)
            time.sleep(3)

        self.auto_resize_columns_with_status_dynamic(self.sheet, 5, 13)
        print(f"Added {self.added} valid jobs")

    def add_to_discarded(self):
        if not self.discarded_jobs:
            print("No new discarded jobs")
            return

        print(f"Adding {len(self.discarded_jobs)} discarded")

        for i in range(0, len(self.discarded_jobs), 10):
            batch = self.discarded_jobs[i : i + 10]

            rows = []
            for idx, job in enumerate(batch):
                sr_no = self.next_discarded_sr_no + i + idx
                rows.append(
                    [
                        sr_no,
                        job.get("reason", "Filtered"),
                        job["company"],
                        job["title"],
                        "N/A",
                        job["url"],
                        job.get("job_id", "N/A"),
                        job["job_type"],
                        job["location"],
                        job["remote"],
                        self.format_date(),
                        job["source"],
                        job.get("sponsorship", "Unknown"),
                    ]
                )

            self.batch_update_with_links_and_dropdowns(
                self.discarded_sheet, self.next_discarded_row + i, rows, False
            )
            self.discarded += len(rows)
            time.sleep(3)

        self.auto_resize_columns_with_status_dynamic(self.discarded_sheet, 5, 13)
        print(f"Added {self.discarded} discarded")

    def print_processing_summary(self):
        print("")
        print("=" * 80)
        print("PROCESSING SUMMARY:")
        print("=" * 80)
        print(f"  ✓ Valid jobs: {self.outcomes['valid']}")
        print(f"  ✗ Discarded: {self.outcomes['discarded']}")
        print(f"  ⊘ Skipped (duplicate URL): {self.outcomes['skipped_duplicate_url']}")
        print(
            f"  ⊘ Skipped (duplicate company+title): {self.outcomes['skipped_duplicate_company_title']}"
        )
        print(f"  ⊘ Skipped (non-job): {self.outcomes['skipped_non_job']}")
        print(f"  ⊘ Skipped (too old): {self.outcomes['skipped_too_old']}")
        print(f"  ⚠ Failed (HTTP): {self.outcomes['failed_http']}")
        print(f"  ⚠ Failed (extraction): {self.outcomes['failed_extraction']}")
        print(f"  ⚠ Low quality: {self.outcomes['low_quality']}")
        print(f"  ✓ Kept both variants: {self.outcomes['kept_both_variants']}")
        print(f"  🔄 URLs resolved: {self.outcomes['url_resolved']}")
        print("")
        print("EXTRACTION METHODS USED:")
        print(f"  Standard requests: {self.outcomes['method_standard']}")
        print(f"  Rotating UA: {self.outcomes['method_rotating_agent']}")
        print(f"  Selenium: {self.outcomes['method_selenium']}")
        print(f"  Email parsing: {self.outcomes['method_email_parsed']}")
        print("=" * 80)

    def run(self):
        start_time = time.time()
        print("\nStarting job aggregation\n")

        if SELENIUM_AVAILABLE:
            if os.path.exists("jobright_cookies.json"):
                print("Found saved Jobright cookies")
                try:
                    with open("jobright_cookies.json", "r") as f:
                        self.jobright_cookies = json.load(f)
                    print(f"✓ Loaded {len(self.jobright_cookies)} cookies\n")
                except Exception as e:
                    print(f"Cookie load error: {e}")
                    print("Attempting fresh login...\n")
                    if not self.login_to_jobright_once():
                        print("Continuing without Jobright authentication\n")
            else:
                print("No saved Jobright cookies - initiating login")
                if not self.login_to_jobright_once():
                    print("Continuing without Jobright authentication\n")
        else:
            print("Selenium not available - Jobright URL resolution disabled\n")

        self.scrape_simplify_github()

        try:
            email_data = self.fetch_swelist_emails()
            if email_data:
                self.process_email_jobs(email_data)
        except Exception as e:
            print(f"Email error: {e}")

        print("\nRunning mutual exclusion check")
        self.ensure_mutual_exclusion()

        print(
            f"\nFinal: {len([j for j in self.valid_jobs if j['source'] == 'GitHub'])} GitHub, "
            f"{len([j for j in self.valid_jobs if j['source'] != 'GitHub'])} Email\n"
        )

        self.add_to_sheet()
        self.add_to_discarded()

        self.print_processing_summary()

        elapsed = time.time() - start_time
        print(f"\nExecution time: {elapsed/60:.1f} minutes")
        print(f"DONE: {self.added} valid, {self.discarded} discarded")
        print("=" * 80 + "\n")


if __name__ == "__main__":
    UnifiedJobAggregator().run()
