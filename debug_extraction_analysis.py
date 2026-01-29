#!/usr/bin/env python3
"""
COMPREHENSIVE DEBUG TEST SCRIPT
Tests all extraction methods with detailed logging and data analysis
Outputs: console logs, detailed log file, JSON analysis file
"""

import re
import json
import pickle
import requests
from collections import defaultdict
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Configuration
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
GMAIL_CREDS_FILE = "gmail_credentials.json"
GMAIL_TOKEN_FILE = "gmail_token.pickle"
TEST_DAYS = 3
MAX_SIMPLIFY_TEST = 10  # Deep analysis on fewer URLs
MAX_JOBRIGHT_TEST = 10

# Output files
DEBUG_LOG_FILE = "extraction_debug.log"
ANALYSIS_JSON_FILE = "extraction_analysis.json"


class DebugLogger:
    """Centralized logging to console + file + JSON"""

    def __init__(self):
        self.log_file = open(DEBUG_LOG_FILE, "w")
        self.analysis_data = {
            "timestamp": datetime.now().isoformat(),
            "simplify": [],
            "jobright": [],
        }

        # Write header
        self._write_to_file("=" * 120)
        self._write_to_file(
            f"EXTRACTION DEBUG LOG - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self._write_to_file("=" * 120 + "\n")

    def log(self, message, also_print=False):
        """Log to file and optionally console"""
        self._write_to_file(message)
        if also_print:
            print(message)

    def log_method(self, platform, url, method_name, status, details):
        """Log method attempt with structured data"""
        msg = f"\n[{platform}] [{method_name}] {status}"
        msg += f"\n  URL: {url[:80]}"
        msg += f"\n  Details: {details}"
        self._write_to_file(msg)

    def add_analysis(self, platform, url, data):
        """Add to JSON analysis"""
        self.analysis_data[platform].append(
            {"url": url, "timestamp": datetime.now().isoformat(), **data}
        )

    def _write_to_file(self, message):
        """Write to log file"""
        self.log_file.write(message + "\n")
        self.log_file.flush()

    def save_analysis(self):
        """Save JSON analysis"""
        with open(ANALYSIS_JSON_FILE, "w") as f:
            json.dump(self.analysis_data, f, indent=2)
        self.log_file.close()
        print(f"\n📊 Analysis saved to: {ANALYSIS_JSON_FILE}")
        print(f"📝 Debug log saved to: {DEBUG_LOG_FILE}")


class GmailFetcher:
    """Fetch emails from Gmail"""

    def __init__(self):
        self.service = self._authenticate()

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
                    GMAIL_CREDS_FILE, GMAIL_SCOPES
                )
                creds = flow.run_local_server(port=0)

            with open(GMAIL_TOKEN_FILE, "wb") as token:
                pickle.dump(creds, token)

        return build("gmail", "v1", credentials=creds)

    def fetch_job_hunt_emails(self, max_emails=50):
        """Fetch emails from Job Hunt label"""
        try:
            after_date = (datetime.now() - timedelta(days=TEST_DAYS)).strftime(
                "%Y/%m/%d"
            )
            query = f"label:Job Hunt after:{after_date}"

            results = (
                self.service.users()
                .messages()
                .list(userId="me", q=query, maxResults=max_emails)
                .execute()
            )

            messages = results.get("messages", [])

            emails = []
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
                sender = headers.get("From", "")

                sender_type = "Unknown"
                if "jobright" in sender.lower():
                    sender_type = "Jobright"
                elif "simplify" in sender.lower() or "swe" in sender.lower():
                    sender_type = "SWE List"

                body = self._get_body(message["payload"])
                urls = self._extract_urls(body, sender_type)

                if urls:
                    emails.append(
                        {
                            "subject": subject,
                            "sender": sender_type,
                            "urls": urls,
                            "html": body,
                        }
                    )

            return emails

        except Exception as e:
            print(f"❌ Error fetching emails: {e}")
            return []

    def _get_body(self, payload):
        """Extract email body"""
        import base64

        if "body" in payload and payload["body"].get("data"):
            return base64.urlsafe_b64decode(payload["body"]["data"]).decode(
                "utf-8", errors="ignore"
            )

        if "parts" in payload:
            for part in payload["parts"]:
                if part["mimeType"] == "text/html" and "data" in part["body"]:
                    return base64.urlsafe_b64decode(part["body"]["data"]).decode(
                        "utf-8", errors="ignore"
                    )
                elif part["mimeType"] == "text/plain" and "data" in part["body"]:
                    return base64.urlsafe_b64decode(part["body"]["data"]).decode(
                        "utf-8", errors="ignore"
                    )

        return ""

    def _extract_urls(self, body, sender_type):
        """Extract job URLs from email body"""
        urls = []
        seen = set()

        try:
            soup = BeautifulSoup(body, "html.parser")
            all_links = [
                a.get("href") for a in soup.find_all("a", href=True) if a.get("href")
            ]
        except:
            all_links = re.findall(r'https?://[^\s<>"]+', body)

        for url in all_links:
            if not url or not url.startswith("http"):
                continue

            url_lower = url.lower()

            if sender_type == "SWE List" and "simplify.jobs/p/" in url_lower:
                if url not in seen:
                    urls.append(url)
                    seen.add(url)

            elif sender_type == "Jobright" and "jobright.ai/jobs/info/" in url_lower:
                if url not in seen:
                    urls.append(url)
                    seen.add(url)

        return urls


class SimplifyDebugger:
    """Deep debug analysis of Simplify pages"""

    def __init__(self, logger):
        self.logger = logger

    def analyze_page(self, url):
        """Comprehensive analysis of Simplify page"""
        print(f"\n{'='*100}")
        print(f"ANALYZING SIMPLIFY PAGE")
        print(f"{'='*100}")
        print(f"URL: {url}")

        analysis = {
            "url": url,
            "methods_attempted": [],
            "data_found": {},
            "final_url": None,
            "success_method": None,
        }

        # METHOD 1: Analyze __NEXT_DATA__ JSON completely
        result = self._method_1_full_json_analysis(url, analysis)
        if result:
            analysis["final_url"] = result
            analysis["success_method"] = "Full JSON Analysis"
            return analysis

        # METHOD 2: Parse Apply button
        result = self._method_2_apply_button(url, analysis)
        if result:
            analysis["final_url"] = result
            analysis["success_method"] = "Apply Button"
            return analysis

        # METHOD 3: Extract all links
        result = self._method_3_all_links(url, analysis)
        if result:
            analysis["final_url"] = result
            analysis["success_method"] = "Link Scraping"
            return analysis

        # METHOD 4: HTTP redirect
        result = self._method_4_redirect(url, analysis)
        if result:
            analysis["final_url"] = result
            analysis["success_method"] = "HTTP Redirect"
            return analysis

        return analysis

    def _method_1_full_json_analysis(self, url, analysis):
        """METHOD 1: Complete __NEXT_DATA__ JSON inspection"""
        try:
            self.logger.log(f"\n--- Simplify Method 1: Full JSON Analysis ---")

            response = requests.get(url, timeout=10)
            html = response.text

            self.logger.log(f"Response size: {len(html)} bytes")

            # Find __NEXT_DATA__ script
            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
                html,
                re.DOTALL,
            )

            if not match:
                self.logger.log("❌ No __NEXT_DATA__ found")
                analysis["methods_attempted"].append(
                    {
                        "method": "Full JSON Analysis",
                        "status": "failed",
                        "reason": "No __NEXT_DATA__ script tag",
                    }
                )
                return None

            # Parse full JSON
            data = json.loads(match.group(1))

            # Log ENTIRE JSON structure (first level keys)
            self.logger.log(f"\n__NEXT_DATA__ top-level keys: {list(data.keys())}")

            if "props" in data and "pageProps" in data["props"]:
                pageProps_keys = list(data["props"]["pageProps"].keys())
                self.logger.log(f"pageProps keys: {pageProps_keys}")

                if "jobPosting" in data["props"]["pageProps"]:
                    jobPosting = data["props"]["pageProps"]["jobPosting"]
                    jobPosting_keys = list(jobPosting.keys())
                    self.logger.log(
                        f"\njobPosting keys ({len(jobPosting_keys)} total): {jobPosting_keys}"
                    )

                    # Store in analysis
                    analysis["data_found"]["jobPosting_keys"] = jobPosting_keys

                    # Check for URL-related fields
                    url_fields = {}
                    for key in jobPosting_keys:
                        if any(
                            keyword in key.lower()
                            for keyword in ["url", "link", "apply", "href", "external"]
                        ):
                            url_fields[key] = jobPosting[key]
                            self.logger.log(
                                f"  → URL-related field '{key}': {str(jobPosting[key])[:100]}"
                            )

                    analysis["data_found"]["url_fields"] = url_fields

                    # Check tracked_obj
                    if "tracked_obj" in jobPosting:
                        tracked_obj = jobPosting["tracked_obj"]
                        self.logger.log(f"\ntracked_obj: {tracked_obj}")
                        analysis["data_found"]["tracked_obj"] = tracked_obj

                    # Check for direct URL field
                    for field_name in [
                        "url",
                        "apply_url",
                        "externalUrl",
                        "external_url",
                        "companyUrl",
                        "applicationUrl",
                    ]:
                        if field_name in jobPosting:
                            value = jobPosting[field_name]
                            self.logger.log(f"\n✓ FOUND '{field_name}': {value}")
                            if value and "http" in str(value):
                                print(f"  ✅ Found URL in JSON field '{field_name}'")
                                print(f"     → {value}")
                                analysis["methods_attempted"].append(
                                    {
                                        "method": "Full JSON Analysis",
                                        "status": "success",
                                        "field": field_name,
                                        "url": value,
                                    }
                                )
                                return value

                    # Check job.url or job.company.url
                    if "job" in jobPosting:
                        job = jobPosting["job"]
                        self.logger.log(f"\njobPosting.job keys: {list(job.keys())}")

                        if "url" in job:
                            self.logger.log(f"job.url: {job['url']}")

                        if "company" in job:
                            company = job["company"]
                            company_keys = list(company.keys())
                            self.logger.log(f"job.company keys: {company_keys}")

                            for key in company_keys:
                                if "url" in key.lower() or "career" in key.lower():
                                    self.logger.log(f"  company.{key}: {company[key]}")

            self.logger.log("\n❌ No direct URL field found in __NEXT_DATA__")
            analysis["methods_attempted"].append(
                {
                    "method": "Full JSON Analysis",
                    "status": "failed",
                    "reason": "No URL field in JSON",
                }
            )

        except Exception as e:
            self.logger.log(f"❌ Method 1 error: {e}")
            analysis["methods_attempted"].append(
                {"method": "Full JSON Analysis", "status": "error", "error": str(e)}
            )

        return None

    def _method_2_apply_button(self, url, analysis):
        """METHOD 2: Apply button inspection"""
        try:
            self.logger.log(f"\n--- Simplify Method 2: Apply Button Analysis ---")

            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            # Find all buttons
            all_buttons = soup.find_all(
                ["button", "a"], text=re.compile(r"apply", re.I)
            )
            self.logger.log(f"Found {len(all_buttons)} Apply-related elements")

            for idx, elem in enumerate(all_buttons[:5], 1):
                tag_name = elem.name
                elem_text = elem.get_text(strip=True)[:30]
                href = elem.get("href")
                onclick = elem.get("onclick")
                data_url = elem.get("data-url")
                data_apply = elem.get("data-apply-url")
                elem_class = elem.get("class", [])

                self.logger.log(f"\n  Button {idx}:")
                self.logger.log(f"    Tag: <{tag_name}>")
                self.logger.log(f"    Text: '{elem_text}'")
                self.logger.log(f"    Class: {elem_class}")
                self.logger.log(f"    href: {href}")
                self.logger.log(f"    onclick: {onclick}")
                self.logger.log(f"    data-url: {data_url}")
                self.logger.log(f"    data-apply-url: {data_apply}")

                # Check if any attribute has company URL
                for attr_value in [href, onclick, data_url, data_apply]:
                    if (
                        attr_value
                        and "http" in str(attr_value)
                        and "simplify.jobs" not in str(attr_value)
                    ):
                        if "linkedin" not in str(attr_value).lower():
                            # Extract URL from onclick if needed
                            if attr_value == onclick:
                                url_match = re.search(r'https?://[^\s\'"]+', attr_value)
                                if url_match:
                                    attr_value = url_match.group(0)

                            print(f"  ✅ Found URL in Apply button")
                            print(f"     → {attr_value}")
                            self.logger.log(f"\n✓ SUCCESS: Found URL in button")
                            analysis["methods_attempted"].append(
                                {
                                    "method": "Apply Button",
                                    "status": "success",
                                    "url": attr_value,
                                }
                            )
                            return attr_value

            self.logger.log("\n❌ No URL in Apply buttons")
            analysis["methods_attempted"].append(
                {
                    "method": "Apply Button",
                    "status": "failed",
                    "reason": "No href in Apply buttons",
                }
            )

        except Exception as e:
            self.logger.log(f"❌ Method 2 error: {e}")
            analysis["methods_attempted"].append(
                {"method": "Apply Button", "status": "error", "error": str(e)}
            )

        return None

    def _method_3_all_links(self, url, analysis):
        """METHOD 3: Extract ALL links and analyze"""
        try:
            self.logger.log(f"\n--- Simplify Method 3: All Links Analysis ---")

            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            all_links = soup.find_all("a", href=True)
            self.logger.log(f"Found {len(all_links)} total links")

            job_board_domains = [
                "workday",
                "greenhouse",
                "lever",
                "icims",
                "taleo",
                "ashby",
                "smartrecruiters",
                "jobvite",
                "bamboohr",
                "myworkdayjobs",
                "careers",
                "jobs",
            ]

            job_links = []
            for link in all_links:
                href = link.get("href")
                if href and "http" in href:
                    if any(domain in href.lower() for domain in job_board_domains):
                        if (
                            "simplify.jobs" not in href
                            and "linkedin" not in href.lower()
                        ):
                            job_links.append(
                                {"href": href, "text": link.get_text(strip=True)[:50]}
                            )

            self.logger.log(f"\nFound {len(job_links)} job board links:")
            for idx, link in enumerate(job_links[:10], 1):
                self.logger.log(f"  {idx}. Text: '{link['text']}'")
                self.logger.log(f"     URL: {link['href'][:80]}")

            if job_links:
                best_link = job_links[0]["href"]
                print(f"  ✅ Found URL via link scraping")
                print(f"     → {best_link}")
                analysis["methods_attempted"].append(
                    {
                        "method": "All Links",
                        "status": "success",
                        "url": best_link,
                        "total_found": len(job_links),
                    }
                )
                return best_link

            self.logger.log("\n❌ No job board links found")
            analysis["methods_attempted"].append(
                {
                    "method": "All Links",
                    "status": "failed",
                    "reason": "No job board domains in links",
                }
            )

        except Exception as e:
            self.logger.log(f"❌ Method 3 error: {e}")
            analysis["methods_attempted"].append(
                {"method": "All Links", "status": "error", "error": str(e)}
            )

        return None

    def _method_4_redirect(self, url, analysis):
        """METHOD 4: HTTP redirect"""
        try:
            self.logger.log(f"\n--- Simplify Method 4: HTTP Redirect ---")

            response = requests.get(url, allow_redirects=True, timeout=5)
            final_url = response.url

            self.logger.log(f"Original: {url}")
            self.logger.log(f"Final: {final_url}")

            if (
                "simplify.jobs" not in final_url.lower()
                and "linkedin" not in final_url.lower()
            ):
                print(f"  ✅ Found URL via HTTP redirect")
                print(f"     → {final_url}")
                analysis["methods_attempted"].append(
                    {"method": "HTTP Redirect", "status": "success", "url": final_url}
                )
                return final_url

            self.logger.log("❌ Redirect stayed on Simplify")
            analysis["methods_attempted"].append(
                {
                    "method": "HTTP Redirect",
                    "status": "failed",
                    "reason": "No redirect to company site",
                }
            )

        except Exception as e:
            self.logger.log(f"❌ Method 4 error: {e}")
            analysis["methods_attempted"].append(
                {"method": "HTTP Redirect", "status": "error", "error": str(e)}
            )

        return None


class JobrightDebugger:
    """Deep debug analysis of Jobright pages"""

    def __init__(self, logger):
        self.logger = logger

    def analyze_page(self, url):
        """Comprehensive analysis of Jobright page"""
        print(f"\n{'='*100}")
        print(f"ANALYZING JOBRIGHT PAGE")
        print(f"{'='*100}")
        print(f"URL: {url}")

        analysis = {
            "url": url,
            "methods_attempted": [],
            "data_found": {},
            "final_url": None,
            "success_method": None,
        }

        # METHOD 1: Stealth HTTP with full debug
        result = self._method_1_stealth_http_debug(url, analysis)
        if result:
            analysis["final_url"] = result
            analysis["success_method"] = "Stealth HTTP"
            return analysis

        return analysis

    def _method_1_stealth_http_debug(self, url, analysis):
        """METHOD 1: Stealth HTTP with comprehensive debugging"""
        try:
            self.logger.log(f"\n--- Jobright Method 1: Stealth HTTP (Full Debug) ---")

            # Build headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://jobright.ai/",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }

            self.logger.log(f"Headers: {json.dumps(headers, indent=2)}")

            # Load cookies
            cookie_dict = {}
            try:
                with open("jobright_cookies.json", "r") as f:
                    cookies_list = json.load(f)
                    for cookie in cookies_list:
                        cookie_dict[cookie["name"]] = cookie["value"]
                    self.logger.log(
                        f"\nLoaded {len(cookie_dict)} cookies: {list(cookie_dict.keys())}"
                    )
            except Exception as e:
                self.logger.log(f"⚠️ Cookie loading failed: {e}")

            # Make request
            self.logger.log(f"\nMaking HTTP request...")
            response = requests.get(
                url, headers=headers, cookies=cookie_dict, timeout=10
            )

            self.logger.log(f"Response status: {response.status_code}")
            self.logger.log(f"Response size: {len(response.text)} bytes")
            self.logger.log(f"Response headers: {dict(response.headers)}")

            html = response.text

            # Find ALL <script> tags
            soup = BeautifulSoup(html, "html.parser")
            script_tags = soup.find_all("script")
            self.logger.log(f"\nFound {len(script_tags)} <script> tags")

            # Analyze each script for JSON data
            json_scripts = []
            for idx, script in enumerate(script_tags, 1):
                script_text = script.string or script.get_text()
                if script_text and len(script_text) > 50:
                    # Check if it's JSON
                    if ("{" in script_text and '"' in script_text) or script.get(
                        "type"
                    ) == "application/json":
                        json_scripts.append(
                            {
                                "index": idx,
                                "type": script.get("type"),
                                "id": script.get("id"),
                                "length": len(script_text),
                                "preview": script_text[:200],
                            }
                        )

            self.logger.log(
                f"\nFound {len(json_scripts)} scripts with JSON-like content:"
            )
            for js in json_scripts:
                self.logger.log(
                    f"  Script {js['index']}: type={js['type']}, id={js['id']}, size={js['length']}"
                )
                self.logger.log(f"    Preview: {js['preview']}")

            # Search for ALL possible URL field names
            url_field_patterns = [
                "originalUrl",
                "applyLink",
                "jobUrl",
                "job_url",
                "canonicalUrl",
                "apply_link",
                "applicationUrl",
                "postingUrl",
                "externalUrl",
                "companyUrl",
                "company_url",
                "career_url",
                "careerUrl",
            ]

            found_fields = {}
            for field_name in url_field_patterns:
                pattern = rf'"{field_name}"\s*:\s*"([^"]+)"'
                matches = re.findall(pattern, html)
                if matches:
                    found_fields[field_name] = matches
                    self.logger.log(
                        f"\nFound field '{field_name}': {len(matches)} instances"
                    )
                    for match in matches[:3]:
                        self.logger.log(f"  → {match[:100]}")

            analysis["data_found"]["json_fields"] = found_fields

            # Validate and return first valid URL
            for field_name, urls in found_fields.items():
                for url_value in urls:
                    if (
                        url_value
                        and "http" in url_value
                        and "jobright.ai" not in url_value
                        and "linkedin" not in url_value.lower()
                    ):

                        print(f"  ✅ Found URL via HTTP '{field_name}'")
                        print(f"     → {url_value[:80]}")

                        self.logger.log(f"\n✓ SUCCESS via '{field_name}': {url_value}")
                        analysis["methods_attempted"].append(
                            {
                                "method": "Stealth HTTP",
                                "status": "success",
                                "field": field_name,
                                "url": url_value,
                            }
                        )
                        return url_value

            # If we found fields but no valid URLs
            if found_fields:
                self.logger.log(
                    f"\n⚠️ Found {len(found_fields)} URL fields but all were invalid (jobright.ai or linkedin)"
                )
                analysis["methods_attempted"].append(
                    {
                        "method": "Stealth HTTP",
                        "status": "failed",
                        "reason": "Found URL fields but all invalid",
                        "fields_found": list(found_fields.keys()),
                    }
                )
            else:
                self.logger.log(f"\n❌ No URL fields found in HTML")
                analysis["methods_attempted"].append(
                    {
                        "method": "Stealth HTTP",
                        "status": "failed",
                        "reason": "No URL fields in HTML",
                    }
                )

        except Exception as e:
            self.logger.log(f"❌ Method 1 error: {e}")
            analysis["methods_attempted"].append(
                {"method": "Stealth HTTP", "status": "error", "error": str(e)}
            )

        return None


def run_comprehensive_test():
    """Main test with comprehensive debugging"""
    print("=" * 100)
    print("COMPREHENSIVE DEBUG TEST - DEEP ANALYSIS")
    print("=" * 100)

    logger = DebugLogger()

    # Fetch emails
    print(f"\n📧 Fetching emails (last {TEST_DAYS} days)...")
    fetcher = GmailFetcher()
    emails = fetcher.fetch_job_hunt_emails(max_emails=50)

    if not emails:
        print("❌ No emails found")
        return

    print(f"✓ Found {len(emails)} emails\n")

    # Separate URLs
    simplify_urls = []
    jobright_urls = []

    for email in emails:
        if email["sender"] == "SWE List":
            simplify_urls.extend(email["urls"])
        elif email["sender"] == "Jobright":
            jobright_urls.extend(email["urls"])

    print(f"📊 URLs to analyze:")
    print(f"   Simplify: {len(simplify_urls)} (will deep-analyze {MAX_SIMPLIFY_TEST})")
    print(f"   Jobright: {len(jobright_urls)} (will deep-analyze {MAX_JOBRIGHT_TEST})")

    # Test Simplify with deep analysis
    print("\n" + "=" * 100)
    print(
        f"DEEP ANALYSIS: SIMPLIFY (analyzing {min(len(simplify_urls), MAX_SIMPLIFY_TEST)} URLs)"
    )
    print("=" * 100)

    simplify_debugger = SimplifyDebugger(logger)
    simplify_results = []

    for idx, url in enumerate(simplify_urls[:MAX_SIMPLIFY_TEST], 1):
        print(f"\n[{idx}/{MAX_SIMPLIFY_TEST}] {url[:60]}...")
        logger.log(f"\n\n{'='*100}")
        logger.log(f"SIMPLIFY URL {idx}/{MAX_SIMPLIFY_TEST}")
        logger.log(f"{'='*100}")
        logger.log(f"URL: {url}")

        analysis = simplify_debugger.analyze_page(url)
        simplify_results.append(analysis)
        logger.add_analysis("simplify", url, analysis)

        if analysis["final_url"]:
            print(f"  ✅ SUCCESS")
        else:
            print(f"  ❌ FAILED - All methods exhausted")

    # Test Jobright with deep analysis
    print("\n" + "=" * 100)
    print(
        f"DEEP ANALYSIS: JOBRIGHT (analyzing {min(len(jobright_urls), MAX_JOBRIGHT_TEST)} URLs)"
    )
    print("=" * 100)

    jobright_debugger = JobrightDebugger(logger)
    jobright_results = []

    for idx, url in enumerate(jobright_urls[:MAX_JOBRIGHT_TEST], 1):
        print(f"\n[{idx}/{MAX_JOBRIGHT_TEST}] {url[:60]}...")
        logger.log(f"\n\n{'='*100}")
        logger.log(f"JOBRIGHT URL {idx}/{MAX_JOBRIGHT_TEST}")
        logger.log(f"{'='*100}")
        logger.log(f"URL: {url}")

        analysis = jobright_debugger.analyze_page(url)
        jobright_results.append(analysis)
        logger.add_analysis("jobright", url, analysis)

        if analysis["final_url"]:
            print(f"  ✅ SUCCESS")
        else:
            print(f"  ❌ FAILED - All methods exhausted")

    # Summary
    print("\n" + "=" * 100)
    print("ANALYSIS SUMMARY")
    print("=" * 100)

    # Simplify summary
    simplify_success = sum(1 for r in simplify_results if r["final_url"])
    simplify_total = len(simplify_results)
    if simplify_total > 0:
        print(f"\n📌 SIMPLIFY:")
        print(
            f"   Success: {simplify_success}/{simplify_total} ({simplify_success/simplify_total*100:.0f}%)"
        )

        # Method breakdown
        method_counts = defaultdict(int)
        for r in simplify_results:
            if r["success_method"]:
                method_counts[r["success_method"]] += 1

        if method_counts:
            print(f"\n   Success by method:")
            for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
                print(f"      • {method}: {count}")

        # Data availability analysis
        has_json_urls = sum(
            1 for r in simplify_results if r["data_found"].get("url_fields")
        )
        print(f"\n   Data availability:")
        print(
            f"      • Pages with URL fields in JSON: {has_json_urls}/{simplify_total}"
        )

    # Jobright summary
    jobright_success = sum(1 for r in jobright_results if r["final_url"])
    jobright_total = len(jobright_results)
    if jobright_total > 0:
        print(f"\n📌 JOBRIGHT:")
        print(
            f"   Success: {jobright_success}/{jobright_total} ({jobright_success/jobright_total*100:.0f}%)"
        )

        # Analyze failures
        failures = [r for r in jobright_results if not r["final_url"]]
        if failures:
            print(f"\n   Failure analysis:")
            failure_reasons = defaultdict(int)
            for f in failures:
                if f["methods_attempted"]:
                    reason = f["methods_attempted"][0].get("reason", "Unknown")
                    failure_reasons[reason] += 1

            for reason, count in sorted(failure_reasons.items(), key=lambda x: -x[1]):
                print(f"      • {reason}: {count}")

        # Check what fields were found
        fields_found_count = defaultdict(int)
        for r in jobright_results:
            if "json_fields" in r["data_found"]:
                for field in r["data_found"]["json_fields"].keys():
                    fields_found_count[field] += 1

        if fields_found_count:
            print(f"\n   URL fields discovered:")
            for field, count in sorted(fields_found_count.items(), key=lambda x: -x[1]):
                print(f"      • {field}: found in {count}/{jobright_total} pages")

    # Save analysis
    logger.save_analysis()

    print("\n" + "=" * 100)
    print("KEY INSIGHTS")
    print("=" * 100)

    print(f"\n📋 Check {DEBUG_LOG_FILE} for:")
    print("   • Full __NEXT_DATA__ JSON structure (Simplify)")
    print("   • All URL fields found in pages")
    print("   • Apply button attributes (href, onclick, data-*)")
    print("   • Exact failure reasons for each method")

    print(f"\n📋 Check {ANALYSIS_JSON_FILE} for:")
    print("   • Structured data for programmatic analysis")
    print("   • Which fields appear most frequently")
    print("   • Pattern recognition for building better extractors")

    print("\n" + "=" * 100)


if __name__ == "__main__":
    try:
        run_comprehensive_test()
    except KeyboardInterrupt:
        print("\n\n⚠️ Test interrupted")
    except Exception as e:
        print(f"\n\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
