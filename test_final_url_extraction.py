#!/usr/bin/env python3

import re
import json
import pickle
import requests
import time
from collections import defaultdict
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
GMAIL_CREDS_FILE = "gmail_credentials.json"
GMAIL_TOKEN_FILE = "gmail_token.pickle"
TEST_DAYS = 1
MAX_EMAILS = 3
MAX_URLS_PER_SOURCE = 30

LOG_FILE = "final_extraction_log.txt"


class Logger:
    def __init__(self):
        self.log_file = open(LOG_FILE, "w")
        self._write(f"{'='*120}")
        self._write(
            f"FINAL URL EXTRACTION TEST - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self._write(f"{'='*120}\n")

    def log(self, msg, also_print=False):
        self._write(msg)
        if also_print:
            print(msg)

    def _write(self, msg):
        self.log_file.write(msg + "\n")
        self.log_file.flush()

    def close(self):
        self.log_file.close()


class GmailFetcher:
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

    def fetch_job_hunt_emails(self, max_emails=10):
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
            for msg in messages[:max_emails]:
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
                            "date": headers.get("Date", ""),
                        }
                    )

            return emails

        except Exception as e:
            print(f"❌ Error: {e}")
            return []

    def _get_body(self, payload):
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


class SimplifyExtractor:
    def __init__(self, logger):
        self.logger = logger

    def extract(self, url):
        self.logger.log(f"\n{'='*100}", also_print=False)
        self.logger.log(f"SIMPLIFY EXTRACTION", also_print=False)
        self.logger.log(f"Input URL: {url}", also_print=False)

        result = self._method_1_json_redirect(url)
        if result:
            return result, "M1:JSON+Redirect"

        result = self._method_2_http_redirect(url)
        if result:
            return result, "M2:HTTP_Redirect"

        self.logger.log("All methods failed", also_print=False)
        return None, "Failed"

    def _method_1_json_redirect(self, url):
        try:
            self.logger.log(
                "\n--- Method 1: JSON url field + redirect ---", also_print=False
            )

            response = requests.get(url, timeout=10)
            self.logger.log(
                f"HTTP response: {len(response.text)} bytes", also_print=False
            )

            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
                response.text,
                re.DOTALL,
            )
            if not match:
                self.logger.log("No __NEXT_DATA__ found", also_print=False)
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
                    self.logger.log(f"Found click URL: {click_url}", also_print=False)

                    self.logger.log("Following redirect...", also_print=False)
                    redirect_response = requests.get(
                        click_url, allow_redirects=True, timeout=10
                    )
                    final_url = redirect_response.url

                    self.logger.log(
                        f"Redirect final URL: {final_url}", also_print=False
                    )

                    if "simplify.jobs" not in final_url.lower():
                        self.logger.log(f"✓ SUCCESS via Method 1", also_print=False)
                        return final_url
                    else:
                        self.logger.log("Redirect stayed on Simplify", also_print=False)

            return None
        except Exception as e:
            self.logger.log(f"Method 1 error: {e}", also_print=False)
            return None

    def _method_2_http_redirect(self, url):
        try:
            self.logger.log(
                "\n--- Method 2: Direct HTTP redirect ---", also_print=False
            )

            response = requests.get(url, allow_redirects=True, timeout=10)
            final_url = response.url

            self.logger.log(f"Redirect final URL: {final_url}", also_print=False)

            if "simplify.jobs" not in final_url.lower():
                self.logger.log(f"✓ SUCCESS via Method 2", also_print=False)
                return final_url

            return None
        except Exception as e:
            self.logger.log(f"Method 2 error: {e}", also_print=False)
            return None


class JobrightExtractor:
    def __init__(self, logger):
        self.logger = logger
        self.driver = None
        self._init_driver()

    def _init_driver(self):
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)

            self.driver = webdriver.Chrome(options=options)
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
        except Exception as e:
            self.logger.log(f"Driver init failed: {e}", also_print=True)

    def extract(self, url):
        self.logger.log(f"\n{'='*100}", also_print=False)
        self.logger.log(f"JOBRIGHT EXTRACTION", also_print=False)
        self.logger.log(f"Input URL: {url}", also_print=False)

        result = self._method_1_stealth_http(url)
        if result:
            return result, "M1:HTTP"

        result = self._method_2_selenium_click_button(url)
        if result:
            return result, "M2:Selenium_Click"

        result = self._method_3_selenium_raw_html(url)
        if result:
            return result, "M3:Selenium_Raw"

        result = self._method_4_selenium_scrape(url)
        if result:
            return result, "M4:Selenium_Scrape"

        self.logger.log("All methods failed", also_print=False)
        return None, "Failed"

    def _method_1_stealth_http(self, url):
        try:
            self.logger.log("\n--- Method 1: Stealth HTTP ---", also_print=False)

            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://jobright.ai/",
                "DNT": "1",
                "Connection": "keep-alive",
            }

            cookie_dict = {}
            try:
                with open("jobright_cookies.json", "r") as f:
                    cookies_list = json.load(f)
                    for cookie in cookies_list:
                        cookie_dict[cookie["name"]] = cookie["value"]
                self.logger.log(f"Loaded {len(cookie_dict)} cookies", also_print=False)
            except:
                self.logger.log("No cookies loaded", also_print=False)

            response = requests.get(
                url, headers=headers, cookies=cookie_dict, timeout=10
            )
            html = response.text

            self.logger.log(f"Response: {len(html)} bytes", also_print=False)

            url_patterns = [
                (r'"originalUrl"\s*:\s*"([^"]+)"', "originalUrl"),
                (r'"applyLink"\s*:\s*"([^"]+)"', "applyLink"),
                (r'"jobUrl"\s*:\s*"([^"]+)"', "jobUrl"),
            ]

            for pattern, field_name in url_patterns:
                matches = re.findall(pattern, html)
                self.logger.log(
                    f"Field '{field_name}': {len(matches)} matches", also_print=False
                )

                for match in matches:
                    if (
                        match
                        and "http" in match
                        and "jobright.ai" not in match
                        and not match.startswith("https://www.linkedin.com/")
                        and not match.startswith("https://linkedin.com/")
                    ):
                        self.logger.log(
                            f"✓ Found via '{field_name}': {match[:80]}",
                            also_print=False,
                        )
                        return match

            self.logger.log("No valid URL in JSON", also_print=False)
            return None

        except Exception as e:
            self.logger.log(f"Method 1 error: {e}", also_print=False)
            return None

    def _method_2_selenium_click_button(self, url):
        if not self.driver:
            return None

        try:
            self.logger.log(
                "\n--- Method 2: Selenium Click Original Job Post Button ---",
                also_print=False,
            )

            self.driver.get(url)
            time.sleep(2)

            self._load_cookies()
            self.driver.refresh()
            time.sleep(3)

            self.logger.log(f"Page loaded: {self.driver.title[:50]}", also_print=False)

            button_selectors = [
                "//a[contains(@class, 'index_origin')]",
                "//a[contains(text(), 'Original Job Post')]",
                "//a[contains(text(), 'Original')]",
                "a.index_origin__7NnDG",
                "a[href*='jobs.'][target='_blank']",
            ]

            original_window = self.driver.current_window_handle

            for selector in button_selectors:
                try:
                    self.logger.log(f"Trying selector: {selector}", also_print=False)

                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    self.logger.log(f"Found {len(elements)} elements", also_print=False)

                    if elements:
                        button = elements[0]
                        href = button.get_attribute("href")
                        self.logger.log(
                            f"Button href: {href[:80] if href else 'None'}",
                            also_print=False,
                        )

                        self.logger.log("Clicking button...", also_print=False)
                        button.click()
                        time.sleep(3)

                        all_windows = self.driver.window_handles
                        self.logger.log(
                            f"Windows after click: {len(all_windows)}", also_print=False
                        )

                        if len(all_windows) > 1:
                            for window in all_windows:
                                if window != original_window:
                                    self.driver.switch_to.window(window)
                                    new_url = self.driver.current_url
                                    self.logger.log(
                                        f"New tab URL: {new_url}", also_print=False
                                    )

                                    if (
                                        "jobright.ai" not in new_url
                                        and not new_url.startswith(
                                            "https://www.linkedin.com/"
                                        )
                                    ):
                                        self.logger.log(
                                            f"✓ SUCCESS via Method 2 (new tab)",
                                            also_print=False,
                                        )
                                        self.driver.close()
                                        self.driver.switch_to.window(original_window)
                                        return new_url

                                    self.driver.close()

                            self.driver.switch_to.window(original_window)
                        else:
                            new_url = self.driver.current_url
                            if (
                                new_url != url
                                and "jobright.ai" not in new_url
                                and not new_url.startswith("https://www.linkedin.com/")
                            ):
                                self.logger.log(
                                    f"✓ SUCCESS via Method 2 (navigation)",
                                    also_print=False,
                                )
                                return new_url

                except Exception as e:
                    self.logger.log(
                        f"Selector {selector} failed: {e}", also_print=False
                    )
                    try:
                        self.driver.switch_to.window(original_window)
                    except:
                        pass

            self.logger.log("No button found/clicked", also_print=False)
            return None

        except Exception as e:
            self.logger.log(f"Method 2 error: {e}", also_print=False)
            return None

    def _method_3_selenium_raw_html(self, url):
        if not self.driver:
            return None

        try:
            self.logger.log("\n--- Method 3: Selenium Raw HTML ---", also_print=False)

            try:
                raw_html = self.driver.execute_script(
                    "return document.documentElement.outerHTML"
                )
            except:
                self.logger.log(
                    "Already on page from Method 2, extracting HTML", also_print=False
                )
                self.driver.get(url)
                time.sleep(2)
                raw_html = self.driver.execute_script(
                    "return document.documentElement.outerHTML"
                )

            self.logger.log(f"Raw HTML: {len(raw_html)} bytes", also_print=False)

            url_patterns = [
                (r'"originalUrl"\s*:\s*"([^"]+)"', "originalUrl"),
                (r'"applyLink"\s*:\s*"([^"]+)"', "applyLink"),
            ]

            for pattern, field_name in url_patterns:
                matches = re.findall(pattern, raw_html)
                for match in matches:
                    if (
                        match
                        and "http" in match
                        and "jobright.ai" not in match
                        and not match.startswith("https://www.linkedin.com/")
                    ):
                        self.logger.log(
                            f"✓ SUCCESS via Method 3 '{field_name}'", also_print=False
                        )
                        return match

            return None
        except Exception as e:
            self.logger.log(f"Method 3 error: {e}", also_print=False)
            return None

    def _method_4_selenium_scrape(self, url):
        if not self.driver:
            return None

        try:
            self.logger.log(
                "\n--- Method 4: Selenium Link Scraping ---", also_print=False
            )

            selectors = [
                'a[href*="workday"]',
                'a[href*="greenhouse"]',
                'a[href*="lever"]',
                'a[href*="icims"]',
                'a[href*="careers"]',
            ]

            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements[:3]:
                        href = element.get_attribute("href")
                        if (
                            href
                            and "jobright.ai" not in href
                            and "http" in href
                            and not href.startswith("https://www.linkedin.com/")
                        ):
                            self.logger.log(
                                f"✓ SUCCESS via Method 4 selector {selector}",
                                also_print=False,
                            )
                            return href
                except:
                    continue

            return None
        except Exception as e:
            self.logger.log(f"Method 4 error: {e}", also_print=False)
            return None

    def _load_cookies(self):
        try:
            with open("jobright_cookies.json", "r") as f:
                cookies = json.load(f)
                for cookie in cookies:
                    if "sameSite" in cookie and cookie["sameSite"] not in [
                        "Strict",
                        "Lax",
                        "None",
                    ]:
                        cookie["sameSite"] = "Lax"
                    self.driver.add_cookie(cookie)
            self.logger.log("Cookies loaded", also_print=False)
        except Exception as e:
            self.logger.log(f"Cookie loading failed: {e}", also_print=False)

    def close(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass


def run_test():
    logger = Logger()
    print("=" * 100)
    print("FINAL COMPREHENSIVE URL EXTRACTION TEST")
    print("=" * 100)

    logger.log("=" * 100, also_print=True)
    logger.log("FETCHING EMAILS", also_print=True)
    logger.log("=" * 100, also_print=True)

    fetcher = GmailFetcher()
    emails = fetcher.fetch_job_hunt_emails(max_emails=MAX_EMAILS)

    if not emails:
        print("❌ No emails found")
        logger.close()
        return

    print(f"\n✓ Found {len(emails)} emails")
    logger.log(f"\nFound {len(emails)} emails", also_print=False)

    for idx, email in enumerate(emails, 1):
        logger.log(
            f"\nEmail {idx}: {email['sender']} - {email['subject'][:50]} - {email['date']}",
            also_print=False,
        )

    simplify_urls = []
    jobright_urls = []

    for email in emails:
        if email["sender"] == "SWE List":
            simplify_urls.extend(email["urls"][:MAX_URLS_PER_SOURCE])
        elif email["sender"] == "Jobright":
            jobright_urls.extend(email["urls"][:MAX_URLS_PER_SOURCE])

    print(f"\n📊 URLs extracted:")
    print(
        f"   Simplify: {len(simplify_urls)} (testing {min(len(simplify_urls), MAX_URLS_PER_SOURCE)})"
    )
    print(
        f"   Jobright: {len(jobright_urls)} (testing {min(len(jobright_urls), MAX_URLS_PER_SOURCE)})"
    )

    logger.log(f"\n📊 URL counts:", also_print=False)
    logger.log(f"Simplify: {len(simplify_urls)}", also_print=False)
    logger.log(f"Jobright: {len(jobright_urls)}", also_print=False)

    print("\n" + "=" * 100)
    print("SIMPLIFY EXTRACTION")
    print("=" * 100)

    logger.log(f"\n{'='*100}", also_print=False)
    logger.log("SIMPLIFY EXTRACTION RESULTS", also_print=False)
    logger.log(f"{'='*100}", also_print=False)

    simplify_extractor = SimplifyExtractor(logger)
    simplify_results = {
        "success": 0,
        "failed": 0,
        "methods": defaultdict(int),
        "urls": [],
    }

    for idx, url in enumerate(simplify_urls[:MAX_URLS_PER_SOURCE], 1):
        final_url, method = simplify_extractor.extract(url)

        if final_url:
            simplify_results["success"] += 1
            simplify_results["methods"][method] += 1
            simplify_results["urls"].append(final_url)

            try:
                domain = final_url.split("//")[1].split("/")[0]
                company = domain.split(".")[0].replace("-", " ").title()[:25]
            except:
                company = "Unknown"

            print(f"  {idx:2d}. ✅ {company:25s} | {method}")
            print(f"       → {final_url[:90]}")

            logger.log(f"\n[{idx}] ✅ SUCCESS", also_print=False)
            logger.log(f"Method: {method}", also_print=False)
            logger.log(f"Final URL: {final_url}", also_print=False)
        else:
            simplify_results["failed"] += 1
            print(f"  {idx:2d}. ❌ Failed | {method}")

            logger.log(f"\n[{idx}] ❌ FAILED", also_print=False)
            logger.log(f"Reason: {method}", also_print=False)
            logger.log(f"Input URL: {url}", also_print=False)

    print("\n" + "=" * 100)
    print("JOBRIGHT EXTRACTION")
    print("=" * 100)

    logger.log(f"\n{'='*100}", also_print=False)
    logger.log("JOBRIGHT EXTRACTION RESULTS", also_print=False)
    logger.log(f"{'='*100}", also_print=False)

    jobright_extractor = JobrightExtractor(logger)
    jobright_results = {
        "success": 0,
        "failed": 0,
        "methods": defaultdict(int),
        "urls": [],
    }

    for idx, url in enumerate(jobright_urls[:MAX_URLS_PER_SOURCE], 1):
        final_url, method = jobright_extractor.extract(url)

        if final_url:
            jobright_results["success"] += 1
            jobright_results["methods"][method] += 1
            jobright_results["urls"].append(final_url)

            try:
                domain = final_url.split("//")[1].split("/")[0]
                company = domain.split(".")[0].replace("-", " ").title()[:25]
            except:
                company = "Unknown"

            print(f"  {idx:2d}. ✅ {company:25s} | {method}")
            print(f"       → {final_url[:90]}")

            logger.log(f"\n[{idx}] ✅ SUCCESS", also_print=False)
            logger.log(f"Method: {method}", also_print=False)
            logger.log(f"Final URL: {final_url}", also_print=False)
        else:
            jobright_results["failed"] += 1
            print(f"  {idx:2d}. ❌ Failed | {method}")

            logger.log(f"\n[{idx}] ❌ FAILED", also_print=False)
            logger.log(f"Reason: {method}", also_print=False)
            logger.log(f"Input URL: {url}", also_print=False)

    jobright_extractor.close()

    print("\n" + "=" * 100)
    print("FINAL RESULTS")
    print("=" * 100)

    logger.log(f"\n{'='*100}", also_print=False)
    logger.log("FINAL RESULTS", also_print=False)
    logger.log(f"{'='*100}", also_print=False)

    simplify_total = simplify_results["success"] + simplify_results["failed"]
    if simplify_total > 0:
        rate = (simplify_results["success"] / simplify_total) * 100
        print(f"\n🎯 SIMPLIFY:")
        print(
            f"   Success: {simplify_results['success']}/{simplify_total} ({rate:.0f}%)"
        )
        print(f"   Failed: {simplify_results['failed']}")

        logger.log(f"\nSIMPLIFY SUMMARY:", also_print=False)
        logger.log(
            f"Success: {simplify_results['success']}/{simplify_total} ({rate:.1f}%)",
            also_print=False,
        )

        if simplify_results["methods"]:
            print(f"\n   Methods:")
            logger.log(f"\nMethods breakdown:", also_print=False)
            for method, count in sorted(
                simplify_results["methods"].items(), key=lambda x: -x[1]
            ):
                pct = count / simplify_total * 100
                print(f"      • {method:20s}: {count:2d} ({pct:.0f}%)")
                logger.log(f"  {method}: {count} ({pct:.1f}%)", also_print=False)

    jobright_total = jobright_results["success"] + jobright_results["failed"]
    if jobright_total > 0:
        rate = (jobright_results["success"] / jobright_total) * 100
        print(f"\n🎯 JOBRIGHT:")
        print(
            f"   Success: {jobright_results['success']}/{jobright_total} ({rate:.0f}%)"
        )
        print(f"   Failed: {jobright_results['failed']}")

        logger.log(f"\nJOBRIGHT SUMMARY:", also_print=False)
        logger.log(
            f"Success: {jobright_results['success']}/{jobright_total} ({rate:.1f}%)",
            also_print=False,
        )

        if jobright_results["methods"]:
            print(f"\n   Methods:")
            logger.log(f"\nMethods breakdown:", also_print=False)
            for method, count in sorted(
                jobright_results["methods"].items(), key=lambda x: -x[1]
            ):
                if count > 0:
                    pct = count / jobright_total * 100
                    print(f"      • {method:20s}: {count:2d} ({pct:.0f}%)")
                    logger.log(f"  {method}: {count} ({pct:.1f}%)", also_print=False)

    print("\n" + "=" * 100)
    print("VERDICT")
    print("=" * 100)

    if simplify_total > 0:
        if rate >= 90:
            verdict = "✅ EXCELLENT - Ready for production"
        elif rate >= 70:
            verdict = "✅ GOOD - Minor improvements possible"
        else:
            verdict = "⚠️ NEEDS WORK"
        print(f"\nSimplify: {verdict}")

    if jobright_total > 0:
        rate = (jobright_results["success"] / jobright_total) * 100
        if rate >= 80:
            verdict = "✅ EXCELLENT - Ready for production"
        elif rate >= 60:
            verdict = "✅ GOOD - Usable but can improve"
        elif rate >= 40:
            verdict = "⚠️ PARTIAL - Needs Selenium fallbacks"
        else:
            verdict = "❌ BROKEN - Major issues"
        print(f"Jobright: {verdict}")

    print(f"\n📝 Detailed logs saved to: {LOG_FILE}")
    print("=" * 100)

    logger.log(f"\n{'='*100}", also_print=False)
    logger.log("TEST COMPLETE", also_print=False)
    logger.log(f"{'='*100}", also_print=False)
    logger.close()


if __name__ == "__main__":
    try:
        run_test()
    except KeyboardInterrupt:
        print("\n\n⚠️ Test interrupted")
    except Exception as e:
        print(f"\n\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
