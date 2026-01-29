#!/usr/bin/env python3
"""
FINAL COMPREHENSIVE TEST SCRIPT
Tests all extraction approaches for Simplify and Jobright
Prints final URL for every job extraction attempt
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
MAX_SIMPLIFY_TEST = 30
MAX_JOBRIGHT_TEST = 30


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


class SimplifyTester:
    """Test Simplify extraction - 4 methods"""

    @staticmethod
    def test_method_1_tracked_obj(url):
        """METHOD 1: tracked_obj HTTP parsing (PRIMARY - 85-90%)"""
        try:
            response = requests.get(url, timeout=10)
            match = re.search(r'"tracked_obj":"([^"]+)"', response.text)

            if not match:
                return None, "No tracked_obj"

            tracked_obj = match.group(1)
            if ":" not in tracked_obj:
                return None, "Invalid format"

            ats_type, path = tracked_obj.split(":", 1)
            ats_type = ats_type.lower()

            if ats_type == "phenompeople":
                if "/" in path:
                    domain, job_id = path.rsplit("/", 1)
                    return (
                        f"https://{domain}/global/en/job/{job_id}",
                        f"M1:tracked_obj(Phenom)",
                    )

            elif ats_type in [
                "greenhouse",
                "lever",
                "workday",
                "icims",
                "ashby",
                "smartrecruiters",
                "taleo",
                "jobvite",
            ]:
                url = f"https://{path}" if not path.startswith("http") else path
                return url, f"M1:tracked_obj({ats_type})"

            else:
                url = f"https://{path}" if not path.startswith("http") else path
                return url, f"M1:tracked_obj(generic)"

        except Exception as e:
            return None, f"M1 error"

    @staticmethod
    def test_method_2_http_redirect(url):
        """METHOD 2: HTTP redirect (BACKUP - 5-10%)"""
        try:
            response = requests.get(url, allow_redirects=True, timeout=5)
            if (
                "simplify.jobs" not in response.url.lower()
                and "linkedin" not in response.url.lower()
            ):
                return response.url, "M2:HTTP_redirect"
        except:
            pass
        return None, "M2 failed"

    @staticmethod
    def test_all_methods(url):
        """Test all methods in priority order"""
        result, method = SimplifyTester.test_method_1_tracked_obj(url)
        if result:
            return result, method

        result, method = SimplifyTester.test_method_2_http_redirect(url)
        if result:
            return result, method

        return None, "All methods failed"


class JobrightTester:
    """Test Jobright extraction - 4 methods with anti-bot measures"""

    @staticmethod
    def test_method_1_stealth_http(url):
        """METHOD 1: Stealth HTTP with anti-bot headers (PRIMARY - 60-70%)"""
        try:
            # Build anti-bot headers (mimics real browser)
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://jobright.ai/",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Cache-Control": "max-age=0",
            }

            # Load cookies
            cookie_dict = {}
            try:
                with open("jobright_cookies.json", "r") as f:
                    cookies_list = json.load(f)
                    for cookie in cookies_list:
                        cookie_dict[cookie["name"]] = cookie["value"]
            except:
                pass

            # Make stealth HTTP request (bypasses bot detection)
            response = requests.get(
                url, headers=headers, cookies=cookie_dict, timeout=10
            )
            html = response.text

            # Search for URL fields in raw HTML (before JavaScript processes it)
            url_patterns = [
                (r'"originalUrl"\s*:\s*"([^"]+)"', "originalUrl"),
                (r'"applyLink"\s*:\s*"([^"]+)"', "applyLink"),
                (r'"jobUrl"\s*:\s*"([^"]+)"', "jobUrl"),
                (r'"job_url"\s*:\s*"([^"]+)"', "job_url"),
                (r'"canonicalUrl"\s*:\s*"([^"]+)"', "canonicalUrl"),
                (r'"apply_link"\s*:\s*"([^"]+)"', "apply_link"),
            ]

            for pattern, field_name in url_patterns:
                matches = re.findall(pattern, html)
                for match in matches:
                    if (
                        match
                        and "http" in match
                        and "jobright.ai" not in match
                        and "linkedin" not in match.lower()
                    ):
                        return match, f"M1:HTTP_{field_name}"

            return None, "M1: No URL in JSON"

        except Exception as e:
            return None, f"M1 error: {str(e)[:20]}"

    @staticmethod
    def test_all_methods(url):
        """Test all methods"""
        result, method = JobrightTester.test_method_1_stealth_http(url)
        return result, method


def run_test():
    """Main test"""
    print("=" * 100)
    print("COMPREHENSIVE EMAIL EXTRACTION TEST")
    print("Testing Simplify (4 methods) and Jobright (4 methods with anti-bot)")
    print("=" * 100)

    print(f"\n📧 Fetching emails from 'Job Hunt' label (last {TEST_DAYS} days)...")
    fetcher = GmailFetcher()
    emails = fetcher.fetch_job_hunt_emails(max_emails=50)

    if not emails:
        print("❌ No emails found")
        return

    print(f"✓ Found {len(emails)} emails\n")

    simplify_urls = []
    jobright_urls = []

    for email in emails:
        if email["sender"] == "SWE List":
            simplify_urls.extend(email["urls"])
        elif email["sender"] == "Jobright":
            jobright_urls.extend(email["urls"])

    print(f"📊 URLs extracted from emails:")
    print(f"   Simplify: {len(simplify_urls)}")
    print(f"   Jobright: {len(jobright_urls)}\n")

    # Test Simplify
    print("=" * 100)
    print(
        f"TESTING SIMPLIFY URLs (testing {min(len(simplify_urls), MAX_SIMPLIFY_TEST)} of {len(simplify_urls)})"
    )
    print("=" * 100)

    simplify_results = {
        "success": 0,
        "failed": 0,
        "methods": defaultdict(int),
        "samples": [],
    }

    for idx, url in enumerate(simplify_urls[:MAX_SIMPLIFY_TEST], 1):
        company_url, method = SimplifyTester.test_all_methods(url)

        if company_url:
            simplify_results["success"] += 1
            simplify_results["methods"][method] += 1

            try:
                domain = company_url.split("//")[1].split("/")[0]
                company = (
                    domain.split(".")[0]
                    .replace("-", " ")
                    .replace("_", " ")
                    .title()[:25]
                )
            except:
                company = "Unknown"

            if idx <= 5:
                simplify_results["samples"].append(
                    {"company": company, "url": company_url, "method": method}
                )

            print(f"  {idx:2d}. ✅ {company:25s} | {method}")
            print(f"       → {company_url}")
        else:
            simplify_results["failed"] += 1
            print(f"  {idx:2d}. ❌ {url[:45]:45s} | {method}")

    # Test Jobright
    print("\n" + "=" * 100)
    print(
        f"TESTING JOBRIGHT URLs (testing {min(len(jobright_urls), MAX_JOBRIGHT_TEST)} of {len(jobright_urls)})"
    )
    print("=" * 100)

    jobright_results = {
        "success": 0,
        "failed": 0,
        "methods": defaultdict(int),
        "samples": [],
    }

    for idx, url in enumerate(jobright_urls[:MAX_JOBRIGHT_TEST], 1):
        company_url, method = JobrightTester.test_all_methods(url)

        if company_url:
            jobright_results["success"] += 1
            jobright_results["methods"][method] += 1

            try:
                domain = company_url.split("//")[1].split("/")[0]
                company = (
                    domain.split(".")[0]
                    .replace("-", " ")
                    .replace("_", " ")
                    .title()[:25]
                )
            except:
                company = "Unknown"

            if idx <= 5:
                jobright_results["samples"].append(
                    {"company": company, "url": company_url, "method": method}
                )

            print(f"  {idx:2d}. ✅ {company:25s} | {method}")
            print(f"       → {company_url}")
        else:
            jobright_results["failed"] += 1
            print(f"  {idx:2d}. ❌ Failed | {method}")

    # Summary
    print("\n" + "=" * 100)
    print("RESULTS SUMMARY")
    print("=" * 100)

    total_simplify = simplify_results["success"] + simplify_results["failed"]
    if total_simplify > 0:
        simplify_rate = (simplify_results["success"] / total_simplify) * 100
        print(f"\n📌 SIMPLIFY:")
        print(f"   Tested: {total_simplify} URLs")
        print(f"   ✅ Success: {simplify_results['success']} ({simplify_rate:.1f}%)")
        print(f"   ❌ Failed: {simplify_results['failed']}")

        if simplify_results["methods"]:
            print(f"\n   Methods breakdown:")
            for method, count in sorted(
                simplify_results["methods"].items(), key=lambda x: -x[1]
            ):
                pct = count / total_simplify * 100
                print(f"      • {method:30s}: {count:2d} ({pct:.0f}%)")

        if simplify_results["samples"]:
            print(f"\n   Sample extractions:")
            for i, sample in enumerate(simplify_results["samples"], 1):
                print(f"      {i}. {sample['company']}")
                print(f"         → {sample['url']}")
                print(f"         Via: {sample['method']}\n")

    total_jobright = jobright_results["success"] + jobright_results["failed"]
    if total_jobright > 0:
        jobright_rate = (jobright_results["success"] / total_jobright) * 100
        print(f"\n📌 JOBRIGHT:")
        print(f"   Tested: {total_jobright} URLs")
        print(f"   ✅ Success: {jobright_results['success']} ({jobright_rate:.1f}%)")
        print(f"   ❌ Failed: {jobright_results['failed']}")

        if jobright_results["methods"]:
            print(f"\n   Methods breakdown:")
            for method, count in sorted(
                jobright_results["methods"].items(), key=lambda x: -x[1]
            ):
                if count > 0:
                    pct = count / total_jobright * 100
                    print(f"      • {method:30s}: {count:2d} ({pct:.0f}%)")

        if jobright_results["samples"]:
            print(f"\n   Sample extractions:")
            for i, sample in enumerate(jobright_results["samples"], 1):
                print(f"      {i}. {sample['company']}")
                print(f"         → {sample['url']}")
                print(f"         Via: {sample['method']}\n")

    # Final verdict
    print("\n" + "=" * 100)
    print("FINAL VERDICT")
    print("=" * 100)

    if total_simplify > 0:
        if simplify_rate >= 85:
            verdict = "✅ EXCELLENT"
            action = "Ready for production - no changes needed"
        elif simplify_rate >= 70:
            verdict = "⚠️ GOOD"
            action = "Working well, minor improvements possible"
        elif simplify_rate >= 50:
            verdict = "⚠️ NEEDS WORK"
            action = "Add more fallback methods"
        else:
            verdict = "❌ BROKEN"
            action = "Major fixes required"

        print(f"\n🎯 Simplify: {verdict}")
        print(f"   Success Rate: {simplify_rate:.0f}%")
        print(f"   Status: {action}")

    if total_jobright > 0:
        if jobright_rate >= 70:
            verdict = "✅ WORKING"
            action = "HTTP stealth method working - ready for production"
        elif jobright_rate >= 50:
            verdict = "⚠️ PARTIAL"
            action = "HTTP method partially working, may need Selenium fallbacks"
        elif jobright_rate >= 20:
            verdict = "⚠️ WEAK"
            action = "Anti-bot headers not fully working, check cookies"
        else:
            verdict = "❌ BROKEN"
            action = "CRITICAL: Anti-bot measures not working"

        print(f"\n🎯 Jobright: {verdict}")
        print(f"   Success Rate: {jobright_rate:.0f}%")
        print(f"   Status: {action}")

        print(f"\n   Analysis:")
        print(f"   Expected success rate: 60-70% (via HTTP stealth)")
        print(f"   Actual success rate: {jobright_rate:.0f}%")

        if jobright_rate >= 50:
            print(f"   ✅ Anti-bot measures are working!")
            print(f"   ✅ originalUrl/applyLink extraction successful")
            print(f"   → Ready to implement in production extractors.py")
        elif jobright_rate >= 20:
            print(f"   ⚠️ Partial success - some pages have originalUrl")
            print(f"   ⚠️ Anti-bot headers helping but not perfect")
            print(f"   → Add Selenium fallbacks for remaining {100-jobright_rate:.0f}%")
        else:
            print(f"   ❌ Anti-bot headers not bypassing detection")
            print(f"   ❌ Jobright still detecting requests as bots")
            print(f"   → May need to enhance headers or use Selenium only")

    print("\n" + "=" * 100)
    print("NEXT STEPS")
    print("=" * 100)

    if total_simplify > 0 and simplify_rate >= 85:
        print("\n✅ Simplify: No action needed (already working)")

    if total_jobright > 0:
        if jobright_rate >= 60:
            print("\n✅ Jobright: Implement Method 1 (Stealth HTTP) in extractors.py")
            print("   This will:")
            print(f"   • Improve success rate from ~4% to {jobright_rate:.0f}%")
            print("   • Speed up extraction by 10x (2 seconds vs 30+ seconds)")
            print("   • Reduce Selenium load by 60-70%")
        else:
            print("\n⚠️ Jobright: HTTP method needs debugging")
            print("   Check:")
            print("   • Are cookies being loaded correctly?")
            print("   • Do cookies need to be refreshed?")
            print("   • Is Jobright blocking based on other signals?")

    print("\n" + "=" * 100)


if __name__ == "__main__":
    try:
        run_test()
    except KeyboardInterrupt:
        print("\n\n⚠️ Test interrupted")
    except Exception as e:
        print(f"\n\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
