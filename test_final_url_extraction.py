#!/usr/bin/env python3

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

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
GMAIL_CREDS_FILE = "gmail_credentials.json"
GMAIL_TOKEN_FILE = "gmail_token.pickle"
TEST_DAYS = 3
MAX_SIMPLIFY_TEST = 30
MAX_JOBRIGHT_TEST = 30


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

    def fetch_job_hunt_emails(self, max_emails=50):
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
    @staticmethod
    def extract_final_url(simplify_url):
        try:
            response = requests.get(simplify_url, timeout=10)
            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
                response.text,
                re.DOTALL,
            )

            if not match:
                return None, "No JSON"

            data = json.loads(match.group(1))

            if (
                "props" in data
                and "pageProps" in data["props"]
                and "jobPosting" in data["props"]["pageProps"]
            ):
                jobPosting = data["props"]["pageProps"]["jobPosting"]

                if "url" in jobPosting:
                    click_url = jobPosting["url"]

                    redirect_response = requests.get(
                        click_url, allow_redirects=True, timeout=10
                    )
                    final_url = redirect_response.url

                    if "simplify.jobs" not in final_url.lower():
                        return final_url, "Redirect"
                    else:
                        return None, "Redirect stayed on Simplify"

            return None, "No url field"

        except Exception as e:
            return None, f"Error: {str(e)[:30]}"


class JobrightExtractor:
    @staticmethod
    def extract_final_url(jobright_url):
        try:
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
            except:
                pass

            response = requests.get(
                jobright_url, headers=headers, cookies=cookie_dict, timeout=10
            )
            html = response.text

            url_patterns = [
                (r'"originalUrl"\s*:\s*"([^"]+)"', "originalUrl"),
                (r'"applyLink"\s*:\s*"([^"]+)"', "applyLink"),
                (r'"jobUrl"\s*:\s*"([^"]+)"', "jobUrl"),
                (r'"canonicalUrl"\s*:\s*"([^"]+)"', "canonicalUrl"),
            ]

            for pattern, field_name in url_patterns:
                matches = re.findall(pattern, html)
                for match in matches:
                    if match and "http" in match and "jobright.ai" not in match:
                        if not match.startswith(
                            "https://www.linkedin.com/"
                        ) and not match.startswith("https://linkedin.com/"):
                            return match, field_name

            return None, "No URL"

        except Exception as e:
            return None, f"Error: {str(e)[:30]}"


def run_test():
    print("=" * 100)
    print("FINAL URL EXTRACTION TEST")
    print("=" * 100)

    print(f"\n📧 Fetching emails (last {TEST_DAYS} days)...")
    fetcher = GmailFetcher()
    emails = fetcher.fetch_job_hunt_emails(max_emails=50)

    if not emails:
        print("❌ No emails")
        return

    print(f"✓ Found {len(emails)} emails\n")

    simplify_urls = []
    jobright_urls = []

    for email in emails:
        if email["sender"] == "SWE List":
            simplify_urls.extend(email["urls"])
        elif email["sender"] == "Jobright":
            jobright_urls.extend(email["urls"])

    print(f"📊 URLs: Simplify: {len(simplify_urls)}, Jobright: {len(jobright_urls)}\n")

    print("=" * 100)
    print(f"SIMPLIFY ({min(len(simplify_urls), MAX_SIMPLIFY_TEST)} URLs)")
    print("=" * 100)

    simplify_success = 0
    simplify_failed = 0

    for idx, url in enumerate(simplify_urls[:MAX_SIMPLIFY_TEST], 1):
        final_url, method = SimplifyExtractor.extract_final_url(url)

        if final_url:
            simplify_success += 1
            try:
                domain = final_url.split("//")[1].split("/")[0]
                company = domain.split(".")[0].replace("-", " ").title()[:25]
            except:
                company = "Unknown"

            print(f"  {idx:2d}. ✅ {company:25s}")
            print(f"       → {final_url}")
        else:
            simplify_failed += 1
            print(f"  {idx:2d}. ❌ {method}")

    print("\n" + "=" * 100)
    print(f"JOBRIGHT ({min(len(jobright_urls), MAX_JOBRIGHT_TEST)} URLs)")
    print("=" * 100)

    jobright_success = 0
    jobright_failed = 0

    for idx, url in enumerate(jobright_urls[:MAX_JOBRIGHT_TEST], 1):
        final_url, method = JobrightExtractor.extract_final_url(url)

        if final_url:
            jobright_success += 1
            try:
                domain = final_url.split("//")[1].split("/")[0]
                company = domain.split(".")[0].replace("-", " ").title()[:25]
            except:
                company = "Unknown"

            print(f"  {idx:2d}. ✅ {company:25s}")
            print(f"       → {final_url[:100]}")
        else:
            jobright_failed += 1
            print(f"  {idx:2d}. ❌ {method}")

    print("\n" + "=" * 100)
    print("FINAL RESULTS")
    print("=" * 100)

    simplify_total = simplify_success + simplify_failed
    jobright_total = jobright_success + jobright_failed

    if simplify_total > 0:
        rate = (simplify_success / simplify_total) * 100
        print(f"\n🎯 Simplify: {simplify_success}/{simplify_total} ({rate:.0f}%)")

    if jobright_total > 0:
        rate = (jobright_success / jobright_total) * 100
        print(f"🎯 Jobright: {jobright_success}/{jobright_total} ({rate:.0f}%)")

    print("\n" + "=" * 100)


if __name__ == "__main__":
    try:
        run_test()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted")
    except Exception as e:
        print(f"\n\n❌ Failed: {e}")
        import traceback

        traceback.print_exc()
