#!/usr/bin/env python3

import json
import time
import random
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


class HandshakePlaywrightScraper:
    def __init__(self, config_path="handshake_config.json"):
        self.config = self._load_config(config_path)
        self.browser_data_dir = Path.home() / ".handshake_browser_data"
        self.jobs = []

    def _load_config(self, config_path):
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {
                "search_url": "https://app.joinhandshake.com/stu/postings?page=1&per_page=25",
                "max_jobs_per_page": 25,
            }

    def _human_delay(self, min_sec=1.5, max_sec=3.5):
        time.sleep(random.uniform(min_sec, max_sec))

    def _slow_scroll(self, page):
        try:
            total_height = page.evaluate("document.body.scrollHeight")
            current_position = 0
            while current_position < total_height:
                scroll_distance = random.randint(200, 400)
                current_position += scroll_distance
                page.evaluate(
                    f"window.scrollTo({{top: {current_position}, behavior: 'smooth'}})"
                )
                time.sleep(random.uniform(0.3, 0.8))
                total_height = page.evaluate("document.body.scrollHeight")
        except:
            pass

    def scrape_jobs(self, max_jobs=25):
        search_url = self.config["search_url"]

        if "/job-search/" in search_url:
            if "?" in search_url:
                query_params = search_url.split("?", 1)[1]
                search_url = (
                    f"https://app.joinhandshake.com/stu/postings?{query_params}"
                )
                self.config["search_url"] = search_url
            else:
                return []

        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(self.browser_data_dir),
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            )

            page = context.pages[0] if context.pages else context.new_page()

            try:
                page.goto(search_url, wait_until="networkidle", timeout=30000)
                self._human_delay(2, 4)
            except PlaywrightTimeout:
                pass

            self._human_delay(2, 3)

            if self._is_login_page(page):
                print("\n" + "=" * 80)
                print("HANDSHAKE LOGIN REQUIRED")
                print("=" * 80)
                input("Press ENTER after logging in: ")
                self._human_delay(1, 2)

            self._slow_scroll(page)

            try:
                page.wait_for_selector('[data-hook="search-result"]', timeout=10000)
            except PlaywrightTimeout:
                try:
                    page.wait_for_selector(".posting-card", timeout=5000)
                except PlaywrightTimeout:
                    context.close()
                    return []

            jobs = self._extract_jobs_from_page(page)
            self._human_delay(1, 2)
            context.close()

            return jobs[:max_jobs]

    def _is_login_page(self, page):
        url = page.url
        title = page.title().lower()

        if "login" in url.lower() or "signin" in url.lower():
            return True
        if "sign in" in title or "log in" in title:
            return True

        try:
            login_form = page.locator(
                'form[action*="login"], input[type="email"], input[type="password"]'
            ).count()
            if login_form > 0:
                return True
        except:
            pass

        return False

    def _extract_jobs_from_page(self, page):
        jobs = []

        selectors = [
            '[data-hook="search-result"]',
            ".posting-card",
            '[class*="PostingCard"]',
            'a[href*="/postings/"]',
            '[data-testid="posting-card"]',
            'div[role="listitem"]',
            ".job-card",
            '[class*="JobCard"]',
        ]

        job_elements = None
        for selector in selectors:
            try:
                count = page.locator(selector).count()
                if count > 0:
                    job_elements = page.locator(selector)
                    break
            except:
                continue

        if not job_elements:
            return []

        count = job_elements.count()

        for i in range(count):
            try:
                element = job_elements.nth(i)

                title = None
                for sel in ["h3", ".posting-name", '[data-hook="posting-name"]', "a"]:
                    try:
                        title = element.locator(sel).first.inner_text(timeout=1000)
                        if title:
                            break
                    except:
                        continue

                company = None
                for sel in [
                    ".company-name",
                    '[data-hook="company-name"]',
                    ".employer-name",
                ]:
                    try:
                        company = element.locator(sel).first.inner_text(timeout=1000)
                        if company:
                            break
                    except:
                        continue

                url = None
                try:
                    href = element.locator("a").first.get_attribute(
                        "href", timeout=1000
                    )
                    if href:
                        url = (
                            href
                            if href.startswith("http")
                            else f"https://app.joinhandshake.com{href}"
                        )
                except:
                    pass

                location = None
                for sel in [
                    ".location",
                    '[data-hook="posting-location"]',
                    ".job-location",
                ]:
                    try:
                        location = element.locator(sel).first.inner_text(timeout=1000)
                        if location:
                            break
                    except:
                        continue

                posted_date = None
                for sel in [".posted-date", '[data-hook="posted-date"]', "time"]:
                    try:
                        posted_date = element.locator(sel).first.inner_text(
                            timeout=1000
                        )
                        if posted_date:
                            break
                    except:
                        continue

                if title and url:
                    jobs.append(
                        {
                            "title": title.strip(),
                            "company": company.strip() if company else "Unknown",
                            "url": url.strip(),
                            "location": location.strip() if location else "Unknown",
                            "posted_date": (
                                posted_date.strip() if posted_date else "Unknown"
                            ),
                        }
                    )
            except:
                continue

        return jobs
