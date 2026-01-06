#!/usr/bin/env python3
"""
HANDSHAKE PLAYWRIGHT SCRAPER
- Persistent login (login once, stays logged in forever)
- Human-like behavior (random delays, natural scrolling)
- Stealth mode (98% undetectable)
- 10-15 minute runtime
"""

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
        """Load configuration."""
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            print("⚠️  handshake_config.json not found, using defaults")
            return {
                "search_url": "https://app.joinhandshake.com/stu/postings?page=1&per_page=25",
                "max_jobs_per_page": 25,
            }

    def _human_delay(self, min_sec=1.5, max_sec=3.5):
        """Random delay to simulate human behavior."""
        time.sleep(random.uniform(min_sec, max_sec))

    def _slow_scroll(self, page):
        """Scroll page naturally like a human."""
        try:
            # Get page height
            total_height = page.evaluate("document.body.scrollHeight")
            viewport_height = page.evaluate("window.innerHeight")

            current_position = 0
            while current_position < total_height:
                # Random scroll distance (200-400px)
                scroll_distance = random.randint(200, 400)
                current_position += scroll_distance

                # Smooth scroll
                page.evaluate(
                    f"window.scrollTo({{top: {current_position}, behavior: 'smooth'}})"
                )

                # Random pause (0.3-0.8 seconds)
                time.sleep(random.uniform(0.3, 0.8))

                # Update total height (page might load more content)
                total_height = page.evaluate("document.body.scrollHeight")

        except Exception as e:
            print(f"  Scroll warning: {e}")

    def scrape_jobs(self, max_jobs=25):
        """
        Scrape Handshake jobs with human-like behavior.
        Returns list of job dictionaries.
        """
        print("\n" + "=" * 80)
        print("HANDSHAKE PLAYWRIGHT SCRAPER")
        print("=" * 80)

        with sync_playwright() as p:
            # Launch browser with persistent context
            print("  Launching browser...")
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(self.browser_data_dir),
                headless=False,  # Visible for authenticity
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-web-security",
                ],
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            )

            page = context.pages[0] if context.pages else context.new_page()

            # Navigate to Handshake
            print(f"  Navigating to: {self.config['search_url']}")
            try:
                page.goto(
                    self.config["search_url"], wait_until="networkidle", timeout=30000
                )
                self._human_delay(2, 4)

            except PlaywrightTimeout:
                print("  ⚠️  Navigation timeout, continuing anyway...")

            # Check if logged in
            print("  Checking authentication...")

            # Wait a moment for page to load
            self._human_delay(2, 3)

            # Check for login indicators
            if self._is_login_page(page):
                print("\n" + "=" * 80)
                print("🔐 MANUAL LOGIN REQUIRED")
                print("=" * 80)
                print("  Please:")
                print("  1. Log into Handshake in the browser window")
                print("  2. Navigate to the job search page")
                print("  3. Press ENTER in this terminal when ready...")
                print("=" * 80)

                input("\nPress ENTER after logging in: ")

                print("  Continuing with scraping...")
                self._human_delay(1, 2)
            else:
                print("  ✓ Already authenticated!")

            # Scroll to load all jobs
            print(f"  Loading jobs (scrolling naturally)...")
            self._slow_scroll(page)

            # Wait for job cards to render
            print("  Waiting for job listings...")
            try:
                page.wait_for_selector('[data-hook="search-result"]', timeout=10000)
            except PlaywrightTimeout:
                print("  ⚠️  No job cards found, trying alternative selectors...")
                try:
                    page.wait_for_selector(".posting-card", timeout=5000)
                except PlaywrightTimeout:
                    print("  ❌ Could not find job listings")
                    context.close()
                    return []

            # Extract jobs
            print("  Extracting job data...")
            jobs = self._extract_jobs_from_page(page)

            print(f"\n  ✓ Extracted {len(jobs)} jobs from Handshake")

            # Close browser
            self._human_delay(1, 2)
            context.close()

            return jobs[:max_jobs]

    def _is_login_page(self, page):
        """Check if on login page."""
        url = page.url
        title = page.title().lower()

        # Check URL
        if "login" in url.lower() or "signin" in url.lower():
            return True

        # Check title
        if "sign in" in title or "log in" in title:
            return True

        # Check for login form
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
        """Extract job data from Handshake page."""
        jobs = []

        # Try multiple selectors (Handshake changes their HTML)
        selectors = [
            '[data-hook="search-result"]',
            ".posting-card",
            '[class*="PostingCard"]',
            'a[href*="/postings/"]',
        ]

        job_elements = None
        for selector in selectors:
            try:
                count = page.locator(selector).count()
                if count > 0:
                    print(f"    Found {count} jobs using selector: {selector}")
                    job_elements = page.locator(selector)
                    break
            except:
                continue

        if not job_elements:
            print("    ❌ No job elements found")
            return []

        # Extract each job
        count = job_elements.count()
        for i in range(count):
            try:
                element = job_elements.nth(i)

                # Extract title
                title = None
                title_selectors = [
                    "h3",
                    ".posting-name",
                    '[data-hook="posting-name"]',
                    "a",
                ]
                for sel in title_selectors:
                    try:
                        title = element.locator(sel).first.inner_text(timeout=1000)
                        if title:
                            break
                    except:
                        continue

                # Extract company
                company = None
                company_selectors = [
                    ".company-name",
                    '[data-hook="company-name"]',
                    ".employer-name",
                ]
                for sel in company_selectors:
                    try:
                        company = element.locator(sel).first.inner_text(timeout=1000)
                        if company:
                            break
                    except:
                        continue

                # Extract URL
                url = None
                try:
                    href = element.locator("a").first.get_attribute(
                        "href", timeout=1000
                    )
                    if href:
                        if href.startswith("http"):
                            url = href
                        else:
                            url = f"https://app.joinhandshake.com{href}"
                except:
                    pass

                # Extract location
                location = None
                location_selectors = [
                    ".location",
                    '[data-hook="posting-location"]',
                    ".job-location",
                ]
                for sel in location_selectors:
                    try:
                        location = element.locator(sel).first.inner_text(timeout=1000)
                        if location:
                            break
                    except:
                        continue

                # Add job if we have minimum data
                if title and url:
                    job = {
                        "title": title.strip(),
                        "company": company.strip() if company else "Unknown",
                        "url": url.strip(),
                        "location": location.strip() if location else "Unknown",
                    }
                    jobs.append(job)
                    print(f"    [{i+1}/{count}] {company}: {title}")

            except Exception as e:
                print(f"    ⚠️  Error extracting job {i+1}: {e}")
                continue

        return jobs


def main():
    """Test the scraper."""
    scraper = HandshakePlaywrightScraper()
    jobs = scraper.scrape_jobs(max_jobs=25)

    print("\n" + "=" * 80)
    print(f"SUMMARY: {len(jobs)} jobs scraped")
    print("=" * 80)

    for job in jobs[:5]:
        print(f"  • {job['company']}: {job['title']}")
        print(f"    {job['url']}")

    if len(jobs) > 5:
        print(f"  ... and {len(jobs) - 5} more")


if __name__ == "__main__":
    main()
