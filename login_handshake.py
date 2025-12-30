#!/usr/bin/env python3
"""
Handshake Authentication Script
Run this manually whenever your Handshake session expires (every ~7 days)

Usage:
    python3 login_handshake.py

This will:
1. Open Chrome browser
2. Navigate to Handshake
3. Wait for you to log in manually
4. Save authentication cookies
5. Close browser

The saved cookies will be used by the main scraper.
"""

import json
import time
from pathlib import Path

try:
    import undetected_chromedriver as uc

    UNDETECTED_AVAILABLE = True
except ImportError:
    print("⚠️  undetected-chromedriver not found")
    print("   Install with: pip install undetected-chromedriver")
    UNDETECTED_AVAILABLE = False
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager


COOKIES_FILE = "handshake_cookies.json"
HANDSHAKE_URL = "https://app.joinhandshake.com"


def login_to_handshake():
    """Interactive Handshake login to capture authentication cookies."""

    print("\n" + "=" * 70)
    print("HANDSHAKE AUTHENTICATION")
    print("=" * 70)
    print()

    driver = None

    try:
        print("[1/6] Starting Chrome browser...")

        if UNDETECTED_AVAILABLE:
            # Use undetected-chromedriver for maximum stealth
            options = uc.ChromeOptions()
            options.add_argument("--disable-blink-features=AutomationControlled")
            driver = uc.Chrome(options=options, use_subprocess=True)
        else:
            # Fallback to regular Selenium
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option(
                "excludeSwitches", ["enable-logging"]
            )
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)

        print("    ✓ Browser opened")

        print("\n[2/6] Navigating to Handshake...")
        driver.get(HANDSHAKE_URL)
        time.sleep(3)

        print("    ✓ Loaded Handshake login page")

        print("\n[3/6] MANUAL LOGIN REQUIRED:")
        print("    → Log in through the browser window")
        print("    → Complete authentication (SSO, password, 2FA, etc.)")
        print("    → Wait until you see your Handshake dashboard")
        print("    → Then press ENTER in this terminal...")
        print()

        input("Press ENTER after you've logged in successfully: ")

        print("\n[4/6] Verifying login...")

        # Check if we're actually logged in
        current_url = driver.current_url
        if "login" in current_url.lower() or "sign" in current_url.lower():
            print("    ✗ ERROR: Still on login page")
            print("    Please make sure you're fully logged in")
            driver.quit()
            return False

        print("    ✓ Login verified")

        print("\n[5/6] Extracting authentication cookies...")
        cookies = driver.get_cookies()

        if not cookies:
            print("    ✗ ERROR: No cookies found")
            print("    Login may have failed. Please try again.")
            driver.quit()
            return False

        print(f"    ✓ Captured {len(cookies)} cookies")

        print("\n[6/6] Saving cookies to file...")
        with open(COOKIES_FILE, "w") as f:
            json.dump(cookies, f, indent=2)

        print(f"    ✓ Cookies saved to: {COOKIES_FILE}")

        print("\n" + "=" * 70)
        print("✅ AUTHENTICATION SUCCESSFUL!")
        print("=" * 70)
        print()
        print("Next steps:")
        print("  1. Run your job aggregator: python3 job_aggregator.py")
        print("  2. Handshake will be scraped automatically")
        print("  3. Re-run this script when cookies expire (~7 days)")
        print()
        print("=" * 70 + "\n")

        driver.quit()
        return True

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        print("  Authentication failed")
        print("  Please try again\n")

        if driver:
            try:
                driver.quit()
            except:
                pass

        return False


if __name__ == "__main__":
    success = login_to_handshake()
    exit(0 if success else 1)
