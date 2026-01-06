#!/usr/bin/env python3
"""
HANDSHAKE DEBUG - Find exact issue
"""

import os
import json
import datetime

print("=" * 80)
print("HANDSHAKE DEBUG DIAGNOSTIC")
print("=" * 80)

# Check 1: Config file
print("\n1️⃣  Checking handshake_config.json...")
try:
    with open("handshake_config.json", "r") as f:
        config = json.load(f)

    search_url = config.get("search_url", "")
    scrape_hours = config.get("scrape_hours", [])

    print(f"   ✓ Config file exists")
    print(f"   URL: {search_url[:80]}...")
    print(f"   Hours: {scrape_hours}")

    if "PASTE_YOUR" in search_url or not search_url.startswith("http"):
        print(f"   ❌ PROBLEM: Search URL not configured properly!")
        print(f"   → Set real Handshake search URL in config")
    else:
        print(f"   ✓ URL looks valid")

except FileNotFoundError:
    print("   ❌ PROBLEM: handshake_config.json not found!")
except Exception as e:
    print(f"   ❌ PROBLEM: {e}")

# Check 2: Cookies file
print("\n2️⃣  Checking handshake_cookies.json...")
try:
    with open("handshake_cookies.json", "r") as f:
        cookies = json.load(f)

    file_stat = os.stat("handshake_cookies.json")
    file_age = datetime.datetime.now() - datetime.datetime.fromtimestamp(
        file_stat.st_mtime
    )

    print(f"   ✓ Cookies file exists")
    print(f"   Cookies count: {len(cookies)}")
    print(f"   File age: {file_age.days} days, {file_age.seconds//3600} hours")

    if file_age.days > 2:
        print(f"   ⚠️  Cookies are {file_age.days} days old - might be expired")
        print(f"   → Run: python3 login_handshake.py")
    else:
        print(f"   ✓ Cookies are fresh")

except FileNotFoundError:
    print("   ❌ PROBLEM: handshake_cookies.json not found!")
    print("   → Run: python3 login_handshake.py")
except Exception as e:
    print(f"   ❌ PROBLEM: {e}")

# Check 3: Time window
print("\n3️⃣  Checking time window...")
now = datetime.datetime.now()
print(f"   Current time: {now.strftime('%I:%M %p')}")
print(f"   Current hour: {now.hour}")
print(f"   Allowed hours: 8-20")

if 8 <= now.hour < 20:
    print(f"   ✓ Within scraping window")
else:
    print(f"   ❌ PROBLEM: Outside scraping hours!")

# Check 4: Undetected Chrome
print("\n4️⃣  Checking undetected-chromedriver...")
try:
    import undetected_chromedriver as uc

    print(f"   ✓ undetected-chromedriver installed")
except ImportError:
    print(f"   ❌ PROBLEM: undetected-chromedriver not installed!")
    print(f"   → Run: pip install undetected-chromedriver")

print("\n" + "=" * 80)
print("DIAGNOSIS COMPLETE")
print("=" * 80)
print("\nIf you see any ❌ PROBLEM above, fix that first.")
print("Otherwise, Handshake should work!")
print("=" * 80)
