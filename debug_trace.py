#!/usr/bin/env python3
"""
DEBUG TRACER - Track RTX and ByteDance through pipeline
Run this to see EXACTLY where the code fails
"""

import sys
import re

# Track these 2 specific jobs
TRACKED_JOBS = {
    "RTX": "globalhr.wd5.myworkdayjobs.com",
    "ByteDance": "joinbytedance.com",
}


def trace_job(url, stage, details):
    """Print debug info for tracked jobs."""
    for job_name, url_pattern in TRACKED_JOBS.items():
        if url_pattern in url:
            print(f"\n{'='*80}")
            print(f"🔍 {job_name} @ {stage}")
            print(f"{'='*80}")
            for key, value in details.items():
                if isinstance(value, str) and len(value) > 200:
                    print(f"{key}: {value[:200]}...")
                else:
                    print(f"{key}: {value}")
            print(f"{'='*80}\n")
            return True
    return False


# Monkey-patch the ValidationHelper to add debug
import processors

original_check_restrictions = processors.ValidationHelper.check_page_restrictions


def debug_check_restrictions(soup):
    """Wrapped version with debug."""
    result = original_check_restrictions(soup)

    if soup:
        page_text = soup.get_text()[:1000]

        # Check if this is RTX
        if (
            "tewksbury" in page_text.lower()
            or "life cycle engineer" in page_text.lower()
        ):
            print(f"\n{'='*80}")
            print(f"🔍 RTX - RESTRICTION CHECK")
            print(f"{'='*80}")
            print(f"Page text sample: {page_text[:300]}...")
            print(f"Result: {result}")

            # Manual checks
            page_lower = page_text.lower()
            if "citizenship" in page_lower:
                print(f"✓ Contains 'citizenship'")
            if "clearance" in page_lower:
                print(f"✓ Contains 'clearance'")
            if "required" in page_lower:
                print(f"✓ Contains 'required'")

            print(f"{'='*80}\n")

    return result


processors.ValidationHelper.check_page_restrictions = debug_check_restrictions

# Monkey-patch location formatting
original_format_location = processors.LocationProcessor.format_location_clean


def debug_format_location(location):
    """Wrapped version with debug."""
    result = original_format_location(location)

    if location and "san jose" in location.lower():
        print(f"\n{'='*80}")
        print(f"🔍 ByteDance - LOCATION FORMAT")
        print(f"{'='*80}")
        print(f"Input: '{location}'")
        print(f"Output: '{result}'")
        print(f"{'='*80}\n")

    return result


processors.LocationProcessor.format_location_clean = debug_format_location

# Now import and run job aggregator
print("=" * 80)
print("DEBUG MODE: Tracking RTX and ByteDance")
print("=" * 80)
print("Will show detailed trace when these jobs are processed\n")

import job_aggregator

# Run the aggregator
if __name__ == "__main__":
    aggregator = job_aggregator.JobAggregator()
    aggregator.run()
