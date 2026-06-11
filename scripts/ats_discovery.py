#!/usr/bin/env python3
"""
ATS Auto-Discovery Engine — the pipeline grows its own source list.

How it works:
1. After every pipeline run, scans ALL URLs processed (valid + discarded)
2. Extracts company slugs from Greenhouse/Lever/Ashby URLs
3. Checks if those companies are already in our direct_sources.py
4. If NOT, tests the API to see if they have intern/newgrad roles
5. If they DO, adds them to brain.json → next run includes them
6. Also periodically scans for NEW companies on each platform

The system gets bigger every single day without human intervention.
"""

import json
import logging
import os
import re
import time
import urllib.request
import ssl
from typing import Dict, Set

log = logging.getLogger(__name__)
_CTX = ssl.create_default_context()
BRAIN_FILE = ".local/brain.json"


def _fetch_json(url, timeout=5):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=timeout, context=_CTX)
        return json.loads(resp.read())
    except Exception:
        return None


def _has_intern_roles(jobs, platform="greenhouse"):
    """Check if job list has intern/newgrad roles."""
    kw = ["intern", "co-op", "coop", "new grad", "entry level", 
          "junior", "early career", "apprentice"]
    for j in jobs:
        title = ""
        if platform == "greenhouse":
            title = j.get("title", "")
        elif platform == "lever":
            title = j.get("text", "")
        elif platform == "ashby":
            title = j.get("title", "")
        if any(k in title.lower() for k in kw):
            return True
    return False


class ATSDiscoveryEngine:
    def __init__(self):
        self.brain = self._load_brain()
        self.discovered = self.brain.get("discovered_ats", {
            "greenhouse": {}, "lever": {}, "ashby": {}
        })
        self.known_slugs = self.brain.get("known_ats_slugs", {
            "greenhouse": set(), "lever": set(), "ashby": set()
        })
        # Convert lists back to sets (JSON doesn't support sets)
        for platform in self.known_slugs:
            if isinstance(self.known_slugs[platform], list):
                self.known_slugs[platform] = set(self.known_slugs[platform])

    def _load_brain(self):
        try:
            if os.path.exists(BRAIN_FILE):
                with open(BRAIN_FILE) as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _save_brain(self):
        self.brain["discovered_ats"] = self.discovered
        # Convert sets to lists for JSON
        self.brain["known_ats_slugs"] = {
            k: list(v) for k, v in self.known_slugs.items()
        }
        with open(BRAIN_FILE, "w") as f:
            json.dump(self.brain, f, indent=2)

    def extract_slugs_from_urls(self, urls):
        """Extract ATS company slugs from a list of URLs."""
        new_slugs = {"greenhouse": set(), "lever": set(), "ashby": set()}

        for url in urls:
            url_lower = url.lower()

            # Greenhouse: boards.greenhouse.io/{slug}/ or job-boards.greenhouse.io/{slug}/
            gh_match = re.search(r"(?:boards|job-boards)\.greenhouse\.io/([a-z0-9_-]+)/", url_lower)
            if gh_match:
                slug = gh_match.group(1)
                if slug not in ("embed", "internal", "api"):
                    new_slugs["greenhouse"].add(slug)

            # Lever: jobs.lever.co/{slug}/
            lv_match = re.search(r"jobs\.lever\.co/([a-z0-9_-]+)/", url_lower)
            if lv_match:
                new_slugs["lever"].add(lv_match.group(1))

            # Ashby: jobs.ashbyhq.com/{slug}/
            ab_match = re.search(r"jobs\.ashbyhq\.com/([a-z0-9._-]+)/", url_lower)
            if ab_match:
                slug = ab_match.group(1)
                if slug not in ("api", "embed"):
                    new_slugs["ashby"].add(slug)

        return new_slugs

    def check_and_add_new_companies(self, new_slugs):
        """Test new slugs against APIs and add if they have intern roles."""
        added = 0

        for platform, slugs in new_slugs.items():
            for slug in slugs:
                # Skip if already known
                if slug in self.known_slugs.get(platform, set()):
                    continue
                if slug in self.discovered.get(platform, {}):
                    continue

                # Test the API
                has_roles = False
                company_name = slug.replace("-", " ").title()

                if platform == "greenhouse":
                    data = _fetch_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs", timeout=3)
                    if data and data.get("jobs"):
                        has_roles = _has_intern_roles(data["jobs"], "greenhouse")
                        # Try to get proper company name
                        if data["jobs"]:
                            loc = data["jobs"][0].get("location", {})
                            # Company name from board info
                            board = _fetch_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}", timeout=3)
                            if board:
                                company_name = board.get("name", company_name)

                elif platform == "lever":
                    data = _fetch_json(f"https://api.lever.co/v0/postings/{slug}?mode=json", timeout=3)
                    if data and isinstance(data, list):
                        has_roles = _has_intern_roles(data, "lever")

                elif platform == "ashby":
                    data = _fetch_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}", timeout=3)
                    if data and data.get("jobs"):
                        has_roles = _has_intern_roles(data["jobs"], "ashby")

                # Track this slug as known (even if no intern roles — don't recheck)
                self.known_slugs.setdefault(platform, set()).add(slug)

                if has_roles:
                    self.discovered.setdefault(platform, {})[slug] = company_name
                    added += 1
                    log.info(f"DISCOVERED: {platform}/{slug} → {company_name} (has intern/newgrad roles)")

                time.sleep(0.2)  # Rate limit

        self._save_brain()
        return added

    def get_all_discovered(self):
        """Return all discovered companies for each platform."""
        return self.discovered

    def run_discovery_from_sheets(self):
        """Extract URLs from Google Sheets and discover new companies."""
        try:
            import gspread
            from google.oauth2.service_account import Credentials

            scopes = ["https://www.googleapis.com/auth/spreadsheets",
                      "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_file(
                ".local/credentials.json", scopes=scopes)
            gc = gspread.authorize(creds)
            ss = gc.open("H1B visa")

            all_urls = []

            # Scan valid entries
            valid = ss.worksheet("Valid Entries").get_all_values()
            for row in valid[1:]:
                if len(row) > 5 and row[5].strip().startswith("http"):
                    all_urls.append(row[5].strip())

            # Scan discarded entries
            disc = ss.worksheet("Discarded Entries").get_all_values()
            for row in disc[1:]:
                if len(row) > 5 and row[5].strip().startswith("http"):
                    all_urls.append(row[5].strip())

            log.info(f"Scanning {len(all_urls)} URLs for ATS company slugs")
            new_slugs = self.extract_slugs_from_urls(all_urls)

            total_new = sum(len(v) for v in new_slugs.values())
            total_known = sum(len(v) for v in self.known_slugs.items())
            log.info(f"Found {total_new} slugs ({total_known} already known)")

            added = self.check_and_add_new_companies(new_slugs)
            log.info(f"Added {added} new companies to discovery cache")

            return added

        except Exception as e:
            log.error(f"Discovery from sheets failed: {e}")
            return 0



def main():
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    engine = ATSDiscoveryEngine()
    added = engine.run_discovery_from_sheets()
    logging.info(f"Discovery complete: {added} new companies added")
    
    # Report totals
    discovered = engine.get_all_discovered()
    for platform, companies in discovered.items():
        if companies:
            logging.info(f"  {platform}: {len(companies)} discovered companies")


if __name__ == "__main__":
    main()
